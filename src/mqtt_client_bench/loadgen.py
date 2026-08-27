"""Ingress load generator: emqtt-bench, plus a native C hammer for QoS0 pubs."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

from mqtt_client_bench.broker import EMQTT_BENCH_IMAGE, image_digest
from mqtt_client_bench.paths import PROJECT_ROOT

# Modern emqtt-bench lines look like:
# 1s pub total=40 rate=39.92/sec
# 1s connect_succ total=2 rate=2.00/sec
# Older builds used:
# pub(27140): total=12345 rate=9876(msg/sec)
RATE_RE = re.compile(
    r"(?:(?P<kind>pub|recv|conn|connect_succ|connect_fail)\(\d+\):\s*total=(?P<total>\d+)(?:,)?\s*rate=(?P<rate>[\d.]+)(?:\(msg/sec\))?"
    r"|\d+s\s+(?P<kind2>pub|recv|conn|connect_succ|connect_fail)\s+total=(?P<total2>\d+)\s+rate=(?P<rate2>[\d.]+)/sec)",
    re.IGNORECASE,
)

# emqtt-bench double-increments the ``pub`` counter on the QoS0 success path
# (publish/2 + loop/5 both call inc_counter(pub) when emqtt:publish returns ok).
# ``recv`` is incremented once. Treat nominal_rate as the real offer for QoS0 pubs.
QOS0_PUB_COUNTER_DOUBLE_COUNT = True

# Two C publisher threads on the loadgen core are enough to hold 100–200k
# QoS0. Unpaced (no --rate) they flood; eight of them just raise $SYS drops.
HAMMER_PUB_CLIENTS = 2
UNPACED_PUB_CLIENTS = HAMMER_PUB_CLIENTS  # alias kept for older tests
# Ranking QoS0 exact-topic cap. Never pace above this.
HAMMER_MAX_RATE_MSGS_PER_S = 200_000
# emqtt-bench -I is milliseconds: more than 100 clients × I=1 overruns on one
# loadgen core (~95–120k observed). Templated / QoS>0 paths stay at or below this.
EMQTT_MAX_OFFER_MSGS_PER_S = 100_000

HAMMER_SRC = PROJECT_ROOT / "scripts" / "mqtt_hammer.c"
HAMMER_BIN = PROJECT_ROOT / "scripts" / "mqtt_hammer"


@dataclass
class LoadgenSpec:
    host: str = "127.0.0.1"
    port: int = 11883
    topic: str = "bench/topic"
    qos: int = 0
    clients: int = 32
    interval_ms: int = 1
    payload_size: int = 256
    inflight: int = 100
    mqtt_version: int = 5  # emqtt-bench defaults to v5; v3.1.1 (-V 4) can reject generated client IDs
    duration_s: float = 60.0
    connect_interval_ms: int = 10
    limit: int = 0
    mode: str = "pub"  # pub | sub
    # Requested aggregate offer before I=1 quantization (informational).
    target_requested: Optional[float] = None
    # Aggregate pubs/s cap for mqtt_hammer --rate. None / 0 = unpaced firehose.
    rate_msgs_per_s: Optional[float] = None
    # auto | emqtt | hammer. auto uses paced hammer for QoS0 pubs without %i.
    engine: str = "auto"


def topic_is_templated(topic: str) -> bool:
    return any(token in (topic or "") for token in ("%i", "%c", "%u", "%I"))


def hammer_eligible(spec: LoadgenSpec) -> bool:
    """mqtt_hammer is MQTT 3.1.1 QoS0 with a single concrete topic."""
    return spec.mode == "pub" and int(spec.qos) == 0 and not topic_is_templated(spec.topic)


def select_loadgen_engine(spec: LoadgenSpec) -> str:
    """Pick emqtt-bench or paced mqtt_hammer.

    emqtt-bench ``-I`` is milliseconds, so more than ~100k needs more than 100
    clients at 1 ms and overruns on one loadgen core. For QoS0 exact-topic pubs,
    auto uses hammer paced at ``rate_msgs_per_s`` (capped at
    HAMMER_MAX_RATE_MSGS_PER_S).

    Override with spec.engine or MQTT_BENCH_LOADGEN=emqtt|hammer.
    hammer + no rate is a firehose (tests / explicit probes only).
    """
    if spec.engine in ("emqtt", "hammer"):
        return spec.engine
    env = (os.environ.get("MQTT_BENCH_LOADGEN") or "auto").strip().lower()
    if env == "emqtt":
        return "emqtt"
    if env in ("hammer", "auto") and hammer_eligible(spec):
        return "hammer"
    return "emqtt"


def clamp_emqtt_offer(clients: int, target_msgs_per_s: float) -> tuple:
    """Keep emqtt-bench at or below EMQTT_MAX_OFFER_MSGS_PER_S.

    Returns ``(clients, target)``. I=1 offer is ``clients * 1000``.

    Lowering the target alone is not enough: ``interval_for_rate(32, 100000)``
    rounds to I=1 and the real offer stays 32k. Raise the publisher count
    when the current I=1 capacity cannot hold the (already capped) target,
    otherwise the 200k ranking default silently remains a 32k emqtt offer
    on templated / QoS>0 paths.
    """
    target = min(float(target_msgs_per_s), float(EMQTT_MAX_OFFER_MSGS_PER_S))
    max_clients = int(EMQTT_MAX_OFFER_MSGS_PER_S // 1000)
    if clients <= 0:
        clients = 1
    i1_cap = float(clients) * 1000.0
    if i1_cap < target:
        needed = max(1, int(round(target / 1000.0)))
        clients = min(max_clients, max(clients, needed))
    elif i1_cap > EMQTT_MAX_OFFER_MSGS_PER_S:
        clients = max_clients
    return clients, target


def nominal_rate(
    clients: int,
    interval_ms: int,
    rate_msgs_per_s: Optional[float] = None,
) -> float:
    """Configured aggregate offer.

    Hammer ``--rate`` is an absolute cap. emqtt-bench ``-I`` is per-client
    interval in milliseconds; global rate ≈ clients * 1000 / I.
    """
    if rate_msgs_per_s is not None and float(rate_msgs_per_s) > 0:
        return float(rate_msgs_per_s)
    if interval_ms <= 0:
        return float("inf")
    return clients * 1000.0 / interval_ms


def interval_for_rate(clients: int, target_msgs_per_s: float) -> int:
    if clients <= 0 or target_msgs_per_s <= 0:
        return 1000
    return max(1, int(round(clients * 1000.0 / target_msgs_per_s)))


def parse_emqtt_output(text: str, *, kind_filter: Optional[str] = None) -> dict:
    """Parse emqtt-bench stdout.

    When ``kind_filter`` is set (e.g. ``\"pub\"`` or ``\"recv\"``), only that
    series is kept — needed when pub and recv lines share one process log.
    """
    totals = []
    rates = []
    kinds = []
    for match in RATE_RE.finditer(text or ""):
        kind = match.group("kind") or match.group("kind2")
        total = match.group("total") or match.group("total2")
        rate = match.group("rate") or match.group("rate2")
        if not kind or total is None or rate is None:
            continue
        kind_l = kind.lower()
        if kind_l.startswith("connect"):
            continue
        if kind_filter is not None and kind_l != kind_filter.lower():
            continue
        kinds.append(kind_l)
        totals.append(int(total))
        rates.append(float(rate))
    return {
        "samples": len(rates),
        "kinds": kinds,
        "totals": totals,
        "rates": rates,
        "last_total": totals[-1] if totals else None,
        "last_rate": rates[-1] if rates else None,
        "max_rate": max(rates) if rates else None,
        "median_rate": sorted(rates)[len(rates) // 2] if rates else None,
    }


def parse_hammer_output(text: str) -> dict:
    """Parse the single JSON object mqtt_hammer prints on stdout at exit."""
    rate = None
    total = None
    for line in reversed((text or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            doc = json.loads(line)
        except json.JSONDecodeError:
            continue
        if doc.get("msgs_per_s") is None:
            continue
        rate = float(doc["msgs_per_s"])
        total = int(doc.get("msgs") or 0)
        break
    rates = [] if rate is None else [rate]
    totals = [] if total is None else [total]
    return {
        "samples": len(rates),
        "kinds": ["pub"] * len(rates),
        "totals": totals,
        "rates": rates,
        "last_total": totals[-1] if totals else None,
        "last_rate": rates[-1] if rates else None,
        "max_rate": max(rates) if rates else None,
        "median_rate": sorted(rates)[len(rates) // 2] if rates else None,
    }


def effective_offer_msgs_per_s(spec: LoadgenSpec) -> float:
    """Best estimate of real aggregate publish rate."""
    return nominal_rate(spec.clients, spec.interval_ms, spec.rate_msgs_per_s)


def loadgen_emitted_msgs(stats: dict) -> Optional[int]:
    """Decoded PUBLISH count the broker should have seen.

    Prefers ``emitted_msgs`` from ``enrich_loadgen_stats``. Falls back to
    ``parsed.last_total`` with the emqtt QoS0 double-count correction so
    hand-built test stats stay honest.
    """
    if stats.get("emitted_msgs") is not None:
        return int(stats["emitted_msgs"])
    total = (stats.get("parsed") or {}).get("last_total")
    if total is None:
        return None
    emitted = int(total)
    if stats.get("qos0_pub_counter_double_count"):
        emitted //= 2
    return emitted


def observed_pub_rate(parsed: dict, *, qos: int, engine: str = "emqtt") -> Optional[float]:
    """Correct observed publish rate from parsed loadgen ``pub`` lines."""
    raw = parsed.get("median_rate")
    if raw is None:
        raw = parsed.get("last_rate")
    if raw is None:
        return None
    if engine != "emqtt":
        return float(raw)
    if int(qos) == 0 and QOS0_PUB_COUNTER_DOUBLE_COUNT:
        return float(raw) / 2.0
    return float(raw)


def build_pub_args(spec: LoadgenSpec) -> List[str]:
    args = [
        "pub",
        "-h",
        spec.host,
        "-p",
        str(spec.port),
        "-V",
        str(spec.mqtt_version),
        "-c",
        str(spec.clients),
        "-i",
        str(spec.connect_interval_ms),
        "-I",
        str(spec.interval_ms),
        "-t",
        spec.topic,
        "-s",
        str(spec.payload_size),
        "-q",
        str(spec.qos),
        "-F",
        str(spec.inflight),
    ]
    # MQTT 3.1 / 3.1.1 reject the long default emqtt-bench client IDs.
    if int(spec.mqtt_version) in (3, 4):
        args.append("--shortids")
    if spec.limit > 0:
        args.extend(["-L", str(spec.limit)])
    # Start publishing only after every worker has connected, otherwise the
    # first clients race ahead of the offer during the connect ramp.
    args.append("-w")
    return args


def build_sub_args(spec: LoadgenSpec) -> List[str]:
    args = [
        "sub",
        "-h",
        spec.host,
        "-p",
        str(spec.port),
        "-V",
        str(spec.mqtt_version),
        "-c",
        str(max(1, int(spec.clients))),
        "-i",
        str(spec.connect_interval_ms),
        "-t",
        spec.topic,
        "-q",
        str(spec.qos),
    ]
    if int(spec.mqtt_version) in (3, 4):
        args.append("--shortids")
    return args


def build_args(spec: LoadgenSpec) -> List[str]:
    if spec.mode == "sub":
        return build_sub_args(spec)
    return build_pub_args(spec)


def hammer_rate_cap() -> int:
    """Hammer pacing cap: the ranking constant, unless the diagnostic offer
    override asks for more.

    The harness validates MQTT_BENCH_INGRESS_OFFER and marks overridden points
    non_comparable; this helper only keeps the clamp from silently truncating
    the offer the harness already accepted. Unparseable values fall back to the
    conservative constant — the harness raises on them first.
    """
    raw = (os.environ.get("MQTT_BENCH_INGRESS_OFFER") or "").strip()
    if raw:
        try:
            return max(HAMMER_MAX_RATE_MSGS_PER_S, int(round(float(raw))))
        except ValueError:
            pass
    return HAMMER_MAX_RATE_MSGS_PER_S


def clamp_hammer_rate(target_msgs_per_s: Optional[float]) -> int:
    """Cap hammer at hammer_rate_cap(). 0 means unpaced."""
    if target_msgs_per_s is None or float(target_msgs_per_s) <= 0:
        return 0
    return max(1, min(int(round(float(target_msgs_per_s))), hammer_rate_cap()))


def resolve_hammer_pub_clients(point: dict, declared_clients: int) -> int:
    """Fan-in sweeps keep the declared publisher count; ranking uses two threads.

    Two hammer threads hold the 200k ranking offer. Overwriting
    ``loadgen_clients`` on ``fanin_scaling`` would collapse 1 / 16 / 128
    into the same topology.
    """
    if point.get("fanin_mode"):
        return max(1, int(declared_clients))
    return HAMMER_PUB_CLIENTS


def build_hammer_cmd(spec: LoadgenSpec, cpuset: Optional[str] = None) -> List[str]:
    duration = max(1, int(spec.duration_s) + 30)
    cmd: List[str] = []
    if cpuset and shutil.which("taskset"):
        cmd.extend(["taskset", "-c", cpuset])
    cmd.extend(
        [
            str(ensure_mqtt_hammer()),
            spec.mode,
            "--host",
            spec.host,
            "--port",
            str(spec.port),
            "--topic",
            spec.topic,
            "--clients",
            str(spec.clients),
            "--payload",
            str(spec.payload_size),
            "--duration",
            str(duration),
        ]
    )
    rate = clamp_hammer_rate(spec.rate_msgs_per_s)
    if rate > 0:
        cmd.extend(["--rate", str(rate)])
    else:
        interval_us = 0 if spec.interval_ms <= 0 else int(spec.interval_ms) * 1000
        cmd.extend(["--interval-us", str(interval_us)])
    return cmd


def ensure_mqtt_hammer() -> Path:
    if not HAMMER_SRC.exists():
        raise FileNotFoundError(f"mqtt_hammer source missing: {HAMMER_SRC}")
    if HAMMER_BIN.exists() and HAMMER_BIN.stat().st_mtime >= HAMMER_SRC.stat().st_mtime:
        return HAMMER_BIN
    cc = os.environ.get("CC", "gcc")
    HAMMER_BIN.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [cc, "-O2", "-pthread", "-Wall", "-o", str(HAMMER_BIN), str(HAMMER_SRC)],
        check=True,
    )
    return HAMMER_BIN


def enrich_loadgen_stats(spec: LoadgenSpec, parsed: dict) -> dict:
    """Attach offer-reference fields; keep raw parsed rates for diagnostics."""
    engine = select_loadgen_engine(spec)
    nom = nominal_rate(spec.clients, spec.interval_ms, spec.rate_msgs_per_s)
    nom_out: Optional[float] = None if nom == float("inf") else nom
    qos0_double = bool(
        spec.mode == "pub"
        and int(spec.qos) == 0
        and engine == "emqtt"
        and QOS0_PUB_COUNTER_DOUBLE_COUNT
    )
    observed = (
        observed_pub_rate(parsed, qos=spec.qos, engine=engine) if spec.mode == "pub" else None
    )
    emitted_msgs = (
        loadgen_emitted_msgs(
            {
                "parsed": parsed,
                "qos0_pub_counter_double_count": qos0_double,
            }
        )
        if spec.mode == "pub"
        else None
    )
    paced = bool(spec.rate_msgs_per_s and float(spec.rate_msgs_per_s) > 0) or spec.interval_ms > 0
    if spec.mode != "pub":
        effective = None
    elif paced:
        effective = nom_out
    else:
        # Firehose: the offer is whatever the generator actually emitted.
        effective = observed
    out = {
        "engine": engine,
        "nominal_rate": nom_out,
        "effective_offer_msgs_per_s": effective,
        "target_requested": spec.target_requested,
        "rate_msgs_per_s": spec.rate_msgs_per_s,
        "paced": paced,
        "interval_ms": spec.interval_ms,
        "clients": spec.clients,
        "mode": spec.mode,
        "qos": spec.qos,
        "qos0_pub_counter_double_count": qos0_double,
        "emitted_msgs": emitted_msgs,
        "parsed": parsed,
        "parsed_pub_rate_raw": parsed.get("median_rate") if spec.mode == "pub" else None,
        "observed_pub_rate": observed,
        "observed_recv_rate": parsed.get("median_rate") if spec.mode == "sub" else None,
    }
    return out


class EmqttBenchProcess:
    def __init__(self, spec: LoadgenSpec, cpuset: Optional[str] = None):
        self.spec = spec
        self.cpuset = cpuset
        self.proc: Optional[subprocess.Popen] = None
        self.stdout_text = ""
        self.started_at = None
        self.image = EMQTT_BENCH_IMAGE

    def start(self) -> None:
        args = build_args(self.spec)
        cmd = ["docker", "run", "--rm", "--network", "host"]
        if self.cpuset:
            cmd.extend(["--cpuset-cpus", self.cpuset])
        cmd.append(self.image)
        cmd.extend(args)
        self.started_at = time.time()
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def stop(self, timeout_s: float = 10.0) -> dict:
        if self.proc is None:
            return {
                "engine": "emqtt",
                "emitted": None,
                "rates": [],
                "image_digest": image_digest(self.image.split("@")[0]),
            }
        try:
            self.proc.terminate()
            out, _ = self.proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate(timeout=5)
        self.stdout_text = out or ""
        kind = "recv" if self.spec.mode == "sub" else "pub"
        parsed = parse_emqtt_output(self.stdout_text, kind_filter=kind)
        stats = enrich_loadgen_stats(self.spec, parsed)
        stats.update(
            {
                "args": build_args(self.spec),
                "image": self.image,
                "image_digest": image_digest(self.image.split("@")[0]),
                "stdout_tail": "\n".join(self.stdout_text.splitlines()[-20:]),
            }
        )
        return stats

    def wait_duration(self, duration_s: float) -> None:
        if self.proc is None:
            return
        deadline = time.time() + duration_s
        while time.time() < deadline:
            if self.proc.poll() is not None:
                break
            time.sleep(0.2)


class HammerProcess:
    """Native QoS0 pub/sub hammer (scripts/mqtt_hammer.c)."""

    def __init__(self, spec: LoadgenSpec, cpuset: Optional[str] = None):
        self.spec = spec
        self.cpuset = cpuset
        self.proc: Optional[subprocess.Popen] = None
        self.stdout_text = ""
        self.started_at = None
        self.image = None

    def start(self) -> None:
        cmd = build_hammer_cmd(self.spec, self.cpuset)
        self.started_at = time.time()
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def stop(self, timeout_s: float = 10.0) -> dict:
        if self.proc is None:
            return {"engine": "hammer", "emitted": None, "rates": []}
        try:
            self.proc.terminate()
            out, err = self.proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, err = self.proc.communicate(timeout=5)
        self.stdout_text = (out or "") + (err or "")
        parsed = parse_hammer_output(out or "")
        stats = enrich_loadgen_stats(self.spec, parsed)
        stats.update(
            {
                "args": build_hammer_cmd(self.spec, self.cpuset),
                "image": None,
                "image_digest": None,
                "stdout_tail": "\n".join(self.stdout_text.splitlines()[-20:]),
            }
        )
        return stats

    def wait_duration(self, duration_s: float) -> None:
        if self.proc is None:
            return
        deadline = time.time() + duration_s
        while time.time() < deadline:
            if self.proc.poll() is not None:
                break
            time.sleep(0.2)


LoadgenProcess = Union[EmqttBenchProcess, HammerProcess]


def spawn_loadgen(spec: LoadgenSpec, cpuset: Optional[str] = None) -> LoadgenProcess:
    engine = select_loadgen_engine(spec)
    if engine == "hammer":
        ensure_mqtt_hammer()
        return HammerProcess(spec, cpuset)
    return EmqttBenchProcess(spec, cpuset)
