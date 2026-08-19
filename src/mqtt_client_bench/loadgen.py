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

# Tight-loop publishers on one loadgen core. 100 × 1 ms Erlang timers overrun.
# One or two C threads at interval 0 coalesce; eight of them just flood the
# broker (440k offer, 100 % CPU, $SYS drops) without raising delivered.
UNPACED_PUB_CLIENTS = 2

HAMMER_SRC = PROJECT_ROOT / "scripts" / "mosquitto_profile" / "mqtt_hammer.c"
HAMMER_BIN = PROJECT_ROOT / "scripts" / "mosquitto_profile" / "mqtt_hammer"


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
    # auto | emqtt | hammer. auto picks hammer for QoS0 pubs without topic templates.
    engine: str = "auto"


def topic_is_templated(topic: str) -> bool:
    return any(token in (topic or "") for token in ("%i", "%c", "%u", "%I"))


def select_loadgen_engine(spec: LoadgenSpec) -> str:
    """Default is emqtt-bench. Hammer is MQTT 3.1.1 QoS0, no %i templates.

    Set spec.engine='hammer' or MQTT_BENCH_LOADGEN=hammer for the C firehose.
    """
    if spec.engine in ("emqtt", "hammer"):
        return spec.engine
    env = (os.environ.get("MQTT_BENCH_LOADGEN") or "emqtt").strip().lower()
    if env == "hammer" and spec.mode == "pub" and int(spec.qos) == 0 and not topic_is_templated(spec.topic):
        return "hammer"
    return "emqtt"


def nominal_rate(clients: int, interval_ms: int) -> float:
    """emqtt-bench -I is per-client interval; global rate ≈ clients * 1000 / I."""
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
        "median_rate": rates[0] if rates else None,
    }


def effective_offer_msgs_per_s(spec: LoadgenSpec) -> float:
    """Best estimate of real aggregate publish rate.

    For paced QoS0 pub, prefer ``nominal_rate`` — raw ``pub`` rates from
    emqtt-bench are ~2× due to double-counting (see QOS0_PUB_COUNTER_DOUBLE_COUNT).
    Unpaced (interval 0) has no nominal; callers should use observed_pub_rate.
    """
    return nominal_rate(spec.clients, spec.interval_ms)


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


def build_hammer_cmd(spec: LoadgenSpec, cpuset: Optional[str] = None) -> List[str]:
    interval_us = 0 if spec.interval_ms <= 0 else int(spec.interval_ms) * 1000
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
            "--interval-us",
            str(interval_us),
        ]
    )
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
    nom = nominal_rate(spec.clients, spec.interval_ms)
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
    if spec.mode != "pub":
        effective = None
    elif spec.interval_ms <= 0:
        # Firehose: the offer is whatever the generator actually emitted.
        effective = observed
    else:
        effective = nom_out
    out = {
        "engine": engine,
        "nominal_rate": nom_out,
        "effective_offer_msgs_per_s": effective,
        "target_requested": spec.target_requested,
        "interval_ms": spec.interval_ms,
        "clients": spec.clients,
        "mode": spec.mode,
        "qos": spec.qos,
        "qos0_pub_counter_double_count": qos0_double,
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
    """Native QoS0 pub/sub hammer (scripts/mosquitto_profile/mqtt_hammer.c)."""

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
        try:
            ensure_mqtt_hammer()
            return HammerProcess(spec, cpuset)
        except (OSError, FileNotFoundError, subprocess.CalledProcessError):
            fallback = LoadgenSpec(
                host=spec.host,
                port=spec.port,
                topic=spec.topic,
                qos=spec.qos,
                clients=spec.clients,
                interval_ms=spec.interval_ms,
                payload_size=spec.payload_size,
                inflight=spec.inflight,
                mqtt_version=spec.mqtt_version,
                duration_s=spec.duration_s,
                connect_interval_ms=spec.connect_interval_ms,
                limit=spec.limit,
                mode=spec.mode,
                target_requested=spec.target_requested,
                engine="emqtt",
            )
            return EmqttBenchProcess(fallback, cpuset)
    return EmqttBenchProcess(spec, cpuset)
