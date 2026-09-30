"""One run of one point for one client.

The orchestrator owns the clock: it starts the peer and the worker, waits for
both to be connected, then sends both the same absolute schedule. While they
run it only reads ``/proc`` and the broker's cgroup at the two window
boundaries. Counts come back from the worker and the peer, and ``$SYS`` is
read fresh before the run and after the drain.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from mqtt_client_bench import broker
from mqtt_client_bench.adapters.registry import get_adapter_class
from mqtt_client_bench.bench import checks, envs, peer, procstat
from mqtt_client_bench.bench.catalog import Point, Profile
from mqtt_client_bench.bench.sysprobe import SysProbe, delta
from mqtt_client_bench.paths import CA_CERT, PROJECT_ROOT

READY_TIMEOUT_S = 45.0
EXIT_GRACE_S = 20.0
START_LEAD_NS = 300_000_000
ALIAS_TOPIC_BYTES = 200
# Offer for the receive-capacity point when the host has no fan-out ceiling on
# record; the host profile's measured ceiling replaces it.
DEFAULT_SUB_OFFER = 60_000
SUB_OFFER_FRACTION = 0.9


@dataclass
class Context:
    host: str
    port: int
    probe: SysProbe
    cpusets: Dict[str, str]  # sut / peer / orch (broker is pinned by docker)
    broker_cgroup: Optional[str]
    sub_offer: int
    scratch: Path


class ChildProcess:
    """A child speaking the ``ready`` / ``GO`` / result-line protocol."""

    def __init__(self, name: str, argv: List[str], *, prefix: str, env: Optional[dict] = None) -> None:
        self.name = name
        self.prefix = prefix
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(PROJECT_ROOT),
            env=env,
        )
        self.lines: List[dict] = []
        self.stderr = ""
        self._ready = threading.Event()
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        self._err = threading.Thread(target=self._read_err, daemon=True)
        self._err.start()

    def _read(self) -> None:
        for raw in self.proc.stdout:
            line = raw.strip()
            if self.prefix:
                if not line.startswith(self.prefix):
                    continue
                line = line[len(self.prefix) :]
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            self.lines.append(msg)
            if msg.get("event") == "ready":
                self._ready.set()

    def _read_err(self) -> None:
        self.stderr = self.proc.stderr.read()

    @property
    def pid(self) -> int:
        return self.proc.pid

    def wait_ready(self, timeout: float) -> Optional[dict]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._ready.wait(0.05):
                return next(m for m in self.lines if m.get("event") == "ready")
            if self.proc.poll() is not None:
                self._reader.join(1.0)
                return next((m for m in self.lines if m.get("event") == "ready"), None)
        return None

    def go(self, line: str) -> None:
        try:
            self.proc.stdin.write(line)
            self.proc.stdin.flush()
        except (BrokenPipeError, ValueError):
            pass

    def finish(self, timeout: float) -> int:
        try:
            rc = self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            rc = self.proc.wait()
        self._reader.join(2.0)
        self._err.join(2.0)
        return rc

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def last(self) -> Optional[dict]:
        results = [m for m in self.lines if m.get("event") != "ready"]
        return results[-1] if results else None


def pinned(cpuset: Optional[str], argv: List[str]) -> List[str]:
    return ["taskset", "-c", cpuset, *argv] if cpuset else argv


def run_topics(point: Point, run_id: str) -> Dict[str, str]:
    """``data``: what the client publishes or receives; ``reply``: the echo's
    replies (rtt) or the source's stream into the client (duplex)."""
    data = f"bench/{run_id}/data"
    if point.topic_alias:
        # Long enough that the alias visibly shrinks every PUBLISH on the wire.
        data = f"bench/{run_id}/" + "t" * (ALIAS_TOPIC_BYTES - len(f"bench/{run_id}/"))
    return {"data": data, "reply": f"bench/{run_id}/reply"}


def fanout_topics(base: str, n: int) -> List[str]:
    """The C peer's ``--topics`` names, in the same round-robin order."""
    return [f"{base}/{i // 10}/{i % 10}" for i in range(n)]


def payload_sizes(point: Point, topic: str) -> List[int]:
    """Payload lengths that make the client's PUBLISH exactly ``remaining_lengths``."""
    if not point.remaining_lengths:
        return [point.payload]
    overhead = 2 + len(topic.encode()) + (2 if point.qos else 0) + (1 if point.protocol == "MQTTv5" else 0)
    return [rl - overhead for rl in point.remaining_lengths]


def _peer_plan(point: Point, ctx: Context, run_id: str, topics: dict) -> List[Tuple[str, List[str], bool]]:
    """``(record key, argv, starts before the worker)`` for each C peer.

    Whatever receives subscribes before whatever sends exists.
    """
    common = {
        "host": ctx.host,
        "port": ctx.port,
        "qos": point.qos,
        "protocol": point.protocol,
        "topics": point.topics,
    }
    props = point.properties != "none"
    sink = peer.command(
        "sink",
        topic=topics["data"],
        client_id=f"peer-{run_id}",
        sizes=payload_sizes(point, topics["data"]),
        **common,
    )
    if point.kind == "pub":
        return [("peer", sink, True)]
    if point.kind == "sub":
        rate = point.rate or ctx.sub_offer
        source = peer.command(
            "source", topic=topics["data"], client_id=f"peer-{run_id}", payload=point.payload,
            rate=rate, properties=props, **common,
        )
        return [("peer", source, False)]
    if point.kind == "rtt":
        echo = peer.command(
            "echo", topic=topics["data"], client_id=f"peer-{run_id}", reply_topic=topics["reply"],
            reply_qos=point.qos, **common,
        )
        return [("peer", echo, True)]
    if point.kind == "duplex":
        source = peer.command(
            "source", topic=topics["reply"], client_id=f"peer-src-{run_id}", payload=point.payload,
            rate=point.rate, properties=props, **common,
        )
        return [("peer", sink, True), ("peer_source", source, False)]
    return []


def _reading(pid: int, peers: Dict[str, "ChildProcess"], ctx: Context) -> dict:
    return {
        "at_ns": time.monotonic_ns(),
        "client": procstat.process(pid),
        "peers": {key: procstat.process(proc.pid) for key, proc in peers.items()},
        "broker_cpu_ns": procstat.cgroup_cpu_ns(ctx.broker_cgroup),
        "host": procstat.host_cpu(),
    }


def _resources(a: dict, b: dict) -> dict:
    wall_s = (b["at_ns"] - a["at_ns"]) / 1e9
    out = {
        "wall_s": wall_s,
        "client": procstat.process_delta(a["client"], b["client"]),
    }
    for key, reading in a["peers"].items():
        out[key] = procstat.process_delta(reading, b["peers"].get(key))
    broker_cores = None
    if a["broker_cpu_ns"] is not None and b["broker_cpu_ns"] is not None and wall_s > 0:
        broker_cores = (b["broker_cpu_ns"] - a["broker_cpu_ns"]) / 1e9 / wall_s
    out["broker_cores"] = broker_cores
    if a["host"] and b["host"] and wall_s > 0:
        busy_s = (b["host"]["busy_ticks"] - a["host"]["busy_ticks"]) / procstat.CLK_TCK
        known = 0.0
        for side in ("client", *a["peers"]):
            d = out[side]
            if d:
                known += d["cpu_ns"] / 1e9
        known += (broker_cores or 0.0) * wall_s
        out["host"] = {
            "busy_cores": busy_s / wall_s,
            "other_cores": max(0.0, (busy_s - known) / wall_s),
        }
    return out


def refusals(client: str, point: Point) -> List[str]:
    """What the client cannot do honestly at this point; empty when it runs."""
    caps = get_adapter_class(client).capabilities()
    return [f"not_implemented:{feature}" for feature in caps.missing_for_point(point.as_dict())]


def unsupported_record(client: str, point: Point, profile: Profile, reasons: List[str]) -> dict:
    return {
        "client": client,
        "point": point.as_dict(),
        "profile": profile.name,
        "status": "unsupported",
        "reasons": reasons,
        "checks": [],
        "metrics": {},
        "flags": [],
    }


def run_once(client: str, point: Point, profile: Profile, ctx: Context, *, run_index: int = 0) -> dict:
    reasons = refusals(client, point)
    if reasons:
        return unsupported_record(client, point, profile, reasons)
    run_id = uuid.uuid4().hex[:12]
    topics = run_topics(point, run_id)
    schedule_s = profile.warmup_s + profile.measure_s + profile.drain_s
    fd, result_path = tempfile.mkstemp(prefix=f"{client}-{point.name}-", suffix=".json", dir=ctx.scratch)
    os.close(fd)
    if point.kind in ("rtt", "duplex"):
        listen = topics["reply"]
    elif point.topics > 1:
        listen = topics["data"] + "/#"
    else:
        listen = topics["data"]
    cfg = {
        "client": client,
        "point": point.as_dict(),
        "host": ctx.host,
        "port": broker.TLS_PORT if point.tls else ctx.port,
        "tls_ca_certs": str(CA_CERT) if point.tls else None,
        "client_id": f"sut-{run_id}",
        "topic": topics["data"],
        "publish_topics": fanout_topics(topics["data"], point.topics) if point.topics > 1 else [topics["data"]],
        "payload_sizes": payload_sizes(point, topics["data"]),
        "listen_topic": listen,
        "schedule_s": schedule_s,
        "result_path": result_path,
    }
    cfg_path = result_path + ".cfg"
    Path(cfg_path).write_text(json.dumps(cfg), encoding="utf-8")

    record: dict = {
        "client": client,
        "point": point.as_dict(),
        "profile": profile.name,
        "run_index": run_index,
        "run_id": run_id,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data_topic_bytes": len(topics["data"].encode()),
    }
    env = dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"), PYTHONUNBUFFERED="1")
    worker_argv = pinned(
        ctx.cpusets.get("sut"),
        [str(envs.env_python(client)), "-m", "mqtt_client_bench.bench.worker", "--config", cfg_path],
    )
    peer_plan = _peer_plan(point, ctx, run_id, topics)
    peers: Dict[str, ChildProcess] = {}
    worker: Optional[ChildProcess] = None

    def start_peers(before_worker: bool) -> None:
        for key, argv, first in peer_plan:
            if first != before_worker:
                continue
            proc = ChildProcess(key, pinned(ctx.cpusets.get("peer"), argv), prefix="")
            peers[key] = proc
            if proc.wait_ready(READY_TIMEOUT_S) is None:
                raise RuntimeError(f"{key} not ready: {proc.stderr or proc.last()}")

    try:
        start_peers(before_worker=True)
        worker = ChildProcess("worker", worker_argv, prefix="@@", env=env)
        ready = worker.wait_ready(READY_TIMEOUT_S)
        if ready is None:
            worker.finish(5.0)
            raise RuntimeError(f"worker not ready: {_worker_error(result_path, worker)}")
        record["ready"] = ready
        start_peers(before_worker=False)
        record["client_at_ready"] = procstat.process(worker.pid)

        sys_before = ctx.probe.snapshot()
        t_start = time.monotonic_ns() + START_LEAD_NS
        t_measure = t_start + int(profile.warmup_s * 1e9)
        t_end = t_measure + int(profile.measure_s * 1e9)
        t_stop = t_end + int(profile.drain_s * 1e9)
        record["schedule"] = {
            "t_start": t_start,
            "t_measure": t_measure,
            "t_end": t_end,
            "t_stop": t_stop,
            "warmup_s": profile.warmup_s,
            "measure_s": profile.measure_s,
            "drain_s": profile.drain_s,
        }
        go = f"GO {t_start} {t_measure} {t_end} {t_stop}\n"
        worker.go(go)
        for proc in peers.values():
            proc.go(go)

        _sleep_until(t_measure)
        procstat.reset_peak_rss(worker.pid)
        at_measure = _reading(worker.pid, peers, ctx)
        _sleep_until(t_end)
        at_end = _reading(worker.pid, peers, ctx)
        record["resources"] = _resources(at_measure, at_end)

        remaining = max(0.0, (t_stop - time.monotonic_ns()) / 1e9)
        record["worker_exit"] = worker.finish(remaining + EXIT_GRACE_S)
        for key, proc in peers.items():
            record[f"{key}_exit"] = proc.finish(EXIT_GRACE_S)
            record[key] = proc.last()
        sys_after = ctx.probe.snapshot()
        record["broker"] = {
            "sys": delta(sys_before, sys_after),
            "cpu_cores": record["resources"]["broker_cores"],
        }
        record["worker"] = _load_json(result_path) or {"ok": False, "error": _worker_error(result_path, worker)}
        if worker.stderr.strip():
            record["worker_stderr"] = worker.stderr.strip()[-2000:]
    except Exception as exc:  # noqa: BLE001
        record["error"] = f"{type(exc).__name__}: {exc}"
        record.setdefault("worker", {"ok": False, "error": record["error"]})
    finally:
        for proc in (worker, *peers.values()):
            if proc is not None:
                proc.kill()
        for path in (result_path, cfg_path):
            try:
                os.unlink(path)
            except OSError:
                pass

    if "schedule" not in record:
        record["schedule"] = {"measure_s": profile.measure_s}
    record.update(checks.evaluate(record, strict=profile.comparable))
    # One copy of each histogram that the metrics were computed from.
    for key in ("latency", "latency_rx", "lag"):
        hist = record["metrics"].pop(key, None)
        for side in ("peer", "peer_source", "worker"):
            if isinstance(record.get(side), dict):
                record[side].pop(key, None)
        if hist:
            record[key] = hist
    if not profile.comparable:
        record["flags"] = sorted(set(record.get("flags", [])) | {"non_comparable"})
    return record


def _sleep_until(deadline_ns: int) -> None:
    delay = deadline_ns - time.monotonic_ns()
    if delay > 0:
        time.sleep(delay / 1e9)


def _load_json(path: str) -> Optional[dict]:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return None
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def _worker_error(result_path: str, worker: ChildProcess) -> str:
    doc = _load_json(result_path)
    if doc and doc.get("error"):
        return doc["error"]
    tail = (worker.stderr or "").strip().splitlines()[-3:]
    return " | ".join(tail) or "no result"
