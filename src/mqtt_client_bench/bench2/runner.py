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
from typing import Dict, List, Optional

from mqtt_client_bench.adapters.registry import get_adapter_class
from mqtt_client_bench.bench2 import checks, envs, peer, procstat
from mqtt_client_bench.bench2.catalog import Point, Profile
from mqtt_client_bench.bench2.sysprobe import SysProbe, delta
from mqtt_client_bench.paths import PROJECT_ROOT

READY_TIMEOUT_S = 45.0
EXIT_GRACE_S = 20.0
START_LEAD_NS = 300_000_000
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


def _peer_argv(point: Point, ctx: Context, run_id: str, topics: dict) -> Optional[List[str]]:
    common = {
        "host": ctx.host,
        "port": ctx.port,
        "qos": point.qos,
        "protocol": point.protocol,
        "client_id": f"peer-{run_id}",
    }
    if point.kind == "pub":
        return peer.command("sink", topic=topics["data"], **common)
    if point.kind == "sub":
        rate = point.rate or ctx.sub_offer
        return peer.command("source", topic=topics["data"], payload=point.payload, rate=rate, **common)
    if point.kind == "rtt":
        return peer.command(
            "echo", topic=topics["data"], reply_topic=topics["reply"], reply_qos=point.qos, **common
        )
    return None


def _reading(pid: int, peer_pid: Optional[int], ctx: Context) -> dict:
    return {
        "at_ns": time.monotonic_ns(),
        "client": procstat.process(pid),
        "peer": procstat.process(peer_pid) if peer_pid else None,
        "broker_cpu_ns": procstat.cgroup_cpu_ns(ctx.broker_cgroup),
        "host": procstat.host_cpu(),
    }


def _resources(a: dict, b: dict) -> dict:
    wall_s = (b["at_ns"] - a["at_ns"]) / 1e9
    out = {
        "wall_s": wall_s,
        "client": procstat.process_delta(a["client"], b["client"]),
        "peer": procstat.process_delta(a["peer"], b["peer"]),
    }
    broker_cores = None
    if a["broker_cpu_ns"] is not None and b["broker_cpu_ns"] is not None and wall_s > 0:
        broker_cores = (b["broker_cpu_ns"] - a["broker_cpu_ns"]) / 1e9 / wall_s
    out["broker_cores"] = broker_cores
    if a["host"] and b["host"] and wall_s > 0:
        busy_s = (b["host"]["busy_ticks"] - a["host"]["busy_ticks"]) / procstat.CLK_TCK
        known = 0.0
        for side in ("client", "peer"):
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
    out = []
    if point.protocol == "MQTTv5" and not caps.mqtt_v5:
        out.append("not_implemented:mqtt_v5")
    if point.protocol == "MQTTv311" and not getattr(caps, "mqtt_v311", True):
        out.append("not_implemented:mqtt_v311")
    return out


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
    topics = {"data": f"bench2/{run_id}/data", "reply": f"bench2/{run_id}/reply"}
    schedule_s = profile.warmup_s + profile.measure_s + profile.drain_s
    fd, result_path = tempfile.mkstemp(prefix=f"{client}-{point.name}-", suffix=".json", dir=ctx.scratch)
    os.close(fd)
    cfg = {
        "client": client,
        "point": point.as_dict(),
        "host": ctx.host,
        "port": ctx.port,
        "client_id": f"sut-{run_id}",
        "topic": topics["data"],
        "listen_topic": topics["reply"] if point.kind == "rtt" else topics["data"],
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
    }
    env = dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"), PYTHONUNBUFFERED="1")
    worker_argv = pinned(
        ctx.cpusets.get("sut"),
        [str(envs.env_python(client)), "-m", "mqtt_client_bench.bench2.worker", "--config", cfg_path],
    )
    peer_argv = _peer_argv(point, ctx, run_id, topics)
    peer_proc: Optional[ChildProcess] = None
    worker: Optional[ChildProcess] = None
    try:
        # The receiving side subscribes before the sending side exists.
        if point.kind in ("pub", "rtt") and peer_argv:
            peer_proc = ChildProcess("peer", pinned(ctx.cpusets.get("peer"), peer_argv), prefix="")
            if peer_proc.wait_ready(READY_TIMEOUT_S) is None:
                raise RuntimeError(f"peer not ready: {peer_proc.stderr or peer_proc.last()}")
        worker = ChildProcess("worker", worker_argv, prefix="@@", env=env)
        ready = worker.wait_ready(READY_TIMEOUT_S)
        if ready is None:
            worker.finish(5.0)
            raise RuntimeError(f"worker not ready: {_worker_error(result_path, worker)}")
        record["ready"] = ready
        if point.kind == "sub" and peer_argv:
            peer_proc = ChildProcess("peer", pinned(ctx.cpusets.get("peer"), peer_argv), prefix="")
            if peer_proc.wait_ready(READY_TIMEOUT_S) is None:
                raise RuntimeError(f"peer not ready: {peer_proc.stderr or peer_proc.last()}")
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
        if peer_proc is not None:
            peer_proc.go(go)

        _sleep_until(t_measure)
        procstat.reset_peak_rss(worker.pid)
        at_measure = _reading(worker.pid, peer_proc.pid if peer_proc else None, ctx)
        _sleep_until(t_end)
        at_end = _reading(worker.pid, peer_proc.pid if peer_proc else None, ctx)
        record["resources"] = _resources(at_measure, at_end)

        remaining = max(0.0, (t_stop - time.monotonic_ns()) / 1e9)
        record["worker_exit"] = worker.finish(remaining + EXIT_GRACE_S)
        if peer_proc is not None:
            record["peer_exit"] = peer_proc.finish(EXIT_GRACE_S)
            record["peer"] = peer_proc.last()
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
        for proc in (worker, peer_proc):
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
