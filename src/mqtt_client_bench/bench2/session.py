"""Everything a campaign sets up once: cores, broker, ``$SYS`` probe, ceiling."""

from __future__ import annotations

import contextlib
import platform
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Iterator, Optional

from mqtt_client_bench import broker, telemetry
from mqtt_client_bench.bench2 import peer, procstat
from mqtt_client_bench.bench2.catalog import Profile
from mqtt_client_bench.bench2.runner import (
    DEFAULT_SUB_OFFER,
    SUB_OFFER_FRACTION,
    Context,
    ChildProcess,
    pinned,
)
from mqtt_client_bench.bench2.sysprobe import SysProbe
from mqtt_client_bench.paths import PROJECT_ROOT

ROLES = ("broker", "sut", "peer", "orch")
CEILING_MEASURE_S = 4.0


def host_info() -> dict:
    cpu_model = None
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="ascii", errors="replace").splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    groups = telemetry.physical_cpu_groups()
    return {
        "hostname": socket.gethostname(),
        "kernel": platform.release(),
        "cpu_model": cpu_model,
        "logical_cpus": sum(len(g) for g in groups),
        "physical_cores": len(groups),
        "governor": procstat.scaling_governor(),
    }


def harness_revision() -> dict:
    def git(*args: str) -> Optional[str]:
        try:
            out = subprocess.run(["git", *args], cwd=PROJECT_ROOT, capture_output=True, text=True, check=False)
        except FileNotFoundError:
            return None
        return out.stdout.strip() if out.returncode == 0 else None

    return {
        "commit": git("rev-parse", "HEAD"),
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
        "peer_digest": peer.source_digest(),
    }


@contextlib.contextmanager
def open_session(profile: Profile, *, measure_ceiling: bool = True) -> Iterator[tuple]:
    cpusets = telemetry.allocate_cpuset(list(ROLES), profile="standard" if profile.comparable else "smoke")
    meta = broker.broker_up(cpuset=cpusets["broker"])
    telemetry.pin_current_process(cpusets["orch"])
    peer.ensure_built()
    probe = SysProbe(broker.DEFAULT_HOST, broker.DEFAULT_PORT).start()
    try:
        with tempfile.TemporaryDirectory(prefix="bench2-") as scratch:
            ctx = Context(
                host=broker.DEFAULT_HOST,
                port=broker.DEFAULT_PORT,
                probe=probe,
                cpusets=cpusets,
                broker_cgroup=telemetry.container_cgroup_path(meta["container_name"]),
                sub_offer=DEFAULT_SUB_OFFER,
                scratch=Path(scratch),
            )
            info = {
                "host": host_info(),
                "harness": harness_revision(),
                "broker": {
                    "image": meta.get("image"),
                    "image_digest": meta.get("image_digest"),
                    "config_hash": meta.get("config_hash"),
                    "cpuset": meta.get("cpuset_observed") or cpusets["broker"],
                },
                "cpusets": cpusets,
            }
            if measure_ceiling:
                ceiling = peer_ceiling(ctx)
                info["ceiling"] = ceiling
                if ceiling.get("msgs_per_s"):
                    ctx.sub_offer = int(ceiling["msgs_per_s"] * SUB_OFFER_FRACTION)
            info["sub_offer"] = ctx.sub_offer
            yield ctx, info
    finally:
        probe.close()


def peer_ceiling(ctx: Context, *, qos: int = 0, payload: int = 256, protocol: str = "MQTTv311") -> dict:
    """What the broker forwards from one C publisher to one C subscriber.

    Run with the sink where the client under test sits, this is the most any
    client can receive here, and the receive-capacity point offers a fixed
    fraction of it so every client faces the same offer, below the broker's
    own limit.
    """
    tag = f"ceiling-{int(time.time() * 1000)}"
    topic = f"bench2/{tag}/data"
    common = {"host": ctx.host, "port": ctx.port, "topic": topic, "qos": qos, "protocol": protocol}
    sink = ChildProcess("sink", pinned(ctx.cpusets.get("sut"), peer.command("sink", client_id=f"{tag}-sink", **common)), prefix="")
    source = None
    try:
        if sink.wait_ready(15.0) is None:
            return {"error": "sink not ready"}
        source = ChildProcess(
            "source",
            pinned(ctx.cpusets.get("peer"), peer.command("source", client_id=f"{tag}-src", payload=payload, rate=0, **common)),
            prefix="",
        )
        if source.wait_ready(15.0) is None:
            return {"error": "source not ready"}
        t_start = time.monotonic_ns() + 200_000_000
        t_measure = t_start + 1_000_000_000
        t_end = t_measure + int(CEILING_MEASURE_S * 1e9)
        t_stop = t_end + 1_000_000_000
        go = f"GO {t_start} {t_measure} {t_end} {t_stop}\n"
        sink.go(go)
        source.go(go)
        source.finish(CEILING_MEASURE_S + 20)
        sink.finish(10)
        s, k = source.last() or {}, sink.last() or {}
        window_s = (k.get("window_ns") or 0) / 1e9
        if not window_s or k.get("error"):
            return {"error": k.get("error") or "no sink result"}
        return {
            "qos": qos,
            "payload": payload,
            "protocol": protocol,
            "window_s": window_s,
            "offered_per_s": int(s.get("sent_window", 0)) / window_s,
            "msgs_per_s": int(k.get("received_window", 0)) / window_s,
        }
    finally:
        for proc in (sink, source):
            if proc is not None:
                proc.kill()
