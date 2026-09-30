"""The client-side process of one run, started with the client's own interpreter.

Protocol with the orchestrator:

1. connect (and subscribe when the point receives), then print
   ``@@{"event": "ready", ...}`` on stdout;
2. read ``GO <t_start> <t_measure> <t_end> <t_stop>`` (CLOCK_MONOTONIC ns) on
   stdin — the same line the peer receives;
3. drive the point; a timer thread snapshots the counters at ``t_measure``
   and ``t_end`` so the drive loop never reads the clock to find out where
   the window is;
4. drain until every publish has completed or ``t_stop``, write the result
   file, print ``@@{"event": "done"}``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import threading
import time
import traceback

from mqtt_client_bench.adapters.registry import (
    adapter_identity,
    create_adapter,
    create_async_adapter,
    get_async_adapter_class,
    has_async_adapter,
)
from mqtt_client_bench.bench import drive, histogram

KEEPALIVE_S = 60
CONNECT_TIMEOUT_S = 30.0
MAX_QUEUED = 10_000
FIXED_RATE_INFLIGHT = 256


def emit(event: str, **fields) -> None:
    sys.stdout.write("@@" + json.dumps({"event": event, **fields}) + "\n")
    sys.stdout.flush()


def read_go() -> dict:
    line = sys.stdin.readline().split()
    if len(line) != 5 or line[0] != "GO":
        raise RuntimeError(f"expected GO line, got {line!r}")
    t_start, t_measure, t_end, t_stop = (int(x) for x in line[1:])
    return {"t_start": t_start, "t_measure": t_measure, "t_end": t_end, "t_stop": t_stop}


def drive_shape(client: str) -> str:
    if has_async_adapter(client):
        caps = get_async_adapter_class(client).capabilities()
        return "nowait" if caps.publish_sync_on_loop else "awaited"
    return "sync"


def _rc_ok(code) -> bool:
    try:
        return int(getattr(code, "value", code) or 0) < 128
    except (TypeError, ValueError):
        return True


class Timer(threading.Thread):
    """Snapshots at the window boundaries; stops a capacity drive at t_end."""

    def __init__(self, run: drive.Run, go: dict, *, stop_at_end: bool) -> None:
        super().__init__(name="bench-window", daemon=True)
        self.run_state = run
        self.go = go
        self.stop_at_end = stop_at_end
        self.snaps: dict = {}

    def run(self) -> None:
        drive.sleep_until_ns(self.go["t_measure"])
        self.snaps["measure"] = self.run_state.snapshot()
        drive.sleep_until_ns(self.go["t_end"])
        self.snaps["end"] = self.run_state.snapshot()
        if self.stop_at_end:
            self.run_state.request_stop()


def latency_capacity(point: dict, go_s: float) -> int:
    if point["kind"] not in ("sub", "rtt") or not point["rate"]:
        return 0
    return int(point["rate"] * go_s * 1.25) + 1024


def _outstanding(run: drive.Run) -> int:
    return run.sent - run.done - run.failed


# ------------------------------------------------------------------ sync


def run_sync(cfg: dict, identity: dict) -> dict:
    point = cfg["point"]
    kind, qos, rate = point["kind"], int(point["qos"]), int(point["rate"])
    connected = threading.Event()
    acked: dict = {}
    ack_event = threading.Event()

    adapter = create_adapter(
        cfg["client"],
        client_id=cfg["client_id"],
        protocol=point["protocol"],
        max_inflight=int(point["window"]) if not rate else FIXED_RATE_INFLIGHT,
        max_queued=MAX_QUEUED,
    )

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if _rc_ok(reason_code):
            connected.set()

    def on_subscribe(client, userdata, mid, reason_codes=None, properties=None):
        codes = reason_codes if isinstance(reason_codes, (list, tuple)) else [reason_codes]
        acked[mid] = all(_rc_ok(c) for c in codes)
        ack_event.set()

    adapter.on_connect = on_connect
    adapter.on_subscribe = on_subscribe
    # Some adapters wire callbacks into the library at connect time.
    run = drive.Run(latency_capacity(point, cfg["schedule_s"]))
    if kind in ("sub", "rtt"):
        adapter.on_message = drive.message_callback(run, stamped=bool(rate))

    t0 = time.monotonic_ns()
    adapter.connect(cfg["host"], int(cfg["port"]), keepalive=KEEPALIVE_S)
    adapter.loop_start()
    if not connected.wait(CONNECT_TIMEOUT_S):
        raise RuntimeError("connect_timeout")
    connect_ns = time.monotonic_ns() - t0

    if kind in ("sub", "rtt"):
        result = adapter.subscribe(cfg["listen_topic"], qos=qos)
        mid = getattr(result, "mid", None)
        if mid is not None:
            deadline = time.monotonic() + CONNECT_TIMEOUT_S
            while mid not in acked:
                if not ack_event.wait(max(0.0, deadline - time.monotonic())):
                    raise RuntimeError("subscribe_timeout")
                ack_event.clear()
            if not acked[mid]:
                raise RuntimeError("subscribe_refused")

    emit("ready", pid=os.getpid(), connect_ns=connect_ns, shape="sync")
    go = read_go()
    timer = Timer(run, go, stop_at_end=(kind == "pub" and not rate))
    timer.start()

    make = drive.payload_maker(int(point["payload"]), stamped=bool(rate))
    topic = cfg["topic"]
    if kind == "pub" and not rate:
        drive.pub_capacity_sync(adapter, run, topic=topic, qos=qos, make=make, window=int(point["window"]), t_start=go["t_start"])
    elif kind in ("pub", "rtt"):
        drive.pub_fixed_sync(adapter, run, topic=topic, qos=qos, make=make, rate=rate, t_start=go["t_start"], t_end=go["t_end"])
    timer.join()
    drive.wait_until_sync(lambda: _outstanding(run) <= 0, go["t_stop"])
    drive.sleep_until_ns(go["t_stop"])
    final = run.snapshot()
    try:
        adapter.disconnect()
        adapter.loop_stop()
    except Exception:  # noqa: BLE001
        pass
    return _result(run, timer.snaps, final, go, connect_ns, "sync")


# ----------------------------------------------------------------- async


async def run_async(cfg: dict, identity: dict, shape: str) -> dict:
    point = cfg["point"]
    kind, qos, rate = point["kind"], int(point["qos"]), int(point["rate"])
    loop = asyncio.get_running_loop()
    adapter = create_async_adapter(
        cfg["client"],
        client_id=cfg["client_id"],
        protocol=point["protocol"],
        max_inflight=int(point["window"]) if not rate else FIXED_RATE_INFLIGHT,
        max_queued=MAX_QUEUED,
    )
    adapter.on_connect = None
    adapter.on_publish = None
    # Some adapters wire callbacks into the library at connect time.
    run = drive.Run(latency_capacity(point, cfg["schedule_s"]))
    if kind in ("sub", "rtt"):
        adapter.on_message = drive.message_callback(run, stamped=bool(rate))

    t0 = time.monotonic_ns()
    await asyncio.wait_for(adapter.connect(cfg["host"], int(cfg["port"]), keepalive=KEEPALIVE_S), CONNECT_TIMEOUT_S)
    connect_ns = time.monotonic_ns() - t0

    if kind in ("sub", "rtt"):
        result = await asyncio.wait_for(adapter.subscribe(cfg["listen_topic"], qos=qos), CONNECT_TIMEOUT_S)
        if not _rc_ok(getattr(result, "rc", 0)):
            raise RuntimeError("subscribe_refused")

    emit("ready", pid=os.getpid(), connect_ns=connect_ns, shape=shape)
    go = await loop.run_in_executor(None, read_go)
    timer = Timer(run, go, stop_at_end=(kind == "pub" and not rate))
    timer.start()

    make = drive.payload_maker(int(point["payload"]), stamped=bool(rate))
    topic = cfg["topic"]
    common = {"topic": topic, "qos": qos, "make": make, "t_start": go["t_start"]}
    if kind == "pub" and not rate:
        fn = drive.pub_capacity_nowait if shape == "nowait" else drive.pub_capacity_awaited
        coro = fn(adapter, run, window=int(point["window"]), **common)
    elif kind in ("pub", "rtt"):
        fn = drive.pub_fixed_nowait if shape == "nowait" else drive.pub_fixed_awaited
        coro = fn(adapter, run, rate=rate, t_end=go["t_end"], **common)
    else:
        coro = None

    if coro is not None:
        task = asyncio.ensure_future(coro)
        remaining = max(0.0, (go["t_stop"] - time.monotonic_ns()) / 1e9)
        done, _ = await asyncio.wait({task}, timeout=remaining)
        if task in done:
            task.result()
        else:
            task.cancel()
    await drive.wait_until(lambda: _outstanding(run) <= 0, go["t_stop"])
    await drive.asleep_until_ns(go["t_stop"])
    final = run.snapshot()
    timer.join(timeout=1.0)
    try:
        await asyncio.wait_for(adapter.disconnect(), 5.0)
    except Exception:  # noqa: BLE001
        pass
    return _result(run, timer.snaps, final, go, connect_ns, shape)


# ---------------------------------------------------------------- result


def _result(run: drive.Run, snaps: dict, final: dict, go: dict, connect_ns: int, shape: str) -> dict:
    start = snaps.get("measure")
    end = snaps.get("end")
    out = {
        "ok": True,
        "shape": shape,
        "connect_ns": connect_ns,
        "window_ns": (end["at_ns"] - start["at_ns"]) if start and end else None,
        "measure": start,
        "end": end,
        "final": final,
        "latency_overflow": run.latency_overflow,
    }
    if start and end and len(run.latencies):
        out["latency"] = histogram.from_values(run.window_latencies(start, end))
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="JSON config file")
    args = parser.parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    identity = adapter_identity(cfg["client"])
    shape = drive_shape(cfg["client"])
    try:
        if shape == "sync":
            result = run_sync(cfg, identity)
        else:
            result = asyncio.run(run_async(cfg, identity, shape))
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc()}
    result["identity"] = identity
    result["python"] = sys.version.split()[0]
    with open(cfg["result_path"], "w", encoding="utf-8") as fh:
        json.dump(result, fh)
    emit("done", ok=result["ok"])
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
