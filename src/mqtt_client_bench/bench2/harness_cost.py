"""What the harness itself costs per message, measured against a null client.

The null client completes every publish immediately and does no I/O, so a
drive loop run against it costs exactly the harness's share of every real
message: the counters, the window token, the payload stamp, the completion
callback. That share is the same for every library by construction; this
module measures it so it can be recorded next to every result and held to a
budget by a test.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from typing import Dict, Optional

from mqtt_client_bench.adapters.base import PublishResult
from mqtt_client_bench.bench2 import drive
from mqtt_client_bench.paths import PROJECT_ROOT

BUDGET_NS = 1_000

_OK = PublishResult(0, 1)


class NullSync:
    def __init__(self) -> None:
        self.on_publish = None

    def publish(self, topic, payload, qos):
        self.on_publish(None, None, 1, 0, None)
        return _OK


class NullNowait:
    def __init__(self) -> None:
        self.on_publish = None

    def publish_nowait(self, topic, payload, qos):
        self.on_publish(None, None, 1, 0, None)
        return 1


class NullAwaited:
    async def publish(self, topic, payload, qos):
        return 1


class _Msg:
    __slots__ = ("payload",)

    def __init__(self, payload: bytes) -> None:
        self.payload = payload


def _timed_sync(n: int) -> float:
    run = drive.Run()
    adapter = NullSync()
    adapter_publish = adapter.publish

    def publish(topic, payload, qos):
        if run.sent >= n:
            run.stop = True
        return adapter_publish(topic, payload, qos)

    adapter.publish = publish
    t0 = time.perf_counter_ns()
    drive.pub_capacity_sync(adapter, run, topic="t", qos=1, make=drive.payload_maker(256, False), window=64, t_start=0)
    return (time.perf_counter_ns() - t0) / max(1, run.done)


def _timed_nowait(n: int) -> float:
    run = drive.Run()
    adapter = NullNowait()
    inner = adapter.publish_nowait

    def publish_nowait(topic, payload, qos):
        if run.sent >= n:
            run.stop = True
        return inner(topic, payload, qos)

    adapter.publish_nowait = publish_nowait

    async def main() -> float:
        t0 = time.perf_counter_ns()
        await drive.pub_capacity_nowait(adapter, run, topic="t", qos=1, make=drive.payload_maker(256, False), window=64, t_start=0)
        return (time.perf_counter_ns() - t0) / max(1, run.done)

    return asyncio.run(main())


def _timed_awaited(n: int) -> float:
    run = drive.Run()
    adapter = NullAwaited()
    inner = adapter.publish

    async def publish(topic, payload, qos):
        if run.sent >= n:
            run.stop = True
        return await inner(topic, payload, qos)

    adapter.publish = publish

    async def main() -> float:
        t0 = time.perf_counter_ns()
        await drive.pub_capacity_awaited(adapter, run, topic="t", qos=1, make=drive.payload_maker(256, False), window=64, t_start=0)
        return (time.perf_counter_ns() - t0) / max(1, run.done)

    return asyncio.run(main())


def _timed_receive(n: int) -> float:
    run = drive.Run(latency_capacity=n)
    on_message = drive.message_callback(run, stamped=True)
    msg = _Msg(drive.payload_maker(256, True)())
    t0 = time.perf_counter_ns()
    for _ in range(n):
        on_message(None, None, msg)
    return (time.perf_counter_ns() - t0) / n


def _timed_stamp(n: int) -> float:
    make = drive.payload_maker(256, True)
    t0 = time.perf_counter_ns()
    for _ in range(n):
        make()
    return (time.perf_counter_ns() - t0) / n


def _timed_loop_floor(n: int) -> float:
    """An empty Python loop: what every per-message figure above includes."""
    t0 = time.perf_counter_ns()
    for _ in range(n):
        pass
    return (time.perf_counter_ns() - t0) / n


SHAPES = {
    "publish_sync": _timed_sync,
    "publish_nowait": _timed_nowait,
    "publish_awaited": _timed_awaited,
    "receive_stamped": _timed_receive,
    "payload_stamp": _timed_stamp,
    "python_loop": _timed_loop_floor,
}


def measure(n: int = 200_000, repeats: int = 3) -> Dict[str, float]:
    """Best-of-``repeats`` ns per message for each drive shape."""
    return {name: round(min(fn(n) for _ in range(repeats)), 1) for name, fn in SHAPES.items()}


def baseline_rss_kb(python: Optional[str] = None) -> Optional[int]:
    """RSS of a worker interpreter with the harness imported and no client.

    Every client's RSS includes this floor; the report shows it next to them.
    """
    code = (
        "import mqtt_client_bench.bench2.worker, sys;"
        "print(open('/proc/self/status').read().split('VmRSS:')[1].split()[0])"
    )
    env = dict(os.environ, PYTHONPATH=str(PROJECT_ROOT / "src"))
    try:
        out = subprocess.run([python or sys.executable, "-c", code], capture_output=True, text=True, env=env, check=True)
    except (OSError, subprocess.CalledProcessError):
        return None
    return int(out.stdout.strip())
