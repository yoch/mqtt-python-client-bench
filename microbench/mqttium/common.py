"""Shared plumbing for the mqttium weakness micro-benchmarks.

These scripts are diagnostics, not part of the published harness: they read
their own CPU from ``/proc/self/task/*/schedstat`` and instrument the asyncio
loop in-process, which the campaign worker must never do. Every client runs in
its own process, through the same adapter the campaign uses, with the same
instrumentation, so the instrumentation cost is equal across clients.

The event-loop counters patch the *instance* (``loop.call_soon`` and
``loop._selector.select``), never the class: mqttium only takes its
``recv_into`` receive path when ``type(loop)`` is a stdlib selector loop, so a
counting subclass would silently change what is measured.
"""

from __future__ import annotations

import asyncio
import cProfile
import glob
import json
import os
import pstats
import resource
import statistics
import subprocess
import sys
import time
import uuid
from typing import Dict, List, Optional

from mqtt_client_bench.bench import peer
from mqtt_client_bench.paths import CA_CERT

HOST = "127.0.0.1"
PORT = 11883
TLS_PORT = 11884
CLIENT_CPU = 2
PEER_CPU = 3

monotonic_ns = time.monotonic_ns


def pin(cpu: Optional[int]) -> None:
    if cpu is not None and cpu >= 0:
        os.sched_setaffinity(0, {cpu})


def process_cpu_ns() -> int:
    """Sum of on-CPU time of every thread of this process (awscrt runs its own)."""
    total = 0
    for path in glob.glob("/proc/self/task/*/schedstat"):
        try:
            with open(path, "r", encoding="ascii") as fh:
                total += int(fh.read().split()[0])
        except (OSError, ValueError, IndexError):
            pass
    return total


def rusage() -> Dict[str, float]:
    """User and system CPU and minor page faults of the whole process, all threads."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return {"user_s": ru.ru_utime, "sys_s": ru.ru_stime, "minflt": ru.ru_minflt}


def rusage_per_msg(a: Dict[str, float], b: Dict[str, float], n: int) -> Dict[str, float]:
    return {
        "user_us": (b["user_s"] - a["user_s"]) * 1e6 / n,
        "sys_us": (b["sys_s"] - a["sys_s"]) * 1e6 / n,
        "minflt": (b["minflt"] - a["minflt"]) / n,
    }


def ctx_switches() -> int:
    total = 0
    for path in glob.glob("/proc/self/task/*/status"):
        try:
            with open(path, "r", encoding="ascii") as fh:
                for line in fh:
                    if line.startswith(("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")):
                        total += int(line.split()[1])
        except (OSError, ValueError):
            pass
    return total


class LoopCounters:
    """Loop iterations (one ``select`` each) and ``call_soon`` scheduling, per instance.

    ``nonblocking`` counts the iterations that polled with a zero timeout,
    i.e. that existed only because a callback was already pending: an extra
    hop through the loop, paid as one ``epoll_wait`` syscall plus the loop's
    own Python bookkeeping.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.selects = 0
        self.nonblocking = 0
        self.call_soon = 0
        selector = loop._selector  # type: ignore[attr-defined]
        orig_select = selector.select
        orig_call_soon = loop.call_soon

        def select(timeout=None):
            self.selects += 1
            if timeout == 0:
                self.nonblocking += 1
            return orig_select(timeout)

        def call_soon(callback, *args, context=None):
            self.call_soon += 1
            return orig_call_soon(callback, *args, context=context)

        selector.select = select
        loop.call_soon = call_soon  # type: ignore[method-assign]

    def snapshot(self) -> Dict[str, int]:
        return {"selects": self.selects, "nonblocking": self.nonblocking, "call_soon": self.call_soon}


class WindowProfiler:
    """cProfile over the measurement window only, reported per message.

    Call counts are deterministic, so ``py_calls`` (Python-level function
    calls per message) is immune to host noise; ``top`` ranks functions by own
    time, inflated by the profiler but comparable within one run.
    """

    def __init__(self) -> None:
        self.profile = cProfile.Profile()

    def start(self) -> None:
        self.profile.enable()

    def stop(self) -> None:
        self.profile.disable()

    def report(self, n_msgs: int, top: int = 18) -> dict:
        stats = pstats.Stats(self.profile)
        rows = []
        for (path, line, name), (_cc, ncalls, tottime, _ct, _callers) in stats.stats.items():
            where = f"{'/'.join(path.split('/')[-2:])}:{line}({name})" if path != "~" else name
            rows.append((tottime, ncalls, where))
        rows.sort(reverse=True)
        return {
            "py_calls": stats.total_calls / n_msgs,
            "top": [[where, round(ncalls / n_msgs, 2), round(tottime * 1e6 / n_msgs, 2)]
                    for tottime, ncalls, where in rows[:top]],
        }


def print_profile(result: dict) -> None:
    print(f"\n{result.get('case') or result['client'] + ':' + result['variant']}: "
          f"{result['py_calls']:.1f} Python calls per message; top own time (calls/msg, us/msg under profiler):")
    for where, calls, us in result["top"]:
        print(f"    {calls:7.2f} {us:7.2f}  {where}")


def delta(a: Dict[str, int], b: Dict[str, int]) -> Dict[str, int]:
    return {k: b[k] - a[k] for k in a}


def unique_topic(tag: str) -> str:
    return f"microbench/{tag}/{uuid.uuid4().hex[:12]}"


class Peer:
    """One C peer (sink or source) on its own core, scheduled with an absolute GO line."""

    def __init__(self, mode: str, *, topic: str, qos: int, rate: int = 0, payload: int = 256,
                 protocol: str = "MQTTv311", topics: int = 1, cpu: int = PEER_CPU) -> None:
        cmd = peer.command(mode, host=HOST, port=PORT, topic=topic, qos=qos, protocol=protocol,
                           client_id=f"mb-peer-{uuid.uuid4().hex[:8]}", payload=payload, rate=rate,
                           topics=topics)
        if cpu is not None and cpu >= 0:
            cmd = ["taskset", "-c", str(cpu), *cmd]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        line = self.proc.stdout.readline()
        if '"ready"' not in line:
            raise RuntimeError(f"peer did not become ready: {line!r}")

    def go(self, t_start: int, t_measure: int, t_end: int, t_stop: int) -> None:
        self.proc.stdin.write(f"GO {t_start} {t_measure} {t_end} {t_stop}\n")
        self.proc.stdin.flush()

    def result(self, timeout: float = 30.0) -> dict:
        out, _ = self.proc.communicate(timeout=timeout)
        lines = [ln for ln in out.splitlines() if ln.startswith("{")]
        return json.loads(lines[-1]) if lines else {}


def percentile_us(values: List[int], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))] / 1000.0


def run_children(script: str, cases: List[List[str]], common: List[str], repeat: int = 1) -> List[dict]:
    """Run each case in a fresh interpreter (one client per process), ``repeat`` rounds interleaved.

    Rounds rotate through every case before repeating, so host drift spreads
    over all clients instead of landing on one. Numeric fields are the median
    over rounds; ``runs`` says how many rounds succeeded.
    """
    rounds: List[List[dict]] = [[] for _ in cases]
    for _ in range(repeat):
        for slot, case in enumerate(cases):
            cmd = [sys.executable, script, "--one", *case, *common]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("@@")]
            if proc.returncode != 0 or not lines:
                sys.stderr.write(f"case {case} failed (rc={proc.returncode}):\n{proc.stderr[-3000:]}\n")
                continue
            rounds[slot].append(json.loads(lines[-1][2:]))
    results = []
    for runs in rounds:
        if not runs:
            continue
        merged = dict(runs[0])
        for key, value in runs[0].items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                values = [r[key] for r in runs if isinstance(r.get(key), (int, float))]
                merged[key] = statistics.median(values)
        merged["runs"] = len(runs)
        results.append(merged)
    return results


def emit(result: dict) -> None:
    sys.stdout.write("@@" + json.dumps(result) + "\n")
    sys.stdout.flush()
