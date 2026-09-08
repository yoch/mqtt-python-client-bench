#!/usr/bin/env python3
"""Diagnose layout-sensitive allocation around ``socket.recv``.

This is deliberately *not* an official MQTT benchmark.  It isolates the CPython
receive allocation that asyncio's selector transport exercises: a fresh process
receives a small payload using either ``recv(N)`` or ``recv_into()`` and reports
minor/major faults plus elapsed time.  The parent repeats fresh processes so
normal ASLR can be treated as a distribution rather than hidden.

``--aslr disabled`` is an optional causal control implemented with ``setarch -R``.
Those samples are labelled diagnostic and must not be interpreted as a
representative performance condition.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import resource
import shutil
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

ADDR_NO_RANDOMIZE = 0x00040000


def _personality() -> int | None:
    try:
        return int(Path("/proc/self/personality").read_text().strip(), 16)
    except (OSError, ValueError):
        return None


def _mapping_layout() -> dict[str, str | None]:
    wanted = {"heap": None, "stack": None, "libc": None, "libpython": None}
    try:
        lines = Path("/proc/self/maps").read_text(encoding="utf-8").splitlines()
    except OSError:
        return wanted
    for line in lines:
        start = line.split("-", 1)[0]
        lower = line.lower()
        if wanted["heap"] is None and "[heap]" in line:
            wanted["heap"] = start
        elif wanted["stack"] is None and "[stack]" in line:
            wanted["stack"] = start
        elif wanted["libpython"] is None and "libpython" in lower:
            wanted["libpython"] = start
        elif wanted["libc"] is None and ("/libc.so" in lower or "/libc-" in lower):
            wanted["libc"] = start
    return wanted


def _usage() -> tuple[int, int]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return int(usage.ru_minflt), int(usage.ru_majflt)


def run_child(*, recv_size: int, payload_size: int, iterations: int, warmup: int, into: bool) -> dict:
    if recv_size <= 0 or payload_size <= 0 or payload_size > recv_size:
        raise ValueError("require 0 < payload_size <= recv_size")
    if iterations <= 0 or warmup < 0:
        raise ValueError("iterations must be > 0 and warmup >= 0")

    tx, rx = socket.socketpair()
    payload = b"x" * payload_size
    target = bytearray(recv_size) if into else None
    gc.disable()

    def one() -> None:
        tx.sendall(payload)
        if target is None:
            data = rx.recv(recv_size)
            if len(data) != payload_size:
                raise RuntimeError(f"short recv: {len(data)} != {payload_size}")
        else:
            n = rx.recv_into(target)
            if n != payload_size:
                raise RuntimeError(f"short recv_into: {n} != {payload_size}")

    try:
        for _ in range(warmup):
            one()
        before_minflt, before_majflt = _usage()
        started = time.perf_counter_ns()
        for _ in range(iterations):
            one()
        elapsed_ns = time.perf_counter_ns() - started
        after_minflt, after_majflt = _usage()
    finally:
        tx.close()
        rx.close()

    personality = _personality()
    return {
        "pid": os.getpid(),
        "python": sys.version.split()[0],
        "machine": platform.machine(),
        "mode": "recv_into" if into else "recv",
        "recv_size": recv_size,
        "payload_size": payload_size,
        "iterations": iterations,
        "warmup": warmup,
        "ru_minflt": after_minflt - before_minflt,
        "ru_majflt": after_majflt - before_majflt,
        "minflt_per_op": (after_minflt - before_minflt) / iterations,
        "majflt_per_op": (after_majflt - before_majflt) / iterations,
        "elapsed_ns": elapsed_ns,
        "ns_per_op": elapsed_ns / iterations,
        "personality": None if personality is None else f"0x{personality:08x}",
        "addr_no_randomize": bool(personality is not None and personality & ADDR_NO_RANDOMIZE),
        "layout": _mapping_layout(),
    }


def _child_command(args, *, recv_size: int, into: bool, aslr: str) -> list[str]:
    base = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--recv-size", str(recv_size),
        "--payload-size", str(args.payload_size),
        "--iterations", str(args.iterations),
        "--warmup", str(args.warmup),
    ]
    if into:
        base.append("--recv-into")
    if aslr == "system":
        return base
    setarch = shutil.which("setarch")
    if setarch is None:
        raise RuntimeError("--aslr disabled requires setarch")
    return [setarch, platform.machine(), "-R", *base]


def _run_fresh(args, *, recv_size: int, into: bool, aslr: str) -> dict:
    completed = subprocess.run(
        _child_command(args, recv_size=recv_size, into=into, aslr=aslr),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    payload["aslr_condition"] = aslr
    payload["representative"] = aslr == "system"
    payload["diagnostic_control"] = aslr != "system"
    if aslr == "disabled" and not payload.get("addr_no_randomize"):
        raise RuntimeError("setarch -R child did not report ADDR_NO_RANDOMIZE")
    return payload


def _summary(samples: list[dict]) -> dict:
    fields = ("minflt_per_op", "majflt_per_op", "ns_per_op")
    out = {"n": len(samples)}
    for field in fields:
        values = [float(row[field]) for row in samples]
        out[field] = {
            "min": min(values),
            "median": statistics.median(values),
            "max": max(values),
        }
    out["unique_layouts"] = len({json.dumps(row.get("layout"), sort_keys=True) for row in samples})
    return out


def run_parent(args) -> dict:
    recv_sizes = [int(item) for item in args.recv_sizes.split(",") if item.strip()]
    aslr_modes = [args.aslr] if args.aslr != "both" else ["system", "disabled"]
    groups = []
    for aslr in aslr_modes:
        for recv_size in recv_sizes:
            for into in (False, True) if args.include_recv_into else (False,):
                samples = [
                    _run_fresh(args, recv_size=recv_size, into=into, aslr=aslr)
                    for _ in range(args.samples)
                ]
                groups.append({
                    "aslr_condition": aslr,
                    "representative": aslr == "system",
                    "diagnostic_control": aslr != "system",
                    "mode": "recv_into" if into else "recv",
                    "recv_size": recv_size,
                    "summary": _summary(samples),
                    "samples": samples,
                })
    return {
        "schema_version": 1,
        "probe": "cpython_socket_recv_allocator",
        "official_benchmark": False,
        "payload_size": args.payload_size,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "groups": groups,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--iterations", type=int, default=10000)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--payload-size", type=int, default=256)
    parser.add_argument("--recv-sizes", default="262144,65536")
    parser.add_argument("--include-recv-into", action="store_true")
    parser.add_argument("--aslr", choices=("system", "disabled", "both"), default="system")
    parser.add_argument("--output")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--recv-size", type=int, default=262144, help=argparse.SUPPRESS)
    parser.add_argument("--recv-into", action="store_true", help=argparse.SUPPRESS)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.child:
        payload = run_child(
            recv_size=args.recv_size,
            payload_size=args.payload_size,
            iterations=args.iterations,
            warmup=args.warmup,
            into=args.recv_into,
        )
    else:
        payload = run_parent(args)
    text = json.dumps(payload, indent=None if args.child else 2, sort_keys=True)
    if args.output and not args.child:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
