"""Dedicated process: emit absolute-schedule pacing tokens.

The initiator binds a Unix DGRAM socket and spawns this module. The calendar
runs here, pinned to the loadgen cpuset, never on the SUT event loop.

stdin is JSON lines: start / stop / quit. Each start emits tokens until
``start_ns + duration_ns`` (absolute deadlines). A late iteration does not
shift the next deadline. If a datagram send would block, the token is dropped
and counted; this process does not wait for the SUT.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys

from mqtt_client_bench.control import write_json
from mqtt_client_bench.pacing import (
    DEFAULT_PACER_SPIN_NS,
    ExternalRatePacer,
    SNDBUF_BYTES,
    SystemClock,
    datagram_send_fn,
)


def _send(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _run(socket_path: str, stats_path: str, default_spin_ns: int) -> int:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.setblocking(False)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SNDBUF_BYTES)
    send_fn = datagram_send_fn(sock, socket_path)
    clock = SystemClock()
    pacer: ExternalRatePacer | None = None
    _send({"ok": True, "pid": os.getpid()})
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            cmd = json.loads(line)
        except json.JSONDecodeError:
            _send({"ok": False, "error": "bad_json"})
            continue
        name = str(cmd.get("cmd") or "")
        if name == "quit":
            if pacer is not None:
                pacer.stop = True
            return 0
        if name == "stop":
            if pacer is not None:
                pacer.stop = True
            continue
        if name != "start":
            _send({"ok": False, "error": f"unknown_cmd:{name}"})
            continue
        interval_ns = int(cmd["interval_ns"])
        duration_ns = int(cmd["duration_ns"])
        spin_ns = int(cmd.get("spin_ns") or default_spin_ns)
        start_ns = clock.monotonic_ns()
        pacer = ExternalRatePacer(
            interval_ns=interval_ns,
            start_ns=start_ns,
            spin_ns=spin_ns,
            clock=clock,
            send_fn=send_fn,
        )
        until_ns = start_ns + duration_ns
        recorder = pacer.emit_until(until_ns)
        summary = recorder.summary(duration_s=duration_ns / 1_000_000_000.0)
        summary["start_ns"] = start_ns
        summary["until_ns"] = until_ns
        summary["pid"] = os.getpid()
        write_json(stats_path, summary)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--stats", required=True)
    parser.add_argument("--spin-ns", type=int, default=DEFAULT_PACER_SPIN_NS)
    args = parser.parse_args(argv)
    return _run(args.socket, args.stats, int(args.spin_ns))


if __name__ == "__main__":
    raise SystemExit(main())
