"""Shared control-plane helpers for worker processes."""

from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


def write_json(path: str, payload: Dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")


def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def wait_for_file(path: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {path}")


def touch(path: str, payload: Optional[dict] = None) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if payload is None:
        Path(path).write_text("ready\n", encoding="utf-8")
    else:
        write_json(path, payload)


class BarrierServer:
    """Line-oriented Unix socket barrier for T0 / T_MEASURE coordination.

    Protocol per worker connection:
      1. Worker connects and waits.
      2. Server broadcasts T0 (start warmup).
      3. Worker sends WARMUP_DRAINED after draining warmup.
      4. Server waits for all WARMUP_DRAINED acks, then broadcasts T_MEASURE.
    """

    def __init__(self, path: str):
        self.path = path
        if os.path.exists(path):
            os.unlink(path)
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(path)
        self.sock.listen(16)
        self.sock.settimeout(0.5)
        self.clients: List[socket.socket] = []
        self.broadcast_failures = 0

    def accept_n(self, n: int, timeout_s: float = 30.0) -> None:
        deadline = time.time() + timeout_s
        while len(self.clients) < n and time.time() < deadline:
            try:
                conn, _ = self.sock.accept()
                conn.settimeout(1.0)
                self.clients.append(conn)
            except socket.timeout:
                continue
        if len(self.clients) < n:
            raise TimeoutError(f"only {len(self.clients)}/{n} workers connected to barrier")

    def _accept_late(self) -> None:
        while True:
            try:
                self.sock.settimeout(0.05)
                conn, _ = self.sock.accept()
                conn.settimeout(1.0)
                self.clients.append(conn)
            except (socket.timeout, OSError):
                break
            finally:
                self.sock.settimeout(0.5)

    def broadcast(self, message: str) -> int:
        """Broadcast a line to all clients. Returns number of send failures."""
        self._accept_late()
        data = (message.strip() + "\n").encode("utf-8")
        failures = 0
        for conn in self.clients:
            try:
                conn.sendall(data)
            except OSError:
                failures += 1
        self.broadcast_failures += failures
        return failures

    def wait_for_acks(self, expected: str, n: int, timeout_s: float = 120.0) -> None:
        """Read one ack line per client connection."""
        deadline = time.time() + timeout_s
        got = 0
        buffers = {id(c): b"" for c in self.clients}
        while got < n and time.time() < deadline:
            progress = False
            for conn in list(self.clients):
                key = id(conn)
                if key not in buffers:
                    continue
                try:
                    chunk = conn.recv(64)
                except socket.timeout:
                    continue
                except OSError:
                    buffers.pop(key, None)
                    continue
                if not chunk:
                    buffers.pop(key, None)
                    continue
                buffers[key] += chunk
                if b"\n" in buffers[key]:
                    line = buffers.pop(key).decode("utf-8").strip()
                    if line == expected:
                        got += 1
                        progress = True
                    else:
                        raise RuntimeError(f"unexpected barrier ack {line!r}, want {expected!r}")
            if not progress:
                time.sleep(0.01)
        if got < n:
            raise TimeoutError(f"only {got}/{n} workers sent {expected!r}")

    def close(self) -> None:
        for conn in self.clients:
            try:
                conn.close()
            except OSError:
                pass
        try:
            self.sock.close()
        except OSError:
            pass
        if os.path.exists(self.path):
            os.unlink(self.path)


def barrier_client_wait(path: str, expected: str, timeout_s: float = 120.0) -> str:
    """Connect once and wait for a broadcast line (keeps the socket open for acks).

    For the full two-phase protocol use :func:`barrier_client_session`.
    """
    session = BarrierClientSession(path, timeout_s=timeout_s)
    try:
        return session.wait(expected)
    finally:
        # One-shot waiters that do not need to ack can close immediately after T0
        # only when they are not participating in T_MEASURE. Prefer session API.
        if expected == "T0":
            # Leave connection open? Old callers only waited for T0 once.
            # New roles use barrier_client_session. Keep backward compatible close.
            session.close()
        else:
            session.close()


class BarrierClientSession:
    """Persistent barrier client for T0 → WARMUP_DRAINED → T_MEASURE."""

    def __init__(self, path: str, timeout_s: float = 120.0):
        self.path = path
        self.timeout_s = timeout_s
        self._deadline = time.time() + timeout_s
        self.sock: Optional[socket.socket] = None
        self._buf = b""
        self._connect()

    def _connect(self) -> None:
        last_err = None
        while time.time() < self._deadline and self.sock is None:
            try:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(self.path)
                sock.settimeout(1.0)
                self.sock = sock
                return
            except OSError as exc:
                last_err = exc
                try:
                    sock.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.05)
        raise TimeoutError(f"barrier connect failed: {last_err}")

    def wait(self, expected: str) -> str:
        if self.sock is None:
            raise RuntimeError("barrier session closed")
        while time.time() < self._deadline and b"\n" not in self._buf:
            try:
                chunk = self.sock.recv(64)
            except socket.timeout:
                continue
            if not chunk:
                raise TimeoutError(f"barrier connection closed before {expected!r}")
            self._buf += chunk
        if b"\n" not in self._buf:
            raise TimeoutError(f"barrier wait for {expected!r} timed out")
        line, _, rest = self._buf.partition(b"\n")
        self._buf = rest
        text = line.decode("utf-8").strip()
        if text != expected:
            # Allow reading past unrelated lines only if exact match required —
            # surface mismatch so harness can mark inconclusive.
            raise RuntimeError(f"barrier expected {expected!r}, got {text!r}")
        return text

    def ack(self, message: str) -> None:
        if self.sock is None:
            raise RuntimeError("barrier session closed")
        self.sock.sendall((message.strip() + "\n").encode("utf-8"))

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None


def barrier_client_session(path: str, timeout_s: float = 120.0) -> BarrierClientSession:
    return BarrierClientSession(path, timeout_s=timeout_s)
