"""The broker's own publish counters, read over ``$SYS`` with no dependency.

Mosquitto publishes ``$SYS`` every ``sys_interval`` (1 s here), so a counter
cannot be read at an arbitrary instant. The runner does not try: it reads a
fresh value *before* anything is published and again *after* everything has
drained, so the deltas cover the whole run and compare exactly with the whole-
run totals of the peer and the client.

``publish/messages/sent`` also counts the ``$SYS`` messages sent to this probe.
The probe counts every PUBLISH it receives, so that share is subtracted
exactly rather than estimated.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from typing import Dict, Optional

TOPICS = {
    "$SYS/broker/publish/messages/received": "received",
    "$SYS/broker/publish/messages/sent": "sent",
    "$SYS/broker/publish/messages/dropped": "dropped",
}
# Mosquitto sends a burst of topics per interval; wait this long after the
# freshness marker so the rest of the burst has landed.
BURST_SETTLE_S = 0.08
KEEPALIVE_S = 120
PING_EVERY_S = 30.0


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            return bytes(out)


def _str(s: str) -> bytes:
    raw = s.encode()
    return struct.pack("!H", len(raw)) + raw


class SysProbe:
    """One persistent MQTT 3.1.1 connection subscribed to the counters above."""

    def __init__(self, host: str, port: int, client_id: str = "bench2-sysprobe") -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._cond = threading.Condition()
        self._values: Dict[str, int] = {}
        self._updated_at: Dict[str, float] = {}
        self._probe_msgs = 0
        self._closed = False
        self.error: Optional[str] = None

    def start(self) -> "SysProbe":
        sock = socket.create_connection((self.host, self.port), timeout=10)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        vh = _str("MQTT") + bytes([4, 0x02]) + struct.pack("!H", KEEPALIVE_S)
        body = vh + _str(self.client_id)
        sock.sendall(b"\x10" + _varint(len(body)) + body)
        hdr = self._recv_exact(sock, 4)
        if hdr[0] != 0x20 or hdr[3] != 0:
            raise RuntimeError(f"$SYS probe CONNACK refused: {hdr!r}")
        payload = b"\x00\x01" + b"".join(_str(t) + b"\x00" for t in TOPICS)
        sock.sendall(b"\x82" + _varint(len(payload)) + payload)
        # Receiving does not count as activity: without a PINGREQ the broker
        # drops the probe 1.5 keepalives after CONNECT.
        sock.settimeout(PING_EVERY_S)
        self._sock = sock
        self._thread = threading.Thread(target=self._loop, name="sysprobe", daemon=True)
        self._thread.start()
        # The first interval after SUBSCRIBE mixes the retained values with a
        # live burst and is off by a message or two; steady state is exact.
        self.snapshot()
        self.snapshot()
        return self

    @staticmethod
    def _recv_exact(sock: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("$SYS probe connection closed")
            buf += chunk
        return buf

    def _loop(self) -> None:
        sock = self._sock
        assert sock is not None
        buf = b""
        try:
            last_ping = time.monotonic()
            while not self._closed:
                if time.monotonic() - last_ping >= PING_EVERY_S:
                    sock.sendall(b"\xc0\x00")
                    last_ping = time.monotonic()
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    raise ConnectionError("$SYS probe connection closed")
                buf += chunk
                while True:
                    if len(buf) < 2:
                        break
                    rl, mul, i = 0, 1, 1
                    while True:
                        if i >= len(buf):
                            rl = -1
                            break
                        b = buf[i]
                        i += 1
                        rl += (b & 0x7F) * mul
                        if not b & 0x80:
                            break
                        mul *= 128
                    if rl < 0 or len(buf) < i + rl:
                        break
                    ptype, body = buf[0], buf[i : i + rl]
                    buf = buf[i + rl :]
                    if ptype & 0xF0 == 0x30:
                        self._on_publish(body)
        except OSError as exc:
            if not self._closed:
                self.error = str(exc)
        finally:
            with self._cond:
                self._cond.notify_all()

    def _on_publish(self, body: bytes) -> None:
        tlen = struct.unpack("!H", body[:2])[0]
        topic = body[2 : 2 + tlen].decode()
        payload = body[2 + tlen :]
        now = time.monotonic()
        with self._cond:
            self._probe_msgs += 1
            key = TOPICS.get(topic)
            if key is not None:
                try:
                    self._values[key] = int(payload)
                except ValueError:
                    pass
                self._updated_at[key] = now
            self._cond.notify_all()

    def snapshot(self, timeout_s: float = 5.0) -> dict:
        """Counters from an update published after this call.

        ``sent`` changes every interval (sending ``$SYS`` to this probe is
        itself a sent publish), so its update is the freshness marker.
        """
        asked = time.monotonic()
        deadline = asked + timeout_s
        with self._cond:
            while self._updated_at.get("sent", 0.0) <= asked:
                if self.error or time.monotonic() >= deadline:
                    raise RuntimeError(f"no fresh $SYS update within {timeout_s}s ({self.error or 'timeout'})")
                self._cond.wait(timeout=max(0.0, deadline - time.monotonic()))
        time.sleep(BURST_SETTLE_S)
        with self._cond:
            return {
                "received": self._values.get("received"),
                "sent": self._values.get("sent"),
                "dropped": self._values.get("dropped", 0),
                "probe_msgs": self._probe_msgs,
                "at": time.monotonic(),
            }

    def close(self) -> None:
        self._closed = True
        if self._sock is not None:
            try:
                self._sock.sendall(b"\xe0\x00")
                self._sock.close()
            except OSError:
                pass


def delta(before: dict, after: dict) -> dict:
    """Whole-run broker counters, with the probe's own traffic removed."""
    probe = after["probe_msgs"] - before["probe_msgs"]
    sent = after["sent"] - before["sent"]
    return {
        "received": after["received"] - before["received"],
        "sent": sent - probe,
        "sent_raw": sent,
        "probe_msgs": probe,
        "dropped": (after.get("dropped") or 0) - (before.get("dropped") or 0),
    }
