"""Seed and clear retained messages without going through a SUT adapter.

The retained-bootstrap scenario measures how a subscriber ingests a snapshot
that is already on the broker. Seeding through the client under test would mix
that client's publish path into the score, so the orchestrator writes the
snapshot itself with stdlib sockets (same stack as the broker health ping).
"""

from __future__ import annotations

import socket
from typing import Iterable, List, Sequence

from mqtt_client_bench.broker import encode_remaining_length, struct_pack_string
from mqtt_client_bench.workloads import HEADER_SIZE, encode_header, retained_topics


def retained_payload(run_id: bytes, index: int, size: int) -> bytes:
    """Fixed-size payload with an integrity header so unique topics stay auditable."""
    if len(run_id) != 8:
        raise ValueError("run_id must be exactly 8 bytes")
    header = encode_header(run_id, 0, int(index), 0, 0)
    if size <= 0:
        return header
    if size <= HEADER_SIZE:
        return header[:size]
    return header + bytes(size - HEADER_SIZE)


def encode_publish_qos0_retain(topic: str, payload: bytes) -> bytes:
    """One MQTT 3.1.1 PUBLISH, QoS 0, retain=1."""
    topic_bytes = topic.encode("utf-8")
    remaining = struct_pack_string(topic_bytes) + payload
    return bytes([0x31]) + encode_remaining_length(len(remaining)) + remaining


def encode_connect(client_id: str) -> bytes:
    """MQTT 3.1.1 CONNECT, clean session, keepalive 10."""
    proto_name = b"MQTT"
    vh = struct_pack_string(proto_name) + bytes([0x04, 0x02, 0x00, 0x0A])
    payload = struct_pack_string(client_id.encode("ascii"))
    remaining = vh + payload
    return bytes([0x10]) + encode_remaining_length(len(remaining)) + remaining


def encode_disconnect() -> bytes:
    return b"\xe0\x00"


def seed_retained_messages(
    host: str,
    port: int,
    topics: Sequence[str],
    *,
    payloads: Sequence[bytes],
    client_id: str = "retseed",
    timeout_s: float = 30.0,
) -> int:
    """Publish ``topics[i]`` with ``payloads[i]`` and retain=1. Returns publishes sent."""
    if len(topics) != len(payloads):
        raise ValueError("topics and payloads must have the same length")
    _mqtt_session(
        host,
        port,
        client_id=client_id,
        packets=[encode_publish_qos0_retain(topic, payload) for topic, payload in zip(topics, payloads)],
        timeout_s=timeout_s,
    )
    return len(topics)


def clear_retained_messages(
    host: str,
    port: int,
    topics: Sequence[str],
    *,
    client_id: str = "retclear",
    timeout_s: float = 30.0,
) -> int:
    """Clear retained messages by publishing an empty retain to each topic."""
    _mqtt_session(
        host,
        port,
        client_id=client_id,
        packets=[encode_publish_qos0_retain(topic, b"") for topic in topics],
        timeout_s=timeout_s,
    )
    return len(topics)


def seed_retained_snapshot(
    host: str,
    port: int,
    run_id: str,
    count: int,
    *,
    payload_size: int = 256,
    timeout_s: float = 120.0,
) -> List[str]:
    """Seed ``count`` retained messages under ``bench/{run_id}/retained/#``."""
    topics = retained_topics(run_id, count)
    run_bytes = run_id.encode("ascii")
    payloads = [retained_payload(run_bytes, i, payload_size) for i in range(count)]
    seed_retained_messages(
        host,
        port,
        topics,
        payloads=payloads,
        client_id=f"rs-{run_id}",
        timeout_s=timeout_s,
    )
    return list(topics)


def clear_retained_snapshot(
    host: str,
    port: int,
    run_id: str,
    count: int,
    *,
    timeout_s: float = 120.0,
) -> int:
    return clear_retained_messages(
        host,
        port,
        retained_topics(run_id, count),
        client_id=f"rc-{run_id}",
        timeout_s=timeout_s,
    )


def _mqtt_session(
    host: str,
    port: int,
    *,
    client_id: str,
    packets: Iterable[bytes],
    timeout_s: float,
) -> None:
    sock = socket.create_connection((host, port), timeout=timeout_s)
    try:
        sock.sendall(encode_connect(client_id))
        connack = _recv_exact(sock, 4)
        if connack[0] != 0x20 or connack[3] != 0x00:
            raise RuntimeError(f"unexpected CONNACK while seeding retained: {connack!r}")
        for packet in packets:
            sock.sendall(packet)
        sock.sendall(encode_disconnect())
    finally:
        sock.close()


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed while reading CONNACK")
        buf.extend(chunk)
    return bytes(buf)
