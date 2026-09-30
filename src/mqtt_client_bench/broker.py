"""Mosquitto lifecycle and readiness, driven through docker compose."""

from __future__ import annotations

import hashlib
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

from mqtt_client_bench.paths import CA_CERT, CERTS_DIR, COMPOSE_FILE, MOSQUITTO_CONF

# Pinned by tag and digest; results from different images are not comparable.
# MQTT_BENCH_MOSQUITTO_IMAGE overrides it for an A/B of the broker itself.
MOSQUITTO_IMAGE = os.environ.get(
    "MQTT_BENCH_MOSQUITTO_IMAGE",
    "eclipse-mosquitto:2.1.2-alpine@sha256:38c0da4f2ef84284d47b3b3eeea1cb3bdeabe81ee10caf0cd5c5ff61ee3ea408",
)

DEFAULT_HOST = "127.0.0.1"
# Off the default 1883 so a system Mosquitto cannot answer in its place.
DEFAULT_PORT = 11883
TLS_PORT = 11884

_CONTAINER: Optional[str] = None

_OPENSSL_CNF = """\
[req]
distinguished_name = dn
prompt = no
[dn]
CN = localhost
[ca_ext]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
[server_ext]
basicConstraints = CA:FALSE
subjectAltName = DNS:localhost, IP:127.0.0.1
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
"""


def _run(cmd, *, check=True, env=None):
    return subprocess.run(cmd, check=check, capture_output=True, text=True, env=env)


def ensure_certs() -> Path:
    """A throwaway CA and a server certificate for 127.0.0.1, made once.

    The broker runs as uid 1883, so the server key is world-readable. It
    secures nothing: it only exists so the TLS points pay a real handshake
    and real record encryption.
    """
    if (CERTS_DIR / "server.key").exists() and CA_CERT.exists():
        return CA_CERT
    if shutil.which("openssl") is None:
        raise RuntimeError("openssl is required to generate the TLS points' certificates")
    CERTS_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=CERTS_DIR.parent) as tmp_dir:
        tmp = Path(tmp_dir)
        cnf = tmp / "openssl.cnf"
        cnf.write_text(_OPENSSL_CNF, encoding="utf-8")
        common = ["-config", str(cnf)]
        _run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3650",
              "-subj", "/CN=mqtt-client-bench CA", "-extensions", "ca_ext", *common,
              "-keyout", str(tmp / "ca.key"), "-out", str(tmp / "ca.crt")])
        _run(["openssl", "req", "-newkey", "rsa:2048", "-nodes", *common,
              "-keyout", str(tmp / "server.key"), "-out", str(tmp / "server.csr")])
        _run(["openssl", "x509", "-req", "-days", "3650", "-in", str(tmp / "server.csr"),
              "-CA", str(tmp / "ca.crt"), "-CAkey", str(tmp / "ca.key"), "-CAcreateserial",
              "-extfile", str(cnf), "-extensions", "server_ext", "-out", str(tmp / "server.crt")])
        (tmp / "server.key").chmod(0o644)
        for name in ("server.key", "server.crt", "ca.crt"):
            (tmp / name).replace(CERTS_DIR / name)
    return CA_CERT


def config_hash() -> str:
    data = MOSQUITTO_CONF.read_bytes() if MOSQUITTO_CONF.exists() else b""
    return hashlib.sha256(data).hexdigest()


def image_digest(image: str) -> Optional[str]:
    try:
        proc = _run(["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image], check=False)
    except FileNotFoundError:
        return None
    return (proc.stdout or "").strip() or None if proc.returncode == 0 else None


def compose_cmd(*args: str) -> list:
    return ["docker", "compose", "-f", str(COMPOSE_FILE), *args]


def container_name() -> str:
    """The compose-prefixed name of the mosquitto service's container."""
    global _CONTAINER
    if _CONTAINER is None:
        try:
            proc = _run(compose_cmd("ps", "-a", "--format", "{{.Name}}", "mosquitto"), check=False)
            lines = [ln.strip() for ln in (proc.stdout or "").splitlines() if ln.strip()]
        except FileNotFoundError:
            lines = []
        if not lines:
            return "mosquitto"
        _CONTAINER = lines[0]
    return _CONTAINER


def _inspect(name: str, fmt: str) -> Optional[str]:
    try:
        proc = _run(["docker", "inspect", "--format", fmt, name], check=False)
    except FileNotFoundError:
        return None
    return (proc.stdout or "").strip() or None if proc.returncode == 0 else None


def broker_up(*, cpuset: Optional[str] = None, timeout_s: float = 30.0) -> dict:
    # Before compose up: a missing bind-mount source would be created empty.
    ensure_certs()
    env = dict(os.environ, MQTT_BENCH_MOSQUITTO_IMAGE=MOSQUITTO_IMAGE)
    _run(compose_cmd("up", "-d", "mosquitto"), env=env)
    name = container_name()
    # With network_mode=host, a broker from another checkout can hold the port:
    # ours then exits on "Address in use" while the stranger answers pings.
    deadline = time.time() + 10.0
    state = _inspect(name, "{{.State.Status}}")
    while state != "running" and time.time() < deadline:
        time.sleep(0.5)
        state = _inspect(name, "{{.State.Status}}")
    if state == "running":
        time.sleep(1.5)
        state = _inspect(name, "{{.State.Status}}")
    if state != "running":
        logs = _run(["docker", "logs", "--tail", "5", name], check=False)
        raise RuntimeError(
            f"mosquitto container {name!r} is {state or 'absent'} after compose up; another broker "
            f"may hold port {DEFAULT_PORT}. Last logs: {(logs.stdout or logs.stderr or '').strip()!r}"
        )
    if cpuset:
        _run(["docker", "update", "--cpuset-cpus", cpuset, name], check=False)
    wait_for_broker(DEFAULT_HOST, DEFAULT_PORT, timeout_s=timeout_s)
    wait_for_port(DEFAULT_HOST, TLS_PORT, timeout_s=timeout_s)
    return {
        "image": MOSQUITTO_IMAGE,
        "image_digest": image_digest(MOSQUITTO_IMAGE),
        "config_hash": config_hash(),
        "container_name": name,
        "cpuset": _inspect(name, "{{.HostConfig.CpusetCpus}}") or cpuset,
    }


def broker_down() -> None:
    _run(compose_cmd("down", "--remove-orphans"), check=False)


def wait_for_broker(host: str, port: int, *, timeout_s: float = 30.0) -> None:
    """Ready means a CONNACK, not merely an accepted TCP connection."""
    deadline = time.time() + timeout_s
    last_err: Optional[Exception] = None
    while time.time() < deadline:
        try:
            _mqtt_ping(host, port)
            return
        except (OSError, RuntimeError) as exc:
            last_err = exc
        time.sleep(0.25)
    raise TimeoutError(f"broker not ready at {host}:{port}: {last_err}")


def wait_for_port(host: str, port: int, *, timeout_s: float = 30.0) -> None:
    deadline = time.time() + timeout_s
    while True:
        try:
            socket.create_connection((host, port), timeout=3.0).close()
            return
        except OSError as exc:
            if time.time() >= deadline:
                raise TimeoutError(f"broker listener not ready at {host}:{port}: {exc}") from exc
        time.sleep(0.25)


def _mqtt_ping(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=3.0) as sock:
        client_id = b"benchping"
        body = b"\x00\x04MQTT\x04\x02\x00\x0a" + len(client_id).to_bytes(2, "big") + client_id
        sock.sendall(bytes([0x10, len(body)]) + body)
        data = b""
        while len(data) < 4:
            chunk = sock.recv(4 - len(data))
            if not chunk:
                raise ConnectionError("socket closed while reading CONNACK")
            data += chunk
        if data[0] != 0x20 or data[3] != 0x00:
            raise RuntimeError(f"unexpected CONNACK: {data!r}")
