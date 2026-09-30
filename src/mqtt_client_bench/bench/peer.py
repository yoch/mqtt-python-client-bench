"""Build and drive the C peer (``peer/mqtt_peer.c``)."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional, Sequence

from mqtt_client_bench.paths import PROJECT_ROOT

PEER_SOURCE = PROJECT_ROOT / "peer" / "mqtt_peer.c"
BUILD_DIR = PROJECT_ROOT / "build"


def source_digest() -> str:
    return hashlib.sha256(PEER_SOURCE.read_bytes()).hexdigest()[:16]


def binary_path() -> Path:
    # Keyed by source digest, so an edited peer can never run as a stale binary.
    return BUILD_DIR / f"mqtt_peer-{source_digest()}"


def ensure_built() -> Path:
    path = binary_path()
    if path.exists():
        return path
    cc = shutil.which("cc") or shutil.which("gcc") or shutil.which("clang")
    if cc is None:
        raise RuntimeError("a C compiler is required to build peer/mqtt_peer.c")
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    subprocess.run(
        [cc, "-O2", "-Wall", "-Wextra", "-pthread", "-o", str(tmp), str(PEER_SOURCE)],
        check=True,
    )
    tmp.replace(path)
    return path


def command(
    mode: str,
    *,
    host: str,
    port: int,
    topic: str,
    qos: int,
    protocol: str,
    client_id: str,
    payload: int = 256,
    rate: int = 0,
    reply_topic: Optional[str] = None,
    reply_qos: int = 0,
    topics: int = 1,
    properties: bool = False,
    sizes: Sequence[int] = (),
) -> List[str]:
    cmd = [
        str(ensure_built()),
        mode,
        "--host", host,
        "--port", str(port),
        "--topic", topic,
        "--qos", str(qos),
        "--client-id", client_id,
    ]
    if protocol == "MQTTv5":
        cmd.append("--v5")
    if topics > 1:
        cmd += ["--topics", str(topics)]
    if mode == "source":
        cmd += ["--payload", str(payload), "--rate", str(rate)]
        if properties:
            cmd.append("--props")
    if mode == "sink" and sizes:
        cmd += ["--sizes", ",".join(str(s) for s in sizes)]
    if mode == "echo":
        cmd += ["--reply-topic", reply_topic or f"{topic}/reply", "--reply-qos", str(reply_qos)]
    return cmd


def bucket_indices(values: List[int]) -> List[int]:
    """The peer's own bucket index for each value (test hook)."""
    out = subprocess.run(
        [str(ensure_built()), "buckets", *(str(v) for v in values)],
        capture_output=True,
        text=True,
        check=True,
    )
    return [int(x) for x in out.stdout.split()]
