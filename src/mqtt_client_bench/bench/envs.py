"""One locked environment per client under test.

aiomqtt v2 and v3 share an import name, and a benchmark should not measure a
library next to eleven others it never asked for. Each client therefore gets
its own environment, synced from the single ``uv.lock`` with only that client's
extra, and its worker is launched with that environment's interpreter. The
orchestrator itself needs nothing beyond the standard library.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from mqtt_client_bench.paths import PROJECT_ROOT

ENVS_DIR = PROJECT_ROOT / ".venvs"

# Client name -> pyproject extra that installs it.
CLIENT_EXTRAS: Dict[str, str] = {
    "paho": "paho",
    "gmqtt": "gmqtt",
    "aiomqtt": "aiomqtt",
    "aiomqtt3": "aiomqtt3",
    "amqtt": "amqtt",
    "awscrt": "awscrt",
    "zmqtt": "zmqtt",
    "mqttium": "mqttium",
}

# Distribution whose version identifies the client in a result.
CLIENT_DISTRIBUTIONS: Dict[str, str] = {
    "paho": "paho-mqtt",
    "gmqtt": "gmqtt",
    "aiomqtt": "aiomqtt",
    "aiomqtt3": "aiomqtt",
    "amqtt": "amqtt",
    "awscrt": "awscrt",
    "zmqtt": "zmqtt",
    "mqttium": "mqttium",
}


def env_dir(client: str) -> Path:
    return ENVS_DIR / CLIENT_EXTRAS[client]


def env_python(client: str) -> Path:
    return env_dir(client) / "bin" / "python"


def env_ready(client: str) -> bool:
    return env_python(client).exists()


def sync_env(client: str, *, frozen: bool = True) -> Path:
    """Create or update ``client``'s environment from ``uv.lock``."""
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv is required to build client environments (https://docs.astral.sh/uv/)")
    extra = CLIENT_EXTRAS[client]
    cmd = [uv, "sync", "--extra", extra, "--no-dev"]
    cmd.append("--frozen" if frozen else "--locked")
    env = dict(os.environ)
    env["UV_PROJECT_ENVIRONMENT"] = str(env_dir(client))
    subprocess.run(cmd, cwd=PROJECT_ROOT, env=env, check=True)
    return env_python(client)


def sync_envs(clients: List[str], *, frozen: bool = True) -> Dict[str, str]:
    done: Dict[str, str] = {}
    seen_extras = set()
    for client in clients:
        extra = CLIENT_EXTRAS[client]
        if extra not in seen_extras:
            sync_env(client, frozen=frozen)
            seen_extras.add(extra)
        done[client] = str(env_python(client))
    return done


def installed_version(client: str) -> Optional[str]:
    """The locked version actually installed in ``client``'s environment."""
    if not env_ready(client):
        return None
    dist = CLIENT_DISTRIBUTIONS[client]
    proc = subprocess.run(
        [str(env_python(client)), "-c", f"import importlib.metadata as m; print(m.version({dist!r}))"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.stdout.strip() or None if proc.returncode == 0 else None
