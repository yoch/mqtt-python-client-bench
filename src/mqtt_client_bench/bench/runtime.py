"""Interpreter identity and reproducible per-build client environments."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from mqtt_client_bench.paths import PROJECT_ROOT


def local_identity() -> dict:
    jit = getattr(sys, "_jit", None)
    return {
        "version": platform.python_version(),
        "version_full": sys.version,
        "implementation": sys.implementation.name,
        "machine": platform.machine(),
        "soabi": sysconfig.get_config_var("SOABI"),
        "build_gil_disabled": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)(),
        "jit_available": bool(jit and jit.is_available()),
        "jit_enabled": bool(jit and jit.is_enabled()),
        "python_jit": os.environ.get("PYTHON_JIT"),
        "python_gil": os.environ.get("PYTHON_GIL"),
        "utf8_mode": sys.flags.utf8_mode,
    }


@dataclass(frozen=True)
class Runtime:
    executable: Path
    identity: dict

    @property
    def key(self) -> str:
        digest = hashlib.sha256(
            json.dumps(self.identity, sort_keys=True).encode()
        ).hexdigest()[:10]
        return f"{self.identity['implementation']}-{self.identity['version']}-{digest}"

    def as_dict(self) -> dict:
        suffix = "t" if self.identity["build_gil_disabled"] else ""
        selector = (
            f"{self.identity['implementation']}@{self.identity['version']}{suffix}"
        )
        return {"key": self.key, "selector": selector, "identity": self.identity}


def inspect_python(python: Path, *, packages: bool = False) -> dict:
    code = "from mqtt_client_bench.bench.runtime import local_identity; import json; result = local_identity();"
    if packages:
        # Hash installation provenance without printing URLs that could carry credentials.
        code += (
            "import importlib.metadata as m, hashlib;"
            "result['packages'] = sorted("
            "[d.metadata['Name'], d.version, hashlib.sha256((d.read_text('direct_url.json') or '').encode()).hexdigest()]"
            " for d in m.distributions());"
        )
    code += "print(json.dumps(result))"
    # Only the harness source is added, independently of the caller's working directory.
    prefix = f"import sys; sys.path.insert(0, {str(PROJECT_ROOT / 'src')!r});"
    proc = subprocess.run(
        [str(python), "-c", prefix + code], capture_output=True, text=True, check=True
    )
    return json.loads(proc.stdout)


def resolve(selector: Optional[str] = None) -> Runtime:
    selector = (
        selector
        or (PROJECT_ROOT / ".python-version").read_text(encoding="utf-8").strip()
    )
    path = Path(selector).expanduser()
    if path.is_file():
        python = path.absolute()
    else:
        uv = shutil.which("uv")
        if uv is None:
            raise RuntimeError(
                "uv is required to resolve Python; pass --python /path/to/python instead"
            )
        proc = subprocess.run(
            [uv, "python", "find", selector], capture_output=True, text=True, check=True
        )
        python = Path(proc.stdout.strip()).absolute()
    identity = inspect_python(python)
    if tuple(map(int, identity["version"].split(".")[:2])) < (3, 12):
        raise ValueError("the benchmark requires Python 3.12 or newer")
    return Runtime(python, identity)


def lock_digest() -> str:
    return hashlib.sha256((PROJECT_ROOT / "uv.lock").read_bytes()).hexdigest()
