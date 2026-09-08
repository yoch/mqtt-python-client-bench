"""Explicit process-layout control for benchmark role workers.

The benchmark orchestrator is intentionally left untouched.  Only Python role
processes spawned through :func:`mqtt_client_bench.harness._spawn_role` are
wrapped.  This keeps the control narrowly scoped to the processes whose
allocator/page layout can affect the measured MQTT path.

``system`` preserves the host's normal ASLR policy. ``disabled`` launches role
workers through ``setarch <machine> -R`` and fails closed unless the resulting
process personality is verified to contain ``ADDR_NO_RANDOMIZE``.
"""

from __future__ import annotations

import json
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

ADDR_NO_RANDOMIZE = 0x00040000
WORKER_ASLR_MODES = ("system", "disabled")


def _setarch() -> str:
    path = shutil.which("setarch")
    if path is None:
        raise RuntimeError("worker ASLR control requires the setarch executable")
    return path


def _disabled_personality() -> int:
    """Return and verify the personality of a child launched with ASLR off."""
    command = [
        _setarch(),
        platform.machine(),
        "-R",
        "/bin/sh",
        "-c",
        "cat /proc/$$/personality",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "setarch -R failed").strip()
        raise RuntimeError(f"cannot disable ASLR for benchmark roles: {detail}")
    try:
        personality = int(completed.stdout.strip(), 16)
    except ValueError as exc:
        raise RuntimeError(
            f"cannot verify worker personality: {completed.stdout.strip()!r}"
        ) from exc
    if not personality & ADDR_NO_RANDOMIZE:
        raise RuntimeError(
            "setarch -R returned successfully but ADDR_NO_RANDOMIZE is not set"
        )
    return personality


def _write_disabled_python_wrapper(directory: Path) -> Path:
    wrapper = directory / "python-no-aslr"
    line = "exec {} {} -R {} \"$@\"\n".format(
        shlex.quote(_setarch()),
        shlex.quote(platform.machine()),
        shlex.quote(sys.executable),
    )
    wrapper.write_text("#!/bin/sh\n" + line, encoding="utf-8")
    wrapper.chmod(0o755)
    return wrapper


def _metadata(mode: str, personality: int | None) -> dict:
    return {
        "worker_aslr": mode,
        "orchestrator_aslr": "unchanged",
        "scope": "python_roles_spawned_by_harness._spawn_role",
        "mechanism": "setarch -R" if mode == "disabled" else "host_default",
        "verified_worker_personality": (
            f"0x{personality:08x}" if personality is not None else None
        ),
        "addr_no_randomize": bool(
            personality is not None and personality & ADDR_NO_RANDOMIZE
        ),
    }


@contextmanager
def worker_process_layout(mode: str) -> Iterator[dict]:
    """Apply an ASLR policy to role workers while leaving the orchestrator alone."""
    if mode not in WORKER_ASLR_MODES:
        raise ValueError(f"unknown worker ASLR mode: {mode}")
    if mode == "system":
        yield _metadata(mode, None)
        return

    personality = _disabled_personality()
    from mqtt_client_bench import harness

    original_python = harness._python
    with tempfile.TemporaryDirectory(prefix="mqtt-bench-no-aslr-") as tmp:
        wrapper = _write_disabled_python_wrapper(Path(tmp))
        harness._python = lambda: str(wrapper)
        try:
            yield _metadata(mode, personality)
        finally:
            harness._python = original_python


def annotate_result(path: Path, metadata: dict) -> bool:
    """Add process-layout provenance to an existing JSON benchmark document."""
    if not path.is_file() or path.suffix != ".json":
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict) or "schema_version" not in payload:
        return False
    payload["role_process_layout"] = dict(metadata)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return True
