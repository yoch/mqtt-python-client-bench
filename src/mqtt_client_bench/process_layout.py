"""Explicit diagnostic control of benchmark role-process ASLR.

The representative benchmark condition is always the host's normal ASLR policy.
This module exists only to run causal controls: ``disabled`` launches Python role
workers through ``setarch <machine> -R`` while leaving the orchestrator and broker
unchanged.  Results produced that way are marked non-publishable/non-comparable;
turning ASLR off must never become a way to make an official benchmark look more
stable.

``system`` leaves the host policy untouched and is publication-eligible.  The
launcher records the selected policy in every result it writes so diagnostic and
representative evidence cannot be mixed silently.
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
DIAGNOSTIC_REASON = "diagnostic_worker_aslr_disabled"


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
    disabled = mode == "disabled"
    return {
        "worker_aslr": mode,
        "orchestrator_aslr": "unchanged",
        "scope": "python_roles_spawned_by_harness._spawn_role",
        "mechanism": "setarch -R" if disabled else "host_default",
        "verified_worker_personality": (
            f"0x{personality:08x}" if personality is not None else None
        ),
        "addr_no_randomize": bool(
            personality is not None and personality & ADDR_NO_RANDOMIZE
        ),
        "experimental_control": disabled,
        "representative_aslr": not disabled,
        "publication_eligible": not disabled,
    }


@contextmanager
def worker_process_layout(mode: str) -> Iterator[dict]:
    """Apply a diagnostic ASLR policy to roles while leaving the orchestrator alone."""
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


def _append_reason(run: dict, reason: str) -> None:
    reasons = [str(item) for item in (run.get("reasons") or [])]
    if reason not in reasons:
        reasons.append(reason)
    run["reasons"] = reasons


def _mark_non_publishable(payload: dict, reason: str) -> None:
    """Fail closed when a diagnostic layout control was used.

    Existing statistical output remains available for causal analysis, but every
    run is tagged ``non_comparable`` and the document itself is explicitly
    ineligible for publication.  This prevents a disabled-ASLR result from being
    mistaken for a representative benchmark merely because it is less noisy.
    """
    payload["diagnostic_control"] = True
    payload["publication_eligible"] = False
    payload["non_comparable"] = True
    payload["non_comparable_reason"] = reason

    top_runs = payload.get("runs")
    if isinstance(top_runs, list):
        for run in top_runs:
            if isinstance(run, dict):
                run["non_comparable"] = True
                _append_reason(run, reason)

    results = payload.get("results")
    if isinstance(results, list):
        for block in results:
            if not isinstance(block, dict):
                continue
            for run in block.get("runs") or []:
                if isinstance(run, dict):
                    run["non_comparable"] = True
                    _append_reason(run, reason)


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
    if not bool(metadata.get("publication_eligible", True)):
        _mark_non_publishable(payload, DIAGNOSTIC_REASON)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return True
