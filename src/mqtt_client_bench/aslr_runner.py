"""Run benchmark roles with an explicit ASLR policy.

This is an experiment/control launcher for process-layout-sensitive measurements.
The orchestrator keeps its normal layout.  Only role processes spawned through
``mqtt_client_bench.harness._spawn_role`` are affected.

``normal`` preserves the historical harness behaviour. ``disabled`` replaces
only the worker Python executable with a tiny wrapper that execs
``setarch <machine> -R <python> ...``.  The wrapper is fail-closed: the launcher
first verifies that the child personality actually contains ADDR_NO_RANDOMIZE.
"""

from __future__ import annotations

import argparse
import json
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Sequence


ADDR_NO_RANDOMIZE = 0x00040000


def _setarch() -> str:
    path = shutil.which("setarch")
    if path is None:
        raise RuntimeError("ASLR control requires the setarch executable")
    return path


def _disabled_personality() -> int:
    command = [
        _setarch(),
        platform.machine(),
        "-R",
        sys.executable,
        "-c",
        "from pathlib import Path; print(Path('/proc/self/personality').read_text().strip())",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "setarch -R failed").strip()
        raise RuntimeError(f"cannot disable ASLR for benchmark roles: {detail}")
    try:
        personality = int(completed.stdout.strip(), 16)
    except ValueError as exc:
        raise RuntimeError(
            f"cannot verify child personality: {completed.stdout.strip()!r}"
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


def _output_path(argv: Sequence[str]) -> Path | None:
    for index, value in enumerate(argv):
        if value == "--output" and index + 1 < len(argv):
            return Path(argv[index + 1])
        if value.startswith("--output="):
            return Path(value.split("=", 1)[1])
    return None


def _annotate_output(path: Path | None, mode: str, personality: int | None) -> None:
    if path is None or not path.is_file():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["role_process_layout"] = {
        "aslr_mode": mode,
        "orchestrator_aslr": "unchanged",
        "worker_mechanism": "setarch -R" if mode == "disabled" else "historical default",
        "verified_worker_personality": personality,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aslr", choices=("normal", "disabled"), required=True)
    parser.add_argument(
        "arguments",
        nargs=argparse.REMAINDER,
        help="arguments for mqtt_client_bench.version_compare (prefix with --)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    forwarded = list(args.arguments)
    if forwarded and forwarded[0] == "--":
        forwarded.pop(0)
    if not forwarded:
        raise SystemExit("missing version_compare arguments after --")

    from mqtt_client_bench import harness, version_compare

    output = _output_path(forwarded)
    personality: int | None = None
    if args.aslr == "normal":
        result = version_compare.main(forwarded)
    else:
        personality = _disabled_personality()
        original_python = harness._python
        with tempfile.TemporaryDirectory(prefix="mqtt-bench-no-aslr-") as tmp:
            wrapper = _write_disabled_python_wrapper(Path(tmp))
            harness._python = lambda: str(wrapper)
            try:
                result = version_compare.main(forwarded)
            finally:
                harness._python = original_python

    _annotate_output(output, args.aslr, personality)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
