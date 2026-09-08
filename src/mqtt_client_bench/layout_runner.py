"""Run benchmark entry points with an explicit worker-process ASLR policy.

This launcher is the publication-facing surface for layout-sensitive campaigns.
It does not change the orchestrator, broker, or benchmark logic.  It only scopes
``harness._python`` so role workers are exec'd with the requested ASLR policy,
then records that policy in every result JSON written by the command.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from mqtt_client_bench.process_layout import WORKER_ASLR_MODES, annotate_result, worker_process_layout


def _flag_values(argv: Sequence[str], name: str) -> list[str]:
    values: list[str] = []
    prefix = name + "="
    for index, value in enumerate(argv):
        if value == name and index + 1 < len(argv):
            values.append(argv[index + 1])
        elif value.startswith(prefix):
            values.append(value[len(prefix) :])
    return values


def _snapshot_output_dirs(argv: Sequence[str]) -> dict[Path, dict[Path, tuple[int, int]]]:
    snapshots: dict[Path, dict[Path, tuple[int, int]]] = {}
    for raw in _flag_values(argv, "--output-dir"):
        root = Path(raw).expanduser().resolve()
        before: dict[Path, tuple[int, int]] = {}
        if root.is_dir():
            for path in root.rglob("*.json"):
                try:
                    stat = path.stat()
                except OSError:
                    continue
                before[path] = (stat.st_mtime_ns, stat.st_size)
        snapshots[root] = before
    return snapshots


def _annotate_outputs(
    argv: Sequence[str], metadata: dict, snapshots: dict[Path, dict[Path, tuple[int, int]]]
) -> int:
    annotated = 0
    seen: set[Path] = set()
    for raw in _flag_values(argv, "--output"):
        path = Path(raw).expanduser().resolve()
        seen.add(path)
        annotated += int(annotate_result(path, metadata))

    for root, before in snapshots.items():
        if not root.is_dir():
            continue
        for path in root.rglob("*.json"):
            if path in seen:
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            previous = before.get(path)
            current = (stat.st_mtime_ns, stat.st_size)
            if previous is not None and previous == current:
                continue
            annotated += int(annotate_result(path, metadata))
    return annotated


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-aslr", required=True, choices=WORKER_ASLR_MODES)
    parser.add_argument(
        "--entrypoint",
        choices=("run", "version_compare"),
        default="run",
        help="benchmark CLI to invoke after --",
    )
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    forwarded = list(args.arguments)
    if forwarded and forwarded[0] == "--":
        forwarded.pop(0)
    if not forwarded:
        raise SystemExit("missing benchmark arguments after --")

    snapshots = _snapshot_output_dirs(forwarded)
    metadata: dict | None = None
    try:
        with worker_process_layout(args.worker_aslr) as layout:
            metadata = layout
            if args.entrypoint == "run":
                from mqtt_client_bench import run as target
            else:
                from mqtt_client_bench import version_compare as target
            return int(target.main(forwarded) or 0)
    finally:
        if metadata is not None:
            _annotate_outputs(forwarded, metadata, snapshots)


if __name__ == "__main__":
    raise SystemExit(main())
