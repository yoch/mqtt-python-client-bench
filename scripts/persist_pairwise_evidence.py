#!/usr/bin/env python3
"""Persist per-run matched-load / capacity / ABBA rows without raw sample vectors."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mqtt_client_bench.archive import extract_compare_summaries, extract_run_rows  # noqa: E402


def _load(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _harness_git_sha(root: Path) -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.strip() or None


def collect(root: Path) -> dict:
    rows = []
    compares = []
    documents = []
    repo = Path(__file__).resolve().parents[1]
    harness_git = _harness_git_sha(repo)
    for path in sorted(root.rglob("*.json")):
        if path.name.startswith("_") or path.name.endswith("-smoke.json"):
            continue
        if path.name in {"pairwise-run-table.json", "campaign-summary.json"}:
            continue
        data = _load(path)
        if not data:
            continue
        if data.get("kind") == "arm64_three_way_matched_rtt_summary":
            continue
        extracted = extract_run_rows(data)
        compare_rows = extract_compare_summaries(data)
        if not extracted and not compare_rows:
            continue
        rel = str(path.relative_to(root))
        documents.append(rel)
        for row in extracted:
            row["source_file"] = rel
            if harness_git and not row.get("harness_git_sha"):
                row["harness_git_sha"] = harness_git
            rows.append(row)
        for row in compare_rows:
            row["source_file"] = rel
            if harness_git:
                row["harness_git_sha"] = harness_git
            compares.append(row)
    return {
        "schema_version": 1,
        "kind": "pairwise_rtt_run_table",
        "root": str(root),
        "harness_git_sha": harness_git,
        "documents": documents,
        "n_runs": len(rows),
        "n_compares": len(compares),
        "runs": rows,
        "compares": compares,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    table = collect(Path(args.root))
    Path(args.output).write_text(json.dumps(table, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "n_runs": table["n_runs"],
                "n_compares": table["n_compares"],
                "documents": table["documents"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
