#!/usr/bin/env python3
"""Summarize mqttium rc10↔rc11 ABBA JSON outputs."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional


def median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return statistics.median(vals)


def cv_pct(vals: List[float]) -> Optional[float]:
    if len(vals) < 2:
        return None
    m = statistics.mean(vals)
    if m == 0:
        return None
    return 100.0 * statistics.pstdev(vals) / m


def summarize_point(point: Dict[str, Any]) -> Dict[str, Any]:
    a_rates = point.get("baseline_rates") or []
    b_rates = point.get("candidate_rates") or []
    ratios = point.get("block_ratios") or []
    verdict = (point.get("verdict") or {}).get("verdict")
    lat = point.get("latency") or {}
    return {
        "label": point.get("label"),
        "rc10": median([float(x) for x in a_rates]) if a_rates else None,
        "rc11": median([float(x) for x in b_rates]) if b_rates else None,
        "b_over_a": median([float(x) for x in ratios]) if ratios else None,
        "rc10_cv_pct": cv_pct([float(x) for x in a_rates]),
        "rc11_cv_pct": cv_pct([float(x) for x in b_rates]),
        "verdict": verdict,
        "rc10_p50_ms": lat.get("baseline_p50_ms"),
        "rc11_p50_ms": lat.get("candidate_p50_ms"),
        "p50_b_over_a": lat.get("b_over_a_p50"),
        "valid_runs_rc10": sum(
            1 for r in point.get("runs", []) if r.get("ab_label") == "A" and r.get("status") == "valid"
        ),
        "valid_runs_rc11": sum(
            1 for r in point.get("runs", []) if r.get("ab_label") == "B" and r.get("status") == "valid"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    args = parser.parse_args()
    rows: List[Dict[str, Any]] = []
    for path in args.inputs:
        data = json.loads(path.read_text(encoding="utf-8"))
        for point in data.get("points", []):
            row = summarize_point(point)
            row["scenario"] = data.get("scenario")
            row["file"] = str(path)
            rows.append(row)
    print(json.dumps(rows, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
