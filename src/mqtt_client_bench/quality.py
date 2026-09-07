"""Benchmark quality contract for same-client A/A controls.

A paired ABBA estimator can be neutral while the machine/client moves between
absolute latency regimes.  That is fine for some paired comparisons and fatal
for an absolute baseline.  This module keeps those questions separate instead
of collapsing them into one misleading PASS/FAIL bit.

The quality contract has four independent dimensions:

* ``comparative``: A/A bias and complementary pair-unit stability.  This is the
  existing pairwise publication question.
* ``stimulus``: every prescribed external-pacer token must arrive, and catch-up
  must stay below an explicit temporal-shape budget.
* ``absolute``: geometric centers of complete 4-slot A/A blocks must stay within
  the benchmark's declared absolute-resolution budget.
* ``completeness``: all requested blocks must be present and usable.

Defaults are policy, not fitted thresholds.  The 3 % comparative/absolute
budget is the benchmark's published minimum-effect resolution.  The 0.2 %
catch-up budget matches the existing matched-load admission-miss budget: more
than that share of prescribed slots being emitted after the next deadline is a
material workload-shape distortion even when the mean offered rate is exact.

This module is deliberately usable as a small post-processor too::

    python -m mqtt_client_bench.quality compare-aa-mqttium.json
    python -m mqtt_client_bench.quality --json results/compare-aa-*.json

It does not retry or discard a measurement because its latency is surprising.
Only predeclared stimulus/environment validity can make a run unusable.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Iterable, Optional

from mqtt_client_bench.pacing import stimulus_invalid_reasons
from mqtt_client_bench.pairwise import (
    AA_CONTROL_MAX_ABS_EFFECT_PCT,
    AA_CONTROL_MAX_PAIR_UNIT_ABS_PCT,
    aa_stability_pct,
)


DEFAULT_MAX_ABSOLUTE_BLOCK_DRIFT_PCT = 3.0
DEFAULT_MAX_CATCH_UP_FRACTION = 0.002
SLOTS_PER_ABBA_BLOCK = 4


def _positive_float(value) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _log_symmetric_pct(value: float, reference: float) -> float:
    """Multiplicative distance, invariant to swapping value/reference."""
    return (math.exp(abs(math.log(value / reference))) - 1.0) * 100.0


def _geometric_mean(values: Iterable[float]) -> Optional[float]:
    vals = [float(v) for v in values if _positive_float(v) is not None]
    if not vals:
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))


def _log_median(values: Iterable[float]) -> Optional[float]:
    logs = sorted(math.log(float(v)) for v in values if _positive_float(v) is not None)
    if not logs:
        return None
    mid = len(logs) // 2
    if len(logs) % 2:
        center = logs[mid]
    else:
        center = (logs[mid - 1] + logs[mid]) / 2.0
    return math.exp(center)


def _run_catch_up_fraction(run: dict) -> Optional[float]:
    pacing = run.get("pacing") or {}
    catch_up = int((pacing.get("catch_up") or {}).get("events") or 0)
    denom = (
        pacing.get("tokens_expected_in_measure_window")
        or pacing.get("tokens_scheduled")
        or pacing.get("tokens_emitted")
    )
    try:
        denom_i = int(denom or 0)
    except (TypeError, ValueError):
        denom_i = 0
    if denom_i <= 0:
        return None
    return catch_up / denom_i


def run_temporal_quality(
    run: dict,
    *,
    max_catch_up_fraction: float = DEFAULT_MAX_CATCH_UP_FRACTION,
) -> dict:
    pacing = run.get("pacing") or {}
    mode = str(pacing.get("mode") or (run.get("point") or {}).get("pacer_mode") or "in_loop")
    integrity_reasons = stimulus_invalid_reasons(pacing, mode=mode)
    catch_fraction = _run_catch_up_fraction(run)
    reasons = list(integrity_reasons)
    if mode == "external" and catch_fraction is not None and catch_fraction > max_catch_up_fraction:
        reasons.append(
            "pacer_temporal_shape_invalid:catch_up_fraction_"
            f"{catch_fraction:.6f}_exceeds_{max_catch_up_fraction:.6f}"
        )
    return {
        "ok": not reasons,
        "mode": mode,
        "catch_up_fraction": catch_fraction,
        "catch_up_events": int((pacing.get("catch_up") or {}).get("events") or 0),
        "max_catch_up_fraction": max_catch_up_fraction,
        "pacer_lateness_p95_ns": (pacing.get("pacer_lateness") or {}).get("p95"),
        "reasons": reasons,
    }


def stimulus_quality(
    doc: dict,
    *,
    max_catch_up_fraction: float = DEFAULT_MAX_CATCH_UP_FRACTION,
) -> dict:
    rows = []
    bad_slots = []
    catch_fractions = []
    for point_index, point in enumerate(doc.get("points") or []):
        for fallback_slot, run in enumerate(point.get("runs") or []):
            slot = run.get("slot")
            if slot is None:
                slot = fallback_slot
            quality = run_temporal_quality(
                run, max_catch_up_fraction=max_catch_up_fraction
            )
            if quality["catch_up_fraction"] is not None:
                catch_fractions.append(float(quality["catch_up_fraction"]))
            if not quality["ok"]:
                bad_slots.append({
                    "point": point_index,
                    "slot": int(slot),
                    "reasons": quality["reasons"],
                })
            rows.append({"point": point_index, "slot": int(slot), **quality})
    return {
        "ok": not bad_slots,
        "max_catch_up_fraction": max(catch_fractions) if catch_fractions else None,
        "budget": max_catch_up_fraction,
        "bad_slots": bad_slots,
        "runs": rows,
    }


def _run_usable_for_absolute(
    run: dict,
    *,
    max_catch_up_fraction: float,
) -> bool:
    if run.get("status") != "valid" or run.get("non_comparable"):
        return False
    if _positive_float(run.get("comparison_value")) is None:
        return False
    return run_temporal_quality(
        run, max_catch_up_fraction=max_catch_up_fraction
    )["ok"]


def absolute_stationarity(
    doc: dict,
    *,
    max_drift_pct: float = DEFAULT_MAX_ABSOLUTE_BLOCK_DRIFT_PCT,
    max_catch_up_fraction: float = DEFAULT_MAX_CATCH_UP_FRACTION,
) -> dict:
    """Measure common-mode drift that an ABBA ratio can legitimately cancel.

    Each complete A/A block has four slots.  Their geometric mean is the
    absolute block center.  Centers are compared to the log-median center using
    a log-symmetric distance, so 0.8x and 1.25x are equally far away.
    """
    point_reports = []
    all_centers = []
    total_complete = 0
    total_requested = 0
    for point_index, point in enumerate(doc.get("points") or []):
        grouped: dict[int, list[dict]] = {}
        runs = list(point.get("runs") or [])
        for fallback_slot, run in enumerate(runs):
            slot = run.get("slot")
            if slot is None:
                slot = fallback_slot
            grouped.setdefault(int(slot) // SLOTS_PER_ABBA_BLOCK, []).append(run)

        requested = doc.get("blocks_requested")
        if requested is None:
            requested = len(grouped)
        requested_i = int(requested or 0)
        total_requested += requested_i

        centers = []
        incomplete_blocks = []
        for block_index in range(requested_i):
            block_runs = grouped.get(block_index, [])
            values = [
                float(run["comparison_value"])
                for run in block_runs
                if _run_usable_for_absolute(
                    run, max_catch_up_fraction=max_catch_up_fraction
                )
            ]
            if len(block_runs) != SLOTS_PER_ABBA_BLOCK or len(values) != SLOTS_PER_ABBA_BLOCK:
                incomplete_blocks.append(block_index)
                continue
            center = _geometric_mean(values)
            if center is not None:
                centers.append({"block": block_index, "center": center})
                all_centers.append(center)
                total_complete += 1

        reference = _log_median([row["center"] for row in centers])
        deviations = []
        if reference is not None:
            deviations = [
                _log_symmetric_pct(row["center"], reference) for row in centers
            ]
        max_dev = max(deviations) if deviations else None
        point_reports.append(
            {
                "point": point_index,
                "requested_blocks": requested_i,
                "complete_blocks": len(centers),
                "incomplete_blocks": incomplete_blocks,
                "block_centers": centers,
                "reference_center": reference,
                "max_log_deviation_pct": max_dev,
                "budget_pct": max_drift_pct,
                "ok": (
                    len(centers) == requested_i
                    and max_dev is not None
                    and max_dev <= max_drift_pct
                ),
            }
        )

    global_reference = _log_median(all_centers)
    global_max = None
    if global_reference is not None and all_centers:
        global_max = max(_log_symmetric_pct(v, global_reference) for v in all_centers)
    return {
        "ok": bool(point_reports) and all(row["ok"] for row in point_reports),
        "requested_blocks": total_requested,
        "complete_blocks": total_complete,
        "reference_center": global_reference,
        "max_log_deviation_pct": global_max,
        "budget_pct": max_drift_pct,
        "points": point_reports,
    }


def comparative_quality(
    doc: dict,
    *,
    max_bias_pct: float = AA_CONTROL_MAX_ABS_EFFECT_PCT,
    max_pair_stability_pct: float = AA_CONTROL_MAX_PAIR_UNIT_ABS_PCT,
) -> dict:
    points = list(doc.get("points") or [])
    verdict = (points[0].get("verdict") or {}) if points else {}
    bias = doc.get("aa_bias_pct")
    if bias is None:
        bias = verdict.get("absolute_effect_pct")
    stability = doc.get("aa_stability_pct")
    if stability is None:
        stability = aa_stability_pct(verdict.get("pair_units") or [])
    requested = doc.get("aa_blocks_requested")
    if requested is None:
        requested = doc.get("blocks_requested")
    complete = doc.get("aa_n_blocks")
    if complete is None:
        complete = verdict.get("n_blocks")
    requested_i = int(requested or 0)
    complete_i = int(complete or 0)
    neutrality_ok = (
        bias is not None
        and stability is not None
        and abs(float(bias)) <= max_bias_pct
        and float(stability) <= max_pair_stability_pct
    )
    complete_ok = requested_i > 0 and complete_i == requested_i
    reasons = []
    if bias is None:
        reasons.append("missing_bias")
    elif abs(float(bias)) > max_bias_pct:
        reasons.append(f"bias_{float(bias):.4f}_exceeds_{max_bias_pct}")
    if stability is None:
        reasons.append("missing_pair_stability")
    elif float(stability) > max_pair_stability_pct:
        reasons.append(
            f"pair_stability_{float(stability):.4f}_exceeds_{max_pair_stability_pct}"
        )
    if not complete_ok:
        reasons.append(f"incomplete_blocks:{complete_i}/{requested_i}")
    return {
        "ok": neutrality_ok and complete_ok,
        "neutrality_ok": neutrality_ok,
        "complete": complete_ok,
        "bias_pct": bias,
        "max_bias_pct": max_bias_pct,
        "pair_stability_pct": stability,
        "max_pair_stability_pct": max_pair_stability_pct,
        "blocks_complete": complete_i,
        "blocks_requested": requested_i,
        "reasons": reasons,
    }


def classify_aa_quality(
    doc: dict,
    *,
    max_bias_pct: float = AA_CONTROL_MAX_ABS_EFFECT_PCT,
    max_pair_stability_pct: float = AA_CONTROL_MAX_PAIR_UNIT_ABS_PCT,
    max_absolute_drift_pct: float = DEFAULT_MAX_ABSOLUTE_BLOCK_DRIFT_PCT,
    max_catch_up_fraction: float = DEFAULT_MAX_CATCH_UP_FRACTION,
) -> dict:
    comparative = comparative_quality(
        doc,
        max_bias_pct=max_bias_pct,
        max_pair_stability_pct=max_pair_stability_pct,
    )
    stimulus = stimulus_quality(
        doc, max_catch_up_fraction=max_catch_up_fraction
    )
    absolute = absolute_stationarity(
        doc,
        max_drift_pct=max_absolute_drift_pct,
        max_catch_up_fraction=max_catch_up_fraction,
    )
    same_client = doc.get("baseline_client") == doc.get("candidate_client")
    paired_ok = bool(same_client and comparative["ok"] and stimulus["ok"])
    absolute_ok = bool(paired_ok and absolute["ok"])
    usable = []
    if paired_ok:
        usable.append("paired_ranking_control")
        usable.append("paired_version_ab_control")
    if absolute_ok:
        usable.append("absolute_baseline")
    usable.append("diagnostic")
    return {
        "schema_version": 1,
        "same_client_aa": same_client,
        "client": doc.get("baseline_client") if same_client else None,
        "variant": doc.get("aa_variant") or doc.get("point"),
        "comparative": comparative,
        "stimulus": stimulus,
        "absolute": absolute,
        "usable_for": usable,
        "not_usable_for": [
            name
            for name, ok in (
                ("paired_ranking_control", paired_ok),
                ("paired_version_ab_control", paired_ok),
                ("absolute_baseline", absolute_ok),
            )
            if not ok
        ],
    }


def _human_line(path: Path, quality: dict) -> str:
    comp = quality["comparative"]
    stim = quality["stimulus"]
    absolute = quality["absolute"]
    bias = "n/a" if comp["bias_pct"] is None else f"{float(comp['bias_pct']):+.2f}%"
    pair = (
        "n/a"
        if comp["pair_stability_pct"] is None
        else f"{float(comp['pair_stability_pct']):.2f}%"
    )
    drift = (
        "n/a"
        if absolute["max_log_deviation_pct"] is None
        else f"{float(absolute['max_log_deviation_pct']):.2f}%"
    )
    catch = (
        "n/a"
        if stim["max_catch_up_fraction"] is None
        else f"{100.0 * float(stim['max_catch_up_fraction']):.3f}%"
    )
    return (
        f"{path.name}: comparative={'PASS' if comp['ok'] else 'FAIL'} "
        f"(bias={bias}, pair={pair}, blocks={comp['blocks_complete']}/{comp['blocks_requested']}); "
        f"stimulus={'PASS' if stim['ok'] else 'FAIL'} (max_catch_up={catch}); "
        f"absolute={'PASS' if absolute['ok'] else 'FAIL'} (block_drift={drift}); "
        f"usable_for={','.join(quality['usable_for'])}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path, help="compare-aa-*.json documents")
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    parser.add_argument(
        "--max-catch-up-fraction",
        type=float,
        default=DEFAULT_MAX_CATCH_UP_FRACTION,
    )
    parser.add_argument(
        "--max-absolute-drift-pct",
        type=float,
        default=DEFAULT_MAX_ABSOLUTE_BLOCK_DRIFT_PCT,
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    reports = []
    failed_to_parse = False
    for path in args.inputs:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            quality = classify_aa_quality(
                doc,
                max_absolute_drift_pct=args.max_absolute_drift_pct,
                max_catch_up_fraction=args.max_catch_up_fraction,
            )
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            failed_to_parse = True
            quality = {"error": str(exc), "usable_for": []}
        reports.append({"file": str(path), "quality": quality})
        if not args.json and "error" not in quality:
            print(_human_line(path, quality))
        elif not args.json:
            print(f"{path.name}: ERROR {quality['error']}")
    if args.json:
        print(json.dumps(reports, indent=2, sort_keys=True))
    return 2 if failed_to_parse else 0


if __name__ == "__main__":
    raise SystemExit(main())
