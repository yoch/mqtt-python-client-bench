"""Statistical helpers and metric aggregation for client benchmarks."""

from __future__ import annotations

import math
import random
from typing import Iterable, List, Optional, Sequence


def sanitize_number(value: Optional[float]) -> Optional[float]:
    """Replace NaN/Inf with None for JSON-safe output."""
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    raise TypeError(f"unsupported numeric type: {type(value)!r}")


def percentile(values: Sequence[float], pct: float) -> Optional[float]:
    """Nearest-rank percentile; returns None for empty input."""
    if not values:
        return None
    if pct <= 0:
        return float(min(values))
    if pct >= 100:
        return float(max(values))
    ordered = sorted(values)
    rank = int(math.ceil((pct / 100.0) * len(ordered))) - 1
    rank = max(0, min(rank, len(ordered) - 1))
    return float(ordered[rank])


def median(values: Sequence[float]) -> Optional[float]:
    return percentile(values, 50.0)


def mad(values: Sequence[float]) -> Optional[float]:
    """Median absolute deviation."""
    med = median(values)
    if med is None:
        return None
    return median([abs(v - med) for v in values])


def mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def summarize_runs(values: Sequence[float]) -> dict:
    cleaned = [float(v) for v in values if v is not None and not math.isnan(v) and not math.isinf(v)]
    return {
        "n": len(cleaned),
        "values": cleaned,
        "median": sanitize_number(median(cleaned)),
        "mad": sanitize_number(mad(cleaned)),
        "min": sanitize_number(min(cleaned) if cleaned else None),
        "max": sanitize_number(max(cleaned) if cleaned else None),
        "mean": sanitize_number(mean(cleaned)),
    }


def summarize_valid_runs(point_runs: Sequence[dict]) -> dict:
    """Summarize primary rates from status=valid runs only; report inconclusives separately."""
    valid_rates = [
        float(r["primary_msgs_per_s"])
        for r in point_runs
        if r.get("status") == "valid"
        and r.get("primary_msgs_per_s") is not None
        and not bool(r.get("non_comparable"))
    ]
    inconclusive = [r for r in point_runs if r.get("status") != "valid"]
    summary = summarize_runs(valid_rates)
    summary["inconclusive_n"] = len(inconclusive)
    summary["inconclusive_rates"] = [
        sanitize_number(r.get("primary_msgs_per_s")) for r in inconclusive
    ]
    summary["total_runs"] = len(point_runs)
    return summary


def latency_summary(samples_ns: Sequence[int], *, min_for_p99: int = 10_000) -> dict:
    """Summarize latency samples in milliseconds with gated p99."""
    samples_ms = [s / 1_000_000.0 for s in samples_ns]
    result = {
        "n_success": len(samples_ms),
        "p50_ms": sanitize_number(percentile(samples_ms, 50.0)),
        "p95_ms": sanitize_number(percentile(samples_ms, 95.0)),
        "p99_ms": None,
        "max_ms": sanitize_number(max(samples_ms) if samples_ms else None),
        "p99_published": False,
    }
    if len(samples_ms) >= min_for_p99:
        result["p99_ms"] = sanitize_number(percentile(samples_ms, 99.0))
        result["p99_published"] = True
    return result


def bootstrap_median_diff(
    baseline: Sequence[float],
    candidate: Sequence[float],
    *,
    n_boot: int = 2000,
    seed: int = 42,
    confidence: float = 0.95,
) -> dict:
    """Paired-style bootstrap on unpaired samples via resampled median ratio."""
    if not baseline or not candidate:
        return {
            "median_ratio": None,
            "ci_low": None,
            "ci_high": None,
            "excludes_zero_effect": False,
            "absolute_effect_pct": None,
        }
    rng = random.Random(seed)
    base_med = median(baseline)
    cand_med = median(candidate)
    if base_med is None or cand_med is None or base_med == 0:
        return {
            "median_ratio": None,
            "ci_low": None,
            "ci_high": None,
            "excludes_zero_effect": False,
            "absolute_effect_pct": None,
        }
    ratio = cand_med / base_med
    diffs = []
    for _ in range(n_boot):
        b = [baseline[rng.randrange(len(baseline))] for _ in range(len(baseline))]
        c = [candidate[rng.randrange(len(candidate))] for _ in range(len(candidate))]
        bm = median(b)
        cm = median(c)
        if bm is None or cm is None or bm == 0:
            continue
        diffs.append((cm / bm) - 1.0)
    if not diffs:
        return {
            "median_ratio": sanitize_number(ratio),
            "ci_low": None,
            "ci_high": None,
            "excludes_zero_effect": False,
            "absolute_effect_pct": sanitize_number((ratio - 1.0) * 100.0),
        }
    alpha = 1.0 - confidence
    lo = percentile(diffs, 100.0 * (alpha / 2.0))
    hi = percentile(diffs, 100.0 * (1.0 - alpha / 2.0))
    excludes_zero = lo is not None and hi is not None and (lo > 0 or hi < 0)
    return {
        "median_ratio": sanitize_number(ratio),
        "ci_low": sanitize_number(lo),
        "ci_high": sanitize_number(hi),
        "excludes_zero_effect": excludes_zero,
        "absolute_effect_pct": sanitize_number((ratio - 1.0) * 100.0),
    }


ABBA_BLOCK = ("A", "B", "B", "A")
BAAB_BLOCK = ("B", "A", "A", "B")


def abba_order(blocks: int) -> List[str]:
    """Return alternating ABBA / BAAB blocks.

    Repeating only ABBA puts B in every inner slot. A warmup or position
    effect then looks like a client effect, including on A/A. Alternating
    the two 4-slot designs gives each label the same number of inner and
    outer slots when ``blocks`` is even.
    """
    if blocks < 1:
        raise ValueError("blocks must be >= 1")
    order: List[str] = []
    for i in range(blocks):
        order.extend(ABBA_BLOCK if i % 2 == 0 else BAAB_BLOCK)
    return order


def abba_block_design(labels: Sequence[str]) -> Optional[str]:
    seq = tuple(labels)
    if seq == ABBA_BLOCK:
        return "ABBA"
    if seq == BAAB_BLOCK:
        return "BAAB"
    return None


def abba_position_counts(order: Sequence[str]) -> dict:
    """Inner (slots 1,2) vs outer (slots 0,3) counts per label, per 4-slot block."""
    counts = {"A": {"inner": 0, "outer": 0}, "B": {"inner": 0, "outer": 0}}
    for i in range(0, len(order), 4):
        chunk = list(order[i : i + 4])
        if len(chunk) < 4:
            break
        for j, label in enumerate(chunk):
            if label not in counts:
                continue
            pos = "outer" if j in (0, 3) else "inner"
            counts[label][pos] += 1
    return counts


def geometric_mean(values: Sequence[float]) -> Optional[float]:
    cleaned = [float(v) for v in values if v is not None and v > 0]
    if not cleaned:
        return None
    return math.exp(sum(math.log(v) for v in cleaned) / len(cleaned))


def balanced_geometric_ratio(
    ratios: Sequence[float],
    designs: Optional[Sequence[Optional[str]]] = None,
) -> Optional[float]:
    """Multiplicative centre of candidate/baseline block ratios.

    When both ABBA and BAAB blocks are present, each design is reduced
    first, then the two design means are combined. Two ABBA blocks plus
    one BAAB therefore cannot re-introduce the inner-slot bias. A pure
    position effect that maps to ``r`` and ``1/r`` recentres on 1.
    """
    if not ratios:
        return None
    if not designs or len(designs) != len(ratios):
        return geometric_mean(ratios)
    grouped: dict[str, List[float]] = {}
    for ratio, design in zip(ratios, designs):
        if ratio is None or ratio <= 0 or not design:
            continue
        grouped.setdefault(str(design), []).append(float(ratio))
    means = [geometric_mean(vals) for vals in grouped.values() if vals]
    means = [m for m in means if m is not None]
    return geometric_mean(means)


HIGHER_IS_BETTER = "higher_is_better"
LOWER_IS_BETTER = "lower_is_better"
LATENCY_P50_METRIC = "latency_p50_ms"
THROUGHPUT_METRIC = "primary_msgs_per_s"
_LATENCY_SCENARIOS = frozenset(
    {
        "application_rtt_fixed_rate",
        "application_rtt_qos1",
    }
)


def comparison_spec(scenario: Optional[str] = None, *, topology: Optional[str] = None) -> dict:
    """Which value ABBA/A-A ranks, and which way is better.

    Throughput scenarios keep ``primary_msgs_per_s`` / ``higher_is_better``.
    Application RTT ranks initiator ``p50_ms``; a matched-load pair that
    holds the offer will have nearly identical completion rates, so ranking
    those rates would be a tautology.
    """
    if scenario in _LATENCY_SCENARIOS or topology == "application_rtt":
        return {
            "comparison_metric": LATENCY_P50_METRIC,
            "comparison_direction": LOWER_IS_BETTER,
        }
    return {
        "comparison_metric": THROUGHPUT_METRIC,
        "comparison_direction": HIGHER_IS_BETTER,
    }


def _initiator_worker(run: dict) -> dict:
    for worker in run.get("workers") or []:
        if worker.get("role") in ("rtt_initiator", "publisher"):
            return worker
    return {}


def comparison_value(run: dict, scenario: Optional[str] = None) -> dict:
    """Extract the ABBA/A-A observation from one run.

    ``value`` is what block ratios are built from. p95/p99 travel alongside
    for latency points but are not the primary verdict.
    """
    point = run.get("point") or {}
    spec = comparison_spec(
        scenario or point.get("scenario") or run.get("scenario"),
        topology=point.get("topology"),
    )
    extras = {"p95_ms": None, "p99_ms": None}
    if spec["comparison_metric"] == LATENCY_P50_METRIC:
        latency = (_initiator_worker(run).get("latency_summary") or {})
        value = latency.get("p50_ms")
        extras["p95_ms"] = latency.get("p95_ms")
        extras["p99_ms"] = latency.get("p99_ms")
    else:
        value = run.get("primary_msgs_per_s")
    return {
        **spec,
        "value": sanitize_number(value) if value is not None else None,
        **extras,
    }


def abba_observation_usable(
    result: dict,
    value: Optional[float],
    *,
    profile: Optional[str] = None,
) -> bool:
    """Whether a run may enter an ABBA/A-A block ratio.

    Official compares drop ``non_comparable`` runs. Smoke tags *every* run
    that way, so a path-proof p50 would never form a ratio if we kept the
    official filter. Smoke still requires ``status=valid``.
    """
    if value is None:
        return False
    if result.get("status") != "valid":
        return False
    if profile == "smoke":
        return True
    return not result.get("non_comparable")


def abba_block_records(
    order: Sequence[str],
    rates_by_slot: Sequence[Optional[float]],
) -> List[dict]:
    """Complete ABBA or BAAB blocks with a candidate/baseline ratio each.

    Values are grouped by label, not by index, so BAAB is not silently
    dropped and the ratio stays ``median(B) / median(A)``.
    """
    records: List[dict] = []
    for i in range(0, len(order), 4):
        labels = list(order[i : i + 4])
        rates = list(rates_by_slot[i : i + 4])
        design = abba_block_design(labels)
        if design is None or len(rates) < 4:
            continue
        if any(r is None for r in rates):
            continue
        a_vals = [float(r) for lab, r in zip(labels, rates) if lab == "A"]
        b_vals = [float(r) for lab, r in zip(labels, rates) if lab == "B"]
        a_med = median(a_vals)
        b_med = median(b_vals)
        if a_med is None or b_med is None or a_med == 0:
            continue
        records.append(
            {
                "design": design,
                "ratio": b_med / a_med,
                "a_median": a_med,
                "b_median": b_med,
            }
        )
    return records


def abba_block_ratios(order: Sequence[str], rates_by_slot: Sequence[Optional[float]]) -> List[float]:
    """For each complete ABBA or BAAB block, return median(B)/median(A).

    The ratio is always ``candidate / baseline``. Interpretation depends on
    ``comparison_direction``: for latency, ``ratio < 1`` means the candidate
    is lower (better).
    """
    return [float(rec["ratio"]) for rec in abba_block_records(order, rates_by_slot)]


def compare_verdict_from_block_ratios(
    block_ratios: Sequence[float],
    *,
    min_effect_pct: float = 3.0,
    seed: int = 42,
    n_boot: int = 2000,
    confidence: float = 0.95,
    direction: str = HIGHER_IS_BETTER,
    designs: Optional[Sequence[Optional[str]]] = None,
) -> dict:
    """Bootstrap the multiplicative centre of candidate/baseline ratios.

    ``median_ratio`` is the geometric centre (kept under that name so
    existing readers keep working). ``absolute_effect_pct`` is
    ``(centre - 1) * 100``. Positive means the candidate observation is
    larger than the baseline. For ``higher_is_better`` that is an
    improvement; for ``lower_is_better`` (latency) it is a regression.

    CI remains on the ``(ratio - 1)`` scale so ``excludes_zero_effect``
    still means "the interval is incompatible with ratio 1".
    """
    empty = {
        "verdict": "inconclusive",
        "median_ratio": None,
        "geometric_ratio": None,
        "ci_low": None,
        "ci_high": None,
        "excludes_zero_effect": False,
        "absolute_effect_pct": None,
        "n_blocks": 0,
        "comparison_direction": direction,
        "block_ratios": [],
        "block_designs": [],
        "estimator": None,
    }
    cleaned = [float(r) for r in block_ratios if r is not None and r > 0]
    if not cleaned:
        return empty
    design_list: List[Optional[str]] = []
    if designs is not None and len(designs) == len(block_ratios):
        design_list = [
            str(d) if d else None
            for r, d in zip(block_ratios, designs)
            if r is not None and r > 0
        ]
        if len(design_list) != len(cleaned):
            design_list = []
    centre = balanced_geometric_ratio(cleaned, design_list or None)
    present = {d for d in design_list if d}
    estimator = (
        "balanced_geometric_mean"
        if len(present) >= 2
        else "geometric_mean"
    )
    grouped: dict[str, List[float]] = {}
    if design_list and len(design_list) == len(cleaned):
        for ratio, design in zip(cleaned, design_list):
            if design:
                grouped.setdefault(design, []).append(ratio)
        if len(grouped) < 2:
            grouped = {}
    rng = random.Random(seed)
    diffs = []
    for _ in range(n_boot):
        if grouped:
            means = []
            ok = True
            for vals in grouped.values():
                sample = [vals[rng.randrange(len(vals))] for _ in vals]
                g = geometric_mean(sample)
                if g is None:
                    ok = False
                    break
                means.append(g)
            m = geometric_mean(means) if ok else None
        else:
            sample = [cleaned[rng.randrange(len(cleaned))] for _ in range(len(cleaned))]
            m = geometric_mean(sample)
        if m is None:
            continue
        diffs.append(m - 1.0)
    alpha = 1.0 - confidence
    lo = percentile(diffs, 100.0 * (alpha / 2.0)) if diffs else None
    hi = percentile(diffs, 100.0 * (1.0 - alpha / 2.0)) if diffs else None
    excludes_zero = lo is not None and hi is not None and (lo > 0 or hi < 0)
    effect = None if centre is None else (centre - 1.0) * 100.0
    if effect is None or not excludes_zero or abs(effect) <= min_effect_pct:
        verdict = "inconclusive"
    elif direction == LOWER_IS_BETTER:
        verdict = "improvement" if effect < 0 else "regression"
    else:
        verdict = "improvement" if effect > 0 else "regression"
    return {
        "verdict": verdict,
        "median_ratio": sanitize_number(centre),
        "geometric_ratio": sanitize_number(centre),
        "ci_low": sanitize_number(lo),
        "ci_high": sanitize_number(hi),
        "excludes_zero_effect": excludes_zero,
        "absolute_effect_pct": sanitize_number(effect),
        "n_blocks": len(cleaned),
        "comparison_direction": direction,
        "block_ratios": [sanitize_number(r) for r in cleaned],
        "block_designs": design_list,
        "estimator": estimator,
    }


def compare_verdict(
    baseline_rates: Sequence[float],
    candidate_rates: Sequence[float],
    *,
    min_effect_pct: float = 3.0,
    seed: int = 42,
) -> dict:
    """Improvement/regression only if CI excludes 0 and |effect| > threshold."""
    boot = bootstrap_median_diff(baseline_rates, candidate_rates, seed=seed)
    effect = boot.get("absolute_effect_pct")
    excludes = boot.get("excludes_zero_effect", False)
    if effect is None or not excludes or abs(effect) <= min_effect_pct:
        verdict = "inconclusive"
    elif effect > 0:
        # Higher rate is better for throughput.
        verdict = "improvement"
    else:
        verdict = "regression"
    return {"verdict": verdict, **boot}


def integrity_counts(expected_sequences: Iterable[int], received_sequences: Iterable[int]) -> dict:
    """Compute unique/missing/duplicate/out-of-order counts for integrity runs."""
    expected = list(expected_sequences)
    received = list(received_sequences)
    expected_set = set(expected)
    seen = set()
    duplicates = 0
    out_of_order = 0
    last = -1
    for seq in received:
        if seq in seen:
            duplicates += 1
        else:
            seen.add(seq)
        if seq < last:
            out_of_order += 1
        last = seq
    unique = len(seen & expected_set)
    missing = len(expected_set - seen)
    unexpected = len(seen - expected_set)
    return {
        "expected": len(expected_set),
        "received": len(received),
        "unique": unique,
        "missing": missing,
        "duplicates": duplicates,
        "out_of_order": out_of_order,
        "unexpected": unexpected,
    }
