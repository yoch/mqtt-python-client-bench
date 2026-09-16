#!/usr/bin/env python3
"""Closed-loop capacity ABBA for two mqttium checkouts.

Unlike ``version_compare``, this does **not** freeze ``target_rate``. Capacity
points must keep the harness closed-loop (bounded outstanding). A frozen rate
would measure open-loop completion of a shared ceiling, not publish/subscribe
capacity.

Retry is inherited from ``version_compare``: whole-block, stimulus/environment
only. Surprising ratios never retry. Every attempt stays in the artifact.
"""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

from mqtt_client_bench.control import write_json
from mqtt_client_bench.harness import ABBA_COOLDOWN_S, HARNESS_FINGERPRINT, broker_up, run_point
from mqtt_client_bench.hostcal import resolve_host_profile
from mqtt_client_bench.metrics import (
    abba_block_records,
    abba_order,
    compare_verdict_from_block_ratios,
)
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario
from mqtt_client_bench.telemetry import allocate_cpuset, environment_metadata, pin_current_process
from mqtt_client_bench.version_compare import (
    DEFAULT_MAX_BLOCK_RETRIES,
    retryable_run_reasons,
    source_provenance,
)

SLOTS_PER_BLOCK = 4


def _unique(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        text = str(item)
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _worker(run: dict, roles: tuple[str, ...]) -> dict:
    for worker in run.get("workers") or []:
        if worker.get("role") in roles:
            return worker
    return {}


def slot_record(run: dict) -> dict:
    """Keep quality + cost fields; drop raw latency vectors (archived separately)."""
    worker = _worker(run, ("publisher", "subscriber", "rtt_initiator"))
    cost = run.get("cost_per_message") or {}
    return {
        "ab_label": run.get("ab_label"),
        "version_arm": run.get("version_arm"),
        "status": run.get("status"),
        "non_comparable": run.get("non_comparable"),
        "bottleneck": run.get("bottleneck"),
        "reasons": list(run.get("reasons") or []),
        "primary_msgs_per_s": run.get("primary_msgs_per_s"),
        "clock_unpinned": run.get("clock_unpinned"),
        "broker_cpu_max_pct": run.get("broker_cpu_max_pct"),
        "delivery_offer_ratio": run.get("delivery_offer_ratio"),
        "effective_offer_msgs_per_s": run.get("effective_offer_msgs_per_s"),
        "cpu_us_per_message": cost.get("cpu_us_per_message"),
        "rss_peak_kb": cost.get("rss_peak_kb"),
        "completed_success": worker.get("completed_success"),
        "completed_failed": worker.get("completed_failed"),
        "missed_due_to_backpressure": worker.get("missed_due_to_backpressure"),
        "sync_rejected": worker.get("sync_rejected"),
        "subscriber_delivered": worker.get("subscriber_delivered"),
        "ru_nvcsw": worker.get("ru_nvcsw"),
        "ru_nivcsw": worker.get("ru_nivcsw"),
        "logical_block": run.get("logical_block"),
        "block_attempt": run.get("block_attempt"),
        "block_slot": run.get("block_slot"),
        "active_for_verdict": run.get("active_for_verdict"),
        "block_retry_reasons": run.get("block_retry_reasons"),
        "client_path": run.get("client_path") or (run.get("client_identity") or {}).get("client_path"),
    }


def _usable_rate(run: dict) -> float | None:
    if run.get("status") != "valid" or run.get("non_comparable"):
        return None
    rate = run.get("primary_msgs_per_s")
    if rate is None:
        return None
    return float(rate)


def _median(values: list[float]) -> float | None:
    cleaned = [float(v) for v in values if v is not None]
    if not cleaned:
        return None
    return float(statistics.median(cleaned))


def _cv_pct(values: list[float]) -> float | None:
    cleaned = [float(v) for v in values if v is not None]
    if len(cleaned) < 2:
        return None
    mean = statistics.mean(cleaned)
    if mean == 0:
        return None
    return 100.0 * statistics.pstdev(cleaned) / mean


def run_capacity_ab(
    *,
    scenario: str,
    variant_index: int,
    baseline_path: str,
    candidate_path: str,
    baseline_src: str | None,
    candidate_src: str | None,
    blocks: int,
    profile: str,
    output: Path,
    ingress_offer: float | None,
    comparison_name: str,
    max_block_retries: int = DEFAULT_MAX_BLOCK_RETRIES,
) -> dict:
    if blocks <= 0 or blocks % 2:
        raise ValueError("blocks must be a positive even number")
    points = expand_scenario(SCENARIO_BY_NAME[scenario], profile)
    if not 0 <= variant_index < len(points):
        raise ValueError(f"variant_index {variant_index} outside 0..{len(points)-1}")
    point = dict(points[variant_index])
    point.pop("target_rate", None)
    point.pop("load_fraction", None)
    point.pop("shared_load_fraction", None)
    if ingress_offer is not None:
        point["ingress_target_msgs_per_s"] = float(ingress_offer)
        point["diagnostic_ingress_offer"] = float(ingress_offer)

    baseline_install = str(Path(baseline_path).resolve())
    candidate_install = str(Path(candidate_path).resolve())
    if baseline_install == candidate_install:
        raise ValueError("baseline and candidate resolve to the same install path")

    sources = {
        "baseline": source_provenance(baseline_src or baseline_install),
        "candidate": source_provenance(candidate_src or candidate_install),
    }
    sources["baseline"]["install"] = baseline_install
    sources["candidate"]["install"] = candidate_install

    host_profile = resolve_host_profile()
    cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    pin_current_process(cpusets.get("orch"))
    meta = broker_up(wait=True, cpuset=cpusets.get("broker"))
    host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]

    order = abba_order(blocks)
    runs: list[dict] = []
    slot_values: list[float | None] = []
    block_attempts: list[dict] = []
    paths = {"A": baseline_install, "B": candidate_install}

    with tempfile.TemporaryDirectory(prefix="mqtt-bench-capacity-ab-") as tmp:
        work_dir = Path(tmp)
        for block_index in range(blocks):
            labels = order[block_index * SLOTS_PER_BLOCK : (block_index + 1) * SLOTS_PER_BLOCK]
            selected = None
            attempts = []
            for attempt_index in range(max_block_retries + 1):
                attempt_runs = []
                run_indices = []
                for block_slot, label in enumerate(labels):
                    if runs:
                        time.sleep(ABBA_COOLDOWN_S)
                    result = run_point(
                        point,
                        client="mqttium",
                        client_path=paths[label],
                        host=host,
                        port=port,
                        tls_port=tls_port,
                        profile=profile,
                        work_dir=work_dir,
                        cpusets=cpusets,
                        host_profile=host_profile,
                        managed_broker=True,
                    )
                    source_key = "baseline" if label == "A" else "candidate"
                    result.update(
                        {
                            "ab_label": label,
                            "version_arm": source_key,
                            "version_source": sources[source_key],
                            "logical_block": block_index,
                            "block_attempt": attempt_index,
                            "block_slot": block_slot,
                            "logical_slot": block_index * SLOTS_PER_BLOCK + block_slot,
                            "execution_slot": len(runs),
                            "cooldown_s": ABBA_COOLDOWN_S,
                            "comparison_name": comparison_name,
                        }
                    )
                    runs.append(result)
                    attempt_runs.append(result)
                    run_indices.append(len(runs) - 1)

                retry_reasons = _unique(
                    [
                        reason
                        for result in attempt_runs
                        for reason in retryable_run_reasons(result)
                    ]
                )
                will_retry = bool(retry_reasons and attempt_index < max_block_retries)
                attempt_record = {
                    "block": block_index,
                    "design": "ABBA" if labels == ["A", "B", "B", "A"] else "BAAB",
                    "attempt": attempt_index,
                    "run_indices": run_indices,
                    "retryable_reasons": retry_reasons,
                    "retried": will_retry,
                    "selected_for_verdict": not will_retry,
                }
                attempts.append(attempt_record)
                for result in attempt_runs:
                    result["active_for_verdict"] = not will_retry
                    result["block_retry_reasons"] = retry_reasons
                if will_retry:
                    continue
                selected = (attempt_runs, attempt_record)
                break

            if selected is None:
                raise RuntimeError(f"capacity A/B block {block_index} produced no final attempt")
            selected_runs, selected_attempt = selected
            block_attempts.extend(attempts)
            for label, result in zip(labels, selected_runs):
                slot_values.append(_usable_rate(result) if result.get("active_for_verdict") else None)

    records = abba_block_records(order, slot_values)
    block_ratios = [float(row["ratio"]) for row in records]
    block_designs = [row["design"] for row in records]
    statistical_verdict = compare_verdict_from_block_ratios(
        block_ratios, designs=block_designs
    )
    complete_blocks = len(records)
    retry_exhausted = sorted(
        {
            int(row["block"])
            for row in block_attempts
            if row["selected_for_verdict"] and row["retryable_reasons"]
        }
    )
    qualification_ok = complete_blocks == blocks and not retry_exhausted
    verdict = dict(statistical_verdict)
    verdict["statistical_verdict"] = statistical_verdict.get("verdict")
    verdict["capacity_ab_qualified"] = qualification_ok
    if not qualification_ok:
        verdict["verdict"] = "inconclusive"
        verdict["reason"] = "capacity_ab_quality_incomplete"

    a_rates = [r for r, lab in zip(slot_values, order) if lab == "A" and r is not None]
    b_rates = [r for r, lab in zip(slot_values, order) if lab == "B" and r is not None]
    same_sha = sources["baseline"].get("git_sha") and sources["baseline"].get("git_sha") == sources[
        "candidate"
    ].get("git_sha")

    payload = {
        "schema_version": 1,
        "comparison_kind": "capacity_aa" if same_sha else "capacity_ab",
        "comparison_name": comparison_name,
        "harness_fingerprint": HARNESS_FINGERPRINT,
        "closed_loop": True,
        "target_rate_frozen": False,
        "scenario": scenario,
        "variant_index": variant_index,
        "profile": profile,
        "point": point,
        "blocks_requested": blocks,
        "max_block_retries": max_block_retries,
        "order": order,
        "baseline_source": sources["baseline"],
        "candidate_source": sources["candidate"],
        "baseline_rates": a_rates,
        "candidate_rates": b_rates,
        "slot_rates": slot_values,
        "block_ratios": block_ratios,
        "block_designs": block_designs,
        "block_attempts": block_attempts,
        "slot_summaries": [slot_record(run) for run in runs],
        "qualification": {
            "ok": qualification_ok,
            "blocks_complete": complete_blocks,
            "blocks_requested": blocks,
            "retry_exhausted_blocks": retry_exhausted,
            "retry_policy": "whole_block_stimulus_or_environment_only",
        },
        "verdict": verdict,
        "statistical_verdict": statistical_verdict,
        "dispersion": {
            "baseline_median": _median(a_rates),
            "candidate_median": _median(b_rates),
            "baseline_cv_pct": _cv_pct(a_rates),
            "candidate_cv_pct": _cv_pct(b_rates),
            "aa_median_ratio": (
                (_median(b_rates) / _median(a_rates))
                if a_rates and b_rates and _median(a_rates)
                else None
            ),
        },
        "cooldown_s": ABBA_COOLDOWN_S,
        "broker": meta,
        "environment": environment_metadata(),
        "cpusets": cpusets,
        "runs": runs,
    }
    write_json(str(output), payload)
    return payload


def run_offer_probe(
    *,
    scenario: str,
    variant_index: int,
    client_path: str,
    src_path: str | None,
    offers: list[float],
    repeats: int,
    profile: str,
    output: Path,
) -> dict:
    """Alternate offers on one checkout so time-drift is not an offer effect."""
    points = expand_scenario(SCENARIO_BY_NAME[scenario], profile)
    point_base = dict(points[variant_index])
    point_base.pop("target_rate", None)
    host_profile = resolve_host_profile()
    cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    pin_current_process(cpusets.get("orch"))
    meta = broker_up(wait=True, cpuset=cpusets.get("broker"))
    host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]
    source = source_provenance(src_path or client_path)
    source["install"] = str(Path(client_path).resolve())
    runs: list[dict] = []
    with tempfile.TemporaryDirectory(prefix="mqtt-bench-offer-probe-") as tmp:
        work_dir = Path(tmp)
        for repeat in range(repeats):
            for offer in offers:
                if runs:
                    time.sleep(ABBA_COOLDOWN_S)
                point = dict(point_base)
                point["ingress_target_msgs_per_s"] = float(offer)
                point["diagnostic_ingress_offer"] = float(offer)
                result = run_point(
                    point,
                    client="mqttium",
                    client_path=str(Path(client_path).resolve()),
                    host=host,
                    port=port,
                    tls_port=tls_port,
                    profile=profile,
                    work_dir=work_dir,
                    cpusets=cpusets,
                    host_profile=host_profile,
                    managed_broker=True,
                )
                result["probe_offer"] = float(offer)
                result["probe_repeat"] = repeat
                runs.append(result)
    by_offer: dict[str, list[float]] = {str(o): [] for o in offers}
    for run in runs:
        rate = _usable_rate(run)
        if rate is not None:
            by_offer[str(run["probe_offer"])].append(rate)
    payload = {
        "schema_version": 1,
        "comparison_kind": "ingress_offer_probe",
        "scenario": scenario,
        "variant_index": variant_index,
        "profile": profile,
        "offers": offers,
        "repeats": repeats,
        "source": source,
        "by_offer": {
            key: {"rates": vals, "median": _median(vals), "cv_pct": _cv_pct(vals)}
            for key, vals in by_offer.items()
        },
        "slot_summaries": [slot_record(run) | {"probe_offer": run.get("probe_offer")} for run in runs],
        "broker": meta,
        "environment": environment_metadata(),
        "cpusets": cpusets,
        "runs": runs,
    }
    write_json(str(output), payload)
    return payload


def _print_summary(payload: dict) -> None:
    kind = payload.get("comparison_kind")
    if kind == "ingress_offer_probe":
        print(json.dumps({"kind": kind, "by_offer": payload.get("by_offer")}, indent=2))
        return
    print(
        json.dumps(
            {
                "comparison_kind": kind,
                "comparison_name": payload.get("comparison_name"),
                "baseline_sha": (payload.get("baseline_source") or {}).get("git_sha"),
                "candidate_sha": (payload.get("candidate_source") or {}).get("git_sha"),
                "qualification": payload.get("qualification"),
                "verdict": payload.get("verdict"),
                "dispersion": payload.get("dispersion"),
                "block_ratios": payload.get("block_ratios"),
                "valid_slots": sum(v is not None for v in payload.get("slot_rates") or []),
                "total_slots": len(payload.get("slot_rates") or []),
            },
            indent=2,
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["ab", "probe"], default="ab")
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--variant-index", type=int, required=True)
    parser.add_argument("--baseline-path")
    parser.add_argument("--candidate-path")
    parser.add_argument("--baseline-src")
    parser.add_argument("--candidate-src")
    parser.add_argument("--client-path", help="single checkout for --mode probe")
    parser.add_argument("--client-src")
    parser.add_argument("--blocks", type=int, default=8)
    parser.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    parser.add_argument("--ingress-offer", type=float)
    parser.add_argument("--probe-offers", default="200000,250000")
    parser.add_argument("--probe-repeats", type=int, default=2)
    parser.add_argument("--name", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-block-retries", type=int, default=DEFAULT_MAX_BLOCK_RETRIES)
    args = parser.parse_args(argv)

    if args.mode == "probe":
        if not args.client_path:
            parser.error("--client-path is required for --mode probe")
        offers = [float(x) for x in args.probe_offers.split(",") if x.strip()]
        payload = run_offer_probe(
            scenario=args.scenario,
            variant_index=args.variant_index,
            client_path=args.client_path,
            src_path=args.client_src,
            offers=offers,
            repeats=args.probe_repeats,
            profile=args.profile,
            output=args.output,
        )
    else:
        if not args.baseline_path or not args.candidate_path:
            parser.error("--baseline-path and --candidate-path are required for --mode ab")
        payload = run_capacity_ab(
            scenario=args.scenario,
            variant_index=args.variant_index,
            baseline_path=args.baseline_path,
            candidate_path=args.candidate_path,
            baseline_src=args.baseline_src,
            candidate_src=args.candidate_src,
            blocks=args.blocks,
            profile=args.profile,
            output=args.output,
            ingress_offer=args.ingress_offer,
            comparison_name=args.name,
            max_block_retries=args.max_block_retries,
        )
    _print_summary(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
