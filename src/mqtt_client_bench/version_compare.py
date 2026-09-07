"""ABBA comparison of two source versions of the same MQTT client.

``compare`` keys source paths by client name, so ``mqttium,mqttium`` cannot
represent two different checkouts.  This command gives A and B explicit source
paths and keeps them interleaved on the same host.

Version A/B deliberately requires a frozen absolute target rate and an external
broker.  It never recalibrates either arm and never calls the A/A gate.  Qualify
a separate same-source A/A first.

A retry is allowed only when a predeclared stimulus / harness / environment
condition invalidated the *whole* four-slot ABBA/BAAB block.  Latency, ratios,
client backpressure, pair effects and verdicts can never request a retry.  Every
attempt remains in the artifact so retry cannot become invisible cherry-picking.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path

from mqtt_client_bench.adapters.registry import CLIENT_NAMES, adapter_identity
from mqtt_client_bench.broker import DEFAULT_TLS_PORT, parse_broker_endpoint, wait_for_broker
from mqtt_client_bench.control import write_json
from mqtt_client_bench.harness import ABBA_COOLDOWN_S, HARNESS_FINGERPRINT, _validate_host_profile, run_point
from mqtt_client_bench.hostcal import resolve_host_profile
from mqtt_client_bench.metrics import (
    abba_block_records,
    abba_observation_usable,
    abba_order,
    compare_verdict_from_block_ratios,
    comparison_spec,
    comparison_value,
)
from mqtt_client_bench.quality import run_temporal_quality
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario
from mqtt_client_bench.telemetry import (
    allocate_cpuset,
    environment_metadata,
    pin_current_process,
    resolve_external_broker_pid,
)

COMPARISON_KIND = "same_client_version_ab"
SLOTS_PER_BLOCK = 4
DEFAULT_MAX_BLOCK_RETRIES = 1

# These are validity failures of the measurement mechanism/environment, not
# client performance outcomes.  In particular, broker headroom, backpressure,
# timeouts, protocol failures and latency results are deliberately absent.
RETRYABLE_RUN_REASON_PREFIXES = (
    "host_busy_at_start:",
    "barrier_failed:",
    "broker_telemetry_missing",
    "broker_pid_unobserved",
    "worker_hang",
)


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


def source_provenance(path: str) -> dict:
    root = Path(path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"client source path is not a directory: {root}")
    out = {"path": str(root), "git_sha": None, "git_dirty": None}
    try:
        out["git_sha"] = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True, timeout=5,
        ).stdout.strip() or None
        out["git_dirty"] = bool(subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"],
            check=True, capture_output=True, text=True, timeout=5,
        ).stdout.strip())
    except (FileNotFoundError, subprocess.SubprocessError):
        pass
    return out


def validate_version_arms(client: str, baseline_path: str, candidate_path: str, *, require_clean_git: bool = True) -> dict:
    if client not in CLIENT_NAMES:
        raise ValueError(f"unknown client: {client}")
    baseline = source_provenance(baseline_path)
    candidate = source_provenance(candidate_path)
    if baseline["path"] == candidate["path"]:
        raise ValueError("baseline and candidate resolve to the same source path")
    if require_clean_git:
        for label, source in (("baseline", baseline), ("candidate", candidate)):
            if not source.get("git_sha"):
                raise ValueError(f"{label} source has no Git SHA")
            if source.get("git_dirty"):
                raise ValueError(f"{label} source checkout is dirty")
    baseline["adapter_identity"] = adapter_identity(client, baseline["path"])
    candidate["adapter_identity"] = adapter_identity(client, candidate["path"])
    return {"baseline": baseline, "candidate": candidate}


def arm_path(label: str, baseline_path: str, candidate_path: str) -> str:
    if label == "A":
        return baseline_path
    if label == "B":
        return candidate_path
    raise ValueError(f"unknown ABBA label: {label}")


def retryable_run_reasons(result: dict) -> list[str]:
    """Reasons that may invalidate and replace an entire four-slot block.

    The temporal contract is evaluated independently of ``result.status`` so a
    historical ``status=valid`` run with excessive catch-up cannot enter a new
    version A/B.  Client-performance reasons never appear in this return value.
    """
    reasons = []
    temporal = run_temporal_quality(result)
    if not temporal["ok"]:
        reasons.extend(str(reason) for reason in temporal["reasons"])
    for reason in result.get("reasons") or []:
        text = str(reason)
        if any(text.startswith(prefix) for prefix in RETRYABLE_RUN_REASON_PREFIXES):
            reasons.append(text)
    return _unique(reasons)


def observation_for_verdict(result: dict, scenario: str, *, profile: str) -> dict:
    observed = comparison_value(result, scenario)
    temporal = run_temporal_quality(result)
    result["temporal_quality"] = temporal
    base_usable = abba_observation_usable(result, observed["value"], profile=profile)
    usable = bool(base_usable and temporal["ok"])
    return {
        **observed,
        "usable": usable,
        "base_usable": bool(base_usable),
        "temporal_ok": bool(temporal["ok"]),
    }


def compare_versions(
    *, client: str, baseline_path: str, candidate_path: str, scenario: str,
    target_rate: float, broker: str, broker_pid: int, blocks: int = 6,
    profile: str = "standard", variant_index: int = 0,
    pacer_mode: str = "external", host_profile_path: str | None = None,
    output: str | None = None, require_clean_git: bool = True,
    max_block_retries: int = DEFAULT_MAX_BLOCK_RETRIES,
) -> dict:
    if scenario not in SCENARIO_BY_NAME:
        raise ValueError(f"unknown scenario: {scenario}")
    if target_rate <= 0:
        raise ValueError("target_rate must be > 0 and frozen before version A/B")
    if blocks <= 0 or blocks % 2:
        raise ValueError("blocks must be a positive even number")
    if max_block_retries < 0:
        raise ValueError("max_block_retries must be >= 0")
    sources = validate_version_arms(
        client, baseline_path, candidate_path, require_clean_git=require_clean_git
    )
    baseline_path = sources["baseline"]["path"]
    candidate_path = sources["candidate"]["path"]

    points = expand_scenario(SCENARIO_BY_NAME[scenario], profile)
    if not 0 <= variant_index < len(points):
        raise ValueError(f"variant_index {variant_index} outside 0..{len(points)-1}")
    point = dict(points[variant_index])
    point["target_rate"] = float(target_rate)
    point["pacer_mode"] = pacer_mode
    point["version_ab_target_frozen"] = True

    cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    pin_current_process(cpusets.get("orch"))
    host, port = parse_broker_endpoint(broker)
    wait_for_broker(host, port, timeout_s=10)
    external_pid = resolve_external_broker_pid(broker_pid)
    if external_pid is None:
        raise ValueError("version A/B requires an observable external broker PID")
    meta = {"managed_broker": False, "host": host, "port": port, "tls_port": DEFAULT_TLS_PORT, "pid": external_pid}

    host_profile = resolve_host_profile(host_profile_path)
    if host_profile is not None:
        _validate_host_profile(host_profile)

    order = abba_order(blocks)
    spec = comparison_spec(scenario)
    baseline_values, candidate_values, slot_values = [], [], []
    slot_p95, slot_p99, runs = [], [], []
    block_attempts = []

    with tempfile.TemporaryDirectory(prefix="mqtt-bench-version-ab-") as tmp:
        work_dir = Path(tmp)
        for block_index in range(blocks):
            labels = order[
                block_index * SLOTS_PER_BLOCK : (block_index + 1) * SLOTS_PER_BLOCK
            ]
            selected = None
            attempts = []
            for attempt_index in range(max_block_retries + 1):
                attempt_runs = []
                attempt_observed = []
                run_indices = []
                for block_slot, label in enumerate(labels):
                    if runs:
                        time.sleep(ABBA_COOLDOWN_S)
                    path = arm_path(label, baseline_path, candidate_path)
                    source_key = "baseline" if label == "A" else "candidate"
                    result = run_point(
                        point, client=client, client_path=path, host=host, port=port,
                        tls_port=DEFAULT_TLS_PORT, profile=profile, work_dir=work_dir,
                        cpusets=cpusets, load_profile=None, host_profile=host_profile,
                        managed_broker=False, external_broker_pid=external_pid, cross_client=True,
                    )
                    result.update({
                        "ab_label": label,
                        "version_arm": source_key,
                        "version_source": sources[source_key],
                        "logical_block": block_index,
                        "block_attempt": attempt_index,
                        "block_slot": block_slot,
                        "logical_slot": block_index * SLOTS_PER_BLOCK + block_slot,
                        "execution_slot": len(runs),
                        "cooldown_s": ABBA_COOLDOWN_S,
                    })
                    observed = observation_for_verdict(result, scenario, profile=profile)
                    result["comparison_metric"] = observed["comparison_metric"]
                    result["comparison_direction"] = observed["comparison_direction"]
                    result["comparison_value"] = observed["value"]
                    result["version_ab_observation_usable"] = observed["usable"]
                    runs.append(result)
                    attempt_runs.append(result)
                    attempt_observed.append(observed)
                    run_indices.append(len(runs) - 1)

                retry_reasons = _unique([
                    reason
                    for result in attempt_runs
                    for reason in retryable_run_reasons(result)
                ])
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
                selected = (attempt_runs, attempt_observed, attempt_record)
                break

            if selected is None:  # defensive; loop always selects final attempt
                raise RuntimeError(f"version A/B block {block_index} produced no final attempt")
            selected_runs, selected_observed, selected_attempt = selected
            block_attempts.extend(attempts)
            for label, result, observed in zip(labels, selected_runs, selected_observed):
                value = float(observed["value"]) if observed["usable"] else None
                slot_values.append(value)
                slot_p95.append(observed.get("p95_ms") if observed["usable"] else None)
                slot_p99.append(observed.get("p99_ms") if observed["usable"] else None)
                if value is not None:
                    (baseline_values if label == "A" else candidate_values).append(value)

    records = abba_block_records(order, slot_values)
    block_ratios = [float(row["ratio"]) for row in records]
    block_designs = [row["design"] for row in records]
    statistical_verdict = compare_verdict_from_block_ratios(
        block_ratios, direction=spec["comparison_direction"], designs=block_designs
    )
    statistical_verdict["comparison_metric"] = spec["comparison_metric"]
    statistical_verdict["comparison_direction"] = spec["comparison_direction"]

    complete_blocks = len(records)
    retry_exhausted_blocks = sorted({
        int(row["block"])
        for row in block_attempts
        if row["selected_for_verdict"] and row["retryable_reasons"]
    })
    qualification_ok = complete_blocks == blocks and not retry_exhausted_blocks
    verdict = dict(statistical_verdict)
    verdict["statistical_verdict"] = statistical_verdict.get("verdict")
    verdict["version_ab_qualified"] = qualification_ok
    verdict["blocks_complete"] = complete_blocks
    verdict["blocks_requested"] = blocks
    if not qualification_ok:
        verdict["verdict"] = "inconclusive"
        verdict["reason"] = "version_ab_quality_incomplete"

    payload = {
        "schema_version": 1,
        "comparison_kind": COMPARISON_KIND,
        "harness_fingerprint": HARNESS_FINGERPRINT,
        "scenario": scenario,
        "profile": profile,
        "point": point,
        "variant_index": variant_index,
        "target_rate_frozen": float(target_rate),
        "pacer_mode": pacer_mode,
        "blocks_requested": blocks,
        "max_block_retries": max_block_retries,
        "order": order,
        "client": client,
        "baseline_client": client,
        "candidate_client": client,
        "baseline_source": sources["baseline"],
        "candidate_source": sources["candidate"],
        "baseline_rates": baseline_values,
        "candidate_rates": candidate_values,
        "slot_rates": slot_values,
        "slot_p95_ms": slot_p95,
        "slot_p99_ms": slot_p99,
        "block_ratios": block_ratios,
        "block_designs": block_designs,
        "block_attempts": block_attempts,
        "qualification": {
            "ok": qualification_ok,
            "blocks_complete": complete_blocks,
            "blocks_requested": blocks,
            "retry_exhausted_blocks": retry_exhausted_blocks,
            "temporal_quality_required": True,
            "retry_policy": "whole_block_stimulus_or_environment_only",
        },
        "verdict": verdict,
        "statistical_verdict": statistical_verdict,
        "runs": runs,
        "comparison_metric": spec["comparison_metric"],
        "comparison_direction": spec["comparison_direction"],
        "cooldown_s": ABBA_COOLDOWN_S,
        "broker": meta,
        "environment": environment_metadata(),
        "cpusets": cpusets,
        "aa_control": None,
        "aa_control_reason": "version_ab_requires_separate_same-source_aa_control",
    }
    if output:
        write_json(output, payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, choices=sorted(CLIENT_NAMES))
    parser.add_argument("--baseline-path", required=True)
    parser.add_argument("--candidate-path", required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--target-rate", required=True, type=float)
    parser.add_argument("--broker", required=True)
    parser.add_argument("--broker-pid", required=True, type=int)
    parser.add_argument("--blocks", type=int, default=6)
    parser.add_argument("--max-block-retries", type=int, default=DEFAULT_MAX_BLOCK_RETRIES)
    parser.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    parser.add_argument("--variant-index", type=int, default=0)
    parser.add_argument("--pacer-mode", choices=["in_loop", "external"], default="external")
    parser.add_argument("--host-profile")
    parser.add_argument("--output")
    parser.add_argument("--allow-unversioned-source", action="store_true")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    payload = compare_versions(
        client=args.client, baseline_path=args.baseline_path,
        candidate_path=args.candidate_path, scenario=args.scenario,
        target_rate=args.target_rate, broker=args.broker, broker_pid=args.broker_pid,
        blocks=args.blocks, profile=args.profile, variant_index=args.variant_index,
        pacer_mode=args.pacer_mode, host_profile_path=args.host_profile,
        output=args.output, require_clean_git=not args.allow_unversioned_source,
        max_block_retries=args.max_block_retries,
    )
    print(json.dumps({
        "comparison_kind": payload["comparison_kind"],
        "baseline_sha": payload["baseline_source"].get("git_sha"),
        "candidate_sha": payload["candidate_source"].get("git_sha"),
        "target_rate": payload["target_rate_frozen"],
        "qualification": payload["qualification"],
        "verdict": payload["verdict"],
        "valid_slots": sum(v is not None for v in payload["slot_rates"]),
        "total_slots": len(payload["slot_rates"]),
        "executed_slots": len(payload["runs"]),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
