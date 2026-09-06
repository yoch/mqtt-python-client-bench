#!/usr/bin/env python3
"""Interleaved rc10↔rc11 ABBA for mqttium on one host.

A slots use mqttium==1.0.0rc10 from --rc10-path; B slots use rc11 from
--rc11-path.  Provenance is recorded in every output JSON.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from mqtt_client_bench.harness import ABBA_COOLDOWN_S, broker_up, read_json, run_point
from mqtt_client_bench.hostcal import resolve_host_profile
from mqtt_client_bench.metrics import abba_block_records, abba_order, compare_verdict_from_block_ratios
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario
from mqtt_client_bench.telemetry import allocate_cpuset, environment_metadata, pin_current_process

RC10_DEFAULT = Path(__file__).resolve().parents[1] / ".mqttium-ab" / "rc10"
RC11_DEFAULT = Path(__file__).resolve().parents[1] / ".mqttium-ab" / "rc11"


def _git_head() -> str:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, cwd=Path(__file__).parents[1])
            .strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _version_from_path(root: Path) -> str:
    import importlib.util

    spec = importlib.util.spec_from_file_location("mqttium_probe", root / "mqttium" / "__init__.py")
    if spec is None or spec.loader is None:
        return "unknown"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return str(getattr(mod, "__version__", "unknown"))


def _latency_from_run(run: Dict[str, Any]) -> Dict[str, Any]:
    for worker in run.get("workers") or []:
        summary = worker.get("latency_summary")
        if summary:
            return dict(summary)
    return {}


def _completion_stats(run: Dict[str, Any]) -> Dict[str, Any]:
    for worker in run.get("workers") or []:
        if worker.get("role") != "publisher":
            continue
        offered = worker.get("offered_count")
        completed = worker.get("completed_success")
        if offered is not None or completed is not None:
            ratio = None
            if offered and completed is not None:
                ratio = float(completed) / float(offered)
            return {
                "offered": offered,
                "completed_success": completed,
                "completion_ratio": ratio,
            }
    return {}


def _usable_rate(run: Dict[str, Any]) -> Optional[float]:
    rate = run.get("primary_msgs_per_s")
    if rate is None or run.get("status") != "valid" or run.get("non_comparable"):
        return None
    return float(rate)


def _point_label(point: Dict[str, Any]) -> str:
    parts = [str(point.get("protocol", "MQTTv311"))]
    if "qos_publish" in point:
        parts.append(f"QoS{point['qos_publish']}")
    if point.get("target_rate") is not None:
        parts.append(f"@{int(point['target_rate'])}")
    if point.get("inflight") is not None and point.get("scenario") == "pub_qos1_inflight":
        parts.append(f"inflight={point['inflight']}")
    if point.get("payload") not in (None, "telemetry256"):
        parts.append(str(point["payload"]))
    return " ".join(parts)


def run_abba_point(
    point: Dict[str, Any],
    *,
    blocks: int,
    profile: str,
    rc10_path: Path,
    rc11_path: Path,
    work_dir: Path,
    cpusets: Dict[str, str],
    host: str,
    port: int,
    tls_port: int,
    host_profile: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    order = abba_order(blocks)
    version_paths = {"A": str(rc10_path), "B": str(rc11_path)}
    slot_rates: List[Optional[float]] = []
    slot_latency_p50: Dict[str, List[float]] = {"A": [], "B": []}
    raw: List[Dict[str, Any]] = []
    for slot, label in enumerate(order):
        if slot > 0:
            time.sleep(ABBA_COOLDOWN_S)
        result = run_point(
            point,
            client="mqttium",
            client_path=version_paths[label],
            host=host,
            port=port,
            tls_port=tls_port,
            profile=profile,
            work_dir=work_dir,
            cpusets=cpusets,
            host_profile=host_profile,
            managed_broker=True,
        )
        result["ab_label"] = label
        result["ab_version"] = _version_from_path(rc10_path if label == "A" else rc11_path)
        result["slot"] = slot
        raw.append(result)
        slot_rates.append(_usable_rate(result))
        lat = _latency_from_run(result)
        if lat.get("p50_ms") is not None:
            slot_latency_p50[label].append(float(lat["p50_ms"]))

    records = abba_block_records(order, slot_rates)
    block_ratios = [rec["ratio"] for rec in records]
    verdict = compare_verdict_from_block_ratios(
        block_ratios,
        designs=[rec["design"] for rec in records],
    )
    a_rates = [r for r, lab in zip(slot_rates, order) if lab == "A" and r is not None]
    b_rates = [r for r, lab in zip(slot_rates, order) if lab == "B" and r is not None]

    def _median(vals: List[float]) -> Optional[float]:
        if not vals:
            return None
        s = sorted(vals)
        return s[len(s) // 2]

    lat_ratio = None
    a_p50 = _median(slot_latency_p50["A"])
    b_p50 = _median(slot_latency_p50["B"])
    if a_p50 and b_p50:
        lat_ratio = b_p50 / a_p50

    return {
        "point": point,
        "label": _point_label(point),
        "order": order,
        "blocks": blocks,
        "baseline_version": _version_from_path(rc10_path),
        "candidate_version": _version_from_path(rc11_path),
        "baseline_rates": a_rates,
        "candidate_rates": b_rates,
        "slot_rates": slot_rates,
        "block_ratios": block_ratios,
        "verdict": verdict,
        "latency": {
            "baseline_p50_ms": a_p50,
            "candidate_p50_ms": b_p50,
            "b_over_a_p50": lat_ratio,
        },
        "runs": raw,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--variant-index", type=int, action="append", default=None)
    parser.add_argument("--blocks", type=int, default=5)
    parser.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    parser.add_argument("--rc10-path", type=Path, default=RC10_DEFAULT)
    parser.add_argument("--rc11-path", type=Path, default=RC11_DEFAULT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--host-profile", type=Path, default=None)
    args = parser.parse_args(argv)

    host_profile = resolve_host_profile(str(args.host_profile) if args.host_profile else None)

    scenario = SCENARIO_BY_NAME[args.scenario]
    points = expand_scenario(scenario, args.profile)
    if args.variant_index is not None:
        points = [points[i] for i in args.variant_index]

    try:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=args.profile)
    except RuntimeError:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")
    pin_current_process(cpusets.get("orch"))
    meta = broker_up(wait=True, cpuset=cpusets.get("broker"))
    host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]

    import tempfile

    point_results = []
    with tempfile.TemporaryDirectory(prefix="mqttium-rc-ab-") as tmp:
        work_dir = Path(tmp)
        for point in points:
            point_results.append(
                run_abba_point(
                    point,
                    blocks=args.blocks,
                    profile=args.profile,
                    rc10_path=args.rc10_path.resolve(),
                    rc11_path=args.rc11_path.resolve(),
                    work_dir=work_dir,
                    cpusets=cpusets,
                    host=host,
                    port=port,
                    tls_port=tls_port,
                    host_profile=host_profile,
                )
            )

    payload: Dict[str, Any] = {
        "schema_version": 1,
        "kind": "mqttium_version_abba",
        "benchmark_head": _git_head(),
        "scenario": args.scenario,
        "profile": args.profile,
        "blocks": args.blocks,
        "baseline_version": _version_from_path(args.rc10_path),
        "candidate_version": _version_from_path(args.rc11_path),
        "rc10_path": str(args.rc10_path.resolve()),
        "rc11_path": str(args.rc11_path.resolve()),
        "environment": environment_metadata(),
        "host_profile": host_profile,
        "broker": meta,
        "points": point_results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "points": len(point_results)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
