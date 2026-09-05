#!/usr/bin/env python3
"""Summarize interleaved mqttium vs gmqtt matrix + ABBA compare outputs."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mqtt_client_bench.report.model import load_results  # noqa: E402

PRIORITY = (
    "pub_qos_sweep_telemetry",
    "pub_qos1_inflight",
    "pub_payload_sweep_qos0",
    "rtt_capacity_qos1",
    "application_rtt_qos1",
    "application_rtt_fixed_rate",
    "puback_latency_qos1",
    "puback_latency_fixed_rate",
)


def qos_rate(doc, qos: int, proto: str):
    for p in doc.points:
        if f"qos={qos}" in p.label and proto in p.label and p.median_msgs_per_s:
            return p.median_msgs_per_s
    return None


def matrix_table(root: Path) -> dict:
    by_client: dict = {}
    for d in load_results(root, reference=None):
        if d.client not in ("mqttium", "gmqtt") or not d.scenario:
            continue
        by_client.setdefault(d.client, {})[d.scenario] = d
    rows = []
    scenarios = sorted({s for docs in by_client.values() for s in docs if s})
    ordered = [s for s in PRIORITY if s in scenarios] + [s for s in scenarios if s not in PRIORITY]
    for scenario in ordered:
        m = by_client.get("mqttium", {}).get(scenario)
        g = by_client.get("gmqtt", {}).get(scenario)
        mv = m.median_msgs_per_s if m else None
        gv = g.median_msgs_per_s if g else None
        ratio = (mv / gv) if mv and gv else None
        rows.append(
            {
                "scenario": scenario,
                "mqttium": mv,
                "gmqtt": gv,
                "mqttium_over_gmqtt": ratio,
            }
        )
    detail = {}
    mdoc = by_client.get("mqttium", {}).get("pub_qos_sweep_telemetry")
    gdoc = by_client.get("gmqtt", {}).get("pub_qos_sweep_telemetry")
    if mdoc and gdoc:
        for qos, proto in ((0, "MQTTv311"), (0, "MQTTv5"), (1, "MQTTv311"), (1, "MQTTv5")):
            mv, gv = qos_rate(mdoc, qos, proto), qos_rate(gdoc, qos, proto)
            detail[f"QoS{qos} {proto}"] = {
                "mqttium": mv,
                "gmqtt": gv,
                "mqttium_over_gmqtt": (mv / gv) if mv and gv else None,
            }
    return {"headlines": rows, "qos_sweep_points": detail}


def abba_table(root: Path) -> list:
    rows = []
    for path in sorted(root.glob("compare-gmqtt-mqttium-*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        for point in data.get("points") or []:
            verdict = point.get("verdict") or {}
            pt = point.get("point") or {}
            label_bits = [str(pt.get("protocol") or "")]
            if pt.get("qos_publish") is not None:
                label_bits.append(f"QoS{pt['qos_publish']}")
            if pt.get("payload") not in (None, "telemetry256"):
                label_bits.append(str(pt["payload"]))
            if pt.get("load_fraction") is not None:
                label_bits.append(f"load={pt['load_fraction']}")
            if pt.get("target_rate") is not None:
                label_bits.append(f"rate={pt['target_rate']}")
            rows.append(
                {
                    "scenario": data.get("scenario"),
                    "label": " ".join(x for x in label_bits if x),
                    "verdict": verdict.get("verdict"),
                    "median_ratio": verdict.get("median_ratio"),
                    "ci_low": verdict.get("ci_low"),
                    "ci_high": verdict.get("ci_high"),
                    "absolute_effect_pct": verdict.get("absolute_effect_pct"),
                    "n_blocks": verdict.get("n_blocks"),
                    "excludes_zero_effect": verdict.get("excludes_zero_effect"),
                }
            )
    return rows


FIXED_RATE_SCENARIOS = (
    "application_rtt_fixed_rate",
    "puback_latency_fixed_rate",
)


def _median(values):
    ordered = [v for v in values if v is not None]
    if not ordered:
        return None
    ordered.sort()
    return ordered[len(ordered) // 2]


def _worker_completion(run: dict) -> dict:
    offered = 0
    completed = 0
    lag_p50 = []
    for worker in run.get("workers") or []:
        offered += int(worker.get("offered") or 0)
        completed += int(
            worker.get("completed_in_window")
            or worker.get("completed_success")
            or 0
        )
        lags = worker.get("scheduler_lags_ns") or []
        if lags:
            ms = sorted(float(x) / 1_000_000.0 for x in lags)
            lag_p50.append(ms[len(ms) // 2])
    return {
        "offered": offered or None,
        "completed": completed or None,
        "completion_ratio": (completed / offered) if offered else None,
        "scheduler_lag_p50_ms": _median(lag_p50),
    }


def _run_latency(run: dict) -> dict:
    p50, p95, p99 = [], [], []
    for worker in run.get("workers") or []:
        summary = worker.get("latency_summary") or {}
        if summary.get("p50_ms") is not None:
            p50.append(float(summary["p50_ms"]))
        if summary.get("p95_ms") is not None:
            p95.append(float(summary["p95_ms"]))
        if summary.get("p99_ms") is not None and summary.get("p99_published"):
            p99.append(float(summary["p99_ms"]))
    return {
        "p50_ms": _median(p50),
        "p95_ms": _median(p95),
        "p99_ms": _median(p99),
    }


def fixed_rate_matrix(root: Path) -> dict:
    by_client: dict = {}
    for d in load_results(root, reference=None):
        if d.client not in ("mqttium", "gmqtt"):
            continue
        if d.scenario not in FIXED_RATE_SCENARIOS:
            continue
        by_client.setdefault(d.scenario, {})[d.client] = d
    tables = {}
    for scenario, clients in by_client.items():
        rows = []
        labels = []
        for client_doc in clients.values():
            for point in client_doc.points:
                if point.label not in labels:
                    labels.append(point.label)
        for label in labels:
            row = {"label": label}
            for client in ("mqttium", "gmqtt"):
                doc = clients.get(client)
                point = next((p for p in (doc.points if doc else []) if p.label == label), None)
                if point is None:
                    row[client] = None
                    continue
                row[client] = {
                    "status": point.status,
                    "valid_runs": point.valid_runs,
                    "observed_msgs_per_s": point.median_msgs_per_s or point.observed_msgs_per_s,
                    "p50_ms": (point.latency or {}).get("p50_ms"),
                    "p95_ms": (point.latency or {}).get("p95_ms"),
                    "p99_ms": (point.latency or {}).get("p99_ms"),
                    "p99_gated": (point.latency or {}).get("p99_gated"),
                    "delivery_offer_ratio": point.delivery_offer_ratio,
                    "cost_us_per_message": point.cost_us_per_message,
                    "bottleneck": point.bottleneck,
                }
            m, g = row.get("mqttium") or {}, row.get("gmqtt") or {}
            for key in ("p50_ms", "p95_ms", "p99_ms", "observed_msgs_per_s"):
                mv, gv = (m or {}).get(key), (g or {}).get(key)
                row[f"mqttium_over_gmqtt_{key}"] = (mv / gv) if mv and gv else None
            rows.append(row)
        tables[scenario] = rows
    return tables


def fixed_rate_abba(root: Path) -> list:
    rows = []
    for path in sorted(root.glob("compare-*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("scenario") not in FIXED_RATE_SCENARIOS and "fixed_rate" not in path.name:
            continue
        for point in data.get("points") or []:
            pt = point.get("point") or {}
            a_lat, b_lat, a_comp, b_comp = [], [], [], []
            for run in point.get("runs") or []:
                lat = _run_latency(run)
                comp = _worker_completion(run)
                bucket_lat = a_lat if run.get("ab_label") == "A" else b_lat
                bucket_comp = a_comp if run.get("ab_label") == "A" else b_comp
                if run.get("status") == "valid":
                    bucket_lat.append(lat)
                    bucket_comp.append(comp)
            def pack(samples, key):
                return _median([s.get(key) for s in samples])

            rows.append(
                {
                    "file": path.name,
                    "scenario": data.get("scenario"),
                    "baseline": data.get("baseline_client"),
                    "candidate": data.get("candidate_client"),
                    "protocol": pt.get("protocol"),
                    "target_rate": pt.get("target_rate"),
                    "verdict": (point.get("verdict") or {}).get("verdict"),
                    "median_ratio": (point.get("verdict") or {}).get("median_ratio"),
                    "absolute_effect_pct": (point.get("verdict") or {}).get("absolute_effect_pct"),
                    "n_blocks": (point.get("verdict") or {}).get("n_blocks"),
                    "baseline_p50_ms": pack(a_lat, "p50_ms"),
                    "candidate_p50_ms": pack(b_lat, "p50_ms"),
                    "baseline_p95_ms": pack(a_lat, "p95_ms"),
                    "candidate_p95_ms": pack(b_lat, "p95_ms"),
                    "baseline_p99_ms": pack(a_lat, "p99_ms"),
                    "candidate_p99_ms": pack(b_lat, "p99_ms"),
                    "baseline_completion_ratio": pack(a_comp, "completion_ratio"),
                    "candidate_completion_ratio": pack(b_comp, "completion_ratio"),
                    "baseline_scheduler_lag_p50_ms": pack(a_comp, "scheduler_lag_p50_ms"),
                    "candidate_scheduler_lag_p50_ms": pack(b_comp, "scheduler_lag_p50_ms"),
                }
            )
    return rows


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    payload = {
        "matrix": matrix_table(root),
        "abba": abba_table(root),
        "fixed_rate_matrix": fixed_rate_matrix(root),
        "fixed_rate_abba": fixed_rate_abba(root),
        "notes": [
            "ABBA A=gmqtt B=mqttium: median_ratio > 1 means mqttium faster on throughput.",
            "gmqtt refuses QoS2 and max_inflight; those points stay inconclusive.",
            "application_rtt_qos1 / puback_latency_qos1 are fraction-of-own-capacity: intra-client only, not matched-load latency.",
            "application_rtt_fixed_rate / puback_latency_fixed_rate are the equal-offer cross-client latency comparisons.",
        ],
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
