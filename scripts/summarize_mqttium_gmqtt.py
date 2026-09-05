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
    "puback_latency_qos1",
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


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    payload = {
        "matrix": matrix_table(root),
        "abba": abba_table(root),
        "notes": [
            "ABBA A=gmqtt B=mqttium: median_ratio > 1 means mqttium faster.",
            "gmqtt refuses QoS2 and max_inflight; those points stay inconclusive.",
        ],
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
