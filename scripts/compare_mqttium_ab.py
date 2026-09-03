#!/usr/bin/env python3
"""Compare two mqttium campaign directories (baseline vs candidate)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mqtt_client_bench.report.model import load_results  # noqa: E402

PRIORITY = (
    "pub_qos_sweep_telemetry",
    "pub_qos1_inflight",
    "rtt_capacity_qos1",
    "application_rtt_qos1",
)
GUARDRAIL = (
    "pub_payload_sweep_qos0",
    "remaining_length_boundaries",
)


def _docs_by_scenario(path: Path) -> dict:
    return {
        d.scenario: d
        for d in load_results(path, reference=None)
        if d.client == "mqttium"
    }


def _point_map(doc):
    out = {}
    for p in doc.points:
        if p.median_msgs_per_s is not None:
            out[p.label] = p.median_msgs_per_s
    return out


def _qos_sweep_qos(doc, qos: int, proto: str):
    for p in doc.points:
        if f"qos={qos}" in p.label and proto in p.label and p.median_msgs_per_s:
            return p.median_msgs_per_s
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="Baseline results directory")
    parser.add_argument("candidate", type=Path, help="Candidate results directory")
    args = parser.parse_args()

    base = _docs_by_scenario(args.baseline)
    cand = _docs_by_scenario(args.candidate)

    def row(scenario: str, base_v: float | None, cand_v: float | None) -> None:
        if base_v is None or cand_v is None or base_v == 0:
            print(f"  {scenario:<35} base={base_v} cand={cand_v}")
            return
        print(f"  {scenario:<35} {base_v:>12,.0f} {cand_v:>12,.0f} {cand_v/base_v:>8.3f}")

    print("=== Priority scenarios (headline median msg/s) ===")
    for s in PRIORITY:
        b = base.get(s)
        c = cand.get(s)
        row(s, b.median_msgs_per_s if b else None, c.median_msgs_per_s if c else None)

    print("\n=== pub_qos_sweep by QoS ===")
    bdoc = base.get("pub_qos_sweep_telemetry")
    cdoc = cand.get("pub_qos_sweep_telemetry")
    if bdoc and cdoc:
        for qos, proto in ((1, "MQTTv311"), (1, "MQTTv5"), (0, "MQTTv311"), (0, "MQTTv5")):
            label = f"QoS{qos} {proto}"
            bv = _qos_sweep_qos(bdoc, qos, proto)
            cv = _qos_sweep_qos(cdoc, qos, proto)
            row(label, bv, cv)

    print("\n=== pub_qos1_inflight ===")
    bdoc = base.get("pub_qos1_inflight")
    cdoc = cand.get("pub_qos1_inflight")
    if bdoc and cdoc:
        for inflight in ("1", "20", "100"):
            label = f"inflight={inflight}"
            bv = next((p.median_msgs_per_s for p in bdoc.points if f"inflight={inflight}" in p.label), None)
            cv = next((p.median_msgs_per_s for p in cdoc.points if f"inflight={inflight}" in p.label), None)
            row(label, bv, cv)

    print("\n=== QoS0 guardrail (headline) ===")
    for s in GUARDRAIL:
        b = base.get(s)
        c = cand.get(s)
        row(s, b.median_msgs_per_s if b else None, c.median_msgs_per_s if c else None)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
