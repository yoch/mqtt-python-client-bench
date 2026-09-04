#!/usr/bin/env python3
"""Compare mqttium campaigns across labelled result directories."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mqtt_client_bench.report.model import load_results  # noqa: E402

HEADLINE_SCENARIOS = (
    "pub_qos_sweep_telemetry",
    "pub_qos1_inflight",
    "pub_payload_sweep_qos0",
    "rtt_capacity_qos1",
    "application_rtt_qos1",
    "puback_latency_qos1",
    "remaining_length_boundaries",
)


def load_labelled(path: Path, label: str) -> Dict[str, float]:
    docs = {
        d.scenario: d.median_msgs_per_s
        for d in load_results(path, reference=None)
        if d.client == "mqttium" and d.median_msgs_per_s is not None
    }
    return docs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "corpora",
        nargs="+",
        metavar="LABEL=PATH",
        help="e.g. rc13=results/host/rc13 rc12=results/host/post422-baseline",
    )
    args = parser.parse_args()

    corpora: Dict[str, Dict[str, float]] = {}
    for item in args.corpora:
        label, _, path_str = item.partition("=")
        corpora[label] = load_labelled(Path(path_str), label)

    labels: List[str] = [item.partition("=")[0] for item in args.corpora]
    scenarios = sorted({s for docs in corpora.values() for s in docs})
    priority = [s for s in HEADLINE_SCENARIOS if s in scenarios]
    rest = [s for s in scenarios if s not in priority]
    ordered = priority + rest

    col_w = max(10, max(len(l) for l in labels) + 2)
    header = f"{'scenario':<35}" + "".join(f"{l:>{col_w}}" for l in labels)
    if len(labels) >= 2:
        header += f"{'Δ last':>10}"
    print(header)
    print("-" * len(header))

    ref = labels[0]
    for scenario in ordered:
        vals = [corpora[l].get(scenario) for l in labels]
        if all(v is None for v in vals):
            continue
        row = f"{scenario:<35}"
        for v in vals:
            row += f"{v:>{col_w},.0f}" if v is not None else f"{'—':>{col_w}}"
        if len(labels) >= 2 and vals[0] and vals[-1]:
            row += f"{vals[-1]/vals[0]:>10.3f}"
        print(row)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
