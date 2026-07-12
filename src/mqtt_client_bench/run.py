#!/usr/bin/env python3
"""Comparative MQTT Python client end-to-end benchmark CLI."""

from __future__ import annotations

import argparse
import json
import sys

from pathlib import Path

from mqtt_client_bench.adapters.registry import CLIENT_NAMES, list_clients
from mqtt_client_bench.broker import broker_down, broker_up, ensure_certs
from mqtt_client_bench.control import write_json
from mqtt_client_bench.harness import calibrate, compare_clients, run_scenario, run_suite
from mqtt_client_bench.network import PROFILES
from mqtt_client_bench.report import build_site
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, default_runs, estimate_suite, list_scenarios


def cmd_broker(args: argparse.Namespace) -> int:
    if args.action == "up":
        ensure_certs()
        meta = broker_up(wait=True)
        print(json.dumps(meta, indent=2))
        return 0
    if args.action == "down":
        broker_down()
        print("broker down")
        return 0
    raise SystemExit(f"unknown broker action: {args.action}")


def cmd_list(args: argparse.Namespace) -> int:
    scenarios = list_scenarios(args.suite)
    for scenario in scenarios:
        tags = ",".join(scenario.tags)
        print(f"{scenario.name:<28} suite={scenario.suite:<4} tags={tags:<28} {scenario.description}")
    if args.suite:
        est = estimate_suite(args.suite, args.profile, default_runs(args.profile))
        print(
            f"\nEstimate ({args.profile}): {est['points']} points, "
            f"{est['runs_per_point']} runs/point, ~{est['estimated_minutes']} min"
        )
    return 0


def cmd_clients(args: argparse.Namespace) -> int:
    for row in list_clients():
        pending = ",".join(row["unimplemented"]) if row["unimplemented"] else "-"
        print(
            f"{row['name']:<10} stability={row['stability']:<12} "
            f"async_bridged={row['async_bridged']!s:<5} "
            f"mqtt_v5={row['mqtt_v5']!s:<5} qos2={row['qos2']!s:<5} "
            f"native_cb={row['native_message_callback_add']!s:<5} "
            f"lang={row['implementation_language']:<8} pending={pending}"
        )
        if args.verbose and row.get("notes"):
            print(f"  {row['notes']}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    client = args.client
    client_path = args.client_path
    if args.suite:
        result = run_suite(
            args.suite,
            client=client,
            client_path=client_path,
            profile=args.profile,
            runs=args.runs,
            broker=args.broker,
            network=args.network,
            output=None,
            load_profile_path=args.load_profile,
            seed=args.seed,
        )
        if args.output:
            write_json(args.output, result)
        else:
            print(json.dumps({"suite": result["suite"], "estimate": result["estimate"]}, indent=2))
        return 0

    if not args.scenario:
        print("error: provide --scenario or --suite", file=sys.stderr)
        return 2
    if args.scenario not in SCENARIO_BY_NAME:
        print(f"error: unknown scenario {args.scenario}", file=sys.stderr)
        return 2

    result = run_scenario(
        args.scenario,
        client=client,
        client_path=client_path,
        profile=args.profile,
        runs=args.runs,
        broker=args.broker,
        network=args.network,
        output=args.output,
        load_profile_path=args.load_profile,
        seed=args.seed,
    )
    if not args.output:
        # Compact stdout summary.
        for block in result.get("results", []):
            point = block["point"]
            summary = block["summary"]
            print(
                f"{result['scenario']} client={client} payload={point.get('payload')} "
                f"qos={point.get('qos_publish')} median_msgs_per_s={summary.get('median')} status_runs="
                f"{sum(1 for r in block['runs'] if r.get('status') == 'valid')}/{len(block['runs'])}"
            )
    return 0


def cmd_calibrate(args: argparse.Namespace) -> int:
    payload = calibrate(
        args.output,
        client=args.client,
        client_path=args.client_path,
        profile=args.profile,
    )
    print(
        json.dumps(
            {
                "capacity_msgs_per_s": payload.get("capacity_msgs_per_s"),
                "rtt_capacity_msgs_per_s": payload.get("rtt_capacity_msgs_per_s"),
                "fractions": payload.get("fractions"),
                "rtt_fractions": payload.get("rtt_fractions"),
            },
            indent=2,
        )
    )
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    clients = [c.strip() for c in args.clients.split(",") if c.strip()]
    if len(clients) < 2:
        print("error: --clients needs at least two names, e.g. paho,gmqtt", file=sys.stderr)
        return 2
    payload = compare_clients(
        clients,
        args.scenario,
        blocks=args.blocks,
        profile=args.profile,
        output=args.output,
        load_profile_path=args.load_profile,
        variant_index=args.variant_index,
    )
    print(json.dumps({"verdict": payload.get("verdict"), "order": payload.get("order"), "points": len(payload.get("points") or [])}, indent=2))
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    if args.action != "build":
        print(f"error: unknown report action {args.action}", file=sys.stderr)
        return 2
    summary = build_site(Path(args.input), Path(args.output))
    print(json.dumps(summary, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command")

    broker_p = sub.add_parser("broker", help="Manage local Mosquitto via docker compose")
    broker_p.add_argument("action", choices=["up", "down"])
    broker_p.set_defaults(func=cmd_broker)

    list_p = sub.add_parser("list", help="List scenarios")
    list_p.add_argument("--suite", choices=["core", "full", "experimental"])
    list_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    list_p.set_defaults(func=cmd_list)

    clients_p = sub.add_parser("clients", help="List MQTT client adapters and capability matrix")
    clients_p.add_argument("-v", "--verbose", action="store_true")
    clients_p.set_defaults(func=cmd_clients)

    run_p = sub.add_parser("run", help="Run a scenario or suite")
    run_p.add_argument("--scenario")
    run_p.add_argument("--suite", choices=["core", "full", "experimental"])
    run_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    run_p.add_argument("--runs", type=int)
    run_p.add_argument("--client", choices=list(CLIENT_NAMES), default="paho", help="SUT MQTT client library")
    run_p.add_argument("--client-path", help="Optional checkout root for the selected client (A/B worktrees)")
    run_p.add_argument("--broker", help="External broker host:port")
    run_p.add_argument("--network", choices=sorted(PROFILES.keys()))
    run_p.add_argument("--load-profile", help="JSON from calibrate")
    run_p.add_argument("--output")
    run_p.add_argument("--seed", type=int, default=42)
    run_p.set_defaults(func=cmd_run)

    cal_p = sub.add_parser("calibrate", help="Create open-loop load profile from baseline capacity")
    cal_p.add_argument("--client", choices=list(CLIENT_NAMES), default="paho")
    cal_p.add_argument("--client-path")
    cal_p.add_argument("--output", required=True)
    cal_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    cal_p.set_defaults(func=cmd_calibrate)

    cmp_p = sub.add_parser("compare", help="ABBA compare two client adapters")
    cmp_p.add_argument("--clients", required=True, help="Comma-separated pair, e.g. paho,gmqtt")
    cmp_p.add_argument("--scenario", required=True)
    cmp_p.add_argument("--blocks", type=int, default=4)
    cmp_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    cmp_p.add_argument("--variant-index", type=int, default=None, help="Compare a single variant index (default: all)")
    cmp_p.add_argument("--load-profile")
    cmp_p.add_argument("--output")
    cmp_p.set_defaults(func=cmd_compare)

    report_p = sub.add_parser("report", help="Build static HTML reports from results/*.json")
    report_p.add_argument("action", choices=["build"])
    report_p.add_argument("--input", default="results", help="Directory of committed JSON results")
    report_p.add_argument("--output", default="site", help="Output directory for the static site")
    report_p.set_defaults(func=cmd_report)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
