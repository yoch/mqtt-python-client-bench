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
from mqtt_client_bench.harness import calibrate, compare_clients, run_matrix, run_scenario, run_suite
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
        publish_path=getattr(args, "publish_path", "native"),
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


def cmd_matrix(args: argparse.Namespace) -> int:
    clients = [c.strip() for c in args.clients.split(",") if c.strip()]
    if len(clients) < 2:
        print("error: --clients needs at least two names, e.g. paho,gmqtt", file=sys.stderr)
        return 2
    unknown = [c for c in clients if c not in CLIENT_NAMES]
    if unknown:
        print(f"error: unknown client(s): {', '.join(unknown)}", file=sys.stderr)
        return 2
    if args.scenario and args.scenario not in SCENARIO_BY_NAME:
        print(f"error: unknown scenario {args.scenario}", file=sys.stderr)
        return 2

    if args.scenario:
        scenarios = [args.scenario]
    else:
        scenarios = [s.name for s in list_scenarios(args.suite) if "planned" not in s.tags]

    load_profiles = {}
    if args.load_profile_dir:
        root = Path(args.load_profile_dir)
        for client in clients:
            candidate = root / f"{client}-load.json"
            if candidate.exists():
                load_profiles[client] = str(candidate)

    for name in scenarios:
        print(f"==> {name} ({', '.join(clients)} interleaved)", flush=True)
        result = run_matrix(
            name,
            clients,
            profile=args.profile,
            runs=args.runs,
            broker=args.broker,
            network=args.network,
            output_dir=args.output_dir,
            load_profiles=load_profiles,
            seed=args.seed,
            variant_index=args.variant_index,
        )
        for client, doc in result["documents"].items():
            medians = [
                block["summary"].get("median")
                for block in doc["results"]
                if block["summary"].get("median") is not None
            ]
            valid = sum(
                1
                for block in doc["results"]
                for run in block["runs"]
                if run.get("status") == "valid"
            )
            total = sum(len(block["runs"]) for block in doc["results"])
            # smoke runs are non_comparable, so they never produce a median.
            best = f"{max(medians):,.0f}" if medians else "n/a"
            print(
                f"    {client:<16} points={len(doc['results'])} "
                f"valid_runs={valid}/{total} best_median={best}"
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


def cmd_calibrate_host(args: argparse.Namespace) -> int:
    from mqtt_client_bench.hostcal import HostNotIdle, calibrate_host, profile_path_name

    try:
        profile = calibrate_host(
            profile=args.profile,
            role=args.role,
            skip_ceilings=args.skip_ceilings,
            allow_busy=args.allow_busy,
        )
    except HostNotIdle as exc:
        # Not a traceback: this is the expected answer on a working machine,
        # and the operator needs the sentence, not the stack.
        print(f"host calibration refused: {exc}")
        return 2

    output = args.output or str(Path("hosts") / profile_path_name(profile))
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    write_json(output, profile)
    print(
        json.dumps(
            {
                "output": output,
                "host_fingerprint": profile["host_fingerprint"],
                "role": profile["role"],
                "idle_verified": profile["idle"]["verified"],
                "host": profile["host"],
                "ceilings": profile["ceilings"],
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


def cmd_results(args: argparse.Namespace) -> int:
    if args.action != "archive":
        print(f"error: unknown results action {args.action}", file=sys.stderr)
        return 2
    from mqtt_client_bench.archive import archive_results

    summary = archive_results(args.input, args.archive, dry_run=args.dry_run)
    print(
        json.dumps(
            {
                "files": summary["files"],
                "bytes_before": summary["bytes_before"],
                "bytes_after": summary["bytes_after"],
                "saved_bytes": summary["saved_bytes"],
                "archive": str(args.archive),
                "dry_run": args.dry_run,
            },
            indent=2,
        )
    )
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
    run_p.add_argument(
        "--publish-path",
        choices=("native", "sync"),
        default="native",
        help="Diagnostic A/B: force a native-capable client through the sync facade",
    )
    run_p.set_defaults(func=cmd_run)

    matrix_p = sub.add_parser(
        "matrix",
        help="Run several clients interleaved within each point (recommended for published rankings)",
    )
    matrix_p.add_argument("--clients", required=True, help="Comma-separated clients, e.g. paho,gmqtt,aiomqtt")
    matrix_p.add_argument("--scenario", help="Single scenario; omit to run a whole suite")
    matrix_p.add_argument("--suite", choices=["core", "full", "experimental"], default="core")
    matrix_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    matrix_p.add_argument("--runs", type=int)
    matrix_p.add_argument("--broker", help="External broker host:port")
    matrix_p.add_argument("--network", choices=sorted(PROFILES.keys()))
    matrix_p.add_argument(
        "--load-profile-dir",
        help="Directory of <client>-load.json calibrations (needed by load_fraction scenarios)",
    )
    matrix_p.add_argument("--output-dir", default="results", help="Where to write <client>-<scenario>.json")
    matrix_p.add_argument("--seed", type=int, default=42)
    matrix_p.add_argument("--variant-index", type=int, default=None, help="Run a single variant index (default: all)")
    matrix_p.set_defaults(func=cmd_matrix)

    cal_p = sub.add_parser("calibrate", help="Create open-loop load profile from baseline capacity")
    cal_p.add_argument("--client", choices=list(CLIENT_NAMES), default="paho")
    cal_p.add_argument("--client-path")
    cal_p.add_argument("--output", required=True)
    cal_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    cal_p.set_defaults(func=cmd_calibrate)

    host_p = sub.add_parser(
        "calibrate-host",
        help="Measure this machine's ceilings and harness cost into a host profile",
    )
    host_p.add_argument(
        "--output",
        help="Default: hosts/<hostname>-<fingerprint>.json",
    )
    host_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    host_p.add_argument(
        "--role",
        choices=["reference", "runner"],
        default="runner",
        help="Only the reference host is published; runners validate locally.",
    )
    host_p.add_argument(
        "--skip-ceilings",
        action="store_true",
        help="Harness cost only; no broker needed.",
    )
    host_p.add_argument(
        "--allow-busy",
        action="store_true",
        help=(
            "Measure anyway on a machine that is not quiet. The profile is "
            "marked idle_verified=false and can never be the reference."
        ),
    )
    host_p.set_defaults(func=cmd_calibrate_host)

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

    results_p = sub.add_parser(
        "results",
        help="Move raw per-message samples out of committed results into a gzipped archive",
    )
    results_p.add_argument("action", choices=["archive"])
    results_p.add_argument("--input", default="results", help="Directory of JSON results")
    results_p.add_argument("--archive", default="archive", help="Destination for *.json.gz")
    results_p.add_argument("--dry-run", action="store_true", help="Report sizes, change nothing")
    results_p.set_defaults(func=cmd_results)

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
