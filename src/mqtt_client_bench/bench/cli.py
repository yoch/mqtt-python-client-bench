"""``python -m mqtt_client_bench.run <command>``."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from mqtt_client_bench import report
from mqtt_client_bench.bench import campaign, catalog, envs, harness_cost
from mqtt_client_bench.bench.runner import run_once
from mqtt_client_bench.bench.session import open_session


def _split(value: Optional[str]) -> List[str]:
    return [v.strip() for v in (value or "").split(",") if v.strip()]


def cmd_envs(args) -> int:
    clients = _split(args.clients) or list(envs.CLIENT_EXTRAS)
    if args.sync:
        envs.sync_envs(clients, frozen=not args.unfrozen)
    for client in clients:
        ready = envs.env_ready(client)
        version = envs.installed_version(client) if ready else None
        print(f"{client:10s} {'ready' if ready else 'missing':8s} {version or ''}")
    return 0


def cmd_list(args) -> int:
    points = catalog.resolve(_split(args.points), _split(args.suites) or None)
    for p in points:
        rate = f"{p.rate}/s" if p.rate else ("max" if p.kind != "idle" else "-")
        print(f"{p.name:22s} {p.suite:9s} {p.kind:4s} qos{p.qos} {p.payload:>6d}B {rate:>9s} {p.protocol:9s} {p.question}")
    profile = catalog.PROFILES[args.profile]
    clients = _split(args.clients) or list(envs.CLIENT_EXTRAS)
    order = campaign.plan(points, clients, profile.runs)
    minutes = campaign.estimate_s(order, profile) / 60
    print(f"\n{len(order)} runs ({len(points)} points x {len(clients)} clients x {profile.runs}, unsupported pairs skipped), {profile.name}: ~{minutes:.0f} min")
    return 0


def cmd_campaign(args) -> int:
    if args.resume:
        given = [f"--{k.replace('_', '-')}" for k in ("profile", "points", "suites", "clients", "runs") if getattr(args, k) is not None]
        if given:
            print(f"--resume reads its settings from the manifest; drop {', '.join(given)}", file=sys.stderr)
            return 2
        root = Path(args.resume)
        try:
            profile, points, clients, runs = campaign.resume_settings(root)
        except (OSError, ValueError) as exc:
            print(exc, file=sys.stderr)
            return 2
    else:
        profile = catalog.PROFILES[args.profile or "standard"]
        points = catalog.resolve(_split(args.points), _split(args.suites) or ["core", "v5"])
        clients = _split(args.clients) or list(envs.CLIENT_EXTRAS)
        runs = args.runs or profile.runs
        name = campaign.campaign_id() + ("" if profile.comparable else f"-{profile.name}")
        root = Path(args.output_dir) / name
    missing = [c for c in clients if not envs.env_ready(c)]
    if missing:
        print(f"no environment for {', '.join(missing)}; run: envs --sync --clients {','.join(missing)}", file=sys.stderr)
        return 2
    order = campaign.plan(points, clients, runs)
    print(f"{root}: {len(order)} runs, ~{campaign.estimate_s(order, profile) / 60:.0f} min")
    store = campaign.run_campaign(points, clients, profile, root, runs=runs, describe=describe)
    statuses: dict = {}
    for doc in store.docs.values():
        for r in doc["runs"]:
            statuses[r["status"]] = statuses.get(r["status"], 0) + 1
    print(f"done: {statuses}")
    return 0


def describe(record: dict) -> str:
    m = record.get("metrics") or {}
    parts = [f"{record['client']:9s} {record['point']['name']:20s} {record['status']:13s}"]
    if record.get("reasons"):
        parts.append(", ".join(record["reasons"]))
    if record.get("flags"):
        parts.append("[" + ",".join(record["flags"]) + "]")
    if "msgs_per_s" in m:
        parts.append(f"{m['msgs_per_s']:>9.0f} msg/s")
    if m.get("cpu_us_per_msg") is not None:
        parts.append(f"cpu {m['cpu_us_per_msg']:6.1f} us/msg")
    if m.get("cpu_cores") is not None:
        parts.append(f"{m['cpu_cores'] * 100:5.1f}% core")
    if m.get("rss_peak_kb"):
        parts.append(f"rss {m['rss_peak_kb'] / 1024:5.1f} MiB")
    s = m.get("latency_summary") or {}
    if s.get("count"):
        parts.append(f"p50 {s['p50_us']:.0f} p99 {s['p99_us']:.0f} us")
    lag = m.get("lag_summary") or {}
    if lag.get("count"):
        parts.append(f"lag p99 {lag['p99_us']:.0f} us")
    if "connect_ms" in m:
        parts.append(f"connect {m['connect_ms']:.1f} ms")
    failed = [c for c in record.get("checks", []) if not c["passed"]]
    if failed:
        parts.append("| " + "; ".join(f"{c['name']}: {c['detail']}" for c in failed))
    if record.get("error"):
        parts.append(f"| {record['error']}")
    return "  ".join(parts)


def cmd_run(args) -> int:
    profile = catalog.PROFILES[args.profile]
    points = catalog.resolve(_split(args.points), _split(args.suites) or None)
    clients = _split(args.clients)
    missing = [c for c in clients if not envs.env_ready(c)]
    if missing:
        print(f"no environment for {', '.join(missing)}; run: envs --sync --clients {','.join(missing)}", file=sys.stderr)
        return 2
    records = []
    with open_session(profile, measure_ceiling=not args.no_ceiling) as (ctx, info):
        if info.get("ceiling"):
            c = info["ceiling"]
            print(f"broker ceiling (C->C): {c.get('msgs_per_s', 0):.0f} msg/s; receive offer {ctx.sub_offer}/s")
        for point in points:
            for client in clients:
                for i in range(args.runs or profile.runs):
                    record = run_once(client, point, profile, ctx, run_index=i)
                    records.append(record)
                    print(describe(record), flush=True)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"session": info, "runs": records}, indent=1), encoding="utf-8")
    return 0 if all(r["status"] != "invalid" for r in records) else 1


def cmd_harness_cost(args) -> int:
    costs = harness_cost.measure()
    for shape, ns in costs.items():
        print(f"{shape:16s} {ns:7.1f} ns/msg")
    print(f"budget {harness_cost.BUDGET_NS} ns/msg; worker RSS floor {harness_cost.baseline_rss_kb()} KiB")
    return 0 if all(ns <= harness_cost.BUDGET_NS for ns in costs.values()) else 1


def cmd_report(args) -> int:
    try:
        name = report.build(Path(args.input), Path(args.output), campaign=args.campaign)
    except (FileNotFoundError, ValueError) as exc:
        print(exc, file=sys.stderr)
        return 2
    print(f"{args.output}: {name or 'no comparable campaign'}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mqtt-client-bench", description="MQTT client benchmark, v2 core.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("envs", help="show or build the per-client uv environments")
    p.add_argument("--clients")
    p.add_argument("--sync", action="store_true")
    p.add_argument("--unfrozen", action="store_true", help="allow uv to update the lock")
    p.set_defaults(func=cmd_envs)

    p = sub.add_parser("list", help="list points and estimate a campaign's duration")
    p.add_argument("--points")
    p.add_argument("--suites", help="comma list of: " + ", ".join(catalog.SUITES))
    p.add_argument("--clients")
    p.add_argument("--profile", default="standard", choices=sorted(catalog.PROFILES))
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("run", help="run points for clients, one after the other")
    p.add_argument("--clients", required=True)
    p.add_argument("--points")
    p.add_argument("--suites")
    p.add_argument("--profile", default="smoke", choices=sorted(catalog.PROFILES))
    p.add_argument("--runs", type=int)
    p.add_argument("--no-ceiling", action="store_true", help="skip the C->C ceiling measurement")
    p.add_argument("--output")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("campaign", help="every point x client x run, interleaved and resumable")
    p.add_argument("--clients", help="default: every client")
    p.add_argument("--points")
    p.add_argument("--suites", help="default: core,v5")
    p.add_argument("--profile", choices=sorted(catalog.PROFILES), help="default: standard")
    p.add_argument("--runs", type=int)
    p.add_argument("--output-dir", default=str(campaign.RESULTS_DIR))
    p.add_argument("--resume", help="campaign directory to continue, with the settings it was started with")
    p.set_defaults(func=cmd_campaign)

    p = sub.add_parser("harness-cost", help="harness ns/message against a null client")
    p.set_defaults(func=cmd_harness_cost)

    p = sub.add_parser("report", help="build the static site from a campaign")
    p.add_argument("--input", default=str(campaign.RESULTS_DIR))
    p.add_argument("--output", default="site")
    p.add_argument("--campaign", help="build this campaign, even a development one (default: newest comparable)")
    p.set_defaults(func=cmd_report)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
