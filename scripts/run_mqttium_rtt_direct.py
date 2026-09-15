#!/usr/bin/env python3
"""Closed-loop rtt_capacity_qos1 ABBA between mqttium source trees.

Calls ``harness.run_point`` on the same expanded matrix point (capacity,
outstanding=inflight=32, standard timings). Does **not** use
``version_compare`` (that freezes ``target_rate`` and would change the
workload).

Refuses to start while the PR #39 campaign still owns workers. Does not
kill anything.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median, pstdev
from typing import List, Optional, Sequence

from mqtt_client_bench.adapters.registry import adapter_identity
from mqtt_client_bench.broker import broker_up
from mqtt_client_bench.control import write_json
from mqtt_client_bench.harness import ABBA_COOLDOWN_S, HARNESS_FINGERPRINT, run_point
from mqtt_client_bench.hostcal import resolve_host_profile
from mqtt_client_bench.metrics import abba_block_design, abba_order, geometric_mean
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario
from mqtt_client_bench.telemetry import allocate_cpuset, pin_current_process

REPO = Path(__file__).resolve().parents[1]
INSTALL_ROOT = Path("/workspace/.mqttium-rtt-direct")
DEFAULT_OUT = Path("results/cursor-bce5e86fefabd33d/mqttium-rtt-direct-c4f477")

REQUESTED_SHAS = {
    "A": "8e29cfa27dcdcb649bbfe38fec24839eef55e05a",
    "B": "c4f477dbc13f5f6740a0b21480403b4e0e7aee1b",
    "C": "c194597bcf5af4951fbec2b560600eef3cb84b3c",
}

ARM_PATHS = {
    "A": INSTALL_ROOT / "inst-A",
    "B1": INSTALL_ROOT / "inst-B1",
    "B2": INSTALL_ROOT / "inst-B2",
    "B": INSTALL_ROOT / "inst-B1",
    "C": INSTALL_ROOT / "inst-C",
}

ARM_SRC = {
    "A": INSTALL_ROOT / "src-A",
    "B": INSTALL_ROOT / "src-B",
    "B1": INSTALL_ROOT / "src-B",
    "B2": INSTALL_ROOT / "src-B",
    "C": INSTALL_ROOT / "src-C",
}

HARNESS_REFERENCE_SHA = "e05b5c47a56e7f4d3a0e2131361e80cb01c61553"


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def git_out(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return proc.stdout.strip()


def campaign_owners() -> List[dict]:
    """Processes belonging to the PR #39 mqttium-pr460-c4f477 campaign."""
    found: List[dict] = []
    proc = Path("/proc")
    if not proc.is_dir():
        return found
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if not raw:
            continue
        text = raw.replace(b"\x00", b" ").decode("utf-8", "replace")
        if "run_mqttium_gmqtt_compare.sh" in text:
            found.append({"pid": int(entry.name), "cmdline": text})
            continue
        if "mqttium-pr460-c4f477" in text and "mqtt_client_bench" in text:
            found.append({"pid": int(entry.name), "cmdline": text})
    return found


def wait_until_idle(*, timeout_s: float = 7200.0, poll_s: float = 20.0) -> None:
    deadline = time.time() + timeout_s
    while True:
        owners = campaign_owners()
        if not owners:
            return
        if time.time() >= deadline:
            raise SystemExit(
                "campaign still active after wait: "
                + json.dumps(owners, indent=2)
            )
        print(f"waiting for PR #39 campaign idle ({len(owners)} procs) {_utc()}", flush=True)
        time.sleep(poll_s)


def tree_sha(src: Path) -> Optional[str]:
    for spec in ("HEAD:src/mqttium", "HEAD:mqttium"):
        try:
            return git_out(src, "rev-parse", spec)
        except subprocess.CalledProcessError:
            continue
    return None


def python_identity(install: Path) -> dict:
    code = r"""
import inspect, json, sys
from pathlib import Path
root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root))
import mqttium
from mqttium.api import AsyncClient
from mqtt_client_bench.adapters.mqttium import async_client_ctor_vocabulary, async_client_param_names
path = Path(mqttium.__file__).resolve()
assert str(path).startswith(str(root)), path
names = sorted(async_client_param_names(AsyncClient))
print(json.dumps({
    "imported_file": str(path),
    "package_dir": str(path.parent),
    "version": getattr(mqttium, "__version__", None),
    "ctor_signature": str(inspect.signature(AsyncClient.__init__)),
    "ctor_vocabulary": async_client_ctor_vocabulary(names),
    "ctor_param_names": names,
    "has_on_publish": hasattr(AsyncClient, "on_publish"),
    "has_settle_publish": hasattr(AsyncClient, "_settle_publish"),
    "publish_nowait": hasattr(AsyncClient, "publish_nowait"),
}))
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(install) + os.pathsep + str(REPO / "src")
    proc = subprocess.run(
        [sys.executable, "-c", code, str(install)],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
        env=env,
    )
    return json.loads(proc.stdout)


def arm_identity(label: str) -> dict:
    install = ARM_PATHS[label].resolve()
    src = ARM_SRC[label]
    requested = REQUESTED_SHAS["B" if label in ("B1", "B2") else label]
    checked = git_out(src, "rev-parse", "HEAD")
    py = python_identity(install)
    adapter = adapter_identity("mqttium", str(install))
    return {
        "label": label,
        "requested_sha": requested,
        "checked_out_sha": checked,
        "sha_match": checked == requested,
        "src_mqttium_tree_sha": tree_sha(src),
        "src_path": str(src.resolve()),
        "install_path": str(install),
        "python_identity": py,
        "ctor_vocabulary": py.get("ctor_vocabulary"),
        "ctor_param_names": py.get("ctor_param_names"),
        "adapter_identity": adapter,
    }


def harness_identity() -> dict:
    return {
        "repo": str(REPO),
        "head_sha": git_out(REPO, "rev-parse", "HEAD"),
        "reference_sha": HARNESS_REFERENCE_SHA,
        "head_matches_reference": git_out(REPO, "rev-parse", "HEAD") == HARNESS_REFERENCE_SHA,
        "harness_fingerprint": HARNESS_FINGERPRINT,
        "python": sys.version,
        "executable": sys.executable,
    }


def rtt_point(protocol: str) -> dict:
    points = expand_scenario(SCENARIO_BY_NAME["rtt_capacity_qos1"], "standard")
    matched = [p for p in points if p.get("protocol") == protocol]
    if len(matched) != 1:
        raise SystemExit(f"expected one rtt_capacity_qos1 point for {protocol}, got {len(matched)}")
    point = dict(matched[0])
    if (
        point.get("qos_publish") != 1
        or point.get("qos_subscribe") != 1
        or point.get("payload") != "telemetry256"
        or int(point.get("outstanding") or 0) != 32
        or int(point.get("inflight") or 0) != 32
        or point.get("cadence") != "capacity"
        or str(point.get("network") or "localhost") != "localhost"
    ):
        raise SystemExit(f"point does not match the matrix contract: {point}")
    return point


def worker_runtime(worker: dict) -> dict:
    runtime = worker.get("runtime") or {}
    delta = runtime.get("measure_delta") or {}
    end = runtime.get("process_end") or runtime.get("measure_end") or {}
    return {
        "role": worker.get("role"),
        "ok": worker.get("ok"),
        "process_exit": worker.get("process_exit"),
        "msgs_per_s": worker.get("msgs_per_s"),
        "cpu_ns_in_window": worker.get("cpu_ns_in_window"),
        "ru_nvcsw": delta.get("ru_nvcsw", end.get("ru_nvcsw")),
        "ru_nivcsw": delta.get("ru_nivcsw", end.get("ru_nivcsw")),
        "ru_utime_s": delta.get("ru_utime_s", end.get("ru_utime_s")),
        "ru_stime_s": delta.get("ru_stime_s", end.get("ru_stime_s")),
        "ru_maxrss_kb": end.get("ru_maxrss_kb"),
        "library": runtime.get("library"),
    }


def last_telemetry_snapshot(result: dict) -> dict:
    samples = result.get("telemetry") or []
    if not samples:
        return {}
    last = samples[-1]
    processes = last.get("processes") or []
    return {
        "loadavg": last.get("loadavg"),
        "processes": [
            {
                "pid": p.get("pid"),
                "rss_kb": p.get("rss_kb"),
                "cpu_ticks": p.get("cpu_ticks"),
                "voluntary_ctxt_switches": p.get("voluntary_ctxt_switches"),
                "nonvoluntary_ctxt_switches": p.get("nonvoluntary_ctxt_switches"),
            }
            for p in processes
            if isinstance(p, dict)
        ],
    }


def slim_run(result: dict) -> dict:
    """Keep ranking + scheduler fields; drop per-message vectors."""
    workers = []
    for worker in result.get("workers") or []:
        workers.append(worker_runtime(worker))
    return {
        "run_id": result.get("run_id"),
        "started_at": result.get("started_at"),
        "finished_at": result.get("finished_at"),
        "status": result.get("status"),
        "reasons": result.get("reasons") or [],
        "bottleneck": result.get("bottleneck"),
        "primary_msgs_per_s": result.get("primary_msgs_per_s"),
        "broker_cpu_max_pct": result.get("broker_cpu_max_pct"),
        "host_state": result.get("host_state"),
        "cpusets": result.get("cpusets"),
        "clock_unpinned": result.get("clock_unpinned"),
        "client_path": result.get("client_path"),
        "harness_fingerprint": result.get("harness_fingerprint"),
        "publish_path": result.get("publish_path"),
        "native_async": result.get("native_async"),
        "protocol_effective": result.get("protocol_effective"),
        "cost_per_message": result.get("cost_per_message"),
        "workers": workers,
        "telemetry_last": last_telemetry_snapshot(result),
    }


def slot_rate(result: dict) -> Optional[float]:
    if result.get("status") != "valid":
        return None
    value = result.get("primary_msgs_per_s")
    if value is None:
        return None
    return float(value)


def coefficient_of_variation(values: Sequence[float]) -> Optional[float]:
    cleaned = [float(v) for v in values if v is not None]
    if len(cleaned) < 2:
        return None
    mean = sum(cleaned) / len(cleaned)
    if mean == 0:
        return None
    return pstdev(cleaned) / mean


def analyze_blocks(slots: List[dict], a_label: str, b_label: str) -> dict:
    blocks = []
    for i in range(0, len(slots), 4):
        chunk = slots[i : i + 4]
        if len(chunk) < 4:
            break
        labels = [s["arm"] for s in chunk]
        design = abba_block_design(labels)
        a_rates = [s["rate"] for s in chunk if s["arm"] == a_label]
        b_rates = [s["rate"] for s in chunk if s["arm"] == b_label]
        a_valid = [r for r in a_rates if r is not None]
        b_valid = [r for r in b_rates if r is not None]
        ratio = None
        if a_valid and b_valid:
            a_c = geometric_mean(a_valid)
            b_c = geometric_mean(b_valid)
            if a_c and b_c:
                ratio = b_c / a_c
        blocks.append(
            {
                "index": i // 4,
                "design": design,
                "order": labels,
                "rate_a": a_rates,
                "rate_b": b_rates,
                "broker_cpu": [s["run"].get("broker_cpu_max_pct") for s in chunk],
                "status": [s["run"].get("status") for s in chunk],
                "reasons": [s["run"].get("reasons") for s in chunk],
                "host_loadavg": [(s["run"].get("host_state") or {}).get("loadavg") for s in chunk],
                "ratio_b_over_a": ratio,
                "complete_valid": all(s["rate"] is not None for s in chunk),
            }
        )
    ratios = [b["ratio_b_over_a"] for b in blocks if b["ratio_b_over_a"]]
    a_all = [s["rate"] for s in slots if s["arm"] == a_label and s["rate"] is not None]
    b_all = [s["rate"] for s in slots if s["arm"] == b_label and s["rate"] is not None]
    median_ratio = median(ratios) if ratios else None
    control_quality = "unknown"
    if median_ratio is not None:
        drift = abs(median_ratio - 1.0)
        cv_a = coefficient_of_variation(a_all)
        cv_b = coefficient_of_variation(b_all)
        noisy = (cv_a is not None and cv_a > 0.05) or (cv_b is not None and cv_b > 0.05)
        if drift > 0.03 or noisy:
            control_quality = "noisy"
        else:
            control_quality = "clean"
    return {
        "a_label": a_label,
        "b_label": b_label,
        "blocks": blocks,
        "median_ratio_b_over_a": median_ratio,
        "geometric_mean_ratio": geometric_mean(ratios) if ratios else None,
        "ratio_min": min(ratios) if ratios else None,
        "ratio_max": max(ratios) if ratios else None,
        "cv_a": coefficient_of_variation(a_all),
        "cv_b": coefficient_of_variation(b_all),
        "median_a": median(a_all) if a_all else None,
        "median_b": median(b_all) if b_all else None,
        "n_valid_a": len(a_all),
        "n_valid_b": len(b_all),
        "n_slots": len(slots),
        "control_quality": control_quality,
    }


def run_abba(
    *,
    a_label: str,
    b_label: str,
    protocol: str,
    blocks: int,
    output: Path,
    comparison_name: str,
) -> dict:
    wait_until_idle()
    point = rtt_point(protocol)
    cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="standard")
    pin_current_process(cpusets.get("orch"))
    meta = broker_up(wait=True, cpuset=cpusets.get("broker"))
    host_profile = resolve_host_profile(None)
    identities = {
        a_label: arm_identity(a_label),
        b_label: arm_identity(b_label),
    }
    for label, ident in identities.items():
        if not ident["sha_match"]:
            raise SystemExit(f"{label} SHA mismatch: {ident}")
        py_file = Path(ident["python_identity"]["imported_file"])
        if not str(py_file).startswith(str(ident["install_path"])):
            raise SystemExit(f"{label} imported {py_file}, not {ident['install_path']}")

    order = abba_order(blocks)
    paths = {a_label: str(ARM_PATHS[a_label]), b_label: str(ARM_PATHS[b_label])}
    slots: List[dict] = []
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mqtt-bench-rtt-direct-") as tmp:
        work_dir = Path(tmp)
        for index, ab_label in enumerate(order):
            arm = a_label if ab_label == "A" else b_label
            if index:
                time.sleep(ABBA_COOLDOWN_S)
            print(
                f"{comparison_name} {protocol} slot {index + 1}/{len(order)} "
                f"arm={arm} { _utc()}",
                flush=True,
            )
            result = run_point(
                point,
                client="mqttium",
                client_path=paths[arm],
                host=meta["host"],
                port=meta["port"],
                tls_port=meta["tls_port"],
                profile="standard",
                work_dir=work_dir,
                cpusets=cpusets,
                load_profile=None,
                host_profile=host_profile,
                managed_broker=True,
                cross_client=True,
            )
            slot = {
                "index": index,
                "abba_label": ab_label,
                "arm": arm,
                "rate": slot_rate(result),
                "run": slim_run(result),
            }
            slots.append(slot)
            write_json(str(output.with_suffix(".partial.json")), {"slots": slots})

    analysis = analyze_blocks(slots, a_label, b_label)
    payload = {
        "kind": "mqttium_rtt_direct_abba",
        "comparison": comparison_name,
        "scenario": "rtt_capacity_qos1",
        "protocol": protocol,
        "profile": "standard",
        "layout": {
            "initiator": "sut",
            "broker": "broker",
            "responder": "orch",
            "cpusets": cpusets,
            "note": "representative PR #39 layout; responder shares orch",
        },
        "point": {
            "protocol": point.get("protocol"),
            "qos_publish": point.get("qos_publish"),
            "qos_subscribe": point.get("qos_subscribe"),
            "payload": point.get("payload"),
            "outstanding": point.get("outstanding"),
            "inflight": point.get("inflight"),
            "cadence": point.get("cadence"),
            "network": point.get("network") or "localhost",
            "duration_s": point.get("duration_s"),
            "warmup_s": point.get("warmup_s"),
            "drain_s": point.get("drain_s"),
        },
        "blocks": blocks,
        "abba_order": order,
        "cooldown_s": ABBA_COOLDOWN_S,
        "harness": harness_identity(),
        "broker": meta,
        "host_profile": host_profile,
        "arms": identities,
        "slots": slots,
        "analysis": analysis,
        "finished_at": _utc(),
        "diagnostic_only": False,
        "non_comparable": False,
    }
    write_json(str(output), payload)
    partial = output.with_suffix(".partial.json")
    if partial.exists():
        partial.unlink()
    return payload


def write_identities(out_dir: Path) -> dict:
    payload = {
        "finished_at": _utc(),
        "harness": harness_identity(),
        "requested_shas": REQUESTED_SHAS,
        "arms": {label: arm_identity(label) for label in ("A", "B1", "B2", "C")},
        "host": {
            "hostname": os.uname().nodename,
            "python": sys.version,
        },
        "host_profile": resolve_host_profile(None),
        "cpusets": allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="standard"),
        "campaign_owners": campaign_owners(),
    }
    write_json(str(out_dir / "identities.json"), payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--phase", required=True, choices=("identity", "compare", "all"))
    parser.add_argument("--name")
    parser.add_argument("--a")
    parser.add_argument("--b")
    parser.add_argument("--protocol", choices=("MQTTv311", "MQTTv5"))
    parser.add_argument("--blocks", type=int, default=4)
    parser.add_argument("--output-name")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.phase in ("identity", "all"):
        payload = write_identities(out_dir)
        print(json.dumps({"identities": str(out_dir / "identities.json"), "campaign_owners": payload["campaign_owners"]}, indent=2))
        if args.phase == "identity":
            return 0
    if args.phase == "compare":
        if not args.name or not args.a or not args.b or not args.protocol or not args.output_name:
            print("compare needs --name --a --b --protocol --output-name", file=sys.stderr)
            return 2
        wait_until_idle()
        payload = run_abba(
            a_label=args.a,
            b_label=args.b,
            protocol=args.protocol,
            blocks=args.blocks,
            output=out_dir / args.output_name,
            comparison_name=args.name,
        )
        print(json.dumps({"output": str(out_dir / args.output_name), "analysis": payload["analysis"]}, indent=2))
        return 0
    if args.phase == "all":
        wait_until_idle()
        jobs = (
            ("control-bb", "B1", "B2", "MQTTv311", "control-bb-v311.json", 4),
            ("control-bb", "B1", "B2", "MQTTv5", "control-bb-v5.json", 4),
            ("8e29-vs-c4", "A", "B", "MQTTv311", "8e29-vs-c4-v311.json", 4),
            ("8e29-vs-c4", "A", "B", "MQTTv5", "8e29-vs-c4-v5.json", 4),
            ("rc14-vs-c4", "C", "B", "MQTTv311", "rc14-vs-c4-v311.json", 4),
            ("rc14-vs-c4", "C", "B", "MQTTv5", "rc14-vs-c4-v5.json", 4),
        )
        for name, a_label, b_label, protocol, filename, blocks in jobs:
            run_abba(
                a_label=a_label,
                b_label=b_label,
                protocol=protocol,
                blocks=blocks,
                output=out_dir / filename,
                comparison_name=name,
            )
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
