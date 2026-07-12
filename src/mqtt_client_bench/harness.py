"""Orchestration of client benchmark runs."""

from __future__ import annotations

import json
import os
import random
import secrets
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from mqtt_client_bench.adapters.registry import (
    EXPERIMENTAL_CLIENTS,
    adapter_identity,
    create_adapter,
    unsupported_for_client,
)
from mqtt_client_bench.broker import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_TLS_PORT,
    EMQTT_BENCH_IMAGE,
    broker_container_name,
    broker_down,
    broker_up,
    ensure_certs,
    image_digest,
    parse_broker_endpoint,
    wait_for_broker,
)
from mqtt_client_bench.control import BarrierServer, read_json, wait_for_file, write_json
from mqtt_client_bench.loadgen import EmqttBenchProcess, LoadgenSpec, interval_for_rate, nominal_rate
from mqtt_client_bench.metrics import (
    abba_order,
    abba_block_ratios,
    compare_verdict_from_block_ratios,
    integrity_counts,
    latency_summary,
    median,
    sanitize_number,
    summarize_valid_runs,
)
from mqtt_client_bench.network import PROFILES as NETWORK_PROFILES
from mqtt_client_bench.network import apply_profile, clear_profile, qdisc_stats
from mqtt_client_bench.paths import PROJECT_ROOT
from mqtt_client_bench.scenarios import (
    SCENARIO_BY_NAME,
    default_runs,
    estimate_suite,
    expand_scenario,
    list_scenarios,
)
from mqtt_client_bench.telemetry import TelemetrySampler, allocate_cpuset, environment_metadata
from mqtt_client_bench.workloads import (
    PAYLOAD_SPECS,
    callback_match_loadgen_topic,
    deep_topic,
    fleet_topics,
    long_topic,
    single_topic,
    unicode_topic,
    wildcard_hash,
)


def make_run_id() -> str:
    # Fixed 8-char ascii id to keep topic sizes stable.
    return secrets.token_hex(4)


def mqtt_version_for_point(point: dict) -> int:
    """Map point.protocol to emqtt-bench -V (3=MQTT 3.1, 4=3.1.1, 5=5.0)."""
    protocol = str(point.get("protocol", "MQTTv311"))
    if protocol == "MQTTv5":
        return 5
    if protocol == "MQTTv31":
        return 3
    return 4


def effective_loadgen_mqtt_version(requested: int) -> int:
    """emqtt-bench client IDs are rejected by Mosquitto on MQTT 3.1/3.1.1.

    Keep the SUT on ``point.protocol``; only the ingress loadgen is forced to v5.
    """
    if int(requested) in (3, 4):
        return 5
    return int(requested)


def _python() -> str:
    return sys.executable


def _spawn_role(script: str, config_path: str, cpuset: Optional[str] = None) -> subprocess.Popen:
    module = f"mqtt_client_bench.roles.{Path(script).stem}"
    cmd = [_python(), "-m", module, "--config", config_path]
    env = os.environ.copy()
    # Prevent accidental imports from ambient site-packages overshadowing client_path.
    env.setdefault("PYTHONNOUSERSITE", "1")
    src = str(PROJECT_ROOT / "src")
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    preexec = None
    if cpuset and hasattr(os, "sched_setaffinity"):
        cpus = {int(x) for x in cpuset.split(",") if x.strip() != ""}

        def _set_affinity():
            os.sched_setaffinity(0, cpus)

        preexec = _set_affinity
    return subprocess.Popen(cmd, env=env, preexec_fn=preexec)


def unsupported_features(point: dict, client: str = "paho") -> List[str]:
    """Scenario knobs declared in the catalogue but not implemented by the harness.

    Points using them are refused up front instead of silently measuring
    something else than what the point claims.
    """
    missing = []
    if point.get("receive_maximum") is not None:
        missing.append("receive_maximum")
    if point.get("retained_count") is not None:
        missing.append("retained_count")
    if point.get("outage_s") is not None:
        missing.append("session_outage")
    if point.get("submit_count") is not None:
        missing.append("queue_rejection_protocol")
    if point.get("properties_profile") in ("topic_alias", "subscription_identifier"):
        missing.append(f"properties_profile:{point['properties_profile']}")
    if point.get("connect_mode") in ("tls_resume", "tcp_concurrent"):
        missing.append(f"connect_mode:{point['connect_mode']}")
    if str(point.get("topic_topology", "")) in ("fleet4k_zipf", "fleet100k"):
        # Loadgen publishes on a single fixed topic; cardinality/skew is not offered.
        missing.append(f"topic_topology:{point['topic_topology']}")
    if str(point.get("network", "")) == "wan_cut":
        missing.append("network:wan_cut")
    if "planned" in (point.get("tags") or ()):
        missing.append("planned_scenario")
    if point.get("integrity") and point.get("topology") == "publisher_only":
        missing.append("integrity_without_oracle")
    missing.extend(unsupported_for_client(client, point))
    return missing


def validate_run(point: dict, worker_results: List[dict], loadgen_stats: Optional[dict], telemetry_samples: List[dict]) -> dict:
    reasons = []
    for result in worker_results:
        if not result.get("ok", False):
            reasons.append(f"worker_error:{result.get('error', 'unknown')}")
        if result.get("error") == "warmup_drain_timeout":
            reasons.append("warmup_drain_timeout")
        failed = int(result.get("completed_failed") or result.get("protocol_failed") or 0)
        if failed:
            reasons.append("protocol_failed")
        timed_out = int(result.get("timed_out") or 0)
        completed = int(result.get("completed_in_window") or 0)
        backlog = int(result.get("backlog_at_end") or 0)
        # A few in-flight leftovers after a short drain are noise; flag only material backlog.
        if timed_out > 64 and (completed == 0 or timed_out / max(completed, 1) > 0.01 or backlog > 64):
            reasons.append("timed_out_mids")
        if result.get("role") == "rtt_initiator":
            sent = int(result.get("sent_in_window") or 0)
            timeouts = int(result.get("timeouts") or 0)
            if timeouts > 0 and (sent == 0 or timeouts / max(sent, 1) > 0.01):
                reasons.append("rtt_timeouts")

    # Open-loop charge adherence.
    if point.get("cadence") in ("steady50", "loaded75", "loaded90", "periodic10") and point.get("target_rate"):
        for result in worker_results:
            if result.get("role") in ("publisher", "rtt_initiator") and result.get("msgs_per_s") is not None:
                target = float(point["target_rate"])
                actual = float(result["msgs_per_s"])
                if target > 0 and abs(actual - target) / target > 0.02:
                    reasons.append("open_loop_rate_out_of_tolerance")

    # An ingress run where the loadgen emitted traffic but nothing was delivered
    # indicates a topic/filter mismatch or a broken subscriber, not a client score.
    if point.get("topology") == "subscriber_ingress":
        parsed = ((loadgen_stats or {}).get("parsed") or {})
        emitted = parsed.get("last_total")
        delivered = sum(int(r.get("subscriber_delivered") or 0) for r in worker_results if r.get("role") == "subscriber")
        if emitted is None:
            # Parser empty / loadgen silent — only flag when nothing was delivered either.
            if delivered == 0:
                reasons.append("loadgen_emitted_nothing")
        elif int(emitted) == 0:
            reasons.append("loadgen_emitted_nothing")
        elif delivered == 0:
            reasons.append("no_delivery_despite_load")

    # Telemetry saturation heuristics.
    for sample in telemetry_samples[-5:]:
        for name, stats in (sample.get("containers") or {}).items():
            if stats and stats.get("cpu_pct") is not None and stats["cpu_pct"] >= 85.0:
                reasons.append(f"container_cpu_high:{name}")
    # Managed-broker runs must observe the broker; a silently dead stats probe
    # would mislabel broker-limited runs as sut_limited.
    watched_any = False
    watched_ok = False
    for sample in telemetry_samples:
        for stats in (sample.get("containers") or {}).values():
            watched_any = True
            if stats is not None:
                watched_ok = True
    if watched_any and not watched_ok:
        reasons.append("broker_telemetry_missing")

    if loadgen_stats and loadgen_stats.get("parsed") and point.get("cadence") not in ("burst", "microburst"):
        parsed = loadgen_stats["parsed"]
        nominal = loadgen_stats.get("nominal_rate")
        last = parsed.get("last_rate")
        if nominal and last is not None and nominal < float("inf"):
            if last < 0.5 * nominal:
                reasons.append("loadgen_below_half_nominal")

    status = "valid" if not reasons else "inconclusive"
    bottleneck = "bottleneck_unattributed"
    if any(r.startswith("container_cpu_high:") and "mosquitto" in r for r in reasons):
        bottleneck = "broker_limited"
    elif any(r.startswith("loadgen_") for r in reasons):
        bottleneck = "loadgen_limited"
    elif not reasons:
        bottleneck = "sut_limited"

    return {"status": status, "reasons": reasons, "bottleneck": bottleneck}


def run_point(
    point: dict,
    *,
    client: str = "paho",
    client_path: Optional[str] = None,
    host: str,
    port: int,
    tls_port: int,
    profile: str,
    work_dir: Path,
    cpusets: Dict[str, str],
    load_profile: Optional[dict] = None,
    managed_broker: bool = True,
) -> dict:
    run_id = make_run_id()
    point = dict(point)
    point["run_id"] = run_id

    missing = unsupported_features(point, client=client)
    if missing:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "client": client,
            "client_path": client_path,
            "status": "inconclusive",
            "reasons": [f"not_implemented:{m}" for m in missing],
            "workers": [],
        }

    if load_profile and point.get("load_fraction") is not None:
        if point.get("topology") == "application_rtt":
            capacity = load_profile.get("rtt_capacity_msgs_per_s")
            capacity_kind = "rtt"
        else:
            capacity = load_profile.get("capacity_msgs_per_s")
            capacity_kind = "publish"
        if capacity:
            point["target_rate"] = float(capacity) * float(point["load_fraction"])
            point["calibration_kind"] = capacity_kind
    if point.get("load_fraction") is not None and not point.get("target_rate"):
        # Without a calibrated capacity the workers would silently fall back to
        # an arbitrary absolute rate, breaking cross-client comparability.
        kind = "rtt" if point.get("topology") == "application_rtt" else "publish"
        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "client": client,
            "client_path": client_path,
            "status": "inconclusive",
            "reasons": [f"load_fraction_without_{kind}_calibration"],
            "workers": [],
        }

    network = point.get("network", "localhost")
    net_result = apply_profile(network)
    if network != "localhost" and not net_result.get("applied"):
        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "status": "inconclusive",
            "reasons": [f"network_unavailable:{net_result.get('reason')}"],
            "network": net_result,
        }

    use_tls = bool(point.get("tls"))
    endpoint_port = tls_port if use_tls else port
    certs = ensure_certs() if use_tls else {}

    barrier_path = str(work_dir / f"barrier-{run_id}.sock")
    barrier = BarrierServer(barrier_path)

    workers = []
    configs = []
    topology = point.get("topology")
    topic = point.get("topic") or single_topic(run_id)

    def base_cfg(role: str, script_stem: str) -> dict:
        ready = str(work_dir / f"{role}-{run_id}.ready")
        result = str(work_dir / f"{role}-{run_id}.json")
        cfg = {
            "client": client,
            "client_path": client_path,
            "run_id": run_id,
            "host": host,
            "port": endpoint_port,
            "tls": use_tls,
            "ca_certs": certs.get("ca_crt"),
            "ready_path": ready,
            "result_path": result,
            "barrier_path": barrier_path,
            "barrier_timeout_s": 180,
            "topic": topic,
            **{k: point.get(k) for k in (
                "qos_publish", "qos_subscribe", "payload", "cadence", "inflight", "max_queued",
                "outstanding", "duration_s", "warmup_s", "drain_s", "protocol", "properties_profile",
                "load_fraction", "target_rate", "session_persistent", "callback_filters",
                "overlapping_callbacks", "subscription", "topic_topology", "subscription_count",
                "keepalive", "batch_size",
            ) if k in point or point.get(k) is not None},
        }
        # Fill defaults from point always.
        for key, default in (
            ("qos_publish", 0),
            ("qos_subscribe", 0),
            ("payload", "telemetry256"),
            ("cadence", "capacity"),
            ("inflight", 20),
            ("max_queued", 200),
            ("outstanding", 64),
            ("duration_s", 3.0 if profile == "smoke" else 20.0),
            ("warmup_s", 1.0 if profile == "smoke" else 5.0),
            ("drain_s", 2.0 if profile == "smoke" else 10.0),
            ("protocol", "MQTTv311"),
            ("force_header", False),
        ):
            cfg.setdefault(key, point.get(key, default))
        if "force_header" in point:
            cfg["force_header"] = point["force_header"]
        return cfg

    loadgen = None
    warmup_loadgen = None
    loadgen_stats = None
    expected_workers = 0
    barrier_failed = False
    barrier_error = None
    requested_mqtt_v: Optional[int] = None
    loadgen_mqtt_v: Optional[int] = None

    try:
        if topology == "publisher_only":
            cfg = base_cfg("publisher", "publisher")
            cfg_path = work_dir / f"publisher-{run_id}.cfg.json"
            write_json(str(cfg_path), cfg)
            workers.append(_spawn_role("publisher.py", str(cfg_path), cpusets.get("sut")))
            configs.append(cfg)
            expected_workers = 1

        elif topology in ("publisher_with_oracle", "fanout"):
            n_sub = int(point.get("subscribers", 1) or 1)
            pub_cfg = base_cfg("publisher", "publisher")
            pub_path = work_dir / f"publisher-{run_id}.cfg.json"
            write_json(str(pub_path), pub_cfg)
            workers.append(_spawn_role("publisher.py", str(pub_path), cpusets.get("sut")))
            configs.append(pub_cfg)
            for i in range(n_sub):
                sub_cfg = base_cfg(f"subscriber{i}", "subscriber")
                sub_cfg["client_id"] = f"sub{i}-{run_id}"
                sub_cfg["qos_subscribe"] = point.get("qos_subscribe", point.get("qos_publish", 0))
                sub_path = work_dir / f"subscriber{i}-{run_id}.cfg.json"
                write_json(str(sub_path), sub_cfg)
                workers.append(_spawn_role("subscriber.py", str(sub_path), cpusets.get("sut")))
                configs.append(sub_cfg)
            expected_workers = 1 + n_sub

        elif topology == "subscriber_ingress":
            sub_cfg = base_cfg("subscriber", "subscriber")
            sub_path = work_dir / f"subscriber-{run_id}.cfg.json"
            write_json(str(sub_path), sub_cfg)
            workers.append(_spawn_role("subscriber.py", str(sub_path), cpusets.get("sut")))
            configs.append(sub_cfg)
            expected_workers = 1
            # Start loadgen after subscriber ready.

        elif topology == "application_rtt":
            req = f"bench/{run_id}/rtt/request"
            resp = f"bench/{run_id}/rtt/response"
            resp_cfg = base_cfg("responder", "responder")
            resp_cfg.update({"request_topic": req, "response_topic": resp})
            resp_path = work_dir / f"responder-{run_id}.cfg.json"
            write_json(str(resp_path), resp_cfg)
            workers.append(_spawn_role("responder.py", str(resp_path), cpusets.get("orch")))
            configs.append(resp_cfg)

            init_cfg = base_cfg("rtt", "rtt_initiator")
            init_cfg.update({"request_topic": req, "response_topic": resp})
            init_path = work_dir / f"rtt-{run_id}.cfg.json"
            write_json(str(init_path), init_cfg)
            workers.append(_spawn_role("rtt_initiator.py", str(init_path), cpusets.get("sut")))
            configs.append(init_cfg)
            expected_workers = 2

        elif topology == "duplex_gateway":
            # SUT publishes telemetry while a SUT subscriber receives commands
            # injected by emqtt-bench (two client processes on the sut cpuset).
            sub_cfg = base_cfg("subscriber", "subscriber")
            sub_cfg["subscription"] = "exact"
            sub_cfg["topic"] = f"bench/{run_id}/commands"
            sub_path = work_dir / f"gateway-sub-{run_id}.cfg.json"
            write_json(str(sub_path), sub_cfg)
            workers.append(_spawn_role("subscriber.py", str(sub_path), cpusets.get("sut")))
            configs.append(sub_cfg)
            pub_cfg = base_cfg("publisher", "publisher")
            pub_cfg["topic"] = f"bench/{run_id}/telemetry"
            pub_path = work_dir / f"gateway-pub-{run_id}.cfg.json"
            write_json(str(pub_path), pub_cfg)
            workers.append(_spawn_role("publisher.py", str(pub_path), cpusets.get("sut")))
            configs.append(pub_cfg)
            expected_workers = 2

        elif topology == "connect":
            # Lightweight in-orchestrator connect probe using a child publisher with duration 0 replaced.
            result = _run_connect_churn(point, client, client_path, host, endpoint_port, use_tls, certs)
            return {
                "schema_version": 1,
                "run_id": run_id,
                "point": point,
                "client": client,
                "client_path": client_path,
                "status": "valid" if result.get("ok") else "inconclusive",
                "reasons": [] if result.get("ok") else ["connect_failed"],
                "workers": [result],
                "managed_broker": managed_broker,
                "environment": environment_metadata(),
            }

        elif topology == "fleet":
            result = _run_fleet_idle(point, client, client_path, host, endpoint_port, use_tls, certs)
            return {
                "schema_version": 1,
                "run_id": run_id,
                "point": point,
                "client": client,
                "client_path": client_path,
                "status": "valid" if result.get("ok") else "inconclusive",
                "reasons": [] if result.get("ok") else ["fleet_failed"],
                "workers": [result],
                "managed_broker": managed_broker,
                "environment": environment_metadata(),
            }

        else:
            return {
                "schema_version": 1,
                "run_id": run_id,
                "point": point,
                "status": "inconclusive",
                "reasons": [f"unsupported_topology:{topology}"],
            }

        # Wait for ready files.
        for cfg in configs:
            wait_for_file(cfg["ready_path"], timeout_s=60.0)

        cadence = str(point.get("cadence", "capacity"))
        burst_ingress = topology == "subscriber_ingress" and cadence in ("burst", "microburst")

        if topology == "subscriber_ingress":
            clients = int(point.get("loadgen_clients", 32) or 32)
            payload = point.get("payload", "telemetry256")
            size = PAYLOAD_SPECS.get(payload, {"size": 256})["size"]
            # Capacity points must exceed the historical ~5k delivery ceiling
            # even in smoke runs, otherwise A/B ingress optimisations are hidden
            # behind the offered rate and incorrectly labelled SUT-limited.
            target = 40000.0
            if point.get("fanin_mode") == "per_publisher":
                target = clients * 1000.0
            if cadence == "periodic10":
                target = 10.0
            callback_filters = int(point.get("callback_filters", 0) or 0)
            overlapping = bool(point.get("overlapping_callbacks", False))
            lg_topic = topic
            if callback_filters > 0:
                # Publish onto cb/%i/data so local message_callback_add filters receive traffic.
                lg_topic = callback_match_loadgen_topic(run_id)
                if not overlapping:
                    # Keep the client count (and thus offered load) comparable across
                    # variants: every message goes through iter_match; messages whose
                    # cb/<i> topic has no registered filter fall back to on_message,
                    # which also records the delivery. Cap avoids a connection storm.
                    clients = max(clients, min(callback_filters, 256))
                # Keep aggregate offered load stable when client count grows with filters.
                target = 40000.0
            elif point.get("subscription") in ("plus", "hash") or str(point.get("topic_topology", "")).startswith("fleet"):
                lg_topic = f"bench/{run_id}/org/acme/site/s0000/device/d0000/telemetry/temperature"
            else:
                # Exact-subscription stress topologies: publish on the same topic
                # the subscriber registered, or nothing gets delivered.
                topo = str(point.get("topic_topology", "single"))
                if topo == "deep32":
                    lg_topic = deep_topic(run_id, 32)
                elif topo == "long_topic_256":
                    lg_topic = long_topic(run_id, 256)
                elif topo == "long_topic_1024":
                    lg_topic = long_topic(run_id, 1024)
                elif topo == "unicode":
                    lg_topic = unicode_topic(run_id)
            limit_total = 0
            interval = interval_for_rate(clients, target)
            if burst_ingress:
                # Offer a bounded burst at max speed, then silence; the subscriber's
                # window rate plus delivered_during_drain expose backlog recovery.
                # emqtt-bench -L is a global cap across all clients.
                limit_total = 1000 if cadence == "microburst" else max(1, int(target * float(point.get("duration_s", 3))))
                interval = 1
            requested_mqtt_v = mqtt_version_for_point(point)
            loadgen_mqtt_v = effective_loadgen_mqtt_version(requested_mqtt_v)
            spec = LoadgenSpec(
                host=host,
                port=endpoint_port,
                topic=lg_topic,
                qos=int(point.get("qos_publish", 0)),
                clients=clients,
                interval_ms=interval,
                payload_size=max(size, 1),
                duration_s=float(point.get("duration_s", 3)),
                limit=limit_total,
                mqtt_version=loadgen_mqtt_v,
            )
            loadgen = EmqttBenchProcess(spec, cpuset=cpusets.get("loadgen"))
            # Warmup uses a separate short-lived loadgen so measure starts clean.
            if not burst_ingress:
                warmup_spec = LoadgenSpec(
                    host=host,
                    port=endpoint_port,
                    topic=lg_topic,
                    qos=int(point.get("qos_publish", 0)),
                    clients=clients,
                    interval_ms=interval,
                    payload_size=max(size, 1),
                    duration_s=float(point.get("warmup_s", 1)),
                    limit=0,
                    mqtt_version=loadgen_mqtt_v,
                )
                warmup_loadgen = EmqttBenchProcess(warmup_spec, cpuset=cpusets.get("loadgen"))
            else:
                warmup_loadgen = None

        elif topology == "duplex_gateway":
            # Modest command stream toward the SUT subscriber while the SUT publishes.
            requested_mqtt_v = mqtt_version_for_point(point)
            loadgen_mqtt_v = effective_loadgen_mqtt_version(requested_mqtt_v)
            spec = LoadgenSpec(
                host=host,
                port=endpoint_port,
                topic=f"bench/{run_id}/commands",
                qos=int(point.get("qos_subscribe", 1)),
                clients=2,
                interval_ms=interval_for_rate(2, 200.0),
                payload_size=256,
                duration_s=float(point.get("duration_s", 3)),
                mqtt_version=loadgen_mqtt_v,
            )
            loadgen = EmqttBenchProcess(spec, cpuset=cpusets.get("loadgen"))
            warmup_loadgen = None
            loadgen.start()

        barrier.accept_n(expected_workers, timeout_s=60.0)
        sampler = TelemetrySampler(
            pids={f"w{i}": w.pid for i, w in enumerate(workers) if w.pid},
            containers=[broker_container_name()] if managed_broker else [],
        )
        sampler.start()

        # Phase 1: warmup.
        if topology == "subscriber_ingress" and warmup_loadgen is not None:
            warmup_loadgen.start()
            ramp_s = min(warmup_loadgen.spec.clients * warmup_loadgen.spec.connect_interval_ms / 1000.0 + 0.5, 15.0)
            time.sleep(ramp_s)
        elif loadgen is not None and loadgen.proc is not None and topology != "subscriber_ingress":
            ramp_s = min(loadgen.spec.clients * loadgen.spec.connect_interval_ms / 1000.0 + 0.5, 15.0)
            time.sleep(ramp_s)

        failures = barrier.broadcast("T0")
        barrier_failed = failures > 0
        try:
            barrier.wait_for_acks("WARMUP_DRAINED", expected_workers, timeout_s=max(60.0, float(point.get("warmup_s", 1)) + float(point.get("drain_s", 2)) + 30))
        except (TimeoutError, RuntimeError) as exc:
            barrier_failed = True
            barrier_error = str(exc)
        else:
            barrier_error = None

        if topology == "subscriber_ingress" and warmup_loadgen is not None:
            warmup_loadgen.stop()
            # Brief quiet so the subscriber can drain late warmup deliveries.
            time.sleep(min(1.0, float(point.get("drain_s", 2))))

        # Phase 2: measure — fresh ingress loadgen when applicable.
        if topology == "subscriber_ingress" and loadgen is not None and not burst_ingress:
            loadgen.start()
            ramp_s = min(loadgen.spec.clients * loadgen.spec.connect_interval_ms / 1000.0 + 0.5, 15.0)
            time.sleep(ramp_s)

        failures = barrier.broadcast("T_MEASURE")
        barrier_failed = barrier_failed or failures > 0
        if burst_ingress and loadgen is not None:
            loadgen.start()

        # Wait workers; a hung worker invalidates the run instead of crashing the harness.
        worker_hang = False
        worker_timeout = max(120.0, float(point.get("duration_s", 3)) + float(point.get("warmup_s", 1)) + float(point.get("drain_s", 2)) + 60)
        for w in workers:
            try:
                w.wait(timeout=worker_timeout)
            except subprocess.TimeoutExpired:
                worker_hang = True
                w.kill()

        telemetry_samples = sampler.stop()
        if loadgen is not None:
            loadgen_stats = loadgen.stop()
            if loadgen_stats is not None:
                loadgen_stats["mqtt_version"] = getattr(loadgen.spec, "mqtt_version", None)
                loadgen_stats["mqtt_version_requested"] = requested_mqtt_v
                if (
                    requested_mqtt_v is not None
                    and loadgen_mqtt_v is not None
                    and requested_mqtt_v != loadgen_mqtt_v
                ):
                    loadgen_stats["mqtt_version_override"] = (
                        "emqtt_bench_v311_client_id_rejected_by_mosquitto"
                    )

        worker_results = []
        for cfg in configs:
            if os.path.exists(cfg["result_path"]):
                worker_results.append(read_json(cfg["result_path"]))
            else:
                worker_results.append({"ok": False, "error": "missing_result", "result_path": cfg["result_path"]})

        validity = validate_run(point, worker_results, loadgen_stats, telemetry_samples)
        if worker_hang:
            validity["status"] = "inconclusive"
            validity["reasons"].append("worker_hang")
        if barrier_failed:
            validity["status"] = "inconclusive"
            validity["reasons"].append(f"barrier_failed:{barrier_error or 'broadcast'}")

        # Integrity enrichment when sequences present.
        pub = next((w for w in worker_results if w.get("role") == "publisher"), None)
        for wr in worker_results:
            if wr.get("role") == "subscriber" and wr.get("sequences"):
                # Warmup traffic uses a disjoint sequence range (>= 2^40); late
                # warmup deliveries are not integrity errors.
                seqs = [s for s in wr["sequences"] if s < (1 << 40)]
                expected = None
                if pub and pub.get("sent_sequences"):
                    expected = pub["sent_sequences"]
                elif pub and pub.get("sent_sequence_start") is not None and pub.get("sent_sequence_end") is not None:
                    expected = range(int(pub["sent_sequence_start"]), int(pub["sent_sequence_end"]) + 1)
                if expected is not None:
                    wr["integrity"] = integrity_counts(expected, seqs)
                elif seqs:
                    wr["integrity"] = integrity_counts(range(min(seqs), max(seqs) + 1), seqs)

        # Latency summaries.
        for wr in worker_results:
            if wr.get("latencies_ns"):
                wr["latency_summary"] = latency_summary(wr["latencies_ns"])

        primary_rate = None
        secondary = {}
        for wr in worker_results:
            if wr.get("msgs_per_s") is not None and wr.get("role") in ("publisher", "subscriber", "rtt_initiator"):
                secondary[wr["role"]] = sanitize_number(wr["msgs_per_s"])
                if topology == "subscriber_ingress" and wr.get("role") == "subscriber":
                    primary_rate = wr["msgs_per_s"]
                elif topology != "subscriber_ingress" and wr.get("role") in ("publisher", "rtt_initiator"):
                    if primary_rate is None:
                        primary_rate = wr["msgs_per_s"]
                elif primary_rate is None:
                    primary_rate = wr["msgs_per_s"]

        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "client": client,
            "client_path": client_path,
            "status": validity["status"],
            "reasons": validity["reasons"],
            "bottleneck": validity["bottleneck"],
            "primary_msgs_per_s": sanitize_number(primary_rate),
            "secondary_msgs_per_s": secondary,
            "workers": worker_results,
            "loadgen": loadgen_stats,
            "telemetry": telemetry_samples[-30:],
            "network": net_result,
            "qdisc": qdisc_stats() if network != "localhost" else None,
            "managed_broker": managed_broker,
            "environment": environment_metadata(),
            "cpusets": cpusets,
            "non_comparable": bool(point.get("non_comparable")),
            "protocol_effective": point.get("protocol", "MQTTv311"),
        }
    finally:
        barrier.close()
        for w in workers:
            if w.poll() is None:
                w.terminate()
        if loadgen is not None and loadgen.proc is not None and loadgen.proc.poll() is None:
            loadgen.stop()
        if network != "localhost":
            clear_profile()


def _run_connect_churn(point, client_name, client_path, host, port, tls, certs) -> dict:
    identity = adapter_identity(client_name, client_path)
    mode = point.get("connect_mode", "tcp_serial")
    count = int(point.get("connect_count", 100))
    latencies = []
    ok = 0
    for i in range(count):
        adapter = create_adapter(
            client_name,
            client_path=client_path,
            client_id=f"conn-{i}-{make_run_id()}",
            protocol="MQTTv311",
            tls_ca_certs=certs["ca_crt"] if (tls or str(mode).startswith("tls")) else None,
        )
        connected = {"ok": False}

        def on_connect(c, u, f, rc, p=None):
            if int(getattr(rc, "value", rc)) == 0:
                connected["ok"] = True

        adapter.on_connect = on_connect
        t0 = time.perf_counter_ns()
        try:
            adapter.connect(host, port, keepalive=30)
            adapter.loop_start()
            deadline = time.time() + 5
            while time.time() < deadline and not connected["ok"]:
                time.sleep(0.001)
            t1 = time.perf_counter_ns()
            if connected["ok"]:
                ok += 1
                latencies.append(t1 - t0)
            adapter.disconnect()
            adapter.loop_stop()
        except Exception as exc:  # noqa: BLE001
            try:
                adapter.loop_stop()
            except Exception:  # noqa: BLE001
                pass
            return {"ok": False, "error": str(exc), "mode": mode, **identity}
    return {
        "ok": ok == count,
        "role": "connect",
        "mode": mode,
        "connect_count": count,
        "successes": ok,
        "latencies_ns": latencies,
        "latency_summary": latency_summary(latencies),
        **identity,
    }


def _run_fleet_idle(point, client_name, client_path, host, port, tls, certs) -> dict:
    import resource

    identity = adapter_identity(client_name, client_path)
    n = int(point.get("fleet_size", 1))
    keepalive = int(point.get("keepalive", 30))
    clients = []
    for i in range(n):
        adapter = create_adapter(
            client_name,
            client_path=client_path,
            client_id=f"fleet-{i}-{make_run_id()}",
            protocol="MQTTv311",
            tls_ca_certs=certs["ca_crt"] if tls else None,
        )
        adapter.connect(host, port, keepalive=keepalive)
        adapter.loop_start()
        clients.append(adapter)
    time.sleep(float(point.get("duration_s", 3)))
    usage = resource.getrusage(resource.RUSAGE_SELF)
    for adapter in clients:
        adapter.disconnect()
        adapter.loop_stop()
    return {
        "ok": True,
        "role": "fleet",
        "fleet_size": n,
        "ru_maxrss_kb": getattr(usage, "ru_maxrss", None),
        **identity,
    }


def run_scenario(
    name: str,
    *,
    client: str = "paho",
    client_path: Optional[str] = None,
    profile: str = "standard",
    runs: Optional[int] = None,
    broker: Optional[str] = None,
    network: Optional[str] = None,
    output: Optional[str] = None,
    load_profile_path: Optional[str] = None,
    seed: int = 42,
) -> dict:
    scenario = SCENARIO_BY_NAME[name]
    if runs is None:
        runs = default_runs(profile)
    points = expand_scenario(scenario, profile)
    if network:
        for p in points:
            p["network"] = network

    try:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    except RuntimeError as exc:
        if profile == "standard":
            raise
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")

    managed = broker is None
    if managed:
        meta = broker_up(wait=True, cpuset=cpusets.get("broker"))
        host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]
    else:
        host, port = parse_broker_endpoint(broker)
        tls_port = DEFAULT_TLS_PORT
        wait_for_broker(host, port, timeout_s=10)
        meta = {"managed_broker": False, "host": host, "port": port, "tls_port": tls_port}

    load_profile = read_json(load_profile_path) if load_profile_path else None
    if load_profile is not None:
        _validate_load_profile(load_profile, client=client, client_path=client_path, broker=meta)

    rng = random.Random(seed)
    ordered_points = list(points)
    rng.shuffle(ordered_points)

    all_results = []
    with tempfile.TemporaryDirectory(prefix="mqtt-bench-") as tmp:
        work_dir = Path(tmp)
        for point in ordered_points:
            point_runs = []
            for run_idx in range(runs):
                result = run_point(
                    point,
                    client=client,
                    client_path=client_path,
                    host=host,
                    port=port,
                    tls_port=tls_port,
                    profile=profile,
                    work_dir=work_dir,
                    cpusets=cpusets,
                    load_profile=load_profile,
                    managed_broker=managed,
                )
                result["run_index"] = run_idx
                point_runs.append(result)
            all_results.append(
                {
                    "point": point,
                    "runs": point_runs,
                    "summary": summarize_valid_runs(point_runs),
                }
            )

    identity = adapter_identity(client, client_path)
    payload = {
        "schema_version": 1,
        "scenario": name,
        "profile": profile,
        "runs": runs,
        "seed": seed,
        "client": client,
        "client_path": str(Path(client_path).resolve()) if client_path else None,
        "client_identity": identity,
        "broker": meta,
        "results": all_results,
        "environment": environment_metadata(),
        "cpusets": cpusets,
    }
    if output:
        write_json(output, payload)
    return payload


def _validate_load_profile(load_profile: dict, *, client: str, client_path: Optional[str], broker: dict) -> None:
    identity = adapter_identity(client, client_path)
    expected_client = load_profile.get("client")
    if expected_client and expected_client != client:
        raise ValueError(f"load profile client {expected_client!r} does not match {client!r}")
    expected_version = (load_profile.get("client_identity") or {}).get("client_version")
    actual_version = identity.get("client_version")
    if expected_version and actual_version and expected_version != actual_version:
        raise ValueError(
            f"load profile version {expected_version!r} does not match installed {actual_version!r}"
        )
    profile_broker = load_profile.get("broker") or {}
    if profile_broker.get("image_digest") and broker.get("image_digest"):
        if profile_broker["image_digest"] != broker["image_digest"]:
            raise ValueError("load profile broker digest mismatch")


def run_suite(suite: str, **kwargs) -> dict:
    client = kwargs.get("client", "paho")
    if suite in ("core", "full") and client in EXPERIMENTAL_CLIENTS:
        raise ValueError(
            f"experimental client {client!r} is excluded from suite {suite!r}; "
            "use --suite experimental (separate rankings)"
        )

    scenarios = list_scenarios(suite)
    # Exclude planned/non-executable scenarios from suite execution.
    scenarios = [s for s in scenarios if "planned" not in s.tags]
    profile = kwargs.get("profile", "standard")
    runs = kwargs.get("runs") or default_runs(profile)
    estimate = estimate_suite(suite, profile, runs)
    print(
        f"Suite {suite}: {estimate['scenarios']} scenarios, "
        f"{estimate['points']} points, {estimate['runs_per_point']} runs/point, "
        f"~{estimate['estimated_minutes']} min",
        flush=True,
    )
    outputs = []
    for scenario in scenarios:
        print(f"==> {scenario.name}", flush=True)
        outputs.append(run_scenario(scenario.name, **kwargs))
    return {"suite": suite, "estimate": estimate, "scenarios": outputs}


def capacity_from_qos_sweep(result: dict) -> Optional[float]:
    """Extract QoS1 publisher capacity for open-loop load fractions.

    Smoke/diagnostic runs are marked ``non_comparable`` so reporting summaries
    exclude them — calibration still needs a numeric capacity to size loaded
    scenarios during mise au point.
    """
    blocks = list(result.get("results") or [])
    qos1 = [b for b in blocks if int((b.get("point") or {}).get("qos_publish", -1)) == 1]
    candidates = qos1 or blocks
    rates: List[float] = []
    for block in candidates:
        summary = block.get("summary") or {}
        if summary.get("median") is not None:
            rates.append(float(summary["median"]))
            continue
        for run in block.get("runs") or []:
            if run.get("status") != "valid":
                continue
            rate = run.get("primary_msgs_per_s")
            if rate is not None:
                rates.append(float(rate))
    return median(rates)


def capacity_from_scenario(result: dict) -> Optional[float]:
    """Median primary rate across valid (or smoke) runs of a single-point scenario."""
    rates: List[float] = []
    for block in result.get("results") or []:
        summary = block.get("summary") or {}
        if summary.get("median") is not None:
            rates.append(float(summary["median"]))
            continue
        for run in block.get("runs") or []:
            if run.get("status") != "valid":
                continue
            rate = run.get("primary_msgs_per_s")
            if rate is not None:
                rates.append(float(rate))
    return median(rates)


def _fraction_map(capacity: Optional[float]) -> dict:
    return {
        "0.25": None if capacity is None else capacity * 0.25,
        "0.50": None if capacity is None else capacity * 0.50,
        "0.75": None if capacity is None else capacity * 0.75,
        "0.90": None if capacity is None else capacity * 0.90,
    }


def calibrate(
    output: str,
    *,
    client: str = "paho",
    client_path: Optional[str] = None,
    profile: str = "standard",
) -> dict:
    """Measure publish + RTT closed-loop capacities and emit open-loop fractions.

    Publish capacity sizes ``puback_latency_qos1``. RTT capacity sizes
    ``application_rtt_qos1`` — the two regimes are not interchangeable: an RTT
    loop pays two publishes and two deliveries per completed sample.
    """
    pub_result = run_scenario(
        "pub_qos_sweep_telemetry",
        client=client,
        client_path=client_path,
        profile=profile,
        runs=default_runs(profile),
    )
    capacity = capacity_from_qos_sweep(pub_result)
    rtt_result = run_scenario(
        "rtt_capacity_qos1",
        client=client,
        client_path=client_path,
        profile=profile,
        runs=default_runs(profile),
    )
    rtt_capacity = capacity_from_scenario(rtt_result)
    identity = adapter_identity(client, client_path)
    payload = {
        "schema_version": 1,
        "client": client,
        "client_path": str(Path(client_path).resolve()) if client_path else None,
        "client_identity": identity,
        "profile": profile,
        "capacity_msgs_per_s": capacity,
        "rtt_capacity_msgs_per_s": rtt_capacity,
        "broker": pub_result.get("broker"),
        "environment": pub_result.get("environment"),
        "scenario": "pub_qos_sweep_telemetry",
        "rtt_scenario": "rtt_capacity_qos1",
        "fractions": _fraction_map(capacity),
        "rtt_fractions": _fraction_map(rtt_capacity),
        "raw": {"publish": pub_result, "rtt": rtt_result},
    }
    write_json(output, payload)
    return payload


ABBA_COOLDOWN_S = 5.0


def compare_clients(
    clients: List[str],
    scenario: str,
    *,
    blocks: int = 4,
    profile: str = "standard",
    output: Optional[str] = None,
    load_profile_path: Optional[str] = None,
    client_paths: Optional[Dict[str, str]] = None,
    variant_index: Optional[int] = None,
) -> dict:
    """ABBA compare two MQTT client adapters across scenario variants."""
    if len(clients) < 2:
        raise ValueError("compare requires at least two --clients entries")
    baseline_client, candidate_client = clients[0], clients[1]
    client_paths = client_paths or {}
    order = abba_order(blocks)

    try:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    except RuntimeError:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")

    meta = broker_up(wait=True, cpuset=cpusets.get("broker"))
    host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]

    scenario_obj = SCENARIO_BY_NAME[scenario]
    points = expand_scenario(scenario_obj, profile)
    if variant_index is not None:
        points = [points[variant_index]]

    shared_load_profile = read_json(load_profile_path) if load_profile_path else None
    point_results = []
    with tempfile.TemporaryDirectory(prefix="mqtt-bench-ab-") as tmp:
        work_dir = Path(tmp)
        for point_idx, point in enumerate(points):
            # Auto-calibrate each client when the point uses load_fraction.
            calibrations = {}
            if point.get("load_fraction") is not None and shared_load_profile is None:
                for name in (baseline_client, candidate_client):
                    cal_path = str(work_dir / f"cal-{name}-{point_idx}.json")
                    calibrations[name] = calibrate(
                        cal_path,
                        client=name,
                        client_path=client_paths.get(name),
                        profile="standard" if profile == "standard" else profile,
                    )
            elif shared_load_profile is not None:
                calibrations[baseline_client] = shared_load_profile
                calibrations[candidate_client] = shared_load_profile

            baseline_rates = []
            candidate_rates = []
            slot_rates: List[Optional[float]] = []
            raw = []
            for slot, label in enumerate(order):
                if slot > 0:
                    time.sleep(ABBA_COOLDOWN_S)
                name = baseline_client if label == "A" else candidate_client
                result = run_point(
                    point,
                    client=name,
                    client_path=client_paths.get(name),
                    host=host,
                    port=port,
                    tls_port=tls_port,
                    profile=profile,
                    work_dir=work_dir,
                    cpusets=cpusets,
                    load_profile=calibrations.get(name),
                    managed_broker=True,
                )
                result["ab_label"] = label
                result["slot"] = slot
                result["cooldown_s"] = ABBA_COOLDOWN_S
                raw.append(result)
                rate = result.get("primary_msgs_per_s")
                usable = rate is not None and result.get("status") == "valid" and not result.get("non_comparable")
                slot_rates.append(float(rate) if usable else None)
                if usable:
                    if label == "A":
                        baseline_rates.append(float(rate))
                    else:
                        candidate_rates.append(float(rate))

            block_ratios = abba_block_ratios(order, slot_rates)
            verdict = compare_verdict_from_block_ratios(block_ratios)
            point_results.append(
                {
                    "point": point,
                    "point_index": point_idx,
                    "order": order,
                    "baseline_rates": baseline_rates,
                    "candidate_rates": candidate_rates,
                    "slot_rates": slot_rates,
                    "block_ratios": block_ratios,
                    "verdict": verdict,
                    "runs": raw,
                    "calibrations": {
                        k: {
                            "capacity_msgs_per_s": v.get("capacity_msgs_per_s"),
                            "rtt_capacity_msgs_per_s": v.get("rtt_capacity_msgs_per_s"),
                            "client": v.get("client"),
                            "client_identity": v.get("client_identity"),
                        }
                        for k, v in calibrations.items()
                    },
                }
            )

    # Overall verdict: prefer first point when single; else aggregate labels.
    overall = point_results[0]["verdict"] if len(point_results) == 1 else {
        "verdict": "multi_point",
        "points": [
            {"index": p["point_index"], "verdict": (p["verdict"] or {}).get("verdict")}
            for p in point_results
        ],
    }
    payload = {
        "schema_version": 1,
        "scenario": scenario,
        "profile": profile,
        "point": points[0] if len(points) == 1 else None,
        "points": point_results,
        "order": order,
        "baseline_client": baseline_client,
        "candidate_client": candidate_client,
        "baseline_identity": adapter_identity(baseline_client, client_paths.get(baseline_client)),
        "candidate_identity": adapter_identity(candidate_client, client_paths.get(candidate_client)),
        "cooldown_s": ABBA_COOLDOWN_S,
        "broker": meta,
        "loadgen": {
            "image": EMQTT_BENCH_IMAGE,
            "image_digest": image_digest(EMQTT_BENCH_IMAGE.split("@")[0]),
        },
        "verdict": overall,
        "environment": environment_metadata(),
        "cpusets": cpusets,
    }
    # Backward-compatible top-level rates from first point.
    if point_results:
        payload["baseline_rates"] = point_results[0]["baseline_rates"]
        payload["candidate_rates"] = point_results[0]["candidate_rates"]
        payload["runs"] = point_results[0]["runs"]
    if output:
        write_json(output, payload)
    return payload


# Backward-compatible alias used by older call sites / docs.
compare_sources = compare_clients
