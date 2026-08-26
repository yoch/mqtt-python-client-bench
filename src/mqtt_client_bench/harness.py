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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from mqtt_client_bench.adapters.registry import (
    EXPERIMENTAL_CLIENTS,
    adapter_identity,
    create_adapter,
    get_adapter_class,
    has_async_adapter,
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
from mqtt_client_bench.loadgen import (
    EMQTT_MAX_OFFER_MSGS_PER_S,
    LoadgenSpec,
    clamp_emqtt_offer,
    clamp_hammer_rate,
    interval_for_rate,
    loadgen_emitted_msgs,
    resolve_hammer_pub_clients,
    select_loadgen_engine,
    spawn_loadgen,
)
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
from mqtt_client_bench.sampling import (
    DEFAULT_METRIC_SAMPLE_LIMIT,
    DEFAULT_PAYLOAD_BACKLOG_BYTES,
    DEFAULT_SEQUENCE_EXACT_LIMIT,
    integrity_from_summaries,
)
from mqtt_client_bench.provenance import harness_fingerprint
from mqtt_client_bench.scenarios import (
    SCENARIO_BY_NAME,
    default_runs,
    estimate_suite,
    expand_scenario,
    list_scenarios,
)
from mqtt_client_bench.sys_probe import SysCountersProbe, sys_counters_delta
from mqtt_client_bench.telemetry import (
    TelemetrySampler,
    allocate_cpuset,
    environment_metadata,
    loadavg,
    scaling_governor,
    pin_current_process,
    process_exit_metadata,
    process_memory_peaks,
    temporarily_pinned,
)
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


# Extra wait so the last $SYS tick (sys_interval = 1 s) lands before the
# after-snapshot is taken.
SYS_SETTLE_S = 1.5

# Computed once: a result is only comparable with another from the same
# measurement path (see provenance.py).
HARNESS_FINGERPRINT = harness_fingerprint()

# Target offer for core sub_* QoS0 exact-topic capacity (paced mqtt_hammer).
# emqtt-bench -I is milliseconds and cannot hold this on one loadgen core;
# those paths are clamped to EMQTT_MAX_OFFER_MSGS_PER_S. MQTT_BENCH_LOADGEN=emqtt
# forces emqtt-bench; MQTT_BENCH_LOADGEN=hammer is the same paced ranking offer.
DEFAULT_INGRESS_OFFER_MSGS_PER_S = 200000.0

# Diagnostic override for the core ingress offer. The default stays 200k because
# committed rankings were measured against it; a different offer produces
# delivery numbers that must not be compared with them, so every point that
# takes the override is forced non_comparable (fail closed, like netem). The
# knob exists because 200k is a *broker* property of the reference host
# (single-threaded Mosquitto saturates at ~206k rx+tx there): on a larger host
# the same offer would silently turn a client ranking into an offer ceiling.
INGRESS_OFFER_ENV = "MQTT_BENCH_INGRESS_OFFER"


def ingress_offer_override() -> Optional[float]:
    """Value of MQTT_BENCH_INGRESS_OFFER, or None. Garbage is refused, not defaulted."""
    raw = (os.environ.get(INGRESS_OFFER_ENV) or "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{INGRESS_OFFER_ENV} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{INGRESS_OFFER_ENV} must be > 0, got {raw!r}")
    return value


# Topologies where a single SUT publisher is the *only* source of PUBLISHes, so
# the broker's received-publish counter can be compared with what the adapter
# reported as completed. Excluded on purpose: anything where emqtt-bench also
# publishes (subscriber_ingress, duplex_gateway, broker_ceiling), and
# application_rtt, where initiator and responder both publish and neither role
# reports completed_success.
SUT_ONLY_PUBLISH_TOPOLOGIES = (
    "publisher_only",
    "publisher_with_oracle",
    "fanout",
)

# $SYS counters are integers refreshed once per second, so the delta carries up
# to ~1 s of quantisation error at each end. The gate is deliberately loose: it
# is meant to catch an adapter reporting completions for messages that never
# reached the broker (QoS0 counted at an in-process queue), not to audit
# individual messages.
# Both bounds are set from the observed distribution, not from intuition: across
# 714 reconciled runs spanning nine clients and thirteen scenarios the ratio of
# broker-received to adapter-completed spans 0.96 to 1.07, p99 at 1.05. The old
# single bound of 0.80 therefore passed a run that lost a fifth of its
# publications on the way to the broker, and there was no upper bound at all — a
# ratio of 3.0 was accepted in silence, although it means the broker saw traffic
# this run did not produce. Foreign traffic on the broker inflates the counter
# and so masks a genuine drop: the check failed open, in the direction that
# hides a fault.
BROKER_RECONCILE_MIN_RATIO = 0.90
BROKER_RECONCILE_MAX_RATIO = 1.20

# Broker headroom. Above this the run still produces a number, but that number
# is partly the broker's limit, so it must not enter a client ranking. The
# higher container_cpu_high threshold stays as the hard saturation signal.
BROKER_CPU_HEADROOM_PCT = 70.0

# Load average per CPU above which the machine is not quiet enough for a
# comparable number. The bench pins its own roles, so moderate unrelated load is
# tolerable; a run queue longer than one task per CPU is not. Kept conservative
# on purpose: a false positive costs a full re-run of the campaign.
HOST_LOADAVG_PER_CPU_MAX = 1.0

# A session outage may take at most this share of the measure window, so there is
# always traffic before it and a replayed backlog after it. Proportional rather
# than absolute, so short smoke windows stay usable for iteration.
MAX_OUTAGE_SHARE_OF_WINDOW = 0.5


def make_run_id() -> str:
    # Fixed 8-char ascii id to keep topic sizes stable.
    return secrets.token_hex(4)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def host_state_snapshot() -> dict:
    """Machine state at T0, so a reader can tell runs apart after the fact.

    Campaign results are produced client by client over hours; without this a
    published median carries no evidence of the conditions it was taken under.
    """
    return {
        "scaling_governor": scaling_governor(),
        "loadavg": loadavg(),
        "cpu_count": os.cpu_count(),
    }


def host_state_reasons(host_state: Optional[dict]) -> List[str]:
    """Environment invariants that must hold for a run to be comparable."""
    reasons: List[str] = []
    if not host_state:
        return reasons
    governor = host_state.get("scaling_governor")
    if governor and governor != "performance":
        reasons.append(f"cpu_governor_not_performance:{governor}")
    load = host_state.get("loadavg") or []
    cpus = int(host_state.get("cpu_count") or os.cpu_count() or 1)
    if load and float(load[0]) > HOST_LOADAVG_PER_CPU_MAX * cpus:
        reasons.append(f"host_busy_at_start:{float(load[0]):.1f}")
    return reasons


def cost_per_message(worker_results: List[dict], telemetry_samples: List[dict]) -> Optional[dict]:
    """Total SUT CPU and peak RSS per message pushed through the pipeline.

    Throughput alone cannot answer "what does this client cost"; two clients at
    the same msgs/s can differ several-fold in CPU.

    CPU comes from each worker's own ``process_time`` across its measure window,
    not from the orchestrator's telemetry samples: those span warmup and drain
    too, which would fold work outside the window into a ratio whose denominator
    counts only in-window messages.

    The denominator is the number of *logical* messages — publisher completions
    when there is a publisher, subscriber deliveries otherwise. Summing both
    would count each message twice in a pub+sub topology and halve the result.
    """
    cpu_ns = 0
    saw_cpu = False
    published = 0
    delivered = 0
    for worker in worker_results:
        value = worker.get("cpu_ns_in_window")
        if value is not None:
            saw_cpu = True
            cpu_ns += int(value)
        if worker.get("role") == "publisher":
            published += int(worker.get("completed_in_window") or 0)
        elif worker.get("role") == "subscriber":
            delivered += int(worker.get("subscriber_delivered") or 0)
    if not saw_cpu:
        return None
    messages = published or delivered
    if messages <= 0:
        return None
    rss_peak_kb = 0
    for sample in telemetry_samples:
        for stats in (sample.get("processes") or {}).values():
            if stats and stats.get("rss_kb"):
                rss_peak_kb = max(rss_peak_kb, int(stats["rss_kb"]))
    return {
        "cpu_ns_in_window": cpu_ns,
        "messages": messages,
        "published": published,
        "delivered": delivered,
        "cpu_us_per_message": (cpu_ns / 1000.0) / messages,
        "rss_peak_kb": rss_peak_kb or None,
    }


def _samples_in_window(
    telemetry_samples: List[dict], measure_window: Optional[tuple]
) -> List[dict]:
    """Telemetry samples taken inside the measure window.

    The sampler runs across warmup, measure and drain. Judging broker headroom on
    all of it would let a warmup ramp spike invalidate a run whose measured
    window was perfectly quiet. Falls back to every sample when the window is
    unknown (older callers) or when filtering would leave nothing to judge.
    """
    if not measure_window or not telemetry_samples:
        return telemetry_samples
    start, end = measure_window
    inside = [
        s
        for s in telemetry_samples
        if s.get("ts") is not None and start <= float(s["ts"]) <= end
    ]
    return inside or telemetry_samples


def reconcile_broker_publishes(
    point: dict,
    worker_results: List[dict],
    sys_counters: Optional[dict],
) -> dict:
    """Compare adapter-reported completions with the broker's received count.

    Returns ``{"applicable", "reason", "completed", "broker_received", "ratio"}``.
    ``reason`` is None when the run reconciles (or when the check does not apply).
    """
    result = {
        "applicable": False,
        "reason": None,
        "completed": None,
        "broker_received": None,
        "ratio": None,
    }
    if point.get("topology") not in SUT_ONLY_PUBLISH_TOPOLOGIES:
        return result
    completed = 0
    saw_publisher = False
    for worker in worker_results:
        if worker.get("role") == "publisher":
            saw_publisher = True
            completed += int(worker.get("completed_success") or 0)
    if not saw_publisher:
        return result
    result["applicable"] = True
    result["completed"] = completed
    if not sys_counters or sys_counters.get("error"):
        result["reason"] = "publisher_completions_unconfirmed"
        return result
    received = sys_counters.get("publish_received_delta")
    if received is None:
        result["reason"] = "publisher_completions_unconfirmed"
        return result
    result["broker_received"] = int(received)
    if completed <= 0:
        return result
    ratio = float(received) / float(completed)
    result["ratio"] = ratio
    # A ratio slightly above 1 is expected: the $SYS window is bounded by 1 s
    # counter ticks at each end and by the drain, so it is a little wider than
    # the publisher's own accounting window. Measured, that is worth up to 7%.
    if ratio < BROKER_RECONCILE_MIN_RATIO:
        result["reason"] = f"broker_received_below_completed:{ratio:.2f}"
    elif ratio > BROKER_RECONCILE_MAX_RATIO:
        # The broker received substantially more than this run published, so it
        # was not alone on that broker. Nothing else in the run is trustworthy
        # either: broker CPU, headroom and drop counts all now include work the
        # measurement did not cause.
        result["reason"] = f"broker_received_above_completed:{ratio:.2f}"
    return result


def reconcile_ingress_loadgen(
    point: dict,
    loadgen_stats: Optional[dict],
    sys_counters: Optional[dict],
) -> dict:
    """Compare loadgen-reported PUBLISHes with the broker's received count.

    A successful write(2) is not a decoded MQTT PUBLISH. If the generator
    claims 200k and ``$SYS received`` saw 80k, the offer is a TCP-buffer
    fiction and the run must not stay valid.
    """
    result = {
        "applicable": False,
        "reason": None,
        "loadgen_msgs": None,
        "broker_received": None,
        "ratio": None,
    }
    if point.get("topology") not in ("subscriber_ingress", "broker_ceiling"):
        return result
    if point.get("cadence") in ("burst", "microburst"):
        return result
    if not loadgen_stats or loadgen_stats.get("mode") == "sub":
        return result
    result["applicable"] = True
    emitted = loadgen_emitted_msgs(loadgen_stats)
    result["loadgen_msgs"] = emitted
    if emitted is None:
        result["reason"] = "loadgen_unconfirmed_by_broker"
        return result
    if not sys_counters or sys_counters.get("error"):
        result["reason"] = "loadgen_unconfirmed_by_broker"
        return result
    received = sys_counters.get("publish_received_delta")
    if received is None:
        result["reason"] = "loadgen_unconfirmed_by_broker"
        return result
    result["broker_received"] = int(received)
    if emitted <= 0:
        return result
    ratio = float(received) / float(emitted)
    result["ratio"] = ratio
    duration_s = float(point.get("duration_s") or 12.0)
    tick_slack = 1.0 / max(duration_s, 3.0)
    lo = BROKER_RECONCILE_MIN_RATIO - tick_slack
    hi = BROKER_RECONCILE_MAX_RATIO + tick_slack
    if ratio < lo or ratio > hi:
        result["reason"] = f"loadgen_unconfirmed_by_broker:{ratio:.2f}"
    return result


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


def resolve_ingress_offer(point: dict, clients: int) -> float:
    """Aggregate msgs/s requested from the ingress loadgen.

    Hammer ``--rate`` is an absolute cap. emqtt-bench ``-I`` is milliseconds, so
    an I=1 offer of N×1000 requires ``ingress_target_msgs_per_s >= N*1000`` (or
    equivalently ``loadgen_clients = N`` and a high enough target). emqtt paths
    are clamped later to EMQTT_MAX_OFFER_MSGS_PER_S.

    MQTT_BENCH_INGRESS_OFFER replaces the 200k default for diagnostic probes
    past the reference broker ceiling. A point that takes the override is
    marked non_comparable: its delivery count answers "what does this host's
    pipeline do at offer X", never "how does this client rank".
    """
    if point.get("ingress_target_msgs_per_s") is not None:
        return float(point["ingress_target_msgs_per_s"])
    if point.get("fanin_mode") == "per_publisher":
        return float(clients) * 1000.0
    override = ingress_offer_override()
    if override is not None:
        point["non_comparable"] = True
        point["ingress_offer_overridden"] = True
        return override
    return DEFAULT_INGRESS_OFFER_MSGS_PER_S


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
    if point.get("submit_count") is not None:
        missing.append("queue_rejection_protocol")
    outage_s = point.get("outage_s")
    if outage_s is not None:
        # The outage has to sit *inside* the measure window with traffic on both
        # sides, otherwise there is no backlog to replay and the run silently
        # measures nothing. Refuse instead of publishing a degenerate point.
        duration_s = float(point.get("duration_s") or 0.0)
        if float(outage_s) > duration_s * MAX_OUTAGE_SHARE_OF_WINDOW:
            missing.append(f"outage_exceeds_window:{outage_s}s_in_{duration_s}s")
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


def enrich_worker_integrity(worker_results: List[dict]) -> None:
    """Attach per-subscriber integrity digests before validate_run sees them.

    Must run before validation: a digest mismatch used to be recorded on the
    worker and then ignored, leaving status=valid and poisoning medians.
    """
    pub = next((w for w in worker_results if w.get("role") == "publisher"), None)
    for wr in worker_results:
        if wr.get("role") != "subscriber":
            continue
        expected_summary = (pub or {}).get("sent_sequence_summary")
        received_summary = wr.get("sequence_summary")
        if expected_summary and received_summary:
            online = integrity_from_summaries(expected_summary, received_summary)
            expected_values = (pub or {}).get("sent_sequences")
            received_values = wr.get("sequences")
            if expected_values is not None and received_values is not None:
                exact = integrity_counts(expected_values, received_values)
                exact.update(
                    {
                        "digest_match": online["digest_match"],
                        "count_delta": online["count_delta"],
                        "probabilistic": False,
                    }
                )
                wr["integrity"] = exact
            else:
                wr["integrity"] = online
            continue
        # Compatibility with results from workers predating online summaries.
        if wr.get("sequences"):
            seqs = [s for s in wr["sequences"] if s < (1 << 40)]
            expected = (pub or {}).get("sent_sequences")
            if expected is not None:
                wr["integrity"] = integrity_counts(expected, seqs)


def _integrity_failed(integ: dict) -> bool:
    """True when a subscriber integrity block reports a mismatch."""
    if "digest_match" in integ:
        return integ["digest_match"] is False
    return int(integ.get("missing") or 0) > 0 or int(integ.get("unexpected") or 0) > 0


def validate_run(
    point: dict,
    worker_results: List[dict],
    loadgen_stats: Optional[dict],
    telemetry_samples: List[dict],
    sys_counters: Optional[dict] = None,
    loadgen_ref_sub: Optional[dict] = None,
    measure_window: Optional[tuple] = None,
) -> dict:
    reasons = []
    for result in worker_results:
        if not result.get("ok", False):
            # worker_error already carries the error string; a second bare reason
            # for the same failure double-counted it in the report's tables.
            reasons.append(f"worker_error:{result.get('error', 'unknown')}")
        tripped = result.get("memory_guard_tripped_kb")
        if tripped:
            # The worker stopped publishing early to protect the host, so the
            # measure window is truncated: the number means nothing.
            reasons.append(f"memory_guard_tripped:{int(tripped) // 1024}MB")
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

    # Sequence integrity is the substance of publisher_with_oracle / integrity
    # points. Enrichment must have run first; a mismatch must not stay valid.
    require_integrity = bool(point.get("integrity")) or point.get("topology") == "publisher_with_oracle"
    if require_integrity:
        for result in worker_results:
            if result.get("role") != "subscriber":
                continue
            integ = result.get("integrity")
            if integ and _integrity_failed(integ):
                reasons.append("integrity_mismatch")

    # Open-loop charge adherence: validate the *offer* rate, not completions.
    # Completions still in flight at T1 drain after the window and would falsely
    # trip a completion-rate gate even when the paceur held the target.
    if point.get("cadence") in ("steady50", "loaded75", "loaded90", "periodic10") and point.get("target_rate"):
        for result in worker_results:
            if result.get("role") not in ("publisher", "rtt_initiator"):
                continue
            target = float(point["target_rate"])
            if target <= 0:
                continue
            duration = float(result.get("duration_s") or point.get("duration_s") or 0.0)
            if duration <= 0:
                continue
            offered = result.get("offered")
            if offered is None:
                offered = result.get("sent_in_window")
            if offered is None:
                continue
            actual_offer = float(offered) / duration
            if abs(actual_offer - target) / target > 0.02:
                reasons.append("open_loop_rate_out_of_tolerance")
            missed = int(result.get("missed_due_to_backpressure") or 0)
            # Material missed slots mean the latency sample is from a different
            # offered shape than the point claimed; fail closed.
            if float(offered) > 0 and missed / float(offered) > 0.02:
                reasons.append("open_loop_backpressure_misses")

    topology = point.get("topology")
    duration_s = float(point.get("duration_s") or 1.0)
    offer = None
    if loadgen_stats:
        offer = loadgen_stats.get("effective_offer_msgs_per_s")
        if offer is None:
            offer = loadgen_stats.get("nominal_rate")

    # An ingress run where the loadgen emitted traffic but nothing was delivered
    # indicates a topic/filter mismatch or a broken subscriber, not a client score.
    if topology == "subscriber_ingress":
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

    if topology == "broker_ceiling":
        pub_parsed = ((loadgen_stats or {}).get("parsed") or {})
        recv_parsed = ((loadgen_ref_sub or {}).get("parsed") or {})
        if pub_parsed.get("last_total") in (None, 0) and recv_parsed.get("last_total") in (None, 0):
            reasons.append("loadgen_emitted_nothing")
        elif (recv_parsed.get("last_total") in (None, 0)) and (pub_parsed.get("last_total") or 0) > 0:
            reasons.append("no_delivery_despite_load")

    # Telemetry saturation heuristics. Peak broker CPU is computed over the whole
    # run (not just the tail) and reported as a first-class field so a reader can
    # see how much headroom the broker had for any published number.
    diagnostic = "diagnostic" in (point.get("tags") or ()) or topology == "broker_ceiling"
    # A 200k ingress offer pegs single-threaded Mosquitto even when clients
    # still differentiate (preview: 14k–60k at 100 % CPU). Ranking subscribe
    # scores callback deliveries; CPU here is the cost of shedding, not a
    # shared ceiling. Diagnostic / ceiling probes still fail closed.
    ingress_ranking = topology == "subscriber_ingress" and not diagnostic
    broker_cpu_max: Optional[float] = None
    saturated_containers = set()
    for sample in _samples_in_window(telemetry_samples, measure_window):
        for name, stats in (sample.get("containers") or {}).items():
            if not stats or stats.get("cpu_pct") is None:
                continue
            cpu = float(stats["cpu_pct"])
            if broker_cpu_max is None or cpu > broker_cpu_max:
                broker_cpu_max = cpu
            if cpu >= 85.0:
                saturated_containers.add(name)
    if not ingress_ranking:
        for name in sorted(saturated_containers):
            reasons.append(f"container_cpu_high:{name}")
        # Headroom gate: below hard saturation the number is still partly the
        # broker's, so it must not enter a client ranking.
        if (
            broker_cpu_max is not None
            and not saturated_containers
            and broker_cpu_max >= BROKER_CPU_HEADROOM_PCT
        ):
            reasons.append(f"broker_headroom_low:{broker_cpu_max:.0f}")
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

    # Loadgen health vs effective offer (never raw QoS0 pub rates — they are ~2×).
    # Subscriber ingress is excluded: a slow SUT back-pressures TCP, the hammer
    # write()s block, and observed pub rate falls with the pipeline. That is the
    # measurement, not a broken generator. $SYS reconciliation still proves the
    # remaining writes were decoded PUBLISHes.
    if (
        loadgen_stats
        and loadgen_stats.get("parsed")
        and point.get("cadence") not in ("burst", "microburst")
        and topology not in ("subscriber_ingress", "broker_ceiling")
    ):
        observed = loadgen_stats.get("observed_pub_rate")
        if observed is None:
            parsed = loadgen_stats["parsed"]
            raw = parsed.get("last_rate")
            if raw is not None and loadgen_stats.get("qos0_pub_counter_double_count"):
                observed = float(raw) / 2.0
            else:
                observed = raw
        floor = (loadgen_stats or {}).get("target_requested") or offer
        if observed is not None and floor and float(floor) < float("inf"):
            if float(observed) < 0.5 * float(floor):
                reasons.append("loadgen_below_half_nominal")

    # $SYS publish drops. On subscriber_ingress a slow SUT is expected to make
    # Mosquitto shed: the score is still callback deliveries. Elsewhere, drops
    # mean the broker could not ingest the offer and the run is not a SUT score.
    dropped_delta = (sys_counters or {}).get("dropped_delta") if sys_counters else None
    drop_threshold = 100
    if offer and float(offer) < float("inf") and duration_s > 0:
        drop_threshold = max(100, int(0.01 * float(offer) * duration_s))
    sys_drops = dropped_delta is not None and int(dropped_delta) > drop_threshold
    ingress_consumer_drops = topology == "subscriber_ingress"
    if sys_drops and not ingress_consumer_drops:
        reasons.append(f"sys_publish_dropped:{int(dropped_delta)}")

    # Delivered rate vs effective offer (ingress / broker ceiling).
    delivered_rate = None
    if topology == "subscriber_ingress" and offer and point.get("cadence") not in ("burst", "microburst"):
        for result in worker_results:
            if result.get("role") == "subscriber" and result.get("msgs_per_s") is not None:
                delivered_rate = float(result["msgs_per_s"])
                break
        if delivered_rate is None:
            delivered = sum(
                int(r.get("subscriber_delivered") or 0) for r in worker_results if r.get("role") == "subscriber"
            )
            if duration_s > 0:
                delivered_rate = delivered / duration_s
    elif topology == "broker_ceiling" and offer and loadgen_ref_sub:
        delivered_rate = loadgen_ref_sub.get("observed_recv_rate")
        if delivered_rate is None:
            delivered_rate = (loadgen_ref_sub.get("parsed") or {}).get("median_rate")
        if delivered_rate is not None:
            delivered_rate = float(delivered_rate)

    delivery_ratio = None
    if (
        delivered_rate is not None
        and offer
        and float(offer) < float("inf")
        and float(offer) > 0
        and point.get("cadence") not in ("burst", "microburst")
    ):
        delivery_ratio = float(delivered_rate) / float(offer)
        # Diagnostic / ceiling: delivered ≪ offer is not a trustworthy SUT score.
        # Core capacity ingress keeps the number (a slow client at 15k of a
        # 200k offer is the ranking). Unpaced firehose: the score *is* delivered.
        unpaced_firehose = (
            (loadgen_stats or {}).get("paced") is False
            and int((loadgen_stats or {}).get("interval_ms") or 0) == 0
            and not (loadgen_stats or {}).get("rate_msgs_per_s")
        )
        if diagnostic and delivery_ratio < 0.5 and not unpaced_firehose:
            reasons.append("delivery_below_half_offer")

    # Does the broker confirm what the adapter claimed to have published?
    reconciliation = reconcile_broker_publishes(point, worker_results, sys_counters)
    if reconciliation["reason"]:
        reasons.append(reconciliation["reason"])
    ingress_reconciliation = reconcile_ingress_loadgen(point, loadgen_stats, sys_counters)
    if ingress_reconciliation["reason"]:
        reasons.append(ingress_reconciliation["reason"])

    status = "valid" if not reasons else "inconclusive"
    bottleneck = "bottleneck_unattributed"
    if reconciliation["reason"]:
        bottleneck = "broker_unconfirmed"
    elif any(r.startswith("container_cpu_high:") and "mosquitto" in r for r in reasons) or (
        sys_drops and not ingress_consumer_drops
    ):
        bottleneck = "broker_limited"
    elif (
        not ingress_ranking
        and broker_cpu_max is not None
        and broker_cpu_max >= BROKER_CPU_HEADROOM_PCT
    ):
        bottleneck = "broker_limited"
    elif any(r.startswith("loadgen_") for r in reasons):
        bottleneck = "loadgen_limited"
    elif any(str(r).startswith("delivery_below_half_offer") for r in reasons):
        # Ingress delivered ≪ offer without $SYS drops: treat as SUT-limited score.
        bottleneck = "sut_limited"
    elif not reasons:
        # Near the configured offer: the point is offer-capped, not a SUT score.
        if delivery_ratio is not None and delivery_ratio >= 0.90:
            bottleneck = "offer_limited"
        else:
            bottleneck = "sut_limited"

    return {
        "status": status,
        "reasons": reasons,
        "bottleneck": bottleneck,
        "effective_offer_msgs_per_s": offer,
        "delivered_rate": delivered_rate,
        "delivery_offer_ratio": delivery_ratio,
        "broker_cpu_max_pct": sanitize_number(broker_cpu_max),
        "broker_reconciliation": reconciliation,
        "ingress_reconciliation": ingress_reconciliation,
    }


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
    started_at = _utc_now()
    host_state = host_state_snapshot()

    missing = unsupported_features(point, client=client)
    if missing:
        return {
            "schema_version": 1,
        "harness_fingerprint": HARNESS_FINGERPRINT,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "host_state": host_state,
            "point": point,
            "client": client,
            "client_path": client_path,
            "status": "inconclusive",
            "reasons": [f"not_implemented:{m}" for m in missing],
            "workers": [],
        }

    if load_profile and point.get("load_fraction") is not None:
        capacity_kind = "rtt" if point.get("topology") == "application_rtt" else "publish"
        protocol = str(point.get("protocol", "MQTTv311"))
        try:
            capacity = capacity_from_load_profile(
                load_profile, protocol=protocol, kind=capacity_kind
            )
        except ValueError as exc:
            return {
                "schema_version": 1,
        "harness_fingerprint": HARNESS_FINGERPRINT,
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": _utc_now(),
                "host_state": host_state,
                "point": point,
                "client": client,
                "client_path": client_path,
                "status": "inconclusive",
                "reasons": [str(exc)],
                "workers": [],
            }
        if capacity:
            point["target_rate"] = float(capacity) * float(point["load_fraction"])
            point["calibration_kind"] = capacity_kind
            point["calibration_protocol"] = protocol
    if point.get("load_fraction") is not None and not point.get("target_rate"):
        # Without a calibrated capacity the workers would silently fall back to
        # an arbitrary absolute rate, breaking cross-client comparability.
        kind = "rtt" if point.get("topology") == "application_rtt" else "publish"
        return {
            "schema_version": 1,
        "harness_fingerprint": HARNESS_FINGERPRINT,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "host_state": host_state,
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
        "harness_fingerprint": HARNESS_FINGERPRINT,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "host_state": host_state,
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
                "load_fraction", "target_rate", "session_persistent", "outage_s",
                "outage_at_s", "callback_filters",
                "overlapping_callbacks", "subscription", "topic_topology", "subscription_count",
                "keepalive", "batch_size", "metric_sample_limit", "integrity_sequence_limit",
                "max_harness_payload_bytes",
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
            ("metric_sample_limit", DEFAULT_METRIC_SAMPLE_LIMIT),
            ("integrity_sequence_limit", DEFAULT_SEQUENCE_EXACT_LIMIT),
            ("max_harness_payload_bytes", DEFAULT_PAYLOAD_BACKLOG_BYTES),
            # Clients with a native async adapter are driven on their own loop.
            # A point can force the sync facade to A/B the harness's own cost.
            ("native_async", True),
        ):
            cfg.setdefault(key, point.get(key, default))
        if "force_header" in point:
            cfg["force_header"] = point["force_header"]
        return cfg

    loadgen = None
    warmup_loadgen = None
    ref_sub_loadgen = None
    loadgen_stats = None
    loadgen_ref_sub_stats = None
    sys_probe = None
    sys_counters = None
    expected_workers = 0
    barrier_failed = False
    barrier_error = None
    requested_mqtt_v: Optional[int] = None
    loadgen_mqtt_v: Optional[int] = None

    try:
        if topology == "publisher_only":
            cfg = base_cfg("publisher", "publisher")
            # Nobody will read the published sequences back: integrity is
            # reconciled against a subscriber, and there is none here. Computing
            # the per-message fingerprints anyway cost 1.2 us on every message of
            # the throughput scenarios — the ones whose per-message budget is
            # tightest, and where instrumentation therefore distorts most.
            cfg["track_sequences"] = False
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

        elif topology == "broker_ceiling":
            # emqtt-bench pub + emqtt-bench sub only — no Python SUT.
            expected_workers = 0

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
            # Pinned to the SUT cores: this is SUT work running in our own process.
            with temporarily_pinned(cpusets.get("sut")):
                result = _run_connect_churn(point, client, client_path, host, endpoint_port, use_tls, certs)
            return {
                "schema_version": 1,
        "harness_fingerprint": HARNESS_FINGERPRINT,
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": _utc_now(),
                "host_state": host_state,
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
            with temporarily_pinned(cpusets.get("sut")):
                result = _run_fleet_idle(point, client, client_path, host, endpoint_port, use_tls, certs)
            return {
                "schema_version": 1,
        "harness_fingerprint": HARNESS_FINGERPRINT,
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": _utc_now(),
                "host_state": host_state,
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
        "harness_fingerprint": HARNESS_FINGERPRINT,
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": _utc_now(),
                "host_state": host_state,
                "point": point,
                "status": "inconclusive",
                "reasons": [f"unsupported_topology:{topology}"],
            }

        # Wait for ready files.
        for cfg in configs:
            wait_for_file(cfg["ready_path"], timeout_s=60.0)

        cadence = str(point.get("cadence", "capacity"))
        burst_ingress = topology == "subscriber_ingress" and cadence in ("burst", "microburst")

        if topology in ("subscriber_ingress", "broker_ceiling"):
            clients = int(point.get("loadgen_clients", 32) or 32)
            payload = point.get("payload", "telemetry256")
            size = PAYLOAD_SPECS.get(payload, {"size": 256})["size"]
            # Capacity points must exceed the historical ~5k delivery ceiling
            # even in smoke runs, otherwise A/B ingress optimisations are hidden
            # behind the offered rate and incorrectly labelled SUT-limited.
            target = resolve_ingress_offer(point, clients)
            if cadence == "periodic10":
                target = 10.0
            callback_filters = int(point.get("callback_filters", 0) or 0)
            overlapping = bool(point.get("overlapping_callbacks", False))
            lg_topic = topic
            if topology == "broker_ceiling":
                lg_topic = single_topic(run_id)
            elif callback_filters > 0:
                # Publish onto cb/%i/data so local message_callback_add filters receive traffic.
                lg_topic = callback_match_loadgen_topic(run_id)
                if not overlapping:
                    # Keep the client count (and thus offered load) comparable across
                    # variants: every message goes through iter_match; messages whose
                    # cb/<i> topic has no registered filter fall back to on_message,
                    # which also records the delivery. Cap avoids a connection storm.
                    clients = max(clients, min(callback_filters, 256))
                # Keep aggregate offered load stable when client count grows with filters.
                target = resolve_ingress_offer(point, clients)
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
            requested_mqtt_v = mqtt_version_for_point(point)
            loadgen_mqtt_v = effective_loadgen_mqtt_version(requested_mqtt_v)
            hammer_rate = None
            if burst_ingress:
                # Keep the burst on the I=1 emqtt offer of the configured
                # client count, not the hammer ranking target.
                target = min(float(clients) * 1000.0, float(EMQTT_MAX_OFFER_MSGS_PER_S))
                limit_total = (
                    1000
                    if cadence == "microburst"
                    else max(1, int(target * float(point.get("duration_s", 3))))
                )
                interval = 1
                engine = "emqtt"
            else:
                engine = select_loadgen_engine(
                    LoadgenSpec(
                        topic=lg_topic,
                        qos=int(point.get("qos_publish", 0)),
                        mode="pub",
                        engine="auto",
                    )
                )
                if engine == "hammer":
                    clients = resolve_hammer_pub_clients(point, clients)
                    interval = 0
                    hammer_rate = float(clamp_hammer_rate(target))
                else:
                    clients, target = clamp_emqtt_offer(clients, target)
                    interval = interval_for_rate(clients, target)
            point["ingress_target_msgs_per_s"] = target
            point["loadgen_clients"] = clients
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
                mode="pub",
                target_requested=target,
                rate_msgs_per_s=hammer_rate,
                engine=engine,
            )
            loadgen = spawn_loadgen(spec, cpuset=cpusets.get("loadgen"))
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
                    mode="pub",
                    target_requested=target,
                    rate_msgs_per_s=hammer_rate,
                    engine=engine,
                )
                warmup_loadgen = spawn_loadgen(warmup_spec, cpuset=cpusets.get("loadgen"))
            else:
                warmup_loadgen = None

            if topology == "broker_ceiling":
                ref_spec = LoadgenSpec(
                    host=host,
                    port=endpoint_port,
                    topic=lg_topic,
                    qos=int(point.get("qos_subscribe", point.get("qos_publish", 0))),
                    clients=max(1, int(point.get("ref_sub_clients", 1) or 1)),
                    interval_ms=1,
                    payload_size=max(size, 1),
                    duration_s=float(point.get("duration_s", 3)),
                    mqtt_version=loadgen_mqtt_v,
                    mode="sub",
                    target_requested=target,
                    engine="emqtt",
                )
                # Keep the ref subscriber off the loadgen cpuset so pub and
                # recv do not contend for the same pinned cores.
                ref_sub_loadgen = spawn_loadgen(ref_spec, cpuset=cpusets.get("orch"))

        elif topology == "duplex_gateway":
            # Modest command stream toward the SUT subscriber while the SUT publishes.
            requested_mqtt_v = mqtt_version_for_point(point)
            loadgen_mqtt_v = effective_loadgen_mqtt_version(requested_mqtt_v)
            duplex_target = 200.0
            spec = LoadgenSpec(
                host=host,
                port=endpoint_port,
                topic=f"bench/{run_id}/commands",
                qos=int(point.get("qos_subscribe", 1)),
                clients=2,
                interval_ms=interval_for_rate(2, duplex_target),
                payload_size=256,
                duration_s=float(point.get("duration_s", 3)),
                mqtt_version=loadgen_mqtt_v,
                mode="pub",
                target_requested=duplex_target,
                engine="emqtt",
            )
            loadgen = spawn_loadgen(spec, cpuset=cpusets.get("loadgen"))
            warmup_loadgen = None
            loadgen.start()

        barrier.accept_n(expected_workers, timeout_s=60.0)
        sampler = TelemetrySampler(
            pids={f"w{i}": w.pid for i, w in enumerate(workers) if w.pid},
            containers=[broker_container_name()] if managed_broker else [],
        )
        sampler.start()

        # $SYS is sampled for every managed-broker run, not just ingress: publisher
        # capacity is the core of the ranking and was never confronted with what
        # the broker actually received. Burst ingress is excluded because its
        # offer is deliberately bounded and bursty, so counters are not meaningful.
        need_sys = managed_broker and not burst_ingress
        if need_sys:
            try:
                sys_probe = SysCountersProbe(host, endpoint_port, client_id=f"sys-{run_id}")
                sys_probe.start(timeout_s=10.0)
            except Exception as exc:  # noqa: BLE001
                sys_probe = None
                sys_counters = {"error": f"sys_probe_start_failed:{exc}"}

        # Phase 1: warmup.
        if topology == "broker_ceiling" and ref_sub_loadgen is not None:
            ref_sub_loadgen.start()
            time.sleep(min(2.0, float(point.get("warmup_s", 1)) + 0.5))

        if topology in ("subscriber_ingress", "broker_ceiling") and warmup_loadgen is not None:
            warmup_loadgen.start()
            ramp_s = min(warmup_loadgen.spec.clients * warmup_loadgen.spec.connect_interval_ms / 1000.0 + 0.5, 15.0)
            time.sleep(ramp_s)
        elif loadgen is not None and loadgen.proc is not None and topology not in ("subscriber_ingress", "broker_ceiling"):
            ramp_s = min(loadgen.spec.clients * loadgen.spec.connect_interval_ms / 1000.0 + 0.5, 15.0)
            time.sleep(ramp_s)

        failures = barrier.broadcast("T0")
        barrier_failed = failures > 0
        if expected_workers > 0:
            try:
                barrier.wait_for_acks("WARMUP_DRAINED", expected_workers, timeout_s=max(60.0, float(point.get("warmup_s", 1)) + float(point.get("drain_s", 2)) + 30))
            except (TimeoutError, RuntimeError) as exc:
                barrier_failed = True
                barrier_error = str(exc)
            else:
                barrier_error = None
        else:
            # No SUT workers: mimic a short warmup drain window.
            time.sleep(min(1.0, float(point.get("warmup_s", 1))))
            barrier_error = None

        if topology in ("subscriber_ingress", "broker_ceiling") and warmup_loadgen is not None:
            warmup_loadgen.stop()
            # Brief quiet so the subscriber can drain late warmup deliveries.
            time.sleep(min(1.0, float(point.get("drain_s", 2))))

        if sys_probe is not None:
            # Symmetric to the settle before sys_after: the last $SYS tick can be
            # up to sys_interval old, so without this the tail of warmup lands
            # inside the delta and inflates the broker-received count.
            time.sleep(SYS_SETTLE_S)
        sys_before = sys_probe.snapshot() if sys_probe is not None else None

        # Phase 2: measure — fresh ingress loadgen when applicable.
        if topology in ("subscriber_ingress", "broker_ceiling") and loadgen is not None and not burst_ingress:
            loadgen.start()
            ramp_s = min(loadgen.spec.clients * loadgen.spec.connect_interval_ms / 1000.0 + 0.5, 15.0)
            time.sleep(ramp_s)

        failures = barrier.broadcast("T_MEASURE")
        barrier_failed = barrier_failed or failures > 0
        # Wall-clock bounds of the measure window, so broker CPU can be judged on
        # the window that produced the number rather than on warmup ramp spikes.
        measure_started_wall = time.time()
        if burst_ingress and loadgen is not None:
            loadgen.start()

        # Wait workers; a hung worker invalidates the run instead of crashing the harness.
        worker_hang = False
        worker_timeout = max(120.0, float(point.get("duration_s", 3)) + float(point.get("warmup_s", 1)) + float(point.get("drain_s", 2)) + 60)
        if topology == "broker_ceiling":
            # No SUT processes — hold the measure window on the orchestrator.
            time.sleep(float(point.get("duration_s", 3)))
        else:
            for w in workers:
                try:
                    w.wait(timeout=worker_timeout)
                except subprocess.TimeoutExpired:
                    worker_hang = True
                    w.kill()

        measure_ended_wall = time.time()
        telemetry_samples = sampler.stop()
        if sys_probe is not None:
            # $SYS counters only refresh every sys_interval (1 s in our config).
            # Let one more tick land so the delta covers the whole run instead of
            # truncating the tail.
            time.sleep(SYS_SETTLE_S)
        worker_memory = process_memory_peaks(telemetry_samples)
        sys_after = sys_probe.snapshot() if sys_probe is not None else None
        if sys_probe is not None:
            sys_probe.stop()
            sys_probe = None
            if not (isinstance(sys_counters, dict) and sys_counters.get("error")):
                sys_counters = sys_counters_delta(sys_before, sys_after)

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

        if ref_sub_loadgen is not None:
            loadgen_ref_sub_stats = ref_sub_loadgen.stop()
            ref_sub_loadgen = None

        worker_results = []
        for index, cfg in enumerate(configs):
            returncode = workers[index].returncode if index < len(workers) else None
            exit_metadata = process_exit_metadata(returncode)
            if os.path.exists(cfg["result_path"]):
                result = read_json(cfg["result_path"])
            else:
                error = (
                    "possible_oom_or_sigkill"
                    if exit_metadata["possible_oom_or_sigkill"]
                    else "missing_result"
                )
                result = {
                    "ok": False,
                    "error": error,
                    "result_path": cfg["result_path"],
                }
            result["process_exit"] = exit_metadata
            result["memory_peak"] = worker_memory.get(f"w{index}")
            worker_results.append(result)

        # Integrity enrichment must precede validate_run so a digest mismatch
        # can fail the run closed instead of remaining a silent annotation.
        enrich_worker_integrity(worker_results)

        validity = validate_run(
            point,
            worker_results,
            loadgen_stats,
            telemetry_samples,
            sys_counters=sys_counters if isinstance(sys_counters, dict) else None,
            loadgen_ref_sub=loadgen_ref_sub_stats,
            measure_window=(measure_started_wall, measure_ended_wall),
        )
        for reason in host_state_reasons(host_state):
            validity["status"] = "inconclusive"
            validity["reasons"].append(reason)
        if worker_hang:
            validity["status"] = "inconclusive"
            validity["reasons"].append("worker_hang")
        if barrier_failed:
            validity["status"] = "inconclusive"
            validity["reasons"].append(f"barrier_failed:{barrier_error or 'broadcast'}")

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
        if topology == "broker_ceiling" and loadgen_ref_sub_stats is not None:
            primary_rate = loadgen_ref_sub_stats.get("observed_recv_rate")
            if primary_rate is None:
                primary_rate = (loadgen_ref_sub_stats.get("parsed") or {}).get("median_rate")
            if loadgen_stats and loadgen_stats.get("effective_offer_msgs_per_s") is not None:
                secondary["effective_offer"] = sanitize_number(loadgen_stats["effective_offer_msgs_per_s"])
            if loadgen_stats and loadgen_stats.get("observed_pub_rate") is not None:
                secondary["observed_pub"] = sanitize_number(loadgen_stats["observed_pub_rate"])

        # Which drive path the publisher actually took. Native and facade runs
        # measure different amounts of harness, so they may never be averaged or
        # ranked against each other; for a client that *has* a native path, a
        # facade run is diagnostic by definition and is marked non_comparable.
        publish_path = None
        for wr in worker_results:
            if wr.get("publish_path"):
                publish_path = wr["publish_path"]
                break
        forced_facade = publish_path == "sync_facade" and has_async_adapter(client)

        return {
            "schema_version": 1,
            "harness_fingerprint": HARNESS_FINGERPRINT,
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "host_state": host_state,
            "point": point,
            "client": client,
            "client_path": client_path,
            "status": validity["status"],
            "reasons": validity["reasons"],
            "bottleneck": validity["bottleneck"],
            "primary_msgs_per_s": sanitize_number(primary_rate),
            "secondary_msgs_per_s": secondary,
            "delivery_offer_ratio": validity.get("delivery_offer_ratio"),
            "effective_offer_msgs_per_s": validity.get("effective_offer_msgs_per_s"),
            "broker_cpu_max_pct": validity.get("broker_cpu_max_pct"),
            "broker_reconciliation": validity.get("broker_reconciliation"),
            "ingress_reconciliation": validity.get("ingress_reconciliation"),
            "cost_per_message": cost_per_message(worker_results, telemetry_samples),
            # Which drive path the publisher actually took. A run through the
            # sync facade and a native run measure different amounts of harness,
            # so they may never be averaged or ranked against each other; a
            # facade run for a client that has a native path is diagnostic only.
            "workers": worker_results,
            "loadgen": loadgen_stats,
            "loadgen_ref_sub": loadgen_ref_sub_stats,
            "sys_counters": sys_counters,
            "telemetry": telemetry_samples[-30:],
            "worker_memory_peaks": worker_memory,
            "network": net_result,
            "qdisc": qdisc_stats() if network != "localhost" else None,
            "managed_broker": managed_broker,
            "environment": environment_metadata(),
            "cpusets": cpusets,
            "non_comparable": bool(point.get("non_comparable")) or forced_facade,
            "protocol_effective": point.get("protocol", "MQTTv311"),
            "publish_path": publish_path,
        }
    finally:
        barrier.close()
        for w in workers:
            if w.poll() is None:
                w.terminate()
        if loadgen is not None and loadgen.proc is not None and loadgen.proc.poll() is None:
            loadgen.stop()
        if ref_sub_loadgen is not None and ref_sub_loadgen.proc is not None and ref_sub_loadgen.proc.poll() is None:
            ref_sub_loadgen.stop()
        if sys_probe is not None:
            try:
                sys_probe.stop()
            except Exception:  # noqa: BLE001
                pass
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


def _scenario_payload(
    *,
    name: str,
    profile: str,
    runs: int,
    seed: int,
    client: str,
    client_path: Optional[str],
    meta: dict,
    all_results: List[dict],
    cpusets: Dict[str, str],
    extra: Optional[dict] = None,
) -> dict:
    """Build the per-client result document consumed by the report."""
    payload = {
        "schema_version": 1,
        "harness_fingerprint": HARNESS_FINGERPRINT,
        "scenario": name,
        "profile": profile,
        "runs": runs,
        "seed": seed,
        "client": client,
        "client_path": str(Path(client_path).resolve()) if client_path else None,
        "client_identity": adapter_identity(client, client_path),
        "broker": meta,
        "results": all_results,
        "environment": environment_metadata(),
        "cpusets": cpusets,
    }
    if extra:
        payload.update(extra)
    return payload


def run_matrix(
    name: str,
    clients: List[str],
    *,
    profile: str = "standard",
    runs: Optional[int] = None,
    broker: Optional[str] = None,
    network: Optional[str] = None,
    output_dir: Optional[str] = None,
    client_paths: Optional[Dict[str, str]] = None,
    load_profiles: Optional[Dict[str, str]] = None,
    seed: int = 42,
    variant_index: Optional[int] = None,
) -> dict:
    """Run several clients interleaved **within each point**, not one after another.

    The published matrix used to come from separate per-client campaigns run
    hours apart, so any thermal drift or background load between them entered the
    ranking as if it were a difference between libraries. Here every client is
    measured on the same point back to back, and the client order rotates between
    repetitions so no client always runs first on a freshly idle machine.

    Writes the same ``<client>-<scenario>.json`` documents as ``run_scenario`` so
    the report and existing tooling are unchanged.
    """
    scenario = SCENARIO_BY_NAME[name]
    if runs is None:
        runs = default_runs(profile)
    if len(clients) < 2:
        raise ValueError("run_matrix needs at least two clients; use `run` for one")
    client_paths = client_paths or {}
    points = expand_scenario(scenario, profile)
    if variant_index is not None:
        # Used by the pre-launch validation, which needs to prove every role
        # still runs without paying for a full sweep of variants.
        points = [points[variant_index]]
    if network:
        for p in points:
            p["network"] = network

    try:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    except RuntimeError:
        if profile == "standard":
            raise
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")
    pin_current_process(cpusets.get("orch"))

    managed = broker is None
    if managed:
        meta = broker_up(wait=True, cpuset=cpusets.get("broker"))
        host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]
    else:
        host, port = parse_broker_endpoint(broker)
        tls_port = DEFAULT_TLS_PORT
        wait_for_broker(host, port, timeout_s=10)
        meta = {"managed_broker": False, "host": host, "port": port, "tls_port": tls_port}

    profiles: Dict[str, Optional[dict]] = {}
    for client in clients:
        path = (load_profiles or {}).get(client)
        loaded = read_json(path) if path else None
        if loaded is not None:
            _validate_load_profile(loaded, client=client, client_path=client_paths.get(client), broker=meta)
        profiles[client] = loaded

    rng = random.Random(seed)
    ordered_points = list(points)
    rng.shuffle(ordered_points)

    per_client: Dict[str, List[dict]] = {c: [] for c in clients}
    with tempfile.TemporaryDirectory(prefix="mqtt-bench-matrix-") as tmp:
        work_dir = Path(tmp)
        for point in ordered_points:
            runs_by_client: Dict[str, List[dict]] = {c: [] for c in clients}
            for run_idx in range(runs):
                # Rotate so position within the point is counterbalanced.
                rotation = clients[run_idx % len(clients):] + clients[: run_idx % len(clients)]
                for slot, client in enumerate(rotation):
                    if run_idx or slot:
                        time.sleep(ABBA_COOLDOWN_S)
                    result = run_point(
                        point,
                        client=client,
                        client_path=client_paths.get(client),
                        host=host,
                        port=port,
                        tls_port=tls_port,
                        profile=profile,
                        work_dir=work_dir,
                        cpusets=cpusets,
                        load_profile=profiles.get(client),
                        managed_broker=managed,
                    )
                    result["run_index"] = run_idx
                    result["matrix_slot"] = slot
                    result["matrix_rotation"] = list(rotation)
                    runs_by_client[client].append(result)
            for client in clients:
                per_client[client].append(
                    {
                        "point": point,
                        "runs": runs_by_client[client],
                        "summary": summarize_valid_runs(runs_by_client[client]),
                    }
                )
            # Checkpoint after every point. A campaign runs for hours and gets
            # interrupted (machine sleep, session teardown, Ctrl-C); writing only
            # at the end meant an interruption threw away the whole scenario, up
            # to an hour of measurement. `points_expected` lets a resuming
            # campaign tell a partial file from a finished one.
            if output_dir:
                _write_matrix_documents(
                    clients,
                    per_client,
                    name=name,
                    profile=profile,
                    runs=runs,
                    seed=seed,
                    client_paths=client_paths,
                    meta=meta,
                    cpusets=cpusets,
                    output_dir=output_dir,
                    points_expected=len(ordered_points),
                )

    documents = _write_matrix_documents(
        clients,
        per_client,
        name=name,
        profile=profile,
        runs=runs,
        seed=seed,
        client_paths=client_paths,
        meta=meta,
        cpusets=cpusets,
        output_dir=output_dir,
        points_expected=len(ordered_points),
    )
    return {"scenario": name, "clients": list(clients), "documents": documents}


def _write_matrix_documents(
    clients: List[str],
    per_client: Dict[str, List[dict]],
    *,
    name: str,
    profile: str,
    runs: int,
    seed: int,
    client_paths: Dict[str, str],
    meta: dict,
    cpusets: Dict[str, str],
    output_dir: Optional[str],
    points_expected: int,
) -> Dict[str, dict]:
    documents: Dict[str, dict] = {}
    for client in clients:
        documents[client] = _scenario_payload(
            name=name,
            profile=profile,
            runs=runs,
            seed=seed,
            client=client,
            client_path=client_paths.get(client),
            meta=meta,
            all_results=per_client[client],
            cpusets=cpusets,
            extra={
                "interleaved_with": [c for c in clients if c != client],
                "points_expected": points_expected,
                "points_complete": len(per_client[client]) >= points_expected,
            },
        )
        if output_dir:
            write_json(str(Path(output_dir) / f"{client}-{name}.json"), documents[client])
    return documents


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
    point_filter: Optional[Callable[[dict], bool]] = None,
    publish_path: str = "native",
) -> dict:
    scenario = SCENARIO_BY_NAME[name]
    if runs is None:
        runs = default_runs(profile)
    points = expand_scenario(scenario, profile)
    if point_filter is not None:
        points = [p for p in points if point_filter(p)]
    if network:
        for p in points:
            p["network"] = network
    if publish_path == "sync":
        # Diagnostic A/B: drive a native-capable client through the sync facade
        # so the harness's own per-message cost can be read off the difference.
        for p in points:
            p["native_async"] = False

    try:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    except RuntimeError as exc:
        if profile == "standard":
            raise
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")
    pin_current_process(cpusets.get("orch"))

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

    payload = _scenario_payload(
        name=name,
        profile=profile,
        runs=runs,
        seed=seed,
        client=client,
        client_path=client_path,
        meta=meta,
        all_results=all_results,
        cpusets=cpusets,
    )
    if output:
        write_json(output, payload)
    return payload


def protocols_for_client(client: str) -> List[str]:
    """Ordered MQTT protocol variants the adapter can speak."""
    caps = get_adapter_class(client).capabilities()
    protocols: List[str] = []
    if caps.mqtt_v311:
        protocols.append("MQTTv311")
    if caps.mqtt_v5:
        protocols.append("MQTTv5")
    return protocols


def capacity_from_load_profile(
    load_profile: dict,
    *,
    protocol: str,
    kind: str,
) -> Optional[float]:
    """Resolve publish or RTT capacity for a concrete MQTT protocol.

    Prefer ``protocol_capacities[protocol]``. Legacy top-level fields apply only
    to MQTTv311 (or when protocol_capacities is absent and protocol is v311).
    """
    key = "rtt_capacity_msgs_per_s" if kind == "rtt" else "capacity_msgs_per_s"
    buckets = load_profile.get("protocol_capacities")
    if isinstance(buckets, dict) and buckets:
        if protocol not in buckets:
            raise ValueError(f"load_profile_missing_protocol:{protocol}")
        bucket = buckets.get(protocol) or {}
        value = bucket.get(key)
        return float(value) if value is not None else None
    if protocol == "MQTTv311":
        value = load_profile.get(key)
        return float(value) if value is not None else None
    raise ValueError(f"load_profile_missing_protocol:{protocol}")


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
    buckets = load_profile.get("protocol_capacities")
    if buckets is not None and not isinstance(buckets, dict):
        raise ValueError("load profile protocol_capacities must be a mapping")


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
        "1.00": capacity,
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

    For dual-protocol clients, only the QoS1 publish point and RTT capacity are
    measured per supported protocol (not the full QoS 0/1/2 sweep ×2).
    """
    protocols = protocols_for_client(client)
    if not protocols:
        raise ValueError(f"client {client!r} supports neither MQTTv311 nor MQTTv5")

    runs = default_runs(profile)
    protocol_capacities: Dict[str, dict] = {}
    raw_by_protocol: Dict[str, dict] = {}
    last_pub: Optional[dict] = None
    last_rtt: Optional[dict] = None

    for proto in protocols:
        pub_result = run_scenario(
            "pub_qos_sweep_telemetry",
            client=client,
            client_path=client_path,
            profile=profile,
            runs=runs,
            point_filter=lambda p, protocol=proto: (
                int(p.get("qos_publish", -1)) == 1 and str(p.get("protocol", "MQTTv311")) == protocol
            ),
        )
        rtt_result = run_scenario(
            "rtt_capacity_qos1",
            client=client,
            client_path=client_path,
            profile=profile,
            runs=runs,
            point_filter=lambda p, protocol=proto: str(p.get("protocol", "MQTTv311")) == protocol,
        )
        capacity = capacity_from_qos_sweep(pub_result)
        rtt_capacity = capacity_from_scenario(rtt_result)
        protocol_capacities[proto] = {
            "capacity_msgs_per_s": capacity,
            "rtt_capacity_msgs_per_s": rtt_capacity,
            "fractions": _fraction_map(capacity),
            "rtt_fractions": _fraction_map(rtt_capacity),
        }
        raw_by_protocol[proto] = {"publish": pub_result, "rtt": rtt_result}
        last_pub, last_rtt = pub_result, rtt_result

    primary = "MQTTv311" if "MQTTv311" in protocol_capacities else protocols[0]
    capacity = protocol_capacities[primary]["capacity_msgs_per_s"]
    rtt_capacity = protocol_capacities[primary]["rtt_capacity_msgs_per_s"]
    identity = adapter_identity(client, client_path)
    payload = {
        "schema_version": 1,
        "harness_fingerprint": HARNESS_FINGERPRINT,
        "client": client,
        "client_path": str(Path(client_path).resolve()) if client_path else None,
        "client_identity": identity,
        "profile": profile,
        "capacity_msgs_per_s": capacity,
        "rtt_capacity_msgs_per_s": rtt_capacity,
        "protocol_capacities": protocol_capacities,
        "broker": (last_pub or {}).get("broker"),
        "environment": (last_pub or {}).get("environment"),
        "scenario": "pub_qos_sweep_telemetry",
        "rtt_scenario": "rtt_capacity_qos1",
        "fractions": _fraction_map(capacity),
        "rtt_fractions": _fraction_map(rtt_capacity),
        "raw": raw_by_protocol,
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
    pin_current_process(cpusets.get("orch"))

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
        # One calibration per client covers every supported protocol. Reusing
        # it for the whole matrix prevents each fraction from silently getting
        # a different gmqtt baseline and guarantees protocol×client alignment.
        calibrations = {}
        if any(point.get("load_fraction") is not None for point in points):
            if shared_load_profile is None:
                for name in (baseline_client, candidate_client):
                    cal_path = str(work_dir / f"cal-{name}.json")
                    calibrations[name] = calibrate(
                        cal_path,
                        client=name,
                        client_path=client_paths.get(name),
                        profile="standard" if profile == "standard" else profile,
                    )
            else:
                calibrations[baseline_client] = shared_load_profile
                calibrations[candidate_client] = shared_load_profile
        for point_idx, point in enumerate(points):
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
                            "protocol_capacities": v.get("protocol_capacities"),
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
        "harness_fingerprint": HARNESS_FINGERPRINT,
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
