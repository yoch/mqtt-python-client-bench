"""
Subscriber worker process.

Usage:
  python -m mqtt_client_bench.roles.subscriber --config /path/config.json
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import threading
import time

from mqtt_client_bench.adapters.registry import adapter_identity, create_adapter
from mqtt_client_bench.control import barrier_client_session, touch, write_json
from mqtt_client_bench.sampling import (
    DEFAULT_METRIC_SAMPLE_LIMIT,
    DEFAULT_SEQUENCE_EXACT_LIMIT,
    ReservoirSampler,
    SequenceTracker,
)
from mqtt_client_bench.workloads import (
    HEADER_SIZE,
    callback_match_topics,
    decode_header,
    deep_topic,
    fleet_topics,
    long_topic,
    overlapping_match_filters,
    single_topic,
    unicode_topic,
    wildcard_hash,
    wildcard_plus,
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    client_name = cfg.get("client", "paho")
    client_path = cfg.get("client_path")
    identity = adapter_identity(client_name, client_path)

    run_id = cfg["run_id"]
    qos = int(cfg.get("qos_subscribe", 0))
    duration_s = float(cfg.get("duration_s", 3.0))
    warmup_s = float(cfg.get("warmup_s", 1.0))
    drain_s = float(cfg.get("drain_s", 2.0))
    protocol = cfg.get("protocol", "MQTTv311")
    metric_sample_limit = int(cfg.get("metric_sample_limit", DEFAULT_METRIC_SAMPLE_LIMIT))
    sequence_exact_limit = int(
        cfg.get("integrity_sequence_limit", DEFAULT_SEQUENCE_EXACT_LIMIT)
    )

    state = {
        "connected": threading.Event(),
        "subscribed": threading.Event(),
        "subscriber_delivered": 0,
        "delivered_in_window": 0,
        "delivered_during_drain": 0,
        "bytes_in_window": 0,
        "callback_invocations": 0,
        "sequences": SequenceTracker(sequence_exact_limit),
        "latencies_ns": ReservoirSampler(metric_sample_limit, seed=43),
        "phase": "init",
        "lock": threading.Lock(),
        "sub_mids": set(),
        "granted_ok": True,
        # Session-resume bookkeeping.
        "delivered_after_resume": 0,
        "session_present_on_resume": None,
        "reconnect_error": None,
    }

    filters = _subscription_filters(cfg, run_id)
    callback_filters = int(cfg.get("callback_filters", 0) or 0)
    overlapping = bool(cfg.get("overlapping_callbacks", False))
    local_callback_topics = (
        callback_match_topics(run_id, callback_filters) if callback_filters > 0 and not overlapping else []
    )

    def _record_delivery_locked(msg, now: int) -> None:
        """Count one application delivery. Caller must hold state['lock'].

        One-way delivery latency is ``now - header.send_ns`` across two
        processes. That is only valid because CPython's ``perf_counter_ns`` maps
        to ``CLOCK_MONOTONIC`` on Linux, whose epoch is system-wide; the numbers
        would be meaningless on a platform with a per-process epoch.
        """
        state["subscriber_delivered"] += 1
        if state["phase"] == "resume":
            state["delivered_after_resume"] += 1
        if state["phase"] in ("measure", "resume"):
            state["delivered_in_window"] += 1
            state["bytes_in_window"] += len(msg.payload or b"")
            payload = msg.payload or b""
            if len(payload) >= HEADER_SIZE:
                try:
                    hdr = decode_header(payload)
                    if hdr["sequence"] < (1 << 40):
                        state["sequences"].add(hdr["sequence"])
                        send_ns = hdr["send_ns"]
                        if send_ns:
                            state["latencies_ns"].add(now - send_ns)
                except ValueError:
                    pass
        elif state["phase"] == "drain":
            state["delivered_during_drain"] += 1
            # Integrity must still account for in-flight messages arriving
            # after T1, otherwise the window edge shows up as false "missing".
            payload = msg.payload or b""
            if len(payload) >= HEADER_SIZE:
                try:
                    sequence = decode_header(payload)["sequence"]
                    if sequence < (1 << 40):
                        state["sequences"].add(sequence)
                except ValueError:
                    pass

    adapter = create_adapter(
        client_name,
        client_path=client_path,
        client_id=cfg.get("client_id", f"sub-{run_id}"),
        protocol=protocol,
        clean_session=not bool(cfg.get("session_persistent", False)),
        tls_ca_certs=cfg.get("ca_certs") if cfg.get("tls") else None,
    )

    def on_connect(client, userdata, flags, reason_code, properties=None):
        rc = int(getattr(reason_code, "value", reason_code))
        if rc != 0:
            return
        if state["phase"] == "outage":
            state["session_present_on_resume"] = _session_present(flags)
        state["connected"].set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        # reason_code_list may be ints (v3) or ReasonCode (v5)
        ok = True
        for item in reason_code_list:
            code = int(getattr(item, "value", item))
            if code >= 128:
                ok = False
        with state["lock"]:
            state["sub_mids"].discard(mid)
            state["granted_ok"] = state["granted_ok"] and ok
            if not state["sub_mids"] and state["granted_ok"]:
                state["subscribed"].set()

    def on_message(client, userdata, msg):
        # Used when no topic-specific callback matches.
        now = time.perf_counter_ns()
        with state["lock"]:
            state["callback_invocations"] += 1
            _record_delivery_locked(msg, now)

    adapter.on_connect = on_connect
    adapter.on_subscribe = on_subscribe
    adapter.on_message = on_message

    # Local callback matching.
    # Paho skips on_message when at least one message_callback_add filter matches,
    # so filtered callbacks must record deliveries themselves.
    if callback_filters > 0:
        if overlapping:
            # Distinct filters that all match the same published topics.
            # (Paho keeps one callback per filter string; duplicates would overwrite.)
            for i, filt in enumerate(overlapping_match_filters(run_id, callback_filters)):
                count_delivery = i == 0

                def _cb(client, userdata, msg, _count_delivery=count_delivery):
                    now = time.perf_counter_ns()
                    with state["lock"]:
                        state["callback_invocations"] += 1
                        if _count_delivery:
                            _record_delivery_locked(msg, now)

                adapter.message_callback_add(filt, _cb)
        else:
            # One disjoint exact filter per callback; traffic should hit exactly one.
            for filt in local_callback_topics:

                def _cb(client, userdata, msg):
                    now = time.perf_counter_ns()
                    with state["lock"]:
                        state["callback_invocations"] += 1
                        _record_delivery_locked(msg, now)

                adapter.message_callback_add(filt, _cb)

    adapter.connect(cfg["host"], int(cfg["port"]), keepalive=int(cfg.get("keepalive", 60)))
    adapter.loop_start()
    if not state["connected"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "connect_timeout", **identity})
        adapter.loop_stop()
        return 1

    # Subscribe from the role thread — never from on_connect.
    # Bridged asyncio adapters deadlock if bridge.run() is re-entered from the loop thread.
    for filt in filters:
        result = adapter.subscribe(filt, qos=qos)
        if result.rc == adapter.MQTT_ERR_SUCCESS and result.mid is not None:
            with state["lock"]:
                state["sub_mids"].add(result.mid)
        elif result.rc == adapter.MQTT_ERR_SUCCESS and result.mid is None:
            # Some adapters ACK synchronously without a mid.
            state["subscribed"].set()
    with state["lock"]:
        if not state["sub_mids"] and state["granted_ok"]:
            state["subscribed"].set()

    if not state["subscribed"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "subscribe_timeout", **identity})
        adapter.loop_stop()
        return 1

    touch(
        cfg["ready_path"],
        {
            "role": "subscriber",
            "pid": os.getpid(),
            "filters": filters,
            "callback_filters": callback_filters,
            "callback_topics": local_callback_topics,
            "overlapping_callbacks": overlapping,
            **identity,
        },
    )
    barrier = barrier_client_session(cfg["barrier_path"], timeout_s=float(cfg.get("barrier_timeout_s", 120)))
    barrier.wait("T0")

    gc.collect()
    state["phase"] = "warmup"
    time.sleep(warmup_s)
    with state["lock"]:
        state["delivered_in_window"] = 0
        state["delivered_during_drain"] = 0
        state["bytes_in_window"] = 0
        state["sequences"].clear()
        state["latencies_ns"].clear()
        state["callback_invocations"] = 0
        state["subscriber_delivered"] = 0

    # Drain any late warmup deliveries before measure.
    time.sleep(min(0.5, drain_s))
    with state["lock"]:
        state["delivered_in_window"] = 0
        state["delivered_during_drain"] = 0
        state["bytes_in_window"] = 0
        state["sequences"].clear()
        state["latencies_ns"].clear()
        state["callback_invocations"] = 0
        state["subscriber_delivered"] = 0

    barrier.ack("WARMUP_DRAINED")
    barrier.wait("T_MEASURE")
    barrier.close()

    # Traffic can arrive between the post-warmup reset and T_MEASURE (e.g. the
    # measure loadgen ramp); it must not inflate the callback rate.
    with state["lock"]:
        state["callback_invocations"] = 0
        state["phase"] = "measure"
    # See the publisher: CPU is measured over the window it is divided by.
    cpu_ns_start = time.process_time_ns()
    t0 = time.perf_counter()
    outage_s = float(cfg.get("outage_s") or 0.0)
    if outage_s > 0:
        # Go offline mid-window so the publisher keeps producing on both sides of
        # the gap; the backlog queued during the outage is what resume replays.
        outage_at_s = float(cfg.get("outage_at_s") or (duration_s / 3.0))
        outage_at_s = max(0.0, min(outage_at_s, max(duration_s - outage_s - 0.5, 0.0)))
        time.sleep(outage_at_s)
        _run_outage_cycle(adapter, state, cfg, outage_s)
        remaining = duration_s - (time.perf_counter() - t0)
        if remaining > 0:
            time.sleep(remaining)
    else:
        time.sleep(duration_s)
    t1 = time.perf_counter()
    cpu_ns_in_window = time.process_time_ns() - cpu_ns_start

    # Snapshot measure-window counters before drain so rates match duration_s.
    with state["lock"]:
        delivered = state["delivered_in_window"]
        bytes_in_window = state["bytes_in_window"]
        latencies = state["latencies_ns"].snapshot()
        latency_sampling = state["latencies_ns"].metadata()
        callback_invocations = state["callback_invocations"]

    state["phase"] = "drain"
    time.sleep(drain_s)

    with state["lock"]:
        during_drain = state["delivered_during_drain"]
        # Include drain-phase sequences for integrity accounting.
        sequences = state["sequences"].exact_values()
        sequence_summary = state["sequences"].summary()

    adapter.disconnect()
    adapter.loop_stop()

    window = max(t1 - t0, 1e-9)
    result = {
        "ok": True,
        "role": "subscriber",
        "pid": os.getpid(),
        "filters": filters,
        "callback_filters": callback_filters,
        "callback_topics_count": len(local_callback_topics),
        "overlapping_callbacks": overlapping,
        "qos": qos,
        "t0_s": t0,
        "t1_s": t1,
        "duration_s": window,
        "subscriber_delivered": delivered,
        "delivered_during_drain": during_drain,
        "msgs_per_s": delivered / window,
        "callbacks_per_s": callback_invocations / window,
        "payload_bytes_in_window": bytes_in_window,
        "payload_bytes_per_s": bytes_in_window / window,
        "callback_invocations": callback_invocations,
        "cpu_ns_in_window": cpu_ns_in_window,
        # Bounding now happens upstream in the samplers, so the slices this used
        # to carry are gone: `sequences` and `latencies_ns` are already the
        # retained subset, and the companion summaries say how much was observed.
        "sequences": sequences,
        "sequence_summary": sequence_summary,
        "latencies_ns": latencies,
        "latency_sampling": latency_sampling,
        **identity,
    }
    if outage_s > 0:
        with state["lock"]:
            result.update(
                {
                    "outage_s": outage_s,
                    "delivered_after_resume": state["delivered_after_resume"],
                    "session_present_on_resume": state["session_present_on_resume"],
                    "reconnect_error": state["reconnect_error"],
                    "reconnect_ok": state["reconnect_error"] is None,
                }
            )
        if state["reconnect_error"] is not None:
            result["ok"] = False
            result["error"] = state["reconnect_error"]
    write_json(cfg["result_path"], result)
    return 0


def _session_present(flags) -> bool | None:
    """Read the CONNACK session-present flag, whatever shape the adapter uses.

    Paho VERSION2 passes a ``ConnectFlags`` object; bridged adapters pass a dict,
    and gmqtt uses the paho v1 spelling with a space. Returns None when the
    adapter does not report it at all — informational only, never a gate: the
    trustworthy signal is whether the backlog was actually drained.
    """
    value = getattr(flags, "session_present", None)
    if value is not None:
        return bool(value)
    if isinstance(flags, dict):
        for key in ("session_present", "session present"):
            if key in flags:
                return bool(flags[key])
    return None


def _run_outage_cycle(adapter, state, cfg, outage_s: float) -> None:
    """Disconnect, stay offline while the publisher keeps going, then resume.

    A graceful DISCONNECT is enough: MQTT retains session state whenever Clean
    Session = 0, so the broker queues QoS 1 messages for the offline subscriber
    and replays them on reconnect. No re-subscribe here — replaying the
    subscription would defeat the point of the persistent session.
    """
    state["phase"] = "outage"
    state["connected"].clear()
    # Only the MQTT connection goes down. Tearing down the I/O machinery as well
    # (loop_stop) would kill the bridged adapters' event loop, and their internal
    # objects stay bound to it — that is a harness artefact, not a client trait.
    try:
        adapter.disconnect()
    except Exception as exc:  # noqa: BLE001
        state["reconnect_error"] = f"disconnect_failed:{exc}"
        return

    time.sleep(outage_s)

    try:
        adapter.connect(cfg["host"], int(cfg["port"]), keepalive=int(cfg.get("keepalive", 60)))
        # Paho's network thread exits on a clean disconnect, so the CONNACK would
        # never be processed without this. For bridged adapters loop_start() is
        # `_ensure_bridge()`, a no-op while the loop is alive.
        try:
            adapter.loop_start()
        except Exception:  # noqa: BLE001 - already running
            pass
    except Exception as exc:  # noqa: BLE001
        state["reconnect_error"] = f"reconnect_failed:{exc}"
        return
    if not state["connected"].wait(30):
        state["reconnect_error"] = "reconnect_timeout"
        return
    state["phase"] = "resume"


def _subscription_filters(cfg, run_id):
    kind = cfg.get("subscription", "exact")
    if kind == "exact":
        topo = cfg.get("topic_topology", "single")
        if topo == "deep32":
            return [deep_topic(run_id, 32)]
        if topo == "long_topic_256":
            return [long_topic(run_id, 256)]
        if topo == "long_topic_1024":
            return [long_topic(run_id, 1024)]
        if topo == "unicode":
            return [unicode_topic(run_id)]
        return [cfg.get("topic") or single_topic(run_id)]
    if kind == "plus":
        return [wildcard_plus(run_id)]
    if kind == "hash":
        return [wildcard_hash(run_id)]
    if kind == "multi_exact":
        count = int(cfg.get("subscription_count", 16))
        topics = fleet_topics(run_id)
        return topics[:count]
    return [cfg.get("topic") or single_topic(run_id)]


if __name__ == "__main__":
    raise SystemExit(main())
