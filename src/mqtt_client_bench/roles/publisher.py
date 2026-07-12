"""
Publisher worker process.

Usage:
  python -m mqtt_client_bench.roles.publisher --config /path/config.json

Publish completion contract (must match adapter capabilities):
  QoS0 — on_publish when the packet is handed to the transport
  QoS1 — on_publish on PUBACK
  QoS2 — on_publish on PUBCOMP

Primary throughput uses completed_success in the measure window only.
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
from mqtt_client_bench.workloads import (
    HEADER_SIZE,
    build_payload,
    build_payload_corpus,
    encode_header,
    make_bytes_of_size,
    rl_boundary_payloads,
    single_topic,
    wrap_with_header,
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

    run_id = cfg["run_id"].encode("ascii")
    if len(run_id) != 8:
        raise SystemExit("run_id must be 8 ascii chars")

    topic = cfg.get("topic") or single_topic(cfg["run_id"])
    qos = int(cfg.get("qos_publish", 0))
    duration_s = float(cfg.get("duration_s", 3.0))
    warmup_s = float(cfg.get("warmup_s", 1.0))
    drain_s = float(cfg.get("drain_s", 2.0))
    outstanding = int(cfg.get("outstanding", 64))
    inflight = int(cfg.get("inflight", 20))
    max_queued = int(cfg.get("max_queued", 200))
    cadence = cfg.get("cadence", "capacity")
    load_fraction = float(cfg.get("load_fraction", 0.75))
    target_rate = cfg.get("target_rate")  # msgs/s for open-loop
    payload_name = cfg.get("payload", "telemetry256")
    protocol = cfg.get("protocol", "MQTTv311")

    # Build payload body.
    if payload_name.startswith("rl_"):
        sizes = rl_boundary_payloads(topic, qos=qos)
        body = make_bytes_of_size(sizes[payload_name], seed=1)
    else:
        raw = build_payload(payload_name, seed=1)
        body = raw.encode("utf-8") if isinstance(raw, str) else raw

    corpus = []
    if payload_name in ("telemetry256", "event1k", "binary64") and not payload_name.startswith("rl_"):
        corpus = build_payload_corpus(payload_name, count=64, seed=7)
        corpus = [c.encode("utf-8") if isinstance(c, str) else c for c in corpus]

    state = {
        "connected": threading.Event(),
        "offered": 0,
        "submitted": 0,
        "sync_rejected": 0,
        "completed_success": 0,
        "completed_failed": 0,
        "missed_due_to_backpressure": 0,
        # Legacy aliases kept for older report consumers.
        "publish_calls": 0,
        "publish_accepted": 0,
        "publish_rejected": 0,
        "protocol_completed": 0,
        "protocol_failed": 0,
        "socket_completed_qos0": 0,
        "completed_in_window": 0,
        "completed_during_drain": 0,
        "latencies_ns": [],
        "scheduler_lags_ns": [],
        "lock": threading.Lock(),
        "inflight_local": 0,
        "phase": "init",
        "mid_send_ns": {},
        # Callbacks that arrive before mid_send_ns registration land here.
        "early_acks": {},
        "warmup_drain_ok": True,
        "seen_mids_inflight": set(),
    }

    def on_connect(client, userdata, flags, reason_code, properties=None):
        rc = int(getattr(reason_code, "value", reason_code))
        if rc == 0:
            state["connected"].set()

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        now = time.perf_counter_ns()
        with state["lock"]:
            failed = False
            if reason_code is not None:
                rc = int(getattr(reason_code, "value", reason_code))
                if rc >= 128:
                    failed = True
            send_ns = state["mid_send_ns"].pop(mid, None)
            if send_ns is None:
                # Callback raced ahead of publish() return — stash until registered.
                state["early_acks"][mid] = (now, failed)
                return
            _consume_completion_locked(state, qos, send_ns, now, failed, mid=mid)

    adapter = create_adapter(
        client_name,
        client_path=client_path,
        client_id=cfg.get("client_id", f"pub-{cfg['run_id']}"),
        protocol=protocol,
        clean_session=not bool(cfg.get("session_persistent", False)),
        max_inflight=inflight,
        max_queued=max_queued,
        tls_ca_certs=cfg.get("ca_certs") if cfg.get("tls") else None,
    )
    adapter.on_connect = on_connect
    adapter.on_publish = on_publish

    host = cfg["host"]
    port = int(cfg["port"])
    adapter.connect(host, port, keepalive=int(cfg.get("keepalive", 60)))
    adapter.loop_start()
    if not state["connected"].wait(timeout=30):
        write_json(cfg["result_path"], {"ok": False, "error": "connect_timeout", **identity})
        adapter.loop_stop()
        return 1

    touch(cfg["ready_path"], {"role": "publisher", "pid": os.getpid(), **identity})

    barrier = barrier_client_session(cfg["barrier_path"], timeout_s=float(cfg.get("barrier_timeout_s", 120)))
    barrier.wait("T0")

    open_loop_rate = None
    if cadence in ("steady50", "loaded75", "loaded90", "periodic10") or cfg.get("load_fraction"):
        if target_rate:
            open_loop_rate = float(target_rate)
        else:
            open_loop_rate = 1000.0 * load_fraction
        if cadence == "steady50":
            open_loop_rate = (target_rate or 2000.0) * 0.50
        elif cadence == "periodic10":
            open_loop_rate = 10.0

    gc.collect()
    gc_start = gc.get_count()
    state["phase"] = "warmup"
    warmup_end = time.perf_counter() + warmup_s
    _run_publish_loop(
        adapter,
        state,
        topic=topic,
        qos=qos,
        body=body,
        corpus=corpus,
        run_id=run_id,
        outstanding=outstanding,
        cadence=cadence,
        until=warmup_end,
        target_rate=open_loop_rate,
        properties_builder=_properties_builder(cfg, adapter),
        force_header=bool(cfg.get("force_header", False)),
        sequence_start=1 << 40,
    )

    # Drain warmup outstanding; fail closed if still active when the deadline hits.
    drain_warmup = time.perf_counter() + min(drain_s, 5.0)
    while time.perf_counter() < drain_warmup:
        with state["lock"]:
            if state["inflight_local"] == 0 and not state["mid_send_ns"] and not state["early_acks"]:
                break
        time.sleep(0.01)
    with state["lock"]:
        if state["inflight_local"] or state["mid_send_ns"] or state["early_acks"]:
            state["warmup_drain_ok"] = False
        # Do not clear mid_send_ns while ACKs may still be in flight — mark inconclusive.
        if state["warmup_drain_ok"]:
            state["completed_in_window"] = 0
            state["completed_during_drain"] = 0
            state["latencies_ns"].clear()
            state["scheduler_lags_ns"].clear()
            state["offered"] = 0
            state["submitted"] = 0
            state["sync_rejected"] = 0
            state["completed_success"] = 0
            state["completed_failed"] = 0
            state["missed_due_to_backpressure"] = 0
            state["publish_calls"] = 0
            state["publish_accepted"] = 0
            state["publish_rejected"] = 0
            state["protocol_completed"] = 0
            state["protocol_failed"] = 0
            state["socket_completed_qos0"] = 0
            state["mid_send_ns"].clear()
            state["early_acks"].clear()
            state["inflight_local"] = 0
            state["seen_mids_inflight"].clear()

    barrier.ack("WARMUP_DRAINED")
    # Second barrier: all roles start measure together.
    barrier.wait("T_MEASURE")
    barrier.close()

    if not state["warmup_drain_ok"]:
        write_json(
            cfg["result_path"],
            {
                "ok": False,
                "error": "warmup_drain_timeout",
                "role": "publisher",
                **identity,
            },
        )
        adapter.disconnect()
        adapter.loop_stop()
        return 1

    state["phase"] = "measure"
    t0 = time.perf_counter()
    measure_end = t0 + duration_s
    measure_sequences = _run_publish_loop(
        adapter,
        state,
        topic=topic,
        qos=qos,
        body=body,
        corpus=corpus,
        run_id=run_id,
        outstanding=outstanding,
        cadence=cadence,
        until=measure_end,
        target_rate=open_loop_rate,
        properties_builder=_properties_builder(cfg, adapter),
        batch_size=int(cfg.get("batch_size", 64)) if cadence == "batch64" else 1,
        reset_sequence=True,
        force_header=bool(cfg.get("force_header", False)),
    )
    t1 = time.perf_counter()

    state["phase"] = "drain"
    drain_deadline = time.perf_counter() + drain_s
    while time.perf_counter() < drain_deadline:
        with state["lock"]:
            inflight_local = state["inflight_local"]
            pending_mids = len(state["mid_send_ns"])
        if inflight_local == 0 and pending_mids == 0:
            break
        time.sleep(0.01)

    with state["lock"]:
        backlog = state["inflight_local"]
        timed_out = backlog if qos == 0 else len(state["mid_send_ns"])
        completed_in_window = state["completed_in_window"]
        completed_during_drain = state["completed_during_drain"]
        latencies = list(state["latencies_ns"])
        lags = list(state["scheduler_lags_ns"])
        counters = {
            "offered": state["offered"],
            "submitted": state["submitted"],
            "sync_rejected": state["sync_rejected"],
            "completed_success": state["completed_success"],
            "completed_failed": state["completed_failed"],
            "missed_due_to_backpressure": state["missed_due_to_backpressure"],
            "publish_calls": state["offered"],
            "publish_accepted": state["submitted"],
            "publish_rejected": state["sync_rejected"],
            "socket_completed_qos0": state["socket_completed_qos0"],
            "protocol_completed": state["protocol_completed"],
            "protocol_failed": state["completed_failed"],
            "mid_map_remaining": len(state["mid_send_ns"]),
            "warmup_drain_ok": state["warmup_drain_ok"],
        }

    adapter.disconnect()
    adapter.loop_stop()

    window = max(t1 - t0, 1e-9)
    payload_len = 0 if body is None else len(body if isinstance(body, (bytes, bytearray)) else str(body).encode())
    # Primary rate uses completed_success in the measure window.
    result = {
        "ok": True,
        "role": "publisher",
        "pid": os.getpid(),
        "topic": topic,
        "qos": qos,
        "payload": payload_name,
        "payload_bytes": payload_len,
        "cadence": cadence,
        "t0_s": t0,
        "t1_s": t1,
        "duration_s": window,
        "completed_in_window": completed_in_window,
        "completed_during_drain": completed_during_drain,
        "backlog_at_end": backlog,
        "timed_out": timed_out,
        "sent_sequence_start": measure_sequences[0] if measure_sequences else None,
        "sent_sequence_end": measure_sequences[-1] if measure_sequences else None,
        "sent_sequence_count": len(measure_sequences),
        "sent_sequences": measure_sequences if len(measure_sequences) <= 20000 else None,
        "msgs_per_s": completed_in_window / window,
        "payload_bytes_per_s": (completed_in_window * payload_len) / window,
        "latencies_ns": latencies[:50000],
        "scheduler_lags_ns": lags[:50000],
        "gc_count_start": list(gc_start),
        "gc_count_end": list(gc.get_count()),
        **identity,
        **counters,
    }
    write_json(cfg["result_path"], result)
    return 0


def _consume_completion_locked(state, qos, send_ns, now, failed: bool, *, mid) -> None:
    state["seen_mids_inflight"].discard(mid)
    if failed:
        state["completed_failed"] += 1
        state["protocol_failed"] += 1
    else:
        state["completed_success"] += 1
        if qos == 0:
            state["socket_completed_qos0"] += 1
        else:
            state["protocol_completed"] += 1
        if send_ns is not None:
            state["latencies_ns"].append(now - send_ns)
        if state["phase"] == "measure":
            state["completed_in_window"] += 1
        elif state["phase"] == "drain":
            state["completed_during_drain"] += 1
    state["inflight_local"] = max(0, state["inflight_local"] - 1)


def _properties_builder(cfg, adapter):
    profile = cfg.get("properties_profile", "none")
    if cfg.get("protocol") != "MQTTv5" or profile in (None, "none"):
        return lambda: None

    def build():
        return adapter.build_publish_properties(profile)

    return build


def _run_publish_loop(
    adapter,
    state,
    *,
    topic,
    qos,
    body,
    corpus,
    run_id,
    outstanding,
    cadence,
    until,
    target_rate,
    properties_builder,
    batch_size=1,
    reset_sequence=False,
    force_header=False,
    sequence_start=0,
):
    sequence = sequence_start
    sent_sequences = []
    loop_start = time.perf_counter()
    next_send = loop_start
    interval = (1.0 / target_rate) if target_rate and target_rate > 0 else 0.0
    corpus_i = 0
    open_loop = target_rate is not None and cadence not in ("capacity", "burst", "microburst", "batch64")

    while time.perf_counter() < until:
        if cadence in ("burst", "microburst"):
            period, duty = (1.0, 0.1) if cadence == "burst" else (0.1, 0.01)
            phase = (time.perf_counter() - loop_start) % period
            if phase > duty:
                time.sleep(min(0.001, period - phase))
                continue

        if open_loop:
            now = time.perf_counter()
            if now < next_send:
                time.sleep(min(0.001, next_send - now))
                continue
            lag_ns = int((now - next_send) * 1e9)
            with state["lock"]:
                state["scheduler_lags_ns"].append(lag_ns)
            next_send += interval

        n = batch_size if cadence == "batch64" else 1
        for _ in range(n):
            if time.perf_counter() >= until:
                break

            with state["lock"]:
                saturated = state["inflight_local"] >= outstanding
            if saturated:
                # Outstanding gate applies to ALL cadences. Open-loop counts a miss
                # instead of spawning unbounded work.
                if open_loop:
                    with state["lock"]:
                        state["offered"] += 1
                        state["missed_due_to_backpressure"] += 1
                        state["publish_calls"] += 1
                    continue
                time.sleep(0.0001)
                break

            sequence += 1
            send_ns = time.perf_counter_ns()
            header = encode_header(run_id, 1, sequence, sequence, send_ns)
            if corpus:
                payload_body = corpus[corpus_i % len(corpus)]
                corpus_i += 1
            else:
                payload_body = body
            if isinstance(payload_body, str):
                if force_header:
                    raw = payload_body.encode("utf-8")
                    payload = wrap_with_header(raw if len(raw) >= HEADER_SIZE else header + raw, header)
                else:
                    payload = payload_body
            else:
                if force_header and len(payload_body) < HEADER_SIZE:
                    payload = header
                elif len(payload_body) >= HEADER_SIZE:
                    payload = wrap_with_header(payload_body, header)
                elif len(payload_body) == 0:
                    payload = header if force_header else b""
                else:
                    payload = payload_body

            props = properties_builder()
            with state["lock"]:
                state["offered"] += 1
                state["publish_calls"] += 1
            info = adapter.publish(topic, payload=payload, qos=qos, retain=False, properties=props)
            if info.rc == 0 and info.mid is not None:
                with state["lock"]:
                    mid = info.mid
                    if mid in state["seen_mids_inflight"]:
                        # Synthetic MID collision while still inflight — treat as failure signal.
                        state["completed_failed"] += 1
                        state["protocol_failed"] += 1
                    early = state["early_acks"].pop(mid, None)
                    state["submitted"] += 1
                    state["publish_accepted"] += 1
                    state["inflight_local"] += 1
                    state["seen_mids_inflight"].add(mid)
                    if early is not None:
                        early_now, early_failed = early
                        state["mid_send_ns"].pop(mid, None)
                        _consume_completion_locked(state, qos, send_ns, early_now, early_failed, mid=mid)
                    else:
                        state["mid_send_ns"][mid] = send_ns
                    sent_sequences.append(sequence)
            else:
                with state["lock"]:
                    state["sync_rejected"] += 1
                    state["publish_rejected"] += 1
            # Keep uniqueness tracker aligned with still-open inflight / early ACKs.
            with state["lock"]:
                state["seen_mids_inflight"] = set(state["mid_send_ns"]) | set(state["early_acks"])
    return sent_sequences


if __name__ == "__main__":
    raise SystemExit(main())
