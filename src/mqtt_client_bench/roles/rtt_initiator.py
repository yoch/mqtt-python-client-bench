"""RTT initiator: publishes requests and measures response latency."""

from __future__ import annotations

import argparse
import gc
import json
import os
import threading
import time

from mqtt_client_bench.adapters.registry import adapter_identity, create_adapter
from mqtt_client_bench.control import barrier_client_session, touch, write_json
from mqtt_client_bench.sampling import DEFAULT_METRIC_SAMPLE_LIMIT, ReservoirSampler
from mqtt_client_bench.workloads import HEADER_SIZE, decode_header_fields, encode_header


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    client_name = cfg.get("client", "paho")
    client_path = cfg.get("client_path")
    identity = adapter_identity(client_name, client_path)

    request_topic = cfg["request_topic"]
    response_topic = cfg["response_topic"]
    qos = int(cfg.get("qos_publish", 1))
    duration_s = float(cfg.get("duration_s", 3))
    warmup_s = float(cfg.get("warmup_s", 1))
    drain_s = float(cfg.get("drain_s", 2))
    outstanding = int(cfg.get("outstanding", 32))
    cadence = str(cfg.get("cadence", "capacity"))
    # Closed-loop capacity: no pacing. Open-loop latency: require an explicit
    # calibrated target_rate (harness refuses load_fraction without one).
    if cadence == "capacity":
        target_rate = None
    elif cfg.get("target_rate") is not None:
        target_rate = float(cfg["target_rate"])
    else:
        write_json(
            cfg["result_path"],
            {
                "ok": False,
                "error": "open_loop_without_target_rate",
                "role": "rtt_initiator",
                **identity,
            },
        )
        return 1
    run_id = cfg["run_id"].encode("ascii")
    protocol = cfg.get("protocol", "MQTTv311")
    metric_sample_limit = int(cfg.get("metric_sample_limit", DEFAULT_METRIC_SAMPLE_LIMIT))

    state = {
        "connected": threading.Event(),
        "subscribed": threading.Event(),
        "phase": "init",
        "inflight": {},
        # Responses that land while publish() is still on the stack are stashed
        # here and only committed after publish returns rc==0. Committing inside
        # on_message left a false RTT sample when publish then failed.
        "early_rtt": {},
        "publishing_seq": None,
        "latencies_ns": ReservoirSampler(metric_sample_limit, seed=71),
        "timeouts": 0,
        "sent_in_window": 0,
        "completed_in_window": 0,
        "offered": 0,
        "missed_due_to_backpressure": 0,
        "retracted_completions": 0,
        "lock": threading.Lock(),
    }

    adapter = create_adapter(
        client_name,
        client_path=client_path,
        client_id=cfg.get("client_id", f"rtt-{cfg['run_id']}"),
        protocol=protocol,
        clean_session=True,
        max_inflight=int(cfg.get("inflight", 20)),
        tls_ca_certs=cfg.get("ca_certs") if cfg.get("tls") else None,
    )

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if int(getattr(reason_code, "value", reason_code)) == 0:
            state["connected"].set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        if all(int(getattr(x, "value", x)) < 128 for x in reason_code_list):
            state["subscribed"].set()

    # Bound once: this callback runs on every response, and the RTT number is
    # the harness's own cost plus the client's. Only `correlation` is read here,
    # so decoding into a five-key dict and a 40-byte slice was allocation the
    # measurement then charged to the client under test.
    lock = state["lock"]
    inflight = state["inflight"]
    early_rtt = state["early_rtt"]

    def on_message(client, userdata, msg):
        now = time.perf_counter_ns()
        payload = msg.payload or b""
        if len(payload) < HEADER_SIZE:
            return
        try:
            _pub, _seq, corr, _send = decode_header_fields(payload)
        except ValueError:
            return
        with lock:
            sent = inflight.pop(corr, None)
            if sent is None:
                return
            latency = now - sent
            # Response arrived while publish() is still returning: stash and let
            # the send loop commit only on rc==0.
            if state["publishing_seq"] == corr:
                early_rtt[corr] = latency
                return
            if state["phase"] == "measure":
                state["latencies_ns"].add(latency)
                state["completed_in_window"] += 1

    adapter.on_connect = on_connect
    adapter.on_subscribe = on_subscribe
    adapter.on_message = on_message
    adapter.connect(cfg["host"], int(cfg["port"]), keepalive=60)
    adapter.loop_start()
    if not state["connected"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "ready_timeout", **identity})
        adapter.loop_stop()
        return 1
    sub = adapter.subscribe(response_topic, qos=qos)
    if sub.mid is None:
        state["subscribed"].set()
    if not state["subscribed"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "ready_timeout", **identity})
        adapter.loop_stop()
        return 1

    touch(cfg["ready_path"], {"role": "rtt_initiator", "pid": os.getpid(), **identity})
    barrier = barrier_client_session(cfg["barrier_path"], timeout_s=float(cfg.get("barrier_timeout_s", 120)))
    barrier.wait("T0")

    gc.collect()
    state["phase"] = "warmup"
    # Warmup correlations live in a disjoint high range so late responses cannot
    # collide with measure-window correlations.
    _send_loop(
        adapter,
        state,
        request_topic,
        qos,
        run_id,
        outstanding,
        target_rate,
        time.perf_counter() + warmup_s,
        sequence_start=1 << 40,
    )
    drain_deadline = time.perf_counter() + min(drain_s, 5.0)
    while time.perf_counter() < drain_deadline:
        with state["lock"]:
            if not state["inflight"]:
                break
        time.sleep(0.01)
    with state["lock"]:
        state["inflight"].clear()
        state["early_rtt"].clear()
        state["latencies_ns"].clear()
        state["sent_in_window"] = 0
        state["completed_in_window"] = 0
        state["offered"] = 0
        state["missed_due_to_backpressure"] = 0
        state["retracted_completions"] = 0
        state["timeouts"] = 0

    barrier.ack("WARMUP_DRAINED")
    barrier.wait("T_MEASURE")
    barrier.close()

    state["phase"] = "measure"
    t0 = time.perf_counter()
    _send_loop(adapter, state, request_topic, qos, run_id, outstanding, target_rate, t0 + duration_s, sequence_start=0)
    t1 = time.perf_counter()
    state["phase"] = "drain"
    deadline = time.perf_counter() + drain_s
    while time.perf_counter() < deadline:
        with state["lock"]:
            if not state["inflight"]:
                break
        time.sleep(0.01)
    with state["lock"]:
        timeouts = len(state["inflight"])
        latencies = state["latencies_ns"].snapshot()
        latency_sampling = state["latencies_ns"].metadata()
        completed = state["completed_in_window"]
        sent = state["sent_in_window"]
        offered = int(state.get("offered") or 0)
        missed = int(state.get("missed_due_to_backpressure") or 0)

    adapter.disconnect()
    adapter.loop_stop()
    window = max(t1 - t0, 1e-9)
    write_json(
        cfg["result_path"],
        {
            "ok": True,
            "role": "rtt_initiator",
            "duration_s": window,
            "sent_in_window": sent,
            # Same shape as the publisher: offered = submitted + missed slots so
            # validate_run's open-loop gate can check the paceur, not completions.
            "offered": offered if offered else sent,
            "missed_due_to_backpressure": missed,
            "completed_in_window": completed,
            "timeouts": timeouts,
            "failure_rate": (timeouts / sent) if sent else None,
            "latencies_ns": latencies,
            "latency_sampling": latency_sampling,
            "msgs_per_s": completed / window,
            **identity,
        },
    )
    return 0


def _send_loop(adapter, state, topic, qos, run_id, outstanding, target_rate, until, sequence_start=0):
    interval = (1.0 / target_rate) if target_rate and target_rate > 0 else 0.0
    open_loop = interval > 0
    next_send = time.perf_counter()
    seq = sequence_start
    n_offered = 0
    n_missed = 0
    measure = state["phase"] == "measure"
    while time.perf_counter() < until:
        if open_loop:
            now = time.perf_counter()
            if now < next_send:
                time.sleep(min(0.001, next_send - now))
                continue
            # Advance the offer clock for this slot before checking capacity —
            # same discipline as the publisher: a full window is a miss, not a
            # stalled paceur that would under-shoot target_rate.
            next_send += interval
            if len(state["inflight"]) >= outstanding:
                if measure:
                    n_offered += 1
                    n_missed += 1
                continue
        elif len(state["inflight"]) >= outstanding:
            # Closed-loop capacity: wait for a response to free a slot.
            time.sleep(0.0005)
            continue

        seq += 1
        send_ns = time.perf_counter_ns()
        payload = encode_header(run_id, 1, seq, seq, send_ns)

        # Register the correlation before entering the client API. A synchronous
        # or cross-thread publish handoff may give the network loop enough time
        # to receive the response before publish() returns; registering after the
        # call turns that valid response into an unmatchable orphan.
        with state["lock"]:
            state["publishing_seq"] = seq
            state["inflight"][seq] = send_ns
        info = adapter.publish(topic, payload=payload, qos=qos, retain=False)
        with state["lock"]:
            state["publishing_seq"] = None
            early = state["early_rtt"].pop(seq, None)
            # Every due slot that reached publish() is part of the offer, whether
            # the client accepted it or not — same as the publisher's offered
            # counter (which includes sync_rejected). Skipping refused publishes
            # under-counted offered and tripped the ±2% open-loop gate.
            if measure:
                n_offered += 1
            if info.rc == 0:
                if measure:
                    state["sent_in_window"] += 1
                    if early is not None:
                        state["latencies_ns"].add(early)
                        state["completed_in_window"] += 1
            else:
                # Publish refused. Drop any response that landed inside publish()
                # without ever committing it to the latency sample.
                state["inflight"].pop(seq, None)
                if early is not None and measure:
                    state["retracted_completions"] = int(state.get("retracted_completions") or 0) + 1

    if measure:
        state["offered"] = int(state.get("offered") or 0) + n_offered
        state["missed_due_to_backpressure"] = (
            int(state.get("missed_due_to_backpressure") or 0) + n_missed
        )


if __name__ == "__main__":
    raise SystemExit(main())
