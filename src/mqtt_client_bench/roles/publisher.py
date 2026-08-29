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
import asyncio
import functools
import gc
import json
import os
import threading
import time

from mqtt_client_bench.adapters.registry import (
    adapter_identity,
    create_adapter,
    create_async_adapter,
    has_async_adapter,
)
from mqtt_client_bench.control import barrier_client_session, touch, write_json
from mqtt_client_bench.sampling import (
    DEFAULT_COMPLETION_LOG_LIMIT,
    DEFAULT_METRIC_SAMPLE_LIMIT,
    DEFAULT_PAYLOAD_BACKLOG_BYTES,
    DEFAULT_SEQUENCE_EXACT_LIMIT,
    FAILED,
    NO_LATENCY,
    CompletionLog,
    ReservoirSampler,
    SequenceTracker,
    sequence_tracker,
    bound_payload_backlog,
)
from mqtt_client_bench.telemetry import MemoryGuard
from mqtt_client_bench.workloads import (
    HEADER_MAGIC,
    HEADER_SIZE,
    HEADER_STRUCT,
    build_payload,
    build_payload_corpus,
    encode_header,
    make_bytes_of_size,
    rl_boundary_payloads,
    payload_tail,
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
    metric_sample_limit = int(cfg.get("metric_sample_limit", DEFAULT_METRIC_SAMPLE_LIMIT))
    sequence_exact_limit = int(
        cfg.get("integrity_sequence_limit", DEFAULT_SEQUENCE_EXACT_LIMIT)
    )
    payload_backlog_limit = cfg.get(
        "max_harness_payload_bytes", DEFAULT_PAYLOAD_BACKLOG_BYTES
    )
    # publisher_only points have no subscriber to reconcile against, so the
    # harness tells the worker not to fingerprint what nobody will read.
    track_sequences = bool(cfg.get("track_sequences", True))
    completion_log_limit = int(cfg.get("completion_log_limit", DEFAULT_COMPLETION_LOG_LIMIT))
    if payload_backlog_limit is not None:
        payload_backlog_limit = int(payload_backlog_limit)

    # Build payload body.
    if payload_name.startswith("rl_"):
        sizes = rl_boundary_payloads(topic, qos=qos)
        body = make_bytes_of_size(sizes[payload_name], seed=1)
    else:
        raw = build_payload(payload_name, seed=1)
        body = raw.encode("utf-8") if isinstance(raw, str) else raw

    payload_len = len(body)
    payload_allocation_bytes = max(payload_len, HEADER_SIZE) if cfg.get("force_header") else payload_len
    payload_backlog = bound_payload_backlog(
        outstanding,
        payload_allocation_bytes,
        payload_backlog_limit,
    )
    outstanding = payload_backlog["effective_outstanding"]

    corpus = []
    if payload_name in ("telemetry256", "event1k", "binary64") and not payload_name.startswith("rl_"):
        corpus = build_payload_corpus(payload_name, count=64, seed=7)
        corpus = [c.encode("utf-8") if isinstance(c, str) else c for c in corpus]

    state = new_publisher_state(
        metric_sample_limit=metric_sample_limit,
        completion_log_limit=completion_log_limit,
    )

    def on_connect(client, userdata, flags, reason_code, properties=None):
        rc = int(getattr(reason_code, "value", reason_code))
        if rc == 0:
            state["connected"].set()

    # Byte-equivalent of the requested queue depth, for libraries that also bound
    # their outbound queue in bytes. Without it their effective window collapses
    # with payload size while the message-bounded clients keep the full depth.
    payload_bytes = len(body) if isinstance(body, (bytes, bytearray)) else len(str(body).encode())

    submit_count = cfg.get("submit_count")
    if submit_count is not None:
        submit_count = int(submit_count)

    # A client with a native path is driven on its own loop unless the point
    # explicitly asks otherwise. Which path ran is recorded in the result: a run
    # through the bridge and a native run are not comparable with each other.
    # Queue-rejection fires publish() without waiting: an awaited native path
    # would never fill the outbound queue, so that protocol stays on the facade.
    native = (
        bool(cfg.get("native_async", True))
        and has_async_adapter(client_name)
        and submit_count is None
    )
    build = create_async_adapter if native else create_adapter
    adapter = build(
        client_name,
        client_path=client_path,
        client_id=cfg.get("client_id", f"pub-{cfg['run_id']}"),
        protocol=protocol,
        clean_session=not bool(cfg.get("session_persistent", False)),
        max_inflight=inflight,
        max_queued=max_queued,
        max_queued_bytes=max_queued * max(1, payload_bytes),
        tls_ca_certs=cfg.get("ca_certs") if cfg.get("tls") else None,
    )
    driver = _AsyncDriver(adapter) if native else _SyncDriver(adapter)
    publish_path = "native_async" if native else "sync_facade"
    adapter.on_connect = on_connect
    adapter.on_publish = _make_on_publish(state, qos, lock=None if native else state["lock"])

    host = cfg["host"]
    port = int(cfg["port"])
    driver.connect(host, port, int(cfg.get("keepalive", 60)))
    if not driver.blocking(state["connected"].wait, 30):
        write_json(cfg["result_path"], {"ok": False, "error": "connect_timeout", **identity})
        driver.close()
        return 1

    touch(cfg["ready_path"], {"role": "publisher", "pid": os.getpid(), **identity})

    barrier = barrier_client_session(cfg["barrier_path"], timeout_s=float(cfg.get("barrier_timeout_s", 120)))
    driver.blocking(barrier.wait, "T0")

    open_loop_rate = None
    if cadence in ("steady50", "loaded75", "loaded90", "periodic10") or cfg.get("load_fraction"):
        if target_rate:
            open_loop_rate = float(target_rate)
        else:
            open_loop_rate = 1000.0 * load_fraction
        # steady50's ×0.50 is the scenario's own default offer, not a modifier
        # on a calibrated target_rate. Applying it after calibration silently
        # halved every integrity/latency point that carried both.
        if cadence == "steady50" and not target_rate:
            open_loop_rate = 2000.0 * 0.50
        elif cadence == "periodic10":
            open_loop_rate = 10.0

    # Cap worker RSS. Default is generous next to the ~180 MB a well-behaved
    # client uses on 1 MiB payloads, but far below what takes a host down.
    memory_guard = MemoryGuard(
        float(cfg.get("memory_limit_mb", 1536)), payload_bytes=payload_bytes
    )

    gc.collect()
    gc_start = gc.get_count()
    state["phase"] = "warmup"
    if submit_count is None:
        warmup_end = time.perf_counter() + warmup_s
        driver.run_loop(
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
            memory_guard=memory_guard,
            sequence_exact_limit=sequence_exact_limit,
            track_sequences=track_sequences,
        )
    else:
        # Do not fill the queue during warmup: the measurement is a single burst.
        driver.sleep(warmup_s)

    # Drain warmup outstanding; fail closed if still active when the deadline hits.
    drain_warmup = time.perf_counter() + min(drain_s, 5.0)
    while time.perf_counter() < drain_warmup:
        with state["lock"]:
            if state["inflight_local"] == 0 and not state["mid_send_ns"] and not state["early_acks"]:
                break
        driver.sleep(0.01)
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
            # Warmup completions are discarded exactly like the counters above.
            state["completions"].clear()
            state["overflow_success"] = 0
            state["overflow_failed"] = 0
            state["overflow_in_window"] = 0
            state["overflow_during_drain"] = 0
            state["fold_pending"] = False
            state["mid_send_ns"].clear()
            state["early_acks"].clear()
            state["inflight_local"] = 0
            state["seen_mids_inflight"].clear()

    driver.blocking(barrier.ack, "WARMUP_DRAINED")
    # Second barrier: all roles start measure together.
    driver.blocking(barrier.wait, "T_MEASURE")
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
        driver.close()
        return 1

    state["phase"] = "measure"
    # Process CPU across the measure window only. Deriving cost-per-message from
    # the orchestrator's telemetry samples instead would fold warmup and drain
    # CPU into a denominator that counts measure-window messages alone.
    cpu_ns_start = time.process_time_ns()
    t0 = time.perf_counter()
    measure_end = t0 + duration_s
    if submit_count is not None:
        measure_sequences = _run_submit_burst(
            adapter,
            state,
            topic=topic,
            qos=qos,
            body=body,
            corpus=corpus,
            run_id=run_id,
            submit_count=submit_count,
            properties_builder=_properties_builder(cfg, adapter),
            force_header=bool(cfg.get("force_header", False)),
            sequence_exact_limit=sequence_exact_limit,
            track_sequences=track_sequences,
        )
        t1 = time.perf_counter()
    else:
        measure_sequences = driver.run_loop(
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
            memory_guard=memory_guard,
            sequence_exact_limit=sequence_exact_limit,
            track_sequences=track_sequences,
        )
        t1 = min(time.perf_counter(), measure_end)
    cpu_ns_in_window = time.process_time_ns() - cpu_ns_start
    # One index is all the phase partition needs: everything logged after this
    # arrived during the drain. More accurate than the old test, too, which read
    # state["phase"] at callback time and so misfiled whatever was in flight
    # across the boundary. Idempotent with the awaited path, which closes at
    # the deadline before its grace period so grace completions are drain.
    state["completions"].close_window()

    state["phase"] = "drain"
    drain_deadline = time.perf_counter() + drain_s
    while time.perf_counter() < drain_deadline:
        with state["lock"]:
            inflight_local = state["inflight_local"]
            pending_mids = len(state["mid_send_ns"])
        if inflight_local == 0 and pending_mids == 0:
            break
        driver.sleep(0.01)

    with state["lock"]:
        backlog = state["inflight_local"]
        timed_out = backlog if qos == 0 else len(state["mid_send_ns"])
        # Every completion counter is derived here, outside the window, from the
        # log. The overflow_* terms are zero unless the log filled up and the
        # callback had to count live again.
        tally = state["completions"].summary(qos)
        completion_logging = {
            "logged": tally["logged"],
            "capacity": tally["capacity"],
            "folds": tally["folds"],
            "counted_live": state["overflow_success"] + state["overflow_failed"],
        }
        success = tally["completed_success"] + state["overflow_success"]
        # completed_failed on state is the live MID-collision counter: those
        # never go through CompletionLog, so they must be folded in here or
        # protocol_failed stays silent.
        failed = (
            tally["completed_failed"]
            + state["overflow_failed"]
            + int(state.get("completed_failed") or 0)
        )
        completed_in_window = tally["completed_in_window"] + state["overflow_in_window"]
        completed_during_drain = tally["completed_during_drain"] + state["overflow_during_drain"]
        latencies = state["latencies_ns"].snapshot()
        lags = state["scheduler_lags_ns"].snapshot()
        latency_sampling = state["latencies_ns"].metadata()
        scheduler_lag_sampling = state["scheduler_lags_ns"].metadata()
        counters = {
            "offered": state["offered"],
            "submitted": state["submitted"],
            "sync_rejected": state["sync_rejected"],
            "completed_success": success,
            "completed_failed": failed,
            "missed_due_to_backpressure": state["missed_due_to_backpressure"],
            "publish_calls": state["offered"],
            "publish_accepted": state["submitted"],
            "publish_rejected": state["sync_rejected"],
            "socket_completed_qos0": tally["socket_completed_qos0"],
            "protocol_completed": tally["protocol_completed"],
            "protocol_failed": failed,
            "mid_map_remaining": len(state["mid_send_ns"]),
            "warmup_drain_ok": state["warmup_drain_ok"],
        }

    driver.close()

    window = max(t1 - t0, 1e-9)
    sequence_summary = measure_sequences.summary()
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
        "publish_path": publish_path,
        "t0_s": t0,
        "t1_s": t1,
        "duration_s": window,
        "completed_in_window": completed_in_window,
        "completed_during_drain": completed_during_drain,
        "backlog_at_end": backlog,
        "timed_out": timed_out,
        "sent_sequence_start": sequence_summary["first"],
        "sent_sequence_end": sequence_summary["last"],
        "sent_sequence_count": sequence_summary["count"],
        "sent_sequences": measure_sequences.exact_values(),
        "sent_sequence_summary": sequence_summary,
        "msgs_per_s": completed_in_window / window,
        "payload_bytes_per_s": (completed_in_window * payload_len) / window,
        "cpu_ns_in_window": cpu_ns_in_window,
        "memory_guard_tripped_kb": state.get("memory_guard_tripped_kb"),
        # Already the retained subset: the reservoir bounds these upstream, so
        # the slices this used to carry are gone and the companion metadata
        # records how much was observed.
        "latencies_ns": latencies,
        "scheduler_lags_ns": lags,
        "latency_sampling": latency_sampling,
        "scheduler_lag_sampling": scheduler_lag_sampling,
        "harness_payload_backlog": payload_backlog,
        "completion_logging": completion_logging,
        "gc_count_start": list(gc_start),
        "gc_count_end": list(gc.get_count()),
        **identity,
        **counters,
    }
    if submit_count is not None:
        result["queue_accounting"] = {
            "submit_count": submit_count,
            "offered": counters["offered"],
            "accepted": counters["publish_accepted"],
            "rejected": counters["sync_rejected"],
            "expected_accepts": cfg.get("expected_accepts"),
            "expected_rejects": cfg.get("expected_rejects"),
        }
    write_json(cfg["result_path"], result)
    return 0



# How long a publish already awaiting an acknowledgement may run past the end of
# the window before it is cancelled. Short enough to bound the phase, long enough
# that a normal in-flight publish is never aborted.
_AWAITED_GRACE_S = 2.0


class _MemoryGuardTripped(Exception):
    """Internal: unwinds the async publish loop through its counter flush."""


def _wake_gate(state) -> None:
    """Release the async loop parked on the outstanding gate.

    Only the async path ever parks; on the sync path the slot stays empty and
    this is one dict lookup per completion.
    """
    fut = state.get("gate_waiter")
    if fut is not None:
        state["gate_waiter"] = None
        if not fut.done():
            fut.set_result(None)


def new_publisher_state(
    *,
    metric_sample_limit: int = DEFAULT_METRIC_SAMPLE_LIMIT,
    completion_log_limit: int = DEFAULT_COMPLETION_LOG_LIMIT,
) -> dict:
    """The publisher's counter block, built in one place.

    The host calibrator drives the same publish loop against a null client to
    price the harness itself, and the unit tests drive it against fakes. All
    three must see the state the production worker sees: a counter that exists
    here but not there would price a loop nobody runs.
    """
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
        "latencies_ns": ReservoirSampler(metric_sample_limit, seed=11),
        "scheduler_lags_ns": ReservoirSampler(metric_sample_limit, seed=29),
        "lock": threading.Lock(),
        "inflight_local": 0,
        "phase": "init",
        "mid_send_ns": {},
        # Callbacks that arrive before mid_send_ns registration land here.
        "early_acks": {},
        "warmup_drain_ok": True,
        "seen_mids_inflight": set(),
        # Async path only: the future the loop parks on, the deadline flag, and
        # the two slots that let a publish acknowledged inside its own call
        # complete without going through the early-ack dictionaries.
        "gate_waiter": None,
        "loop_expired": False,
        "pending_send_ns": None,
        "completed_inline": None,
        # Completions are logged, not counted, inside the measure window.
        # The overflow_* counters only move if the log fills up.
        "completions": None,  # needs the latency sampler; set just below
        "overflow_success": 0,
        "overflow_failed": 0,
        "overflow_in_window": 0,
        "overflow_during_drain": 0,
        "fold_pending": False,
    }
    # The log hands folded latencies straight to the sampler, so a fold loses
    # nothing: the sampler keeps its own bounded copy and the buffer is reused.
    state["completions"] = CompletionLog(completion_log_limit, sampler=state["latencies_ns"])
    return state


def _make_on_publish(state, qos, *, lock):
    """Completion handler shared by both paths.

    `lock` is None on the async path: the loop, the transport and this callback
    all run on the same thread, so there is nothing to serialise against and an
    uncontended acquire per message would be pure harness tax on exactly the
    clients this path exists to stop taxing.

    Everything the hot path touches is bound here as a closure local. A dict
    lookup per message is only tens of nanoseconds, but this runs once per
    message on the fastest client in the field, and the whole point of the
    exercise is that the harness must not cost more where it is reached more
    often. Both branches are written out rather than sharing a helper for the
    same reason: whatever this costs, it must cost the same on both.
    """
    mid_send_ns = state["mid_send_ns"]
    early_acks = state["early_acks"]
    seen_inflight = state["seen_mids_inflight"]
    log_add = state["completions"].add

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        now = time.perf_counter_ns()
        failed = False
        if reason_code is not None:
            rc = int(getattr(reason_code, "value", reason_code))
            if rc >= 128:
                failed = True
        if lock is None:
            send_ns = mid_send_ns.pop(mid, None)
            if send_ns is None:
                pending = state["pending_send_ns"]
                if pending is None:
                    early_acks[mid] = (now, failed)
                    return
                # Acknowledged inside the publish call itself, which is what a
                # QoS 0 transport hand-off looks like. The mid the loop is about
                # to see is this one, so complete it here and tell the loop to
                # skip registering it: the dict insert, the dict pop and the
                # in-flight set churn were all pure harness cost.
                state["completed_inline"] = mid
                send_ns = pending
            else:
                seen_inflight.discard(mid)
            if not log_add(FAILED if failed else now - send_ns):
                _count_completion_live(state, qos, FAILED if failed else now - send_ns)
            inflight = state["inflight_local"] - 1
            state["inflight_local"] = inflight if inflight > 0 else 0
            # A freed slot has to wake the loop parked on the gate. Dropping this
            # left the loop asleep after the first full window: QoS 1 offered
            # exactly `outstanding` messages and then nothing until the deadline
            # timer fired. QoS 0 hid it, because a completion that lands inside
            # the publish call never parks in the first place.
            if state["gate_waiter"] is not None:
                _wake_gate(state)
            return
        with lock:
            send_ns = mid_send_ns.pop(mid, None)
            if send_ns is None:
                # Callback raced ahead of publish() return - stash until registered.
                early_acks[mid] = (now, failed)
                return
            seen_inflight.discard(mid)
            if not log_add(FAILED if failed else now - send_ns):
                _count_completion_live(state, qos, FAILED if failed else now - send_ns)
            inflight = state["inflight_local"] - 1
            state["inflight_local"] = inflight if inflight > 0 else 0

    return on_publish


class _SyncDriver:
    """Drives an adapter through the sync facade and its bridge thread."""

    native_async = False

    def __init__(self, adapter):
        self.adapter = adapter

    def connect(self, host, port, keepalive):
        self.adapter.connect(host, port, keepalive=keepalive)
        self.adapter.loop_start()

    def blocking(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def sleep(self, seconds):
        time.sleep(seconds)

    def run_loop(self, state, **kwargs):
        return _run_publish_loop(self.adapter, state, **kwargs)

    def close(self):
        self.adapter.disconnect()
        self.adapter.loop_stop()


class _AsyncDriver:
    """Drives an async adapter on this thread's own loop - no bridge at all.

    The bridge it replaces cost a measured 18.5 us per message. That is a fixed
    cost, so it compressed the field: at 25,000 msgs/s it inflated a client's
    period by 46%, at 6,000 msgs/s by 11%. Removing it is not an optimisation,
    it is the difference between comparing libraries and comparing them through
    a filter that favours the slow ones.
    """

    native_async = True

    def __init__(self, adapter):
        self.adapter = adapter
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def connect(self, host, port, keepalive):
        self.loop.run_until_complete(self.adapter.connect(host, port, keepalive=keepalive))

    def blocking(self, fn, *args, **kwargs):
        # Barrier waits and connect acks would otherwise stall this thread with
        # nobody reading the socket - a 30 s wait is a keepalive timeout. Hand
        # them to a helper thread and keep the loop turning.
        call = functools.partial(fn, *args, **kwargs)
        return self.loop.run_until_complete(self.loop.run_in_executor(None, call))

    def sleep(self, seconds):
        self.loop.run_until_complete(asyncio.sleep(seconds))

    def run_loop(self, state, **kwargs):
        return self.loop.run_until_complete(
            _run_publish_loop_async(self.adapter, state, **kwargs)
        )

    def close(self):
        self.loop.run_until_complete(self.adapter.disconnect())
        self.loop.close()


def _consume_completion_locked(state, qos, send_ns, now, failed: bool, *, mid) -> None:
    """Record one completion. Everything derivable is derived after the window.

    Only the in-flight gate and the duplicate-mid guard have to be live. The
    counters used to be maintained here, once per message, inside the measure
    window - about 1.45 us of harness cost per message, charged to whichever
    client was fast enough to reach it often. They are now read off the log
    when the run is over, from the buffer and the index where the window closed.
    """
    state["seen_mids_inflight"].discard(mid)
    if failed:
        value = FAILED
    elif send_ns is None:
        value = NO_LATENCY
    else:
        value = now - send_ns
    if not state["completions"].add(value):
        _count_completion_live(state, qos, value)
    inflight = state["inflight_local"] - 1
    state["inflight_local"] = inflight if inflight > 0 else 0


def _count_completion_live(state, qos, value) -> None:
    """Count the handful of completions between a full buffer and its fold.

    The callback must not fold: that is a scan of the whole buffer, and doing it
    here would put a millisecond-scale pause inside one message's completion.
    It raises the flag instead, the publish loop folds at its next batch
    boundary, and these few land in the running totals directly.
    """
    state["fold_pending"] = True
    if value == FAILED:
        state["overflow_failed"] += 1
        return
    state["overflow_success"] += 1
    if value >= 0:
        state["latencies_ns"].add(value)
    # Follow the log's window cut, not state["phase"]: the awaited grace still
    # leaves phase=measure while close_window has already fired at the deadline.
    if state["completions"].window_closed or state["phase"] == "drain":
        state["overflow_during_drain"] += 1
    else:
        state["overflow_in_window"] += 1


def _run_submit_burst(
    adapter,
    state,
    *,
    topic,
    qos,
    body,
    corpus,
    run_id,
    submit_count,
    properties_builder,
    force_header=False,
    sequence_exact_limit=DEFAULT_SEQUENCE_EXACT_LIMIT,
    track_sequences=True,
):
    """Fire ``submit_count`` publishes without waiting for completions.

    This is the queue-rejection protocol: the outstanding gate would hide the
    queue, so every call is issued immediately and accept/reject is the score.
    """
    stamp = _make_stamper(body, corpus, run_id, force_header)
    sent_sequences = sequence_tracker(sequence_exact_limit, enabled=track_sequences)
    lock = state["lock"]
    n_offered = n_calls = n_submitted = 0
    n_sync_rejected = n_accepted = n_rejected = 0
    for sequence in range(1, int(submit_count) + 1):
        send_ns = time.perf_counter_ns()
        payload = stamp(sequence, send_ns)
        props = properties_builder()
        n_offered += 1
        n_calls += 1
        info = adapter.publish(topic, payload=payload, qos=qos, retain=False, properties=props)
        if info.rc == 0 and (qos == 0 or info.mid is not None):
            with lock:
                mid = info.mid
                if mid is not None:
                    if mid in state["seen_mids_inflight"]:
                        state["completed_failed"] += 1
                        state["protocol_failed"] += 1
                    early = state["early_acks"].pop(mid, None)
                    n_submitted += 1
                    n_accepted += 1
                    state["inflight_local"] += 1
                    state["seen_mids_inflight"].add(mid)
                    if early is not None:
                        early_now, early_failed = early
                        state["mid_send_ns"].pop(mid, None)
                        _consume_completion_locked(
                            state, qos, send_ns, early_now, early_failed, mid=mid
                        )
                    else:
                        state["mid_send_ns"][mid] = send_ns
                else:
                    n_submitted += 1
                    n_accepted += 1
                sent_sequences.add(sequence)
        else:
            n_sync_rejected += 1
            n_rejected += 1
    state["offered"] += n_offered
    state["publish_calls"] += n_calls
    state["submitted"] += n_submitted
    state["sync_rejected"] += n_sync_rejected
    state["publish_accepted"] += n_accepted
    state["publish_rejected"] += n_rejected
    return sent_sequences


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
    memory_guard=None,
    sequence_start=0,
    sequence_exact_limit=DEFAULT_SEQUENCE_EXACT_LIMIT,
    track_sequences=True,
):
    sequence = sequence_start
    sent_sequences = sequence_tracker(sequence_exact_limit, enabled=track_sequences)
    # The body is fixed for the run; only the header changes. Slicing it per
    # message cost two full-payload allocations each time (about 1 ms on 1 MiB),
    # so the tails are cut once here and the loop only concatenates. Indexed in
    # parallel with the corpus rather than keyed by identity, which a collected
    # and reused id() would silently corrupt.
    # Every `state[...]` on the hot path is a dict lookup the interpreter repeats
    # per message. The lock object and the counters only this thread writes are
    # bound to locals and flushed back once, in a finally so no exit path can
    # skip it. Shared counters stay in `state`: on_publish writes them.
    lock = state["lock"]
    n_offered = n_calls = n_submitted = 0
    n_sync_rejected = n_accepted = n_rejected = n_missed = 0
    body_tail = payload_tail(body) if isinstance(body, (bytes, bytearray)) else None
    corpus_tails = [
        payload_tail(c) if isinstance(c, (bytes, bytearray)) else None for c in corpus
    ]
    loop_start = time.perf_counter()
    next_send = loop_start
    interval = (1.0 / target_rate) if target_rate and target_rate > 0 else 0.0
    corpus_i = 0
    open_loop = target_rate is not None and cadence not in ("capacity", "burst", "microburst", "batch64")

    perf = time.perf_counter          # bound once: looked up per iteration otherwise
    # Stamp the header inline: encode_header re-validates an invariant run_id and
    # costs a call plus a tuple build on every message. The masks it applied are
    # kept, since sequence and send_ns are the only values that can grow.
    pack_header = HEADER_STRUCT.pack
    header_magic = HEADER_MAGIC
    is_bursty = cadence in ("burst", "microburst")
    batch_n = batch_size if cadence == "batch64" else 1
    while perf() < until:
        if is_bursty:
            period, duty = (1.0, 0.1) if cadence == "burst" else (0.1, 0.01)
            phase = (perf() - loop_start) % period
            if phase > duty:
                time.sleep(min(0.001, period - phase))
                continue

        if open_loop:
            now = perf()
            if now < next_send:
                time.sleep(min(0.001, next_send - now))
                continue
            lag_ns = int((now - next_send) * 1e9)
            with lock:
                state["scheduler_lags_ns"].add(lag_ns)
            next_send += interval

        if state["fold_pending"]:
            with lock:
                state["fold_pending"] = False
                state["completions"].fold()

        for _ in range(batch_n):
            if perf() >= until:
                break
            # A QoS0 path that completes at an in-process queue gives the
            # outstanding gate nothing to hold back, so this is the only thing
            # standing between a 1 MiB fire-and-forget loop and the host's RAM.
            if memory_guard is not None and memory_guard.exceeded():
                state["memory_guard_tripped_kb"] = memory_guard.tripped_at_kb
                state["offered"] += n_offered
                state["publish_calls"] += n_calls
                state["submitted"] += n_submitted
                state["sync_rejected"] += n_sync_rejected
                state["publish_accepted"] += n_accepted
                state["publish_rejected"] += n_rejected
                state["missed_due_to_backpressure"] += n_missed
                return sent_sequences

            # Plain int read: atomic under the GIL, and a stale-by-one value only
            # shifts the gate by one message. Taking the lock here would add a
            # per-message acquire on the hot path for no accuracy gain.
            if state["inflight_local"] >= outstanding:
                # Outstanding gate applies to ALL cadences. Open-loop counts a miss
                # instead of spawning unbounded work.
                if open_loop:
                    # Publisher-thread-only counters (see the offered/publish_calls
                    # note below): no lock needed.
                    n_offered += 1
                    n_missed += 1
                    n_calls += 1
                    continue
                time.sleep(0.0001)
                break

            sequence += 1
            send_ns = time.perf_counter_ns()
            header = pack_header(
                header_magic, run_id, 1,
                sequence & 0xFFFFFFFFFFFFFFFF,
                sequence & 0xFFFFFFFFFFFFFFFF,
                send_ns & 0xFFFFFFFFFFFFFFFF,
            )
            if corpus:
                idx = corpus_i % len(corpus)
                payload_body = corpus[idx]
                tail = corpus_tails[idx]
                corpus_i += 1
            else:
                payload_body = body
                tail = body_tail
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
                    payload = (header + tail) if tail is not None else wrap_with_header(payload_body, header)
                elif len(payload_body) == 0:
                    payload = header if force_header else b""
                else:
                    payload = payload_body

            props = properties_builder()
            # One critical section per publish: rebuilding trackers or taking the
            # lock several times per message is harness cost that scales with the
            # client's own rate, which compresses inter-client ratios.
            # offered/publish_calls/submitted/sync_rejected are written only by this
            # thread (on_publish never touches them), so they need no lock; the final
            # snapshot reads them under the lock.
            n_offered += 1
            n_calls += 1
            info = adapter.publish(topic, payload=payload, qos=qos, retain=False, properties=props)
            with lock:
                if info.rc == 0 and info.mid is not None:
                    mid = info.mid
                    if mid in state["seen_mids_inflight"]:
                        # Synthetic MID collision while still inflight — treat as failure signal.
                        state["completed_failed"] += 1
                        state["protocol_failed"] += 1
                    early = state["early_acks"].pop(mid, None)
                    n_submitted += 1
                    n_accepted += 1
                    state["inflight_local"] += 1
                    # seen_mids_inflight is maintained incrementally: added here,
                    # discarded by _consume_completion_locked on completion.
                    state["seen_mids_inflight"].add(mid)
                    if early is not None:
                        early_now, early_failed = early
                        state["mid_send_ns"].pop(mid, None)
                        _consume_completion_locked(state, qos, send_ns, early_now, early_failed, mid=mid)
                    else:
                        state["mid_send_ns"][mid] = send_ns
                    sent_sequences.add(sequence)
                else:
                    n_sync_rejected += 1
                    n_rejected += 1
    state["offered"] += n_offered
    state["publish_calls"] += n_calls
    state["submitted"] += n_submitted
    state["sync_rejected"] += n_sync_rejected
    state["publish_accepted"] += n_accepted
    state["publish_rejected"] += n_rejected
    state["missed_due_to_backpressure"] += n_missed
    return sent_sequences


def _make_stamper(body, corpus, run_id, force_header):
    """Build the per-message payload closure once, outside the loop.

    The payload's *shape* - bytes or str, shorter or longer than the header,
    corpus or single body - is fixed for the whole run, so it is resolved here
    and the returned closure carries no branch on it. Re-deciding it per message
    cost 811 ns; the specialised forms cost about a third of that, and at 25,000
    msgs/s that difference is 1% of the client's entire period.
    """
    pack_header = HEADER_STRUCT.pack
    header_magic = HEADER_MAGIC
    mask = 0xFFFFFFFFFFFFFFFF

    if corpus:
        tails = [payload_tail(c) if isinstance(c, (bytes, bytearray)) else None for c in corpus]
        bodies = list(corpus)
        n = len(bodies)
        cursor = [0]

        def stamp_corpus(sequence, send_ns):
            masked = sequence & mask
            header = pack_header(header_magic, run_id, 1, masked, masked, send_ns & mask)
            idx = cursor[0]
            cursor[0] = 0 if idx + 1 >= n else idx + 1
            tail = tails[idx]
            return (header + tail) if tail is not None else wrap_with_header(bodies[idx], header)

        return stamp_corpus

    if isinstance(body, str):
        if not force_header:
            return lambda sequence, send_ns: body
        raw = body.encode("utf-8")

        def stamp_str(sequence, send_ns):
            masked = sequence & mask
            header = pack_header(header_magic, run_id, 1, masked, masked, send_ns & mask)
            return wrap_with_header(raw if len(raw) >= HEADER_SIZE else header + raw, header)

        return stamp_str

    if len(body) >= HEADER_SIZE:
        tail = payload_tail(body)

        def stamp_bytes(sequence, send_ns):
            masked = sequence & mask
            return pack_header(header_magic, run_id, 1, masked, masked, send_ns & mask) + tail

        return stamp_bytes

    if not force_header:
        # Too short to carry a header and nobody asked for one: nothing to stamp.
        return lambda sequence, send_ns: body

    def stamp_short(sequence, send_ns):
        masked = sequence & mask
        return pack_header(header_magic, run_id, 1, masked, masked, send_ns & mask)

    return stamp_short


def _account_awaited(state, qos, send_ns, now, failed) -> None:
    """Completion accounting for the awaited shape.

    An awaited publish *is* its own completion, so there is no mid to correlate
    and no early-ack window. Same log, same derivation as every other path -
    the harness must not cost one shape more than another.
    """
    value = FAILED if failed else (now - send_ns)
    if not state["completions"].add(value):
        _count_completion_live(state, qos, value)


async def _run_publish_loop_async(adapter, state, **kwargs):
    """Dispatch to the shape this client's API actually supports.

    Resolved once per phase, never per message. Both shapes keep the same
    contract as the sync loop - same completion definition, same outstanding
    window, same counters - and differ only in how the library lets a caller
    keep more than one publish in flight.
    """
    if adapter.capabilities().publish_sync_on_loop:
        return await _publish_loop_sync_on_loop(adapter, state, **kwargs)
    return await _publish_loop_awaited(adapter, state, **kwargs)


async def _publish_loop_sync_on_loop(
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
    memory_guard=None,
    sequence_start=0,
    sequence_exact_limit=DEFAULT_SEQUENCE_EXACT_LIMIT,
    track_sequences=True,
):
    """One coroutine, for libraries that admit a publish on the loop.

    The sync loop's twin: same contract, same counters, same gate. What is gone
    is the thread boundary - no lock per message, no cross-thread wakeup, no
    coroutine allocated per message.
    """
    sequence = sequence_start
    sent_sequences = sequence_tracker(sequence_exact_limit, enabled=track_sequences)
    n_offered = n_calls = n_submitted = 0
    n_sync_rejected = n_accepted = n_rejected = n_missed = 0
    stamp = _make_stamper(body, corpus, run_id, force_header)
    loop_start = time.perf_counter()
    next_send = loop_start
    interval = (1.0 / target_rate) if target_rate and target_rate > 0 else 0.0
    open_loop = target_rate is not None and cadence not in ("capacity", "burst", "microburst", "batch64")

    perf = time.perf_counter
    is_bursty = cadence in ("burst", "microburst")
    batch_n = batch_size if cadence == "batch64" else 1

    loop = asyncio.get_running_loop()
    publish_nowait = adapter.publish_nowait
    mid_send_ns = state["mid_send_ns"]
    early_acks = state["early_acks"]
    seen_inflight = state["seen_mids_inflight"]
    lag_add = state["scheduler_lags_ns"].add

    # One timer for the whole loop, not one per park: a client that stops
    # completing must not leave the loop parked past the window's end.
    state["loop_expired"] = False

    def _expire():
        state["loop_expired"] = True
        # Close here, not when main() returns: any completion callback already
        # queued on this loop turn after the deadline must land in drain, same
        # contract as the awaited path's _stop_at_deadline. Leaving the window
        # open until run_loop returns credited those acks to sync-on-loop
        # clients only.
        state["completions"].close_window()
        _wake_gate(state)

    expiry = loop.call_later(max(0.0, until - perf()), _expire)

    # A publisher that never parks on the gate - QoS 0 completes inside the
    # publish call - would never hand control back, so the transport would never
    # be read. Yield once per outstanding-window of messages: the scenario
    # already declares that window, so this is not a new tuning knob.
    yield_every = max(1, outstanding)
    since_yield = 0

    try:
        # Both conditions on purpose: the flag is what a completion-starved loop
        # would never see, since the timer that sets it can only fire if the loop
        # gets control. The deadline read costs ~40 ns per outer iteration and
        # removes any dependency on that.
        while not state["loop_expired"] and perf() < until:
            if is_bursty:
                period, duty = (1.0, 0.1) if cadence == "burst" else (0.1, 0.01)
                phase = (perf() - loop_start) % period
                if phase > duty:
                    await asyncio.sleep(min(0.001, period - phase))
                    since_yield = 0
                    continue

            if open_loop:
                now = perf()
                if now < next_send:
                    await asyncio.sleep(min(0.001, next_send - now))
                    since_yield = 0
                    continue
                lag_add(int((now - next_send) * 1e9))
                next_send += interval

            for _ in range(batch_n):
                if state["loop_expired"] or perf() >= until:
                    break
                if memory_guard is not None and memory_guard.exceeded():
                    state["memory_guard_tripped_kb"] = memory_guard.tripped_at_kb
                    raise _MemoryGuardTripped

                if state["inflight_local"] >= outstanding:
                    if open_loop:
                        n_offered += 1
                        n_missed += 1
                        n_calls += 1
                        continue
                    # Park until a completion frees a slot. This is the steady
                    # state of a closed-loop capacity run, and the same shape a
                    # native application would use.
                    fut = loop.create_future()
                    state["gate_waiter"] = fut
                    try:
                        await fut
                    finally:
                        state["gate_waiter"] = None
                    since_yield = 0
                    break

                sequence += 1
                send_ns = time.perf_counter_ns()
                payload = stamp(sequence, send_ns)
                props = properties_builder()
                n_offered += 1
                n_calls += 1
                state["inflight_local"] += 1
                state["pending_send_ns"] = send_ns
                mid = publish_nowait(topic, payload, qos, False, props)
                state["pending_send_ns"] = None

                if mid is None:
                    # Not admitted (queue / write-pump full). That is
                    # backpressure, not a failed completion: firing on_publish
                    # rc=128 here made mqttium native the only client whose
                    # large-payload QoS0 runs came back protocol_failed.
                    state["inflight_local"] -= 1
                    sequence -= 1
                    if open_loop:
                        # One attempt per tick. Retrying in this interval would
                        # offer above target_rate.
                        n_missed += 1
                        break
                    n_sync_rejected += 1
                    n_rejected += 1
                    # QoS0 completes inline on success, so this loop otherwise
                    # never awaits and the write pump never drains.
                    await asyncio.sleep(0)
                    since_yield = 0
                    continue
                n_submitted += 1
                n_accepted += 1
                if state["completed_inline"] == mid:
                    state["completed_inline"] = None
                    sent_sequences.add(sequence)
                    since_yield += 1
                    if since_yield >= yield_every:
                        since_yield = 0
                        await asyncio.sleep(0)
                    continue
                if mid in seen_inflight:
                    # Synthetic MID collision while still inflight.
                    state["completed_failed"] += 1
                    state["protocol_failed"] += 1
                early = early_acks.pop(mid, None)
                seen_inflight.add(mid)
                if early is not None:
                    early_now, early_failed = early
                    mid_send_ns.pop(mid, None)
                    _consume_completion_locked(state, qos, send_ns, early_now, early_failed, mid=mid)
                else:
                    mid_send_ns[mid] = send_ns
                sent_sequences.add(sequence)

                since_yield += 1
                if since_yield >= yield_every:
                    since_yield = 0
                    if state["fold_pending"]:
                        state["fold_pending"] = False
                        state["completions"].fold()
                    await asyncio.sleep(0)
    except _MemoryGuardTripped:
        pass
    finally:
        expiry.cancel()
        _wake_gate(state)
        state["offered"] += n_offered
        state["publish_calls"] += n_calls
        state["submitted"] += n_submitted
        state["sync_rejected"] += n_sync_rejected
        state["publish_accepted"] += n_accepted
        state["publish_rejected"] += n_rejected
        state["missed_due_to_backpressure"] += n_missed
    return sent_sequences


async def _publish_loop_awaited(
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
    memory_guard=None,
    sequence_start=0,
    sequence_exact_limit=DEFAULT_SEQUENCE_EXACT_LIMIT,
    track_sequences=True,
):
    """`outstanding` concurrent workers, for await-only publish APIs.

    Awaiting publishes one after another would pin the window at 1 and measure
    round-trip time instead of capacity. The window is what the scenario asks
    for, so it becomes the worker count: `outstanding` coroutines, each awaiting
    its own publish. That is how an application with this API gets concurrency,
    and it costs one reused coroutine per slot rather than one task per message.
    """
    sent_sequences = sequence_tracker(sequence_exact_limit, enabled=track_sequences)
    stamp = _make_stamper(body, corpus, run_id, force_header)
    perf = time.perf_counter
    loop_start = perf()
    interval = (1.0 / target_rate) if target_rate and target_rate > 0 else 0.0
    open_loop = target_rate is not None and cadence not in ("capacity", "burst", "microburst", "batch64")
    is_bursty = cadence in ("burst", "microburst")
    publish = adapter.publish
    lag_add = state["scheduler_lags_ns"].add

    counters = {
        "offered": 0, "calls": 0, "submitted": 0,
        "sync_rejected": 0, "accepted": 0, "rejected": 0, "missed": 0,
    }
    cursor = {"sequence": sequence_start, "next_send": loop_start, "stop": False, "since_yield": 0}
    # `await publish(...)` is not a guarantee that the loop gets to run: a QoS 0
    # path that only enqueues has no suspension point, so a worker can spin
    # through its whole window without the client's own writer task ever being
    # scheduled - its outbound queue then grows without bound. Measured: amqtt
    # QoS 0 tripped the 1.5 GB memory guard where the bridged path, which
    # crossed a thread and therefore always yielded, sustained 10,315 msgs/s.
    # One yield per outstanding-window of messages, same discipline as the
    # sync-on-loop shape.
    yield_every = max(1, outstanding)

    async def worker():
        while not cursor["stop"] and perf() < until:
            if is_bursty:
                period, duty = (1.0, 0.1) if cadence == "burst" else (0.1, 0.01)
                phase = (perf() - loop_start) % period
                if phase > duty:
                    await asyncio.sleep(min(0.001, period - phase))
                    continue

            if open_loop:
                now = perf()
                slot = cursor["next_send"]
                if now < slot:
                    await asyncio.sleep(min(0.001, slot - now))
                    continue
                # Claim exactly one interval. No await between the read and the
                # write, so cooperative multitasking cannot double-claim a slot.
                # Catch-up from sleep resolution advances one interval per loop
                # (same as sync-on-loop): jumping the cursor was 9e1eab5 and
                # permanently under-shot the target.
                lag_add(int((now - slot) * 1e9))
                cursor["next_send"] = slot + interval

            if memory_guard is not None and memory_guard.exceeded():
                state["memory_guard_tripped_kb"] = memory_guard.tripped_at_kb
                cursor["stop"] = True
                return

            sequence = cursor["sequence"] + 1
            cursor["sequence"] = sequence
            send_ns = time.perf_counter_ns()
            payload = stamp(sequence, send_ns)
            props = properties_builder()
            counters["offered"] += 1
            counters["calls"] += 1
            counters["submitted"] += 1
            counters["accepted"] += 1
            state["inflight_local"] += 1
            failed = False
            try:
                await publish(topic, payload, qos, False, props)
            except asyncio.CancelledError:
                # The slot has to be released on every exit path: a cancelled
                # worker that left its slot held made the warmup drain time out,
                # which is a harness fault reported as a client fault.
                state["inflight_local"] -= 1
                raise
            except Exception:  # noqa: BLE001
                failed = True
            state["inflight_local"] -= 1
            _account_awaited(state, qos, send_ns, time.perf_counter_ns(), failed)
            sent_sequences.add(sequence)

            # After a publish that held this worker, the shared paceur may have
            # fallen behind. Sync-on-loop charges those intervals as misses
            # while the outstanding window is full; do the same here so the two
            # shapes cannot disagree on offered/missed under backpressure. Do
            # not burst-publish the backlog — that biased latency samples.
            #
            # Cap the catch-up at `until` (measure deadline), not wall-clock
            # after stop: workers still awaiting at T1 finish during grace, and
            # skipping the block when cursor["stop"] is set under-counted
            # measure-window misses. Slots after `until` are drain, not offer.
            if open_loop and interval > 0:
                now = min(perf(), until)
                behind = now - cursor["next_send"]
                if behind > interval:
                    missed_slots = int(behind / interval)
                    counters["missed"] += missed_slots
                    counters["offered"] += missed_slots
                    counters["calls"] += missed_slots
                    cursor["next_send"] += missed_slots * interval

            cursor["since_yield"] += 1
            if cursor["since_yield"] >= yield_every:
                cursor["since_yield"] = 0
                if state["fold_pending"]:
                    state["fold_pending"] = False
                    state["completions"].fold()
                await asyncio.sleep(0)

    loop = asyncio.get_running_loop()
    workers = [asyncio.ensure_future(worker()) for _ in range(max(1, outstanding))]

    def _stop_at_deadline() -> None:
        cursor["stop"] = True
        # Grace completions below must land in drain, not in the measure window.
        state["completions"].close_window()

    # Close the window at the deadline, then give publishes already awaiting an
    # acknowledgement a bounded grace to land. Cancelling at the deadline
    # instead would abort one publish per worker mid-flight and charge the
    # client for it; leaving the window open through grace would inflate
    # completed_in_window for awaited clients only.
    stopper = loop.call_later(max(0.0, until - perf()), _stop_at_deadline)
    try:
        await asyncio.wait(workers, timeout=max(0.0, until - perf()) + _AWAITED_GRACE_S)
    finally:
        stopper.cancel()
        cursor["stop"] = True
        for task in workers:
            if not task.done():
                task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        state["offered"] += counters["offered"]
        state["publish_calls"] += counters["calls"]
        state["submitted"] += counters["submitted"]
        state["sync_rejected"] += counters["sync_rejected"]
        state["publish_accepted"] += counters["accepted"]
        state["publish_rejected"] += counters["rejected"]
        state["missed_due_to_backpressure"] += counters["missed"]
    return sent_sequences


if __name__ == "__main__":
    raise SystemExit(main())
