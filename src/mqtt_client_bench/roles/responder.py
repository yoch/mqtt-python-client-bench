"""
Responder worker for application RTT measurements.

Subscribes to request topic and republishes payload to response topic.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import json
import os
import threading
import time

from mqtt_client_bench.adapters.registry import adapter_identity
from mqtt_client_bench.control import barrier_client_session, touch, write_json
from mqtt_client_bench.roles.rtt_drive import (
    adapter_library_snapshot,
    drive_identity,
    process_runtime_snapshot,
    require_native_for_async_peer,
    select_rtt_drive,
)


def note_request(state: dict) -> None:
    with state["lock"]:
        state["requests_received"] = int(state.get("requests_received") or 0) + 1


def admit_echo(state: dict, mid) -> None:
    """Count an echo only when the library admitted it (mid is not None)."""
    with state["lock"]:
        if mid is None:
            state["echo_refused"] = int(state.get("echo_refused") or 0) + 1
        else:
            state["responses"] = int(state.get("responses") or 0) + 1


def note_echo_error(state: dict) -> None:
    with state["lock"]:
        state["echo_errors"] = int(state.get("echo_errors") or 0) + 1


def responder_result_fields(state: dict, *, pending_at_end: int = 0) -> dict:
    """Persist the echo identity: received ≈ responses + refused + errors + pending."""
    with state["lock"]:
        received = int(state.get("requests_received") or 0)
        responses = int(state.get("responses") or 0)
        echo_refused = int(state.get("echo_refused") or 0)
        echo_errors = int(state.get("echo_errors") or 0)
    pending = int(pending_at_end)
    accounted = responses + echo_refused + echo_errors + pending
    return {
        "requests_received": received,
        "responses": responses,
        "echo_refused": echo_refused,
        "echo_errors": echo_errors,
        "pending_at_end": pending,
        "echo_accounted": accounted,
        "echo_unaccounted": received - accounted,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    client_name = cfg.get("client", "paho")
    client_path = cfg.get("client_path")
    want_native = bool(cfg.get("native_async", True))
    plan = select_rtt_drive(client_name, native_async=want_native)
    if want_native:
        require_native_for_async_peer(client_name, plan)
    identity = drive_identity(plan, adapter_identity(client_name, client_path))

    request_topic = cfg["request_topic"]
    response_topic = cfg["response_topic"]
    qos = int(cfg.get("qos_subscribe", 1))
    protocol = cfg.get("protocol", "MQTTv311")

    state = {
        "connected": threading.Event(),
        "subscribed": threading.Event(),
        "responses": 0,
        "requests_received": 0,
        "echo_refused": 0,
        "echo_errors": 0,
        "pending": set(),
        "lock": threading.Lock(),
        "sub_mid": None,
        "loop": None,
    }

    if plan["native_async"]:
        return _run_native(cfg, plan, identity, state, request_topic, response_topic, qos, protocol)
    return _run_facade(cfg, plan, identity, state, request_topic, response_topic, qos, protocol)


def _run_facade(cfg, plan, identity, state, request_topic, response_topic, qos, protocol) -> int:
    adapter = plan["builder"](
        cfg.get("client", "paho"),
        client_path=cfg.get("client_path"),
        client_id=cfg.get("client_id", f"resp-{cfg['run_id']}"),
        protocol=protocol,
        clean_session=True,
        tls_ca_certs=cfg.get("ca_certs") if cfg.get("tls") else None,
    )

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if int(getattr(reason_code, "value", reason_code)) == 0:
            state["connected"].set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        ok = True
        for item in reason_code_list:
            if int(getattr(item, "value", item)) >= 128:
                ok = False
        if ok:
            state["subscribed"].set()

    def on_message(client, userdata, msg):
        note_request(state)
        try:
            info = adapter.publish(response_topic, payload=msg.payload, qos=qos, retain=False)
        except Exception:  # noqa: BLE001
            note_echo_error(state)
            return
        if getattr(info, "rc", 0) != 0 or getattr(info, "mid", 0) is None:
            admit_echo(state, None)
            return
        admit_echo(state, getattr(info, "mid", 1))

    adapter.on_connect = on_connect
    adapter.on_subscribe = on_subscribe
    adapter.on_message = on_message
    adapter.connect(cfg["host"], int(cfg["port"]), keepalive=60)
    adapter.loop_start()
    if not state["connected"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "ready_timeout", **identity})
        adapter.loop_stop()
        return 1
    result = adapter.subscribe(request_topic, qos=qos)
    state["sub_mid"] = result.mid
    if result.mid is None:
        state["subscribed"].set()
    if not state["subscribed"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "ready_timeout", **identity})
        adapter.loop_stop()
        return 1

    touch(cfg["ready_path"], {"role": "responder", "pid": os.getpid(), **identity})
    barrier = barrier_client_session(cfg["barrier_path"], timeout_s=float(cfg.get("barrier_timeout_s", 120)))
    barrier.wait("T0")
    # Mirror initiator warmup duration, then join the measure barrier.
    time.sleep(float(cfg.get("warmup_s", 1)))
    barrier.ack("WARMUP_DRAINED")
    barrier.wait("T_MEASURE")
    barrier.close()
    # Stay alive for measure+drain.
    alive = float(cfg.get("duration_s", 3)) + float(cfg.get("drain_s", 2)) + 2
    time.sleep(alive)
    runtime_end = process_runtime_snapshot()
    library = adapter_library_snapshot(adapter)
    adapter.disconnect()
    adapter.loop_stop()
    payload = {
        "ok": True,
        "role": "responder",
        **responder_result_fields(state, pending_at_end=0),
        "runtime": {"process_end": runtime_end},
        **identity,
    }
    if library is not None:
        payload["runtime"]["library"] = library
    write_json(
        cfg["result_path"],
        payload,
    )
    return 0


def _run_native(cfg, plan, identity, state, request_topic, response_topic, qos, protocol) -> int:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state["loop"] = loop
    adapter = plan["builder"](
        cfg.get("client", "paho"),
        client_path=cfg.get("client_path"),
        client_id=cfg.get("client_id", f"resp-{cfg['run_id']}"),
        protocol=protocol,
        clean_session=True,
        tls_ca_certs=cfg.get("ca_certs") if cfg.get("tls") else None,
    )
    sync_on_loop = bool(plan["publish_sync_on_loop"])

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if int(getattr(reason_code, "value", reason_code)) == 0:
            state["connected"].set()

    async def _echo(payload):
        try:
            mid = await adapter.publish(response_topic, payload=payload, qos=qos, retain=False)
        except Exception:  # noqa: BLE001
            note_echo_error(state)
            return
        admit_echo(state, mid)

    def on_message(client, userdata, msg):
        note_request(state)
        try:
            if sync_on_loop:
                # mqttium: None is FlowControlError (not admitted). A returned
                # mid is an accepted write-pump slot, not an on-wire PUBACK.
                # gmqtt: None is an exception on the private nowait path.
                mid = adapter.publish_nowait(response_topic, payload=msg.payload, qos=qos, retain=False)
                admit_echo(state, mid)
                return
            task = loop.create_task(_echo(msg.payload))
            state["pending"].add(task)

            def _done(done):
                state["pending"].discard(done)
                if done.cancelled():
                    return
                exc = done.exception()
                if exc is not None:
                    note_echo_error(state)

            task.add_done_callback(_done)
        except Exception:  # noqa: BLE001
            note_echo_error(state)

    adapter.on_connect = on_connect
    adapter.on_message = on_message

    async def _blocking(fn, *args, **kwargs):
        call = functools.partial(fn, *args, **kwargs)
        return await loop.run_in_executor(None, call)

    async def _drive():
        await adapter.connect(cfg["host"], int(cfg["port"]), keepalive=60)
        if not state["connected"].is_set():
            state["connected"].set()
        result = await adapter.subscribe(request_topic, qos=qos)
        state["sub_mid"] = getattr(result, "mid", None)
        state["subscribed"].set()

        touch(cfg["ready_path"], {"role": "responder", "pid": os.getpid(), **identity})
        barrier = barrier_client_session(
            cfg["barrier_path"], timeout_s=float(cfg.get("barrier_timeout_s", 120))
        )
        await _blocking(barrier.wait, "T0")
        await asyncio.sleep(float(cfg.get("warmup_s", 1)))
        await _blocking(barrier.ack, "WARMUP_DRAINED")
        await _blocking(barrier.wait, "T_MEASURE")
        barrier.close()
        alive = float(cfg.get("duration_s", 3)) + float(cfg.get("drain_s", 2)) + 2
        await asyncio.sleep(alive)
        pending = list(state["pending"])
        cancelled = 0
        if pending:
            _done, still = await asyncio.wait(pending, timeout=float(cfg.get("drain_s", 2)))
            cancelled = len(still)
            for task in still:
                task.cancel()
            if still:
                await asyncio.gather(*still, return_exceptions=True)
        pending_at_end = len(state["pending"]) + cancelled
        runtime_end = process_runtime_snapshot()
        library = adapter_library_snapshot(adapter)
        await adapter.disconnect()
        payload = {
            "ok": True,
            "role": "responder",
            **responder_result_fields(state, pending_at_end=pending_at_end),
            "runtime": {"process_end": runtime_end},
            **identity,
        }
        if library is not None:
            payload["runtime"]["library"] = library
        write_json(
            cfg["result_path"],
            payload,
        )
        return 0

    try:
        return int(loop.run_until_complete(_drive()))
    finally:
        loop.close()


if __name__ == "__main__":
    raise SystemExit(main())
