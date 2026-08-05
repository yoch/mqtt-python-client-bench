#!/usr/bin/env python3
"""Compare mqttium QoS0 publish paths: bridged-task vs bridged-sync vs loop-native.

Not part of the ranked campaign — experimental methodology probe.
"""
from __future__ import annotations

import argparse
import asyncio
import threading
import time
from typing import Any, Callable, Optional


PAYLOAD = b"x" * 256
HOST = "127.0.0.1"
PORT = 11883


def _run_bridged(
    *,
    label: str,
    duration_s: float,
    outstanding: int,
    qos0_sync: bool,
) -> dict[str, Any]:
    """Sync worker + AsyncioBridge, mirroring the harness publisher contract."""
    from mqtt_client_bench.adapters.mqttium import MqttiumAdapter

    # Temporarily force path via monkeypatch if measuring legacy task path.
    adapter = MqttiumAdapter.create(
        client_id=f"probe-{label}",
        max_inflight=20,
        max_queued=10_000,
    )
    completed = 0
    lock = threading.Lock()
    done = threading.Event()
    inflight = 0

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        nonlocal completed, inflight
        with lock:
            inflight = max(0, inflight - 1)
            if done.is_set():
                return
            completed += 1

    adapter.on_publish = on_publish
    adapter.connect(HOST, PORT, keepalive=60)
    adapter.loop_start()
    time.sleep(0.2)

    topic = f"bench/probe/{label}/qos0"
    # Optionally force legacy schedule_coro path for A/B inside same process.
    if not qos0_sync:
        orig = adapter.publish

        def publish_legacy(topic, payload=None, qos=0, retain=False, properties=None):
            # Bypass QoS0 schedule_call: always use coro path.
            mid = adapter.alloc_mid()
            client = adapter._client
            data = b"" if payload is None else payload
            if isinstance(data, str):
                data = data.encode("utf-8")

            async def _publish():
                try:
                    receipt = client.publish_nowait(
                        topic, data, qos=qos, retain=retain, properties=properties
                    )
                    if receipt._event is not None:
                        await receipt.wait()
                    adapter._fire_on_publish(mid, reason_code=0)
                except Exception:  # noqa: BLE001
                    adapter._fire_on_publish(mid, reason_code=128)

            adapter.schedule_coro(_publish())
            from mqtt_client_bench.adapters.base import PublishResult

            return PublishResult(rc=0, mid=mid)

        adapter.publish = publish_legacy  # type: ignore[method-assign]
        _ = orig

    t0 = time.perf_counter()
    end = t0 + duration_s
    submitted = 0
    while time.perf_counter() < end:
        with lock:
            if inflight >= outstanding:
                busy = True
            else:
                inflight += 1
                busy = False
        if busy:
            time.sleep(0)
            continue
        adapter.publish(topic, PAYLOAD, qos=0)
        submitted += 1
    done.set()
    # drain
    drain_deadline = time.perf_counter() + 2.0
    while time.perf_counter() < drain_deadline:
        with lock:
            if inflight == 0:
                break
        time.sleep(0.001)
    elapsed = time.perf_counter() - t0
    adapter.disconnect()
    adapter.loop_stop()
    with lock:
        rate = completed / elapsed if elapsed > 0 else 0.0
        return {
            "label": label,
            "completed": completed,
            "submitted": submitted,
            "elapsed_s": round(elapsed, 3),
            "msgs_per_s": round(rate, 1),
        }


async def _run_asyncio_native(*, duration_s: float) -> dict[str, Any]:
    """Producer already on the client event loop — no bridge."""
    from mqttium.api import AsyncClient

    client = AsyncClient(
        client_id="probe-asyncio-native",
        max_pending_outbound_messages=10_000,
        publish_backpressure="error",
    )
    await client.connect(HOST, PORT)
    topic = "bench/probe/asyncio-native/qos0"
    t0 = time.perf_counter()
    end = t0 + duration_s
    n = 0
    while time.perf_counter() < end:
        try:
            client.publish_nowait(topic, PAYLOAD, qos=0)
            n += 1
        except Exception:  # noqa: BLE001
            await asyncio.sleep(0)
            continue
        if n % 512 == 0:
            await asyncio.sleep(0)
    elapsed = time.perf_counter() - t0
    await client.disconnect()
    return {
        "label": "asyncio_native_loop",
        "completed": n,
        "submitted": n,
        "elapsed_s": round(elapsed, 3),
        "msgs_per_s": round(n / elapsed if elapsed else 0.0, 1),
    }


def _run_compat_sync(*, duration_s: float, outstanding: int) -> dict[str, Any]:
    from mqtt_client_bench.adapters.registry import create_adapter

    adapter = create_adapter(
        "mqttium-compat",
        client_id="probe-compat",
        max_inflight=20,
        max_queued=10_000,
    )
    completed = 0
    lock = threading.Lock()
    done = threading.Event()
    inflight = 0

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        nonlocal completed, inflight
        with lock:
            inflight = max(0, inflight - 1)
            if done.is_set():
                return
            completed += 1

    adapter.on_publish = on_publish
    adapter.loop_start()
    adapter.connect(HOST, PORT, keepalive=60)
    time.sleep(0.3)
    topic = "bench/probe/compat/qos0"
    t0 = time.perf_counter()
    end = t0 + duration_s
    submitted = 0
    while time.perf_counter() < end:
        with lock:
            if inflight >= outstanding:
                busy = True
            else:
                inflight += 1
                busy = False
        if busy:
            time.sleep(0)
            continue
        adapter.publish(topic, PAYLOAD, qos=0)
        submitted += 1
    done.set()
    drain_deadline = time.perf_counter() + 2.0
    while time.perf_counter() < drain_deadline:
        with lock:
            if inflight == 0:
                break
        time.sleep(0.001)
    elapsed = time.perf_counter() - t0
    adapter.disconnect()
    adapter.loop_stop()
    with lock:
        return {
            "label": "mqttium_compat_sync",
            "completed": completed,
            "submitted": submitted,
            "elapsed_s": round(elapsed, 3),
            "msgs_per_s": round(completed / elapsed if elapsed else 0.0, 1),
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=3.0)
    parser.add_argument("--outstanding", type=int, default=256)
    args = parser.parse_args()
    rows = []
    rows.append(
        _run_bridged(
            label="bridged_task_legacy",
            duration_s=args.duration,
            outstanding=args.outstanding,
            qos0_sync=False,
        )
    )
    time.sleep(0.5)
    rows.append(
        _run_bridged(
            label="bridged_adapter_qos0_sync",
            duration_s=args.duration,
            outstanding=args.outstanding,
            qos0_sync=True,
        )
    )
    time.sleep(0.5)
    rows.append(asyncio.run(_run_asyncio_native(duration_s=args.duration)))
    time.sleep(0.5)
    rows.append(
        _run_compat_sync(duration_s=args.duration, outstanding=args.outstanding)
    )
    print("=== mqttium QoS0 path probe (telemetry256, localhost Mosquitto) ===")
    for r in rows:
        print(
            f"{r['label']:28s}  {r['msgs_per_s']:10.1f} msg/s  "
            f"(completed={r['completed']}, {r['elapsed_s']}s)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
