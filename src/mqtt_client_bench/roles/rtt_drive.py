"""Choose the fastest honest drive path for application-RTT roles.

Publisher already takes ``create_async_adapter()`` when the library exposes a
native loop. Initiator and responder used the sync facade for every client,
so mqttium and gmqtt paid a cross-thread handoff that paho never did. That
is an unequal harness tax, not a library difference.

The rule is the same as the publisher: same contract and same workload, each
library on the most direct path its API actually exposes. A client without a
native adapter stays on the sync facade. Forcing the facade on a native
client is diagnostic only and must be recorded as ``sync_facade``.
"""

from __future__ import annotations

import gc
import resource

from mqtt_client_bench.adapters.registry import (
    adapter_identity,
    create_adapter,
    create_async_adapter,
    get_adapter_class,
    get_async_adapter_class,
    has_async_adapter,
)


def select_rtt_drive(client: str, *, native_async: bool = True) -> dict:
    """Return the drive plan application-RTT roles must honour.

    ``builder`` is the factory the role calls. ``publish_path`` is what the
    result records. Client order never enters this decision.
    """
    use_native = bool(native_async) and has_async_adapter(client)
    if use_native:
        caps = get_async_adapter_class(client).capabilities()
        identity = adapter_identity(client)
        return {
            "client": client,
            "native_async": True,
            "publish_path": "native_async",
            "builder": create_async_adapter,
            "publish_sync_on_loop": bool(caps.publish_sync_on_loop),
            "io_model": identity.get("io_model") or caps.io_model,
            "completion_mechanism": identity.get("completion_mechanism")
            or caps.completion_mechanism,
        }
    caps = get_adapter_class(client).capabilities()
    identity = adapter_identity(client)
    return {
        "client": client,
        "native_async": False,
        "publish_path": "sync_facade",
        "builder": create_adapter,
        "publish_sync_on_loop": False,
        "io_model": identity.get("io_model") or (caps.io_model if caps else None),
        "completion_mechanism": identity.get("completion_mechanism")
        or (caps.completion_mechanism if caps else None),
    }


def drive_identity(plan: dict, identity: dict) -> dict:
    """Merge adapter identity with the path that will actually run.

    ``io_model`` names the adapter architecture (``sync`` /
    ``asyncio_bridged`` / ``crt_event_loop``). It is not the application-RTT
    drive. mqttium stays ``asyncio_bridged`` even when this role publishes
    via ``native_async``. Read ``publish_path`` for the measured path.
    """
    merged = dict(identity)
    merged["publish_path"] = plan["publish_path"]
    merged["native_async"] = plan["native_async"]
    adapter_io = identity.get("io_model") or plan.get("io_model")
    merged["adapter_io_model"] = adapter_io
    merged["io_model"] = adapter_io
    merged["rtt_publish_path"] = plan["publish_path"]
    merged["completion_mechanism"] = (
        plan.get("completion_mechanism") or identity.get("completion_mechanism")
    )
    return merged


def require_native_for_async_peer(client: str, plan: dict) -> None:
    """Fail loud if mqttium/gmqtt would silently fall back to the facade."""
    if client in ("mqttium", "gmqtt") and plan.get("publish_path") != "native_async":
        raise RuntimeError(
            f"{client} has a native async adapter but application_rtt selected "
            f"{plan.get('publish_path')!r}; that run is not a native RTT ranking"
        )


def process_runtime_snapshot() -> dict:
    """GC and rusage counters. Call off the publish hot path."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    stats = gc.get_stats()
    return {
        "gc_count": [int(value) for value in gc.get_count()],
        "gc_collections": [int(item.get("collections", 0)) for item in stats],
        "ru_nvcsw": int(usage.ru_nvcsw),
        "ru_nivcsw": int(usage.ru_nivcsw),
        "ru_utime_s": float(usage.ru_utime),
        "ru_stime_s": float(usage.ru_stime),
        "ru_maxrss_kb": int(usage.ru_maxrss),
    }


def process_runtime_delta(start: dict, end: dict) -> dict:
    """Subtract two ``process_runtime_snapshot`` payloads."""
    delta = {}
    for key in ("ru_nvcsw", "ru_nivcsw", "ru_utime_s", "ru_stime_s"):
        if key in start and key in end:
            delta[key] = end[key] - start[key]
    start_gc = list(start.get("gc_collections") or [])
    end_gc = list(end.get("gc_collections") or [])
    if start_gc and end_gc and len(start_gc) == len(end_gc):
        delta["gc_collections"] = [finish - begin for begin, finish in zip(start_gc, end_gc)]
    return delta


def adapter_library_snapshot(adapter) -> dict | None:
    """Optional post-window library counters. Missing method → None."""
    reader = getattr(adapter, "library_runtime_snapshot", None)
    if not callable(reader):
        return None
    try:
        payload = reader()
    except Exception:  # noqa: BLE001
        return None
    return payload if isinstance(payload, dict) else None


def measure_runtime_report(adapter, start: dict, end: dict) -> dict:
    """Bundle process deltas plus any library snapshot for the result JSON."""
    payload = {
        "measure_start": start,
        "measure_end": end,
        "measure_delta": process_runtime_delta(start, end),
    }
    library = adapter_library_snapshot(adapter)
    if library is not None:
        payload["library"] = library
    return payload
