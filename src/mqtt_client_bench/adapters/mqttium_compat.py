"""MQTTium Paho-compat façade adapter — separate from native AsyncClient rankings."""

from __future__ import annotations

import ssl
import threading
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from mqtt_client_bench.adapters.base import (
    AdapterCapabilities,
    PublishResult,
    SubscribeResult,
)


class MqttiumCompatAdapter:
    """Bench ``mqttium.compat.paho`` only — not comparable to native ``mqttium``."""

    MQTT_ERR_SUCCESS = 0
    # See GmqttAdapter._PRIVATE_API for the rationale of this inventory.
    # Re-verified against mqttium 1.0.0rc1; MqttiumPrivateApiShapeTests pins it.
    _PRIVATE_API = {
        "Client._async": "façade ctor exposes no max_outbound_inflight (through 1.0.0rc1); the inner AsyncClient is rebuilt so QoS>=1 runs the same window as peers",
        "Client._loop": "needed to schedule the properties publish path on the façade's own loop",
        "Client._submit": "connect() must pass an SSLContext, which the façade's own connect() does not accept",
        "Client._run_loop_mutation": "applies the bench keepalive on the façade's loop before connecting",
        "Client._queue_qos0_on_loop / _queue_qosn_on_loop / _finalize_async_commands": "façade caches these bound methods at construction; rebound after the inner client is replaced",
        "AsyncClient._queue_qos0_on_loop / _queue_qosn_on_loop / _finalize_loop_commands": "source of the rebound façade fast path",
        "AsyncClient._engine / _engine_lock / _sub_futs / _collect_effects_locked / _drain_effects": "subscribe registration mirrored from AsyncClient.subscribe: the façade has no on_subscribe hook to deliver grants",
        "AsyncClient._reconfigure": "loop-confined config boundary used to set keepalive before connect",
    }

    def __init__(self, client: Any):
        self._client = client
        self._tls_ctx: Optional[ssl.SSLContext] = None
        self._lock = threading.Lock()
        self._qos0_fifo: deque[int] = deque()
        self._real_to_synth: Dict[int, deque[int]] = {}
        self._early_real: Dict[int, deque[Tuple[Any, Any]]] = {}
        self._next_synth_mid = 1
        self._user_on_publish: Any = None
        self._user_on_subscribe: Any = None
        self._client.on_publish = self._dispatch_publish

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="mqttium-compat",
            sync_api=True,
            async_bridged=False,
            mqtt_v311=True,
            mqtt_v5=True,
            qos2=True,
            tls=True,
            max_inflight=True,
            max_queued=True,
            max_queued_bytes=True,
            message_callback_add=True,
            native_message_callback_add=True,
            v5_publish_properties=True,
            stability="experimental",
            io_model="sync",
            implementation_language="python",
            completion_mechanism="sync",
            synthetic_mids=True,
            tcp_nodelay=True,
            notes=(
                "MQTTium Paho VERSION2 compatibility façade only "
                "(mqttium.compat.paho) — ranked separately from native `mqttium`."
            ),
        )

    @classmethod
    def identity(cls) -> dict:
        import mqttium

        caps = cls.capabilities()
        version = getattr(mqttium, "__version__", None)
        if version is None:
            try:
                from importlib.metadata import version as pkg_version

                version = pkg_version("mqttium")
            except Exception:  # noqa: BLE001
                version = None
        return {
            "client": "mqttium-compat",
            "adapter": "mqttium-compat",
            "client_module": str(Path(mqttium.__file__).resolve()),
            "client_version": version,
            "stability": caps.stability,
            "io_model": caps.io_model,
            "implementation_language": caps.implementation_language,
            "completion_mechanism": caps.completion_mechanism,
            "synthetic_mids": caps.synthetic_mids,
            "display_note": caps.notes,
            "private_api": dict(cls._PRIVATE_API),
            # Façade publish_nowait admits to the write pump; Paho fires after
            # the socket send completes. Same peer group (sync) but different
            # QoS0 completion boundary.
            "qos0_boundary": "queue",
        }

    @classmethod
    def create(
        cls,
        *,
        client_id: str,
        protocol: str = "MQTTv311",
        clean_session: bool = True,
        max_inflight: int = 20,
        max_queued: int = 200,
        max_queued_bytes: Optional[int] = None,
        tls_ca_certs: Optional[str] = None,
    ) -> "MqttiumCompatAdapter":
        try:
            from mqttium.compat import paho as mqtt
            from mqttium.enums import MQTTProtocolVersion
        except ImportError as exc:
            raise ImportError(
                "mqttium is not installed. Install with: pip install 'mqtt-client-bench[mqttium]'"
            ) from exc

        from mqttium.api import AsyncClient

        proto = getattr(MQTTProtocolVersion, protocol)
        pending = max(0, int(max_queued))
        inflight = max(1, int(max_inflight))
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=proto,
            clean_session=clean_session,
            max_pending_outbound_messages=pending,
        )
        # Through 1.0.0rc1: max_outbound_inflight is attach-time only; the façade
        # ctor does not expose it and it is absent from
        # _RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS, so _reconfigure() rejects it
        # once attached. Replace the inner AsyncClient before loop_start/connect
        # so QoS>=1 points run with the same window as the other clients.
        # Re-check on each mqttium bump: if the façade gains the parameter, drop
        # this rebuild and pass it straight to Client(...).
        # Same byte-bound sizing as the native adapter: the façade's inner client
        # carries the 1 MiB write-pump default too. See MqttiumAdapter.connect.
        byte_kwargs = {}
        if max_queued_bytes:
            byte_kwargs["max_outbound_bytes"] = max(1 << 20, int(max_queued_bytes))
            byte_kwargs["max_pending_outbound_bytes"] = max(64 << 20, int(max_queued_bytes))
        inner = AsyncClient(
            client_id=client_id,
            protocol=proto,
            clean_start=bool(clean_session),
            max_outbound_inflight=inflight,
            max_pending_outbound_messages=pending,
            publish_backpressure="error",
            **byte_kwargs,
        )
        inner.on_connect = client._async.on_connect
        inner.on_disconnect = client._async.on_disconnect
        inner.on_message = client._async.on_message
        inner.on_publish = client._async.on_publish
        client._async = inner
        client._queue_qosn_on_loop = inner._queue_qosn_on_loop
        client._queue_qos0_on_loop = inner._queue_qos0_on_loop
        client._finalize_async_commands = inner._finalize_loop_commands
        # Keep public queue setters in sync for identity / later mutations.
        if hasattr(client, "max_queued_messages_set"):
            client.max_queued_messages_set(pending)
        adapter = cls(client)
        if tls_ca_certs:
            adapter.tls_set(ca_certs=tls_ca_certs)
        return adapter

    def tls_set(self, ca_certs: str) -> None:
        self._tls_ctx = ssl.create_default_context(cafile=ca_certs)

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        # Same call the façade's own connect() makes; it is bypassed here only
        # because that connect() takes no SSLContext.
        self._client._run_loop_mutation(
            lambda: self._client._async._reconfigure(keepalive=keepalive)
        )
        self._client._submit(self._client._async.connect(host, port, ssl=self._tls_ctx))

    def disconnect(self) -> None:
        self._client.disconnect()

    def loop_start(self) -> None:
        self._client.loop_start()

    def loop_stop(self) -> None:
        self._client.loop_stop()

    def _alloc_synth_mid(self) -> int:
        with self._lock:
            mid = self._next_synth_mid
            self._next_synth_mid = 1 if self._next_synth_mid >= 65535 else self._next_synth_mid + 1
            return mid

    def _bind_real_mid(self, real_mid: int, synth_mid: int) -> Optional[Tuple[Any, Any]]:
        with self._lock:
            early_q = self._early_real.get(real_mid)
            if early_q:
                early = early_q.popleft()
                if not early_q:
                    self._early_real.pop(real_mid, None)
                return early
            self._real_to_synth.setdefault(real_mid, deque()).append(synth_mid)
            return None

    def _translate_real_mid(self, real_mid: int, reason_code: Any, properties: Any) -> Optional[int]:
        with self._lock:
            pending = self._real_to_synth.get(real_mid)
            if pending:
                synth = pending.popleft()
                if not pending:
                    self._real_to_synth.pop(real_mid, None)
                return synth
            self._early_real.setdefault(real_mid, deque()).append((reason_code, properties))
            return None

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult:
        data = b"" if payload is None else payload
        if isinstance(data, str):
            data = data.encode("utf-8")

        synth = self._alloc_synth_mid()
        if properties is None:
            if int(qos) == 0:
                with self._lock:
                    self._qos0_fifo.append(synth)
            info = self._client.publish(topic, payload=data, qos=qos, retain=retain)
            if int(info.rc) != 0:
                if int(qos) == 0:
                    with self._lock:
                        try:
                            self._qos0_fifo.remove(synth)
                        except ValueError:
                            pass
                return PublishResult(rc=int(info.rc), mid=None)
            if int(qos) == 0:
                return PublishResult(rc=0, mid=synth)
            assert info.mid is not None
            early = self._bind_real_mid(int(info.mid), synth)
            if early is not None:
                reason_code, props = early
                self._fire_user_publish(synth, reason_code, props)
            return PublishResult(rc=0, mid=synth)

        # Properties path: façade publish() has no properties kwarg. Schedule
        # publish_nowait on the owning loop without a per-message blocking
        # cross-thread wait — that wait violated the harness tax budget and
        # silently penalised mqttium-compat on mqttv5_properties vs Paho.
        import asyncio

        if self._client._loop is None:
            self._client.loop_start()
        assert self._client._loop is not None
        if int(qos) == 0:
            with self._lock:
                self._qos0_fifo.append(synth)

        async def _queue() -> None:
            try:
                receipt = self._client._async.publish_nowait(
                    topic, data, qos=qos, retain=retain, properties=properties
                )
            except BaseException:  # noqa: BLE001
                if int(qos) == 0:
                    with self._lock:
                        try:
                            self._qos0_fifo.remove(synth)
                        except ValueError:
                            pass
                self._fire_user_publish(synth, 128, None)
                return
            if int(qos) == 0 or receipt.mid is None:
                if int(qos) != 0:
                    self._fire_user_publish(synth, 128, None)
                return
            early = self._bind_real_mid(int(receipt.mid), synth)
            if early is not None:
                reason_code, props = early
                self._fire_user_publish(synth, reason_code, props)

        asyncio.run_coroutine_threadsafe(_queue(), self._client._loop)
        return PublishResult(rc=0, mid=synth)

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        """Paho-shaped subscribe: return mid before SUBACK, then fire on_subscribe.

        The façade (through 1.0.0rc1) exposes ``Client.subscribe`` but has no
        ``on_subscribe`` hook, so the bench mirrors ``AsyncClient.subscribe``
        registration (queue + ``_sub_futs`` + effect drain) to deliver grants to
        the harness.
        Mid must be returned before the callback: roles add mid to a wait-set
        only after ``subscribe()`` returns.
        """
        import asyncio

        if self._client._loop is None:
            self._client.loop_start()
        assert self._client._loop is not None

        mid_holder: dict[str, Any] = {}
        done = threading.Event()

        async def _subscribe() -> None:
            try:
                loop = asyncio.get_running_loop()
                client = self._client._async
                async with client._engine_lock:
                    mid = client._engine.queue_subscribe(topic, qos=qos)
                    fut: asyncio.Future = loop.create_future()
                    client._sub_futs[mid] = fut
                    client._collect_effects_locked()
                mid_holder["mid"] = mid
                done.set()
                await client._drain_effects()

                async def _wait_suback() -> None:
                    try:
                        result = await fut
                        grants = list(result.reason_codes)
                    except Exception:  # noqa: BLE001
                        grants = [128]
                    cb = self._user_on_subscribe
                    if cb is not None:
                        cb(self, None, mid, grants, None)

                asyncio.create_task(_wait_suback())
            except BaseException as exc:  # noqa: BLE001
                mid_holder["error"] = exc
                done.set()

        fut = asyncio.run_coroutine_threadsafe(_subscribe(), self._client._loop)
        if not done.wait(timeout=5.0):
            fut.cancel()
            raise RuntimeError("mqttium-compat subscribe handoff timed out")
        if "error" in mid_holder:
            raise mid_holder["error"]
        return SubscribeResult(rc=0, mid=int(mid_holder["mid"]))

    def message_callback_add(self, topic: str, callback) -> None:
        self._client.message_callback_add(topic, callback)

    def build_publish_properties(self, profile: str) -> Any:
        if profile in (None, "none"):
            return None
        from mqttium.types import Properties

        props = Properties()
        if profile == "realistic":
            props.set("payload_format_indicator", 1)
            props.set("content_type", "application/json")
            props.set("message_expiry_interval", 60)
            props.set("user_property", [("schema", "telemetry.v1"), ("region", "eu-west-1")])
        elif profile == "rich":
            props.set("payload_format_indicator", 1)
            props.set("content_type", "application/json")
            props.set("message_expiry_interval", 60)
            props.set("correlation_data", b"c" * 32)
            props.set("response_topic", "bench/response/" + ("r" * 48))
            props.set("user_property", [(f"k{i:02d}", "v" * 64) for i in range(16)])
        else:
            return None
        return props

    def _fire_user_publish(self, mid: int, reason_code: Any = 0, properties: Any = None) -> None:
        cb = self._user_on_publish
        if cb is not None:
            cb(self, None, mid, reason_code, properties)

    def _dispatch_publish(self, client, userdata, mid, reason_code=None, properties=None) -> None:
        if mid is None:
            with self._lock:
                synth = self._qos0_fifo.popleft() if self._qos0_fifo else None
            if synth is None:
                return
            self._fire_user_publish(synth, reason_code, properties)
            return
        synth = self._translate_real_mid(int(mid), reason_code, properties)
        if synth is not None:
            self._fire_user_publish(synth, reason_code, properties)

    @property
    def on_connect(self):
        return self._client.on_connect

    @on_connect.setter
    def on_connect(self, cb) -> None:
        self._client.on_connect = cb

    @property
    def on_publish(self):
        return self._user_on_publish

    @on_publish.setter
    def on_publish(self, cb) -> None:
        self._user_on_publish = cb

    @property
    def on_message(self):
        return self._client.on_message

    @on_message.setter
    def on_message(self, cb) -> None:
        self._client.on_message = cb

    @property
    def on_subscribe(self):
        return self._user_on_subscribe

    @on_subscribe.setter
    def on_subscribe(self, cb) -> None:
        self._user_on_subscribe = cb
