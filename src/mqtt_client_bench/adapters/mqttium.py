"""MQTTium native adapter — AsyncClient via AsyncioBridge (PyPI ≥1.0.0rc1)."""

from __future__ import annotations

import ssl
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, PublishResult, SubscribeResult


class MqttiumAdapter(BridgedAdapterBase):
    """Bench the native ``mqttium.api.AsyncClient`` API (not the Paho façade).

    Publishes go through ``publish_nowait()``: loop-bound, non-suspending
    admission + coalesced effect flush. Completions report via synthetic mid +
    ``on_publish`` after ``receipt.wait()``, which returns immediately for QoS0
    and raises whatever the admission path recorded, so a refused publish is
    counted as a failure rather than as a completion.

    This adapter sets no ``AsyncClient.on_publish``: it fires the bench callback
    itself. That is deliberate — mqttium's direct QoS0 transport write is only
    taken while ``on_publish is None`` (``_direct_qos0_ready``), so installing a
    library-level callback would benchmark the slower path.
    """

    _NAME = "mqttium"
    _NOTES = (
        "MQTTium AsyncClient (https://pypi.org/project/mqttium/) — async-native MQTT "
        "3.1.1/5; QoS0 via publish_nowait + schedule_call on the bridge loop "
        "(PyPI ≥1.0.0rc1). Release candidate; ranked under --suite experimental. Paho VERSION2 "
        "façade is `mqttium-compat`."
    )

    def __init__(self) -> None:
        super().__init__()
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None
        self._max_inflight = 20
        self._max_queued = 200
        self._max_queued_bytes: Optional[int] = None
        # Real packet id -> the synthetic mids the role worker is waiting on,
        # FIFO because mqttium reuses ids. Written and read on the loop thread
        # only: publish_nowait admits on that thread and on_publish is delivered
        # on it, so no lock is needed on the hot path.
        self._real_to_synth: Dict[int, Deque[int]] = {}
        self._on_publish_cb: Any = None

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="mqttium",
            sync_api=False,
            async_bridged=True,
            mqtt_v311=True,
            mqtt_v5=True,
            qos2=True,
            tls=True,
            max_inflight=True,
            max_queued=True,
            max_queued_bytes=True,
            message_callback_add=True,
            native_message_callback_add=False,
            v5_publish_properties=True,
            stability="experimental",
            io_model="asyncio_bridged",
            implementation_language="python",
            completion_mechanism="callback",
            synthetic_mids=True,
            tcp_nodelay=True,
            notes=cls._NOTES,
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
            "client": "mqttium",
            "adapter": "mqttium",
            "client_module": str(Path(mqttium.__file__).resolve()),
            "client_version": version,
            "stability": caps.stability,
            "io_model": caps.io_model,
            "implementation_language": caps.implementation_language,
            "completion_mechanism": caps.completion_mechanism,
            "synthetic_mids": caps.synthetic_mids,
            "display_note": caps.notes,
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
    ) -> "MqttiumAdapter":
        try:
            import mqttium  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "mqttium is not installed. Install with: pip install 'mqtt-client-bench[mqttium]'"
            ) from exc
        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        adapter._max_inflight = max_inflight
        adapter._max_queued = max_queued
        adapter._max_queued_bytes = max_queued_bytes
        return adapter

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        from mqttium.api import AsyncClient
        from mqttium.enums import MQTTProtocolVersion

        self._ensure_bridge()
        self._stopping = False
        proto = getattr(MQTTProtocolVersion, self._protocol)
        tls: Any = None
        if self._tls_ca_certs:
            tls = ssl.create_default_context(cafile=self._tls_ca_certs)

        async def _connect():
            # a2+ removed EngineConfig.max_queued — map bench max_queued onto
            # max_pending_outbound_messages (admission before MID allocation).
            #
            # 0.2.0b2 also bounds the write pump in bytes (max_outbound_bytes,
            # 1 MiB by default), and publish_nowait raises FlowControlError as
            # soon as *either* bound is full. At 1 MiB that is 16 slots for a
            # 64 KiB payload and 1 for a 1 MiB one, i.e. a queue orders of
            # magnitude shallower than the max_queued messages every client is
            # given — measured as a 76-98% refusal rate on the payload sweep.
            # Size the byte bounds from the requested depth so the message
            # window is what binds, and never shrink them below the library's
            # own defaults.
            kwargs = {}
            if self._max_queued_bytes:
                kwargs["max_outbound_bytes"] = max(1 << 20, int(self._max_queued_bytes))
                kwargs["max_pending_outbound_bytes"] = max(
                    64 << 20, int(self._max_queued_bytes)
                )
            self._client = AsyncClient(
                client_id=self._client_id,
                protocol=proto,
                clean_start=self._clean_session,
                keepalive=keepalive,
                max_outbound_inflight=max(1, int(self._max_inflight)),
                max_pending_outbound_messages=max(0, int(self._max_queued)),
                message_delivery="callback",
                **kwargs,
            )

            def _on_publish(mid, reason=None) -> None:
                # PUBLISH_COMPLETE is emitted at PUBACK for QoS1 and at PUBCOMP
                # for QoS2 (protocol/outbound.py:535 and :610), so this honours
                # the bench's completion contract exactly as awaiting the
                # receipt did.
                if mid is None:
                    return
                pending = self._real_to_synth.get(int(mid))
                if not pending:
                    return
                synth = pending.popleft()
                if not pending:
                    self._real_to_synth.pop(int(mid), None)
                self._fire_on_publish(synth, reason_code=0 if reason is None else 128)

            # Installed lazily by the first QoS>=1 publish, never at connect:
            # mqttium takes its direct QoS0 transport write only while
            # on_publish is None (_direct_qos0_ready), so installing it here
            # cost 38% of the QoS0 rate — 39,118 msgs/s down to 24,039. QoS is
            # fixed per measurement point, so a QoS0 point never installs it.
            self._on_publish_cb = _on_publish

            def _on_message(msg) -> None:
                self._dispatch_message(
                    IncomingMessage(
                        topic=str(msg.topic),
                        payload=msg.payload,
                        qos=int(msg.qos),
                        retain=bool(msg.retain),
                    )
                )

            self._client.on_message = _on_message
            await self._client.connect(host, port, ssl=tls)
            self._connected = True
            self._fire_on_connect(flags={}, reason_code=0, properties=None)

        self._bridge.run(_connect())

    async def _message_pump(self) -> None:
        return None

    def disconnect(self) -> None:
        if self._client is None or not self._connected:
            return
        self._ensure_bridge()

        async def _disconnect():
            client = self._client
            self._client = None
            self._connected = False
            self._stopping = True
            if client is not None:
                await client.disconnect()

        try:
            self._bridge.run(_disconnect(), timeout=10.0)
        except Exception:  # noqa: BLE001
            self._connected = False

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult:
        mid = self.alloc_mid()
        client = self._client
        if client is None or not self._connected:
            return PublishResult(rc=1, mid=None)

        data = b"" if payload is None else payload
        if isinstance(data, str):
            data = data.encode("utf-8")

        # QoS0 contract: on_publish = handed to transport. A sync loop-thread
        # callback (no asyncio.Task per message) via schedule_call.
        if int(qos) == 0:

            def _publish_qos0() -> None:
                try:
                    client.publish_nowait(
                        topic, data, qos=0, retain=retain, properties=properties
                    )
                    self._fire_on_publish(mid, reason_code=0)
                except Exception:  # noqa: BLE001
                    self._fire_on_publish(mid, reason_code=128)

            self.schedule_call(_publish_qos0)
            return PublishResult(rc=0, mid=mid)

        # Correlate the ack instead of suspending a coroutine for the whole
        # round trip. publish_nowait is synchronous on the loop thread, so
        # submission and registration happen in one call and the completion
        # arrives later through on_publish — the same discipline gmqtt and
        # awscrt use, and measured 11-34% cheaper than awaiting the receipt,
        # growing with load. Registering after submission is race-free: both run
        # on the loop thread.
        def _publish_qosn() -> None:
            try:
                if client.on_publish is None:
                    # Same loop thread that will later deliver the ack, so the
                    # callback is in place before any completion can arrive.
                    client.on_publish = self._on_publish_cb
                receipt = client.publish_nowait(
                    topic, data, qos=qos, retain=retain, properties=properties
                )
                if receipt.mid is None:
                    self._fire_on_publish(mid, reason_code=0)
                    return
                self._real_to_synth.setdefault(int(receipt.mid), deque()).append(mid)
            except Exception:  # noqa: BLE001
                self._fire_on_publish(mid, reason_code=128)

        self.schedule_call(_publish_qosn)
        return PublishResult(rc=0, mid=mid)

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        mid = self.alloc_mid()
        client = self._client
        if client is None or not self._connected:
            return SubscribeResult(rc=1, mid=None)

        async def _subscribe():
            try:
                result = await client.subscribe(topic, qos=qos)
                grants = list(result.reason_codes)
                self._fire_on_subscribe(mid, grants, None)
            except Exception:  # noqa: BLE001
                self._fire_on_subscribe(mid, [128], None)

        self.schedule_coro(_subscribe())
        return SubscribeResult(rc=0, mid=mid)

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
