"""gmqtt adapter — asyncio MQTT client exposed via AsyncioBridge sync facade."""

from __future__ import annotations

import ssl
from pathlib import Path
from typing import Any, Optional

from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, PublishResult, SubscribeResult


class GmqttAdapter(BridgedAdapterBase):
    _NAME = "gmqtt"
    _NOTES = (
        "Wialon gmqtt — asyncio MQTT client with callback API. "
        "Sync facade runs the event loop on a dedicated thread."
    )

    def __init__(self) -> None:
        super().__init__()
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None
        self._ssl_context: Any = None
        # Real gmqtt packet id -> synthetic mid returned by publish().
        # Only touched on the bridge loop thread (publish coroutine + PUBACK
        # handler both run there), so no extra locking is needed.
        self._real_to_synth: dict[int, int] = {}

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="gmqtt",
            sync_api=False,
            async_bridged=True,
            mqtt_v311=True,
            mqtt_v5=True,
            # gmqtt 0.7 fires completion at PUBREC, not PUBCOMP — refuse QoS2 points.
            qos2=False,
            tls=True,
            max_inflight=False,
            max_queued=False,
            message_callback_add=True,
            native_message_callback_add=False,
            v5_publish_properties=True,
            stability="stable",
            io_model="asyncio_bridged",
            implementation_language="python",
            # publish() returns synthetic mids; PUBACKs are translated back
            # from real packet ids via an on-loop mapping.
            synthetic_mids=True,
            notes=cls._NOTES,
            unimplemented=[],
        )

    @classmethod
    def identity(cls) -> dict:
        import gmqtt

        caps = cls.capabilities()
        return {
            "client": "gmqtt",
            "adapter": "gmqtt",
            "client_module": str(Path(gmqtt.__file__).resolve()),
            "client_version": getattr(gmqtt, "__version__", None),
            "stability": caps.stability,
            "io_model": caps.io_model,
            "implementation_language": caps.implementation_language,
            "synthetic_mids": caps.synthetic_mids,
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
        tls_ca_certs: Optional[str] = None,
    ) -> "GmqttAdapter":
        try:
            from gmqtt import Client as MQTTClient
        except ImportError as exc:
            raise ImportError(
                "gmqtt is not installed. Install with: pip install 'mqtt-client-bench[gmqtt]'"
            ) from exc

        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        adapter._client = MQTTClient(client_id, clean_session=clean_session)
        if tls_ca_certs:
            ctx = ssl.create_default_context(cafile=tls_ca_certs)
            adapter._ssl_context = ctx
        adapter._wire_native_callbacks()
        return adapter

    def _wire_native_callbacks(self) -> None:
        client = self._client

        def _on_connect(gmqtt_client, session_present, result, properties):
            flags = {"session present": bool(session_present)}
            self._fire_on_connect(flags=flags, reason_code=result, properties=properties)

        def _on_message(gmqtt_client, topic, payload, qos, properties):
            retain = False
            if isinstance(properties, dict) and "retain" in properties:
                raw = properties["retain"]
                retain = bool(raw[0] if isinstance(raw, list) else raw)
            msg = IncomingMessage(topic=topic, payload=payload, qos=int(qos), retain=retain)
            self._dispatch_message(msg)

        def _on_subscribe(gmqtt_client, mid, granted_qoses, properties):
            self._fire_on_subscribe(int(mid), list(granted_qoses), properties)

        client.on_connect = _on_connect
        client.on_message = _on_message
        client.on_subscribe = _on_subscribe

        orig = client._remove_message_from_query

        def _remove_and_ack(mid):
            orig(mid)
            synth = self._real_to_synth.pop(int(mid), None)
            self._fire_on_publish(int(mid) if synth is None else synth, reason_code=0)

        client._remove_message_from_query = _remove_and_ack

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        from gmqtt.mqtt.constants import MQTTv311, MQTTv50

        self._ensure_bridge()
        version = MQTTv50 if self._protocol == "MQTTv5" else MQTTv311
        ssl_arg = self._ssl_context if self._ssl_context is not None else False

        async def _connect():
            await self._client.connect(
                host,
                port=port,
                ssl=ssl_arg,
                keepalive=keepalive,
                version=version,
            )

        self._bridge.run(_connect())
        self._connected = True

    def disconnect(self) -> None:
        if self._client is None or not self._connected:
            return
        self._ensure_bridge()

        async def _disconnect():
            await self._client.disconnect()

        try:
            self._bridge.run(_disconnect(), timeout=10.0)
        except Exception:  # noqa: BLE001
            pass
        self._connected = False

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult:
        from gmqtt import Message

        self._ensure_bridge()
        kwargs: dict[str, Any] = {}
        if isinstance(properties, dict):
            kwargs.update(properties)
        message = Message(topic, payload, qos=qos, retain=retain, **kwargs)
        client = self._client
        # Same submission discipline as the other bridged adapters: allocate a
        # synthetic mid, schedule the publish on the loop, return immediately.
        # A blocking bridge round-trip per publish would forbid pipelining the
        # outstanding window and structurally bias gmqtt against its peers.
        synth_mid = self.alloc_mid()

        async def _publish():
            real_mid = None
            try:
                real_mid, package = client._connection.publish(message)
                if qos > 0 and real_mid is not None:
                    # Register before storing so the PUBACK hook (same loop
                    # thread) can translate the real packet id back.
                    self._real_to_synth[int(real_mid)] = synth_mid
                    push = getattr(client._persistent_storage, "push_message_nowait", None)
                    if push is not None:
                        push(int(real_mid), package)
                    else:
                        await client._persistent_storage.push_message(int(real_mid), package)
                else:
                    self._fire_on_publish(synth_mid, reason_code=0)
            except Exception:  # noqa: BLE001
                if real_mid is not None:
                    self._real_to_synth.pop(int(real_mid), None)
                self._fire_on_publish(synth_mid, reason_code=128)

        self._bridge.create_task(_publish())
        return PublishResult(rc=0, mid=synth_mid)

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        self._ensure_bridge()

        async def _subscribe():
            return self._client.subscribe(topic, qos=qos)

        mid = self._bridge.run(_subscribe())
        return SubscribeResult(rc=0, mid=int(mid) if mid is not None else None)

    def build_publish_properties(self, profile: str) -> Any:
        # Align field set with Paho/aiomqtt (incl. payload_format_indicator).
        if profile in (None, "none"):
            return None
        if profile == "realistic":
            return {
                "payload_format_indicator": 1,
                "content_type": "application/json",
                "message_expiry_interval": 60,
                "user_property": [("schema", "telemetry.v1"), ("region", "eu-west-1")],
            }
        if profile == "rich":
            return {
                "payload_format_indicator": 1,
                "content_type": "application/json",
                "message_expiry_interval": 60,
                "correlation_data": b"c" * 32,
                "response_topic": "bench/response/" + ("r" * 48),
                "user_property": [(f"k{i:02d}", "v" * 64) for i in range(16)],
            }
        return None
