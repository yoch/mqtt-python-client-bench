"""amqtt adapter — asyncio MQTT client (broker unused) via AsyncioBridge."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Optional

from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, PublishResult, SubscribeResult


class AmqttAdapter(BridgedAdapterBase):
    _NAME = "amqtt"
    _NOTES = (
        "amqtt — asyncio MQTT client (and optional broker). "
        "Only the client side is in scope for this bench."
    )

    def __init__(self) -> None:
        super().__init__()
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="amqtt",
            sync_api=False,
            async_bridged=True,
            mqtt_v311=True,
            # amqtt client path used here is MQTT 3.1.1 only.
            mqtt_v5=False,
            qos2=True,
            tls=True,
            max_inflight=False,
            max_queued=False,
            message_callback_add=True,
            native_message_callback_add=False,
            v5_publish_properties=False,
            stability="stable",
            io_model="asyncio_bridged",
            implementation_language="python",
            synthetic_mids=True,
            notes=cls._NOTES,
            unimplemented=[],
        )

    @classmethod
    def identity(cls) -> dict:
        import amqtt

        caps = cls.capabilities()
        return {
            "client": "amqtt",
            "adapter": "amqtt",
            "client_module": str(Path(amqtt.__file__).resolve()),
            "client_version": getattr(amqtt, "__version__", None),
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
    ) -> "AmqttAdapter":
        try:
            from amqtt.client import MQTTClient
        except ImportError as exc:
            raise ImportError(
                "amqtt is not installed. Install with: pip install 'mqtt-client-bench[amqtt]'"
            ) from exc

        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        adapter._client = MQTTClient(
            client_id=client_id,
            config={
                "auto_reconnect": False,
                "cleansession": clean_session,
            },
        )
        return adapter

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        self._ensure_bridge()
        self._stopping = False
        scheme = "mqtts" if self._tls_ca_certs else "mqtt"
        uri = f"{scheme}://{host}:{port}/"
        cafile = self._tls_ca_certs

        async def _connect():
            await self._client.connect(
                uri,
                cleansession=self._clean_session,
                cafile=cafile,
            )
            self._connected = True
            self._fire_on_connect(flags={}, reason_code=0, properties=None)
            self._start_pump()

        self._bridge.run(_connect())

    async def _message_pump(self) -> None:
        assert self._client is not None
        while not self._stopping:
            try:
                message = await self._client.deliver_message(timeout_duration=1.0)
            except asyncio.TimeoutError:
                continue
            except Exception:  # noqa: BLE001
                if not self._stopping:
                    raise
                break
            if message is None:
                continue
            self._dispatch_message(
                IncomingMessage(
                    topic=message.topic,
                    payload=message.data,
                    qos=int(message.qos or 0),
                    retain=bool(message.retain),
                )
            )

    def disconnect(self) -> None:
        if self._client is None or not self._connected:
            return
        self._ensure_bridge()

        async def _disconnect():
            await self._stop_pump()
            try:
                await self._client.disconnect()
            finally:
                self._connected = False

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

        if payload is None:
            data = b""
        elif isinstance(payload, (bytes, bytearray)):
            data = bytes(payload)
        elif isinstance(payload, str):
            data = payload.encode("utf-8")
        else:
            data = bytes(payload)

        async def _publish():
            try:
                await client.publish(topic, data, qos=qos, retain=retain)
                self._fire_on_publish(mid, reason_code=0)
            except Exception:  # noqa: BLE001
                self._fire_on_publish(mid, reason_code=128)

        self._bridge.create_task(_publish())
        return PublishResult(rc=0, mid=mid)

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        mid = self.alloc_mid()
        client = self._client
        if client is None or not self._connected:
            return SubscribeResult(rc=1, mid=None)

        async def _subscribe():
            try:
                grants = await client.subscribe([(topic, qos)])
                self._fire_on_subscribe(mid, list(grants) if grants else [qos], None)
            except Exception:  # noqa: BLE001
                self._fire_on_subscribe(mid, [128], None)

        self._bridge.create_task(_subscribe())
        return SubscribeResult(rc=0, mid=mid)
