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
    # Reading the session delivery queue directly is required for ingress
    # throughput (deliver_message() collapses under load). Surfaced so a
    # reader can judge how faithful the measurement is.
    _PRIVATE_API = {
        "MQTTClient.session.delivered_message_queue": (
            "deliver_message() spawns a Task per wait and collapses to tens of "
            "msg/s under emqtt-bench ingress; the queue is the only path that "
            "sustains thousands of msg/s"
        ),
    }

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
            completion_mechanism="awaited",
            native_async=True,
            publish_sync_on_loop=False,
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
            "completion_mechanism": caps.completion_mechanism,
            "synthetic_mids": caps.synthetic_mids,
            "private_api": dict(cls._PRIVATE_API),
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
        self._bridge.run(self.aconnect(host, port, keepalive))

    async def aconnect(self, host: str, port: int, keepalive: int = 60) -> None:
        """The library calls, on whichever loop is running.

        The sync facade hands this to the bridge; the native driver awaits it on
        the role worker's own loop. One call site either way.
        """
        self._stopping = False
        scheme = "mqtts" if self._tls_ca_certs else "mqtt"
        uri = f"{scheme}://{host}:{port}/"
        await self._client.connect(
            uri,
            cleansession=self._clean_session,
            cafile=self._tls_ca_certs,
        )
        self._connected = True
        self._fire_on_connect(flags={}, reason_code=0, properties=None)
        self._start_pump()

    async def _message_pump(self) -> None:
        """Drain inbound publishes from amqtt's delivery queue.

        Do **not** call ``MQTTClient.deliver_message()`` in a tight loop: that
        helper spawns a new ``asyncio.Task`` per wait and uses ``asyncio.wait``
        with cancel-on-timeout, which collapses to tens of msg/s under
        emqtt-bench ingress. Reading ``session.delivered_message_queue``
        directly sustains thousands of msg/s on the same workload.
        """
        assert self._client is not None
        session = getattr(self._client, "session", None)
        queue = getattr(session, "delivered_message_queue", None)
        if queue is None:
            raise RuntimeError("amqtt session delivery queue is not available")
        while not self._stopping:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=1.0)
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

    async def adisconnect(self) -> None:
        await self._stop_pump()
        try:
            await self._client.disconnect()
        finally:
            self._connected = False

    def disconnect(self) -> None:
        if self._client is None or not self._connected:
            return
        self._ensure_bridge()
        try:
            self._bridge.run(self.adisconnect(), timeout=10.0)
        except Exception:  # noqa: BLE001
            self._connected = False

    @staticmethod
    def _as_bytes(payload: Any) -> bytes:
        if payload is None:
            return b""
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
        if isinstance(payload, str):
            return payload.encode("utf-8")
        return bytes(payload)

    async def apublish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> Optional[int]:
        """Await one publish to completion; the await *is* the completion."""
        client = self._client
        if client is None or not self._connected:
            raise RuntimeError("amqtt client is not connected")
        await client.publish(topic, self._as_bytes(payload), qos=qos, retain=retain)
        return self.alloc_mid()

    async def asubscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        client = self._client
        if client is None or not self._connected:
            raise RuntimeError("amqtt client is not connected")
        grants = await client.subscribe([(topic, qos)])
        mid = self.alloc_mid()
        self._fire_on_subscribe(mid, list(grants) if grants else [qos], None)
        return SubscribeResult(rc=0, mid=mid)

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

        self.schedule_coro(_publish())
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

        self.schedule_coro(_subscribe())
        return SubscribeResult(rc=0, mid=mid)
