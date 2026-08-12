"""zMQTT adapter — pure asyncio MQTT 3.1.1/5 client (experimental)."""

from __future__ import annotations

import asyncio
import ssl
from pathlib import Path
from typing import Any, Optional

from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, PublishResult, SubscribeResult


class ZmqttAdapter(BridgedAdapterBase):
    _NAME = "zmqtt"
    _NOTES = (
        "zMQTT (FastStream Community) — pure asyncio MQTT 3.1.1/5 client. "
        "Alpha maturity; excluded from stable rankings."
    )

    def __init__(self) -> None:
        super().__init__()
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None
        self._subscription: Any = None

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="zmqtt",
            sync_api=False,
            async_bridged=True,
            mqtt_v311=True,
            mqtt_v5=True,
            qos2=True,
            tls=True,
            max_inflight=False,
            max_queued=False,
            message_callback_add=True,
            native_message_callback_add=False,
            v5_publish_properties=True,
            stability="experimental",
            io_model="asyncio_bridged",
            implementation_language="python",
            completion_mechanism="awaited",
            native_async=True,
            publish_sync_on_loop=False,
            synthetic_mids=True,
            notes=cls._NOTES,
        )

    @classmethod
    def identity(cls) -> dict:
        import zmqtt

        version = getattr(zmqtt, "__version__", None)
        if version is None:
            try:
                from importlib.metadata import version as pkg_version

                version = pkg_version("zmqtt")
            except Exception:  # noqa: BLE001
                version = None
        caps = cls.capabilities()
        return {
            "client": "zmqtt",
            "adapter": "zmqtt",
            "client_module": str(Path(zmqtt.__file__).resolve()),
            "client_version": version,
            "stability": caps.stability,
            "io_model": caps.io_model,
            "implementation_language": caps.implementation_language,
            "completion_mechanism": caps.completion_mechanism,
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
    ) -> "ZmqttAdapter":
        try:
            import zmqtt  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "zmqtt is not installed. Install with: pip install 'mqtt-client-bench[zmqtt]'"
            ) from exc
        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        return adapter

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        self._ensure_bridge()
        self._bridge.run(self.aconnect(host, port, keepalive))

    async def aconnect(self, host: str, port: int, keepalive: int = 60) -> None:
        """The library calls, on whichever loop is running.

        The sync facade hands this to the bridge; the native driver awaits it on
        the role worker's own loop. One call site either way.
        """
        import zmqtt

        self._stopping = False
        tls: Any = False
        if self._tls_ca_certs:
            tls = ssl.create_default_context(cafile=self._tls_ca_certs)
        self._client = zmqtt.create_client(
            host,
            port,
            client_id=self._client_id,
            keepalive=keepalive,
            clean_session=self._clean_session,
            tls=tls,
            version="5.0" if self._protocol == "MQTTv5" else "3.1.1",
        )
        await self._client.connect()
        self._connected = True
        self._fire_on_connect(flags={}, reason_code=0, properties=None)

    async def _message_pump(self) -> None:
        # Activated after first subscribe via _ensure_subscription_pump.
        return None

    async def _ensure_subscription_pump(self, topic: str, qos: int) -> None:
        import zmqtt

        assert self._client is not None
        qos_enum = zmqtt.QoS(qos)

        async def _pump(sub):
            async with sub:
                async for message in sub:
                    if self._stopping:
                        break
                    self._dispatch_message(
                        IncomingMessage(
                            topic=str(message.topic),
                            payload=message.payload,
                            qos=int(getattr(message, "qos", qos) or qos),
                            retain=bool(getattr(message, "retain", False)),
                        )
                    )

        sub = self._client.subscribe(topic, qos=qos_enum)
        # Native mode has no bridge: the running loop is the client's own.
        if self._bridge is not None:
            self._bridge.create_task(_pump(sub))
        else:
            asyncio.ensure_future(_pump(sub))

    async def adisconnect(self) -> None:
        self._stopping = True
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

    def _publish_kwargs(self, qos: int, retain: bool, properties: Any) -> dict:
        import zmqtt

        kwargs: dict[str, Any] = {"qos": zmqtt.QoS(qos), "retain": retain}
        if properties is not None and self._protocol == "MQTTv5":
            props = properties
            if isinstance(properties, dict):
                props = zmqtt.PublishProperties(
                    content_type=properties.get("content_type"),
                    message_expiry_interval=properties.get("message_expiry_interval"),
                    correlation_data=properties.get("correlation_data"),
                    response_topic=properties.get("response_topic"),
                    payload_format_indicator=properties.get("payload_format_indicator"),
                    user_properties=tuple(properties.get("user_property") or ()),
                )
            kwargs["properties"] = props
        return kwargs

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
            raise RuntimeError("zmqtt client is not connected")
        await client.publish(
            topic, b"" if payload is None else payload,
            **self._publish_kwargs(qos, retain, properties),
        )
        return self.alloc_mid()

    async def asubscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        client = self._client
        if client is None or not self._connected:
            raise RuntimeError("zmqtt client is not connected")
        await self._ensure_subscription_pump(topic, qos)
        mid = self.alloc_mid()
        self._fire_on_subscribe(mid, [qos], None)
        return SubscribeResult(rc=0, mid=mid)

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult:
        import zmqtt

        mid = self.alloc_mid()
        client = self._client
        if client is None or not self._connected:
            return PublishResult(rc=1, mid=None)

        if payload is None:
            data: Any = b""
        else:
            data = payload

        async def _publish():
            try:
                kwargs: dict[str, Any] = {"qos": zmqtt.QoS(qos), "retain": retain}
                if properties is not None and self._protocol == "MQTTv5":
                    props = properties
                    if isinstance(properties, dict):
                        props = zmqtt.PublishProperties(
                            content_type=properties.get("content_type"),
                            message_expiry_interval=properties.get("message_expiry_interval"),
                            correlation_data=properties.get("correlation_data"),
                            response_topic=properties.get("response_topic"),
                            payload_format_indicator=properties.get("payload_format_indicator"),
                            user_properties=tuple(properties.get("user_property") or ()),
                        )
                    kwargs["properties"] = props
                await client.publish(topic, data, **kwargs)
                self._fire_on_publish(mid, reason_code=0)
            except Exception:  # noqa: BLE001
                self._fire_on_publish(mid, reason_code=128)

        self.schedule_coro(_publish())
        return PublishResult(rc=0, mid=mid)

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        mid = self.alloc_mid()
        if self._client is None or not self._connected:
            return SubscribeResult(rc=1, mid=None)

        async def _subscribe():
            try:
                await self._ensure_subscription_pump(topic, qos)
                self._fire_on_subscribe(mid, [qos], None)
            except Exception:  # noqa: BLE001
                self._fire_on_subscribe(mid, [128], None)

        self.schedule_coro(_subscribe())
        return SubscribeResult(rc=0, mid=mid)

    def build_publish_properties(self, profile: str) -> Any:
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
