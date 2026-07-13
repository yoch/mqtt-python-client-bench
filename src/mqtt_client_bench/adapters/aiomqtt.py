"""aiomqtt adapter — idiomatic asyncio MQTT client (v2.x) via AsyncioBridge."""

from __future__ import annotations

import logging
import socket
from pathlib import Path
from typing import Any, Optional

from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, PublishResult, SubscribeResult
from mqtt_client_bench.adapters.paho import build_paho_publish_properties


class AiomqttAdapter(BridgedAdapterBase):
    _NAME = "aiomqtt"
    _NOTES = (
        "aiomqtt — idiomatic asyncio MQTT client. Bench targets v2.x (paho backend). "
        "v3 (mqtt5 sans-io) is the separate experimental client id `aiomqtt3`."
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

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="aiomqtt",
            sync_api=False,
            async_bridged=True,
            mqtt_v311=True,
            mqtt_v5=True,
            qos2=True,
            tls=True,
            max_inflight=True,
            max_queued=True,
            message_callback_add=True,
            native_message_callback_add=False,
            v5_publish_properties=True,
            stability="stable",
            io_model="asyncio_bridged",
            implementation_language="python",
            synthetic_mids=True,
            notes=cls._NOTES,
            unimplemented=[],
        )

    @classmethod
    def identity(cls) -> dict:
        import aiomqtt

        caps = cls.capabilities()
        return {
            "client": "aiomqtt",
            "adapter": "aiomqtt",
            "client_module": str(Path(aiomqtt.__file__).resolve()),
            "client_version": getattr(aiomqtt, "__version__", None),
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
    ) -> "AiomqttAdapter":
        try:
            import aiomqtt  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "aiomqtt is not installed. Install with: pip install 'mqtt-client-bench[aiomqtt]'"
            ) from exc

        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        adapter._max_inflight = max_inflight
        adapter._max_queued = max_queued
        return adapter

    def _protocol_enum(self):
        import aiomqtt

        if self._protocol == "MQTTv5":
            return aiomqtt.ProtocolVersion.V5
        if self._protocol == "MQTTv31":
            return aiomqtt.ProtocolVersion.V31
        return aiomqtt.ProtocolVersion.V311

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        import aiomqtt

        self._ensure_bridge()
        self._stopping = False
        tls_params = None
        if self._tls_ca_certs:
            tls_params = aiomqtt.TLSParameters(ca_certs=self._tls_ca_certs)

        # aiomqtt logs a WARNING per publish once pending calls exceed its
        # threshold (default 10) — at bench outstanding windows that floods
        # stderr with per-message I/O and biases throughput. Keep it quiet.
        quiet_logger = logging.getLogger("mqtt_client_bench.aiomqtt")
        quiet_logger.setLevel(logging.ERROR)
        quiet_logger.propagate = False
        if not quiet_logger.handlers:
            quiet_logger.addHandler(logging.NullHandler())

        kwargs: dict[str, Any] = {
            "hostname": host,
            "port": port,
            "identifier": self._client_id,
            "protocol": self._protocol_enum(),
            "keepalive": keepalive,
            "tls_params": tls_params,
            "max_inflight_messages": self._max_inflight,
            "max_queued_outgoing_messages": self._max_queued,
            "logger": quiet_logger,
        }
        # Paho rejects clean_session for MQTT 5; use clean_start instead.
        if self._protocol == "MQTTv5":
            kwargs["clean_start"] = self._clean_session
            kwargs["clean_session"] = None
        else:
            kwargs["clean_session"] = self._clean_session

        async def _connect():
            self._client = aiomqtt.Client(**kwargs)
            # Skip the per-publish pending-calls warning branch entirely.
            self._client.pending_calls_threshold = 1 << 30
            await self._client.__aenter__()
            # aiomqtt drives paho's raw socket itself (Nagle left on); align
            # with asyncio clients which enable TCP_NODELAY by default.
            # aiomqtt claims on_socket_open for its loop glue, so set the
            # option on the live socket instead.
            try:
                sock = self._client._client.socket()  # noqa: SLF001
                if sock is not None:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except (OSError, ValueError, AttributeError):
                pass
            self._connected = True
            self._fire_on_connect(flags={}, reason_code=0, properties=None)
            self._start_pump()

        self._bridge.run(_connect())

    async def _message_pump(self) -> None:
        assert self._client is not None
        try:
            async for message in self._client.messages:
                if self._stopping:
                    break
                msg = IncomingMessage(
                    topic=str(message.topic),
                    payload=message.payload,
                    qos=int(getattr(message, "qos", 0) or 0),
                    retain=bool(getattr(message, "retain", False)),
                )
                self._dispatch_message(msg)
        except Exception:  # noqa: BLE001
            if not self._stopping:
                raise

    def disconnect(self) -> None:
        if self._client is None or not self._connected:
            return
        self._ensure_bridge()

        async def _disconnect():
            await self._stop_pump()
            client = self._client
            self._client = None
            self._connected = False
            if client is not None:
                await client.__aexit__(None, None, None)

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

        async def _publish():
            try:
                kwargs: dict[str, Any] = {"payload": payload, "qos": qos, "retain": retain}
                if properties is not None:
                    kwargs["properties"] = properties
                await client.publish(topic, **kwargs)
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
                result = await client.subscribe(topic, qos=qos)
                grants = list(result) if isinstance(result, (list, tuple)) else [0]
                self._fire_on_subscribe(mid, grants, None)
            except Exception:  # noqa: BLE001
                self._fire_on_subscribe(mid, [128], None)

        self._bridge.create_task(_subscribe())
        return SubscribeResult(rc=0, mid=mid)

    def build_publish_properties(self, profile: str) -> Any:
        return build_paho_publish_properties(profile)
