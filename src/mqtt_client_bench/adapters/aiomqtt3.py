"""aiomqtt v3 adapter — pure asyncio + mqtt5 sans-io (experimental, MQTT 5 only).

aiomqtt v2 and v3 publish the same import name and cannot share an environment.
Install via: pip install 'mqtt-client-bench[aiomqtt3]'
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, PublishResult, SubscribeResult


def _require_aiomqtt_v3():
    import aiomqtt

    version = getattr(aiomqtt, "__version__", "") or ""
    major = 0
    try:
        major = int(str(version).split(".")[0].split("a")[0].split("b")[0])
    except ValueError:
        major = 0
    if major < 3:
        raise ImportError(
            f"aiomqtt3 adapter requires aiomqtt>=3 (found {version!r}). "
            "Use a separate environment: pip install 'mqtt-client-bench[aiomqtt3]'"
        )
    return aiomqtt


class Aiomqtt3Adapter(BridgedAdapterBase):
    _NAME = "aiomqtt3"
    _NOTES = (
        "aiomqtt v3 alpha — pure asyncio on mqtt5 (Rust sans-io). MQTT 5 only. "
        "Experimental; must not share an env with aiomqtt v2."
    )

    def __init__(self) -> None:
        super().__init__()
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv5"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="aiomqtt3",
            sync_api=False,
            async_bridged=True,
            mqtt_v311=False,
            mqtt_v5=True,
            qos2=True,
            tls=True,
            max_inflight=False,
            max_queued=False,
            message_callback_add=True,
            native_message_callback_add=False,
            v5_publish_properties=False,
            stability="experimental",
            io_model="asyncio_bridged",
            implementation_language="python",
            synthetic_mids=True,
            notes=cls._NOTES,
        )

    @classmethod
    def identity(cls) -> dict:
        aiomqtt = _require_aiomqtt_v3()
        caps = cls.capabilities()
        return {
            "client": "aiomqtt3",
            "adapter": "aiomqtt3",
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
        protocol: str = "MQTTv5",
        clean_session: bool = True,
        max_inflight: int = 20,
        max_queued: int = 200,
        tls_ca_certs: Optional[str] = None,
    ) -> "Aiomqtt3Adapter":
        try:
            _require_aiomqtt_v3()
        except ImportError as exc:
            raise ImportError(str(exc)) from exc
        if protocol != "MQTTv5":
            raise ValueError("aiomqtt3 only supports MQTTv5")
        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        return adapter

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        aiomqtt = _require_aiomqtt_v3()
        self._ensure_bridge()
        self._stopping = False
        kwargs: dict[str, Any] = {
            "hostname": host,
            "port": port,
            "identifier": self._client_id,
            "keepalive": keepalive,
        }
        if self._tls_ca_certs:
            import ssl

            kwargs["ssl_context"] = ssl.create_default_context(cafile=self._tls_ca_certs)

        async def _connect():
            self._client = aiomqtt.Client(**kwargs)
            await self._client.__aenter__()
            self._connected = True
            self._fire_on_connect(flags={}, reason_code=0, properties=None)
            self._start_pump()

        self._bridge.run(_connect())

    async def _message_pump(self) -> None:
        assert self._client is not None
        try:
            messages = self._client.messages
            if callable(messages):
                messages = messages()
            async for message in messages:
                if self._stopping:
                    break
                topic = getattr(message, "topic", None)
                topic_s = str(topic) if topic is not None else ""
                payload = getattr(message, "payload", b"")
                qos = int(getattr(message, "qos", 0) or 0)
                retain = bool(getattr(message, "retain", False))
                self._dispatch_message(
                    IncomingMessage(topic=topic_s, payload=payload, qos=qos, retain=retain)
                )
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
                data = b"" if payload is None else payload
                if isinstance(data, str):
                    data = data.encode("utf-8")
                # v3 uses positional bytes payload.
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
                # v3 renamed qos -> max_qos
                try:
                    await client.subscribe(topic, max_qos=qos)
                except TypeError:
                    await client.subscribe(topic, qos=qos)
                self._fire_on_subscribe(mid, [qos], None)
            except Exception:  # noqa: BLE001
                self._fire_on_subscribe(mid, [128], None)

        self._bridge.create_task(_subscribe())
        return SubscribeResult(rc=0, mid=mid)

    def build_publish_properties(self, profile: str) -> Any:
        # v3 exposes properties as packet attributes; bench profiles are advisory for now.
        if profile in (None, "none"):
            return None
        return {"profile": profile}
