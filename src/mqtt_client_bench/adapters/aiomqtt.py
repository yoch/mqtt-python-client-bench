"""aiomqtt adapter stub (asyncio idiomatic API; v2 wraps paho)."""

from __future__ import annotations

from typing import Optional

from mqtt_client_bench.adapters.async_bridge import AsyncAdapterStub
from mqtt_client_bench.adapters.base import AdapterCapabilities


class AiomqttAdapter(AsyncAdapterStub):
    _NAME = "aiomqtt"
    _NOTES = (
        "aiomqtt — idiomatic asyncio MQTT client. Bench targets v2.x (paho backend). "
        "v3 (mqtt5 sans-io) may be added later as a separate client id."
    )
    _UNIMPLEMENTED = ["connect", "publish", "subscribe", "callbacks", "tls"]

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
            max_inflight=False,
            max_queued=False,
            message_callback_add=False,
            v5_publish_properties=True,
            notes=cls._NOTES,
            unimplemented=list(cls._UNIMPLEMENTED),
        )

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
        return adapter
