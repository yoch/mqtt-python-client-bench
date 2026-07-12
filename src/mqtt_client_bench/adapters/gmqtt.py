"""gmqtt adapter stub (asyncio + callbacks; MQTT 5 native)."""

from __future__ import annotations

from typing import Optional

from mqtt_client_bench.adapters.async_bridge import AsyncAdapterStub
from mqtt_client_bench.adapters.base import AdapterCapabilities


class GmqttAdapter(AsyncAdapterStub):
    _NAME = "gmqtt"
    _NOTES = (
        "Wialon gmqtt — asyncio MQTT v5 client with callback/plugin API. "
        "Sync facade will run the event loop on a dedicated thread."
    )
    _UNIMPLEMENTED = ["connect", "publish", "subscribe", "callbacks", "tls"]

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="gmqtt",
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
    ) -> "GmqttAdapter":
        # Import is deferred so listing capabilities does not require the package.
        try:
            import gmqtt  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "gmqtt is not installed. Install with: pip install 'mqtt-client-bench[gmqtt]'"
            ) from exc
        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        return adapter
