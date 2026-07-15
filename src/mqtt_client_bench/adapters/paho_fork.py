"""Experimental label for https://github.com/yoch/paho.mqtt.python (Paho fork A/B)."""

from __future__ import annotations

from mqtt_client_bench.adapters.base import AdapterCapabilities
from mqtt_client_bench.adapters.paho import PahoAdapter


class PahoForkAdapter(PahoAdapter):
    """Alias of Paho for the yoch/paho.mqtt.python fork (``--client-path``)."""

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        base = PahoAdapter.capabilities()
        return AdapterCapabilities(
            name="paho-fork",
            sync_api=base.sync_api,
            async_bridged=base.async_bridged,
            mqtt_v311=base.mqtt_v311,
            mqtt_v5=base.mqtt_v5,
            qos2=base.qos2,
            tls=base.tls,
            max_inflight=base.max_inflight,
            max_queued=base.max_queued,
            message_callback_add=base.message_callback_add,
            native_message_callback_add=base.native_message_callback_add,
            v5_publish_properties=base.v5_publish_properties,
            stability="experimental",
            io_model=base.io_model,
            implementation_language=base.implementation_language,
            synthetic_mids=base.synthetic_mids,
            tcp_nodelay=base.tcp_nodelay,
            notes=(
                "yoch/paho.mqtt.python fork "
                "(https://github.com/yoch/paho.mqtt.python); "
                "same adapter as `paho`, measured via --client-path."
            ),
            unimplemented=list(base.unimplemented),
        )

    @classmethod
    def identity(cls) -> dict:
        info = PahoAdapter.identity()
        caps = cls.capabilities()
        info["client"] = "paho-fork"
        info["adapter"] = "paho"
        info["stability"] = caps.stability
        info["display_note"] = caps.notes
        return info
