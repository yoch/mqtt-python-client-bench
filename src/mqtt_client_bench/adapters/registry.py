"""Registry mapping --client names to adapter classes."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional, Type

from mqtt_client_bench.adapters.aiomqtt import AiomqttAdapter
from mqtt_client_bench.adapters.aiomqtt3 import Aiomqtt3Adapter
from mqtt_client_bench.adapters.amqtt import AmqttAdapter
from mqtt_client_bench.adapters.awscrt import AwscrtAdapter
from mqtt_client_bench.adapters.base import AdapterCapabilities, MqttClientAdapter
from mqtt_client_bench.adapters.gmqtt import GmqttAdapter
from mqtt_client_bench.adapters.paho import PahoAdapter
from mqtt_client_bench.adapters.paho_fork import PahoForkAdapter
from mqtt_client_bench.adapters.zmqtt import ZmqttAdapter

_ADAPTERS: Dict[str, Type] = {
    "paho": PahoAdapter,
    "gmqtt": GmqttAdapter,
    "aiomqtt": AiomqttAdapter,
    "amqtt": AmqttAdapter,
    "awscrt": AwscrtAdapter,
    "zmqtt": ZmqttAdapter,
    "aiomqtt3": Aiomqtt3Adapter,
    "paho-fork": PahoForkAdapter,
}

CLIENT_NAMES = tuple(_ADAPTERS.keys())
STABLE_CLIENTS = tuple(
    name for name, cls in _ADAPTERS.items() if cls.capabilities().stability == "stable"
)
EXPERIMENTAL_CLIENTS = tuple(
    name for name, cls in _ADAPTERS.items() if cls.capabilities().stability == "experimental"
)

# Module prefixes purged when injecting a client_path checkout.
_CLIENT_MODULE_PREFIXES = {
    "paho": ("paho",),
    "paho-fork": ("paho",),
    "gmqtt": ("gmqtt",),
    "aiomqtt": ("aiomqtt",),
    "aiomqtt3": ("aiomqtt", "mqtt5"),
    "amqtt": ("amqtt", "hbmqtt"),
    "awscrt": ("awscrt",),
    "zmqtt": ("zmqtt",),
}


def list_clients() -> List[dict]:
    rows = []
    for name, cls in _ADAPTERS.items():
        caps: AdapterCapabilities = cls.capabilities()
        rows.append(
            {
                "name": name,
                "async_bridged": caps.async_bridged,
                "mqtt_v5": caps.mqtt_v5,
                "qos2": caps.qos2,
                "max_inflight": caps.max_inflight,
                "message_callback_add": caps.message_callback_add,
                "native_message_callback_add": caps.native_message_callback_add,
                "stability": caps.stability,
                "io_model": caps.io_model,
                "implementation_language": caps.implementation_language,
                "synthetic_mids": caps.synthetic_mids,
                "unimplemented": list(caps.unimplemented),
                "notes": caps.notes,
            }
        )
    return rows


def get_adapter_class(name: str) -> Type:
    key = name.strip().lower()
    if key not in _ADAPTERS:
        raise KeyError(f"unknown MQTT client adapter {name!r}; choose from {', '.join(CLIENT_NAMES)}")
    return _ADAPTERS[key]


def configure_client_path(client: str, client_path: Optional[str]) -> Optional[str]:
    """Optionally put a checkout ahead of sys.path and purge cached modules.

    Returns the resolved path that was injected, or None when using the installed package.
    Layouts supported:
      - <root>/src/<pkg>  (src layout, e.g. paho)
      - <root>/<pkg>      (flat layout)
      - <root>            (root itself on path)
    """
    if not client_path:
        return None
    root = Path(client_path).resolve()
    candidates = [root / "src", root]
    injected = None
    for candidate in candidates:
        if candidate.is_dir():
            path = str(candidate)
            while path in sys.path:
                sys.path.remove(path)
            sys.path.insert(0, path)
            injected = path
            break
    prefixes = _CLIENT_MODULE_PREFIXES.get(client, (client,))
    for name in list(sys.modules):
        for prefix in prefixes:
            if name == prefix or name.startswith(prefix + "."):
                del sys.modules[name]
                break
    return injected


def create_adapter(
    client: str,
    *,
    client_path: Optional[str] = None,
    client_id: str,
    protocol: str = "MQTTv311",
    clean_session: bool = True,
    max_inflight: int = 20,
    max_queued: int = 200,
    tls_ca_certs: Optional[str] = None,
) -> MqttClientAdapter:
    configure_client_path(client, client_path)
    cls = get_adapter_class(client)
    return cls.create(
        client_id=client_id,
        protocol=protocol,
        clean_session=clean_session,
        max_inflight=max_inflight,
        max_queued=max_queued,
        tls_ca_certs=tls_ca_certs,
    )


def adapter_identity(client: str, client_path: Optional[str] = None) -> dict:
    configure_client_path(client, client_path)
    cls = get_adapter_class(client)
    try:
        info = cls.identity()
    except Exception as exc:  # noqa: BLE001
        info = {"client": client, "adapter": client, "error": str(exc)}
    if client_path:
        info["client_path"] = str(Path(client_path).resolve())
    return info


def unsupported_for_client(client: str, point: dict) -> List[str]:
    caps = get_adapter_class(client).capabilities()
    return caps.missing_for_point(point)
