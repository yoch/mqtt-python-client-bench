"""Minimal sync adapter interface shared by MQTT client libraries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Protocol, runtime_checkable


class AdapterNotImplemented(NotImplementedError):
    """Raised when an adapter method or capability is not yet wired."""


@dataclass(frozen=True)
class PublishResult:
    rc: int
    mid: Optional[int] = None


@dataclass(frozen=True)
class SubscribeResult:
    rc: int
    mid: Optional[int] = None


@dataclass(frozen=True)
class AdapterCapabilities:
    """Feature matrix used to refuse unsupported scenario knobs early."""

    name: str
    sync_api: bool = True
    async_bridged: bool = False
    mqtt_v311: bool = True
    mqtt_v5: bool = False
    qos2: bool = True
    tls: bool = True
    max_inflight: bool = False
    max_queued: bool = False
    message_callback_add: bool = False
    # Native broker-side filter matching (Paho). Emulated matching must not enter
    # inter-client rankings for sub_callback_matching.
    native_message_callback_add: bool = False
    v5_publish_properties: bool = False
    stability: str = "stable"  # stable | experimental
    io_model: str = "sync"  # sync | asyncio_bridged | crt_event_loop
    implementation_language: str = "python"  # python | native
    synthetic_mids: bool = False
    # Whether the transport runs with TCP_NODELAY (set by the adapter or by the
    # runtime, e.g. asyncio). Without it, request/response scenarios measure a
    # deterministic Nagle+delayed-ACK plateau (~40 ms/hop) instead of the client.
    tcp_nodelay: bool = True
    notes: str = ""
    unimplemented: List[str] = field(default_factory=list)

    def missing_for_point(self, point: dict) -> List[str]:
        missing: List[str] = []
        protocol = point.get("protocol", "MQTTv311")
        if protocol == "MQTTv5" and not self.mqtt_v5:
            missing.append("mqtt_v5")
        if protocol == "MQTTv311" and not self.mqtt_v311:
            missing.append("mqtt_v311")
        qos_pub = int(point.get("qos_publish", 0) or 0)
        qos_sub = int(point.get("qos_subscribe", 0) or 0)
        if max(qos_pub, qos_sub) >= 2 and not self.qos2:
            missing.append("qos2")
        if point.get("tls") and not self.tls:
            missing.append("tls")
        if point.get("require_max_inflight") and not self.max_inflight:
            missing.append("max_inflight")
        if point.get("require_max_queued") and not self.max_queued:
            missing.append("max_queued")
        if int(point.get("callback_filters", 0) or 0) > 0 and not self.native_message_callback_add:
            missing.append("native_message_callback_add")
        if point.get("topology") == "fleet" and self.async_bridged:
            missing.append("fleet_async_bridged")
        if point.get("topology") == "application_rtt" and not self.tcp_nodelay:
            # Ping-pong traffic without TCP_NODELAY measures the TCP stack's
            # Nagle/delayed-ACK plateau, not the client library.
            missing.append("tcp_nodelay")
        profile = point.get("properties_profile", "none")
        if protocol == "MQTTv5" and profile not in (None, "none") and not self.v5_publish_properties:
            missing.append(f"properties_profile:{profile}")
        for item in self.unimplemented:
            missing.append(f"adapter:{item}")
        return missing


MessageCallback = Callable[..., Any]


@runtime_checkable
class MqttClientAdapter(Protocol):
    """Sync facade used by role workers (async libs bridge via a private loop).

    Publish completion contract (primary metric boundary):
      - QoS 0: on_publish fires when the packet has been handed to the transport
      - QoS 1: on_publish fires on PUBACK
      - QoS 2: on_publish fires on PUBCOMP (not PUBREC)
    Adapters that cannot honour a boundary must set the matching capability False
    (e.g. qos2=False) so scenarios requiring it are refused, not approximated.
    """

    MQTT_ERR_SUCCESS: int

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
    ) -> "MqttClientAdapter": ...

    @classmethod
    def capabilities(cls) -> AdapterCapabilities: ...

    @classmethod
    def identity(cls) -> dict: ...

    def connect(self, host: str, port: int, keepalive: int = 60) -> None: ...

    def disconnect(self) -> None: ...

    def loop_start(self) -> None: ...

    def loop_stop(self) -> None: ...

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult: ...

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult: ...

    def message_callback_add(self, topic: str, callback: MessageCallback) -> None: ...

    def build_publish_properties(self, profile: str) -> Any: ...

    # Callback attributes (paho VERSION2-compatible signatures where possible).
    on_connect: Optional[MessageCallback]
    on_publish: Optional[MessageCallback]
    on_message: Optional[MessageCallback]
    on_subscribe: Optional[MessageCallback]
