"""Minimal sync adapter interface shared by MQTT client libraries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, NamedTuple, Optional, Protocol, runtime_checkable


class AdapterNotImplemented(NotImplementedError):
    """Raised when an adapter method or capability is not yet wired."""


class PublishResult(NamedTuple):
    """Outcome of one publish call.

    A NamedTuple rather than a frozen dataclass because every adapter builds one
    for every message: the frozen dataclass __init__ routes through
    object.__setattr__ and measured 495 ns against 315 ns here, a cost the
    harness imposes on every client. Still immutable, so the guarantee that made
    it frozen is intact; a mutable slots dataclass was faster still and was not
    taken for that reason.
    """

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
    # Whether the library bounds its outbound queue in *bytes* as well as in
    # messages. Such a bound is invisible at small payloads and silently becomes
    # the binding window at large ones, leaving the client with a queue far
    # shallower than the `max_queued` messages every client is given. Declaring
    # it lets the role worker size it from the point's payload so the message
    # window is what binds, as it does for libraries with no byte bound at all.
    max_queued_bytes: bool = False
    message_callback_add: bool = False
    # Native broker-side filter matching (Paho). Emulated matching must not enter
    # inter-client rankings for sub_callback_matching.
    native_message_callback_add: bool = False
    v5_publish_properties: bool = False
    # Whether connect() may be called again on the same adapter instance after a
    # disconnect, resuming a persistent session. Required by the session-resume
    # scenarios; an adapter that cannot do it is refused rather than measured
    # against a fresh session.
    reconnect: bool = True
    stability: str = "stable"  # stable | experimental
    io_model: str = "sync"  # sync | asyncio_bridged | crt_event_loop
    implementation_language: str = "python"  # python | native
    # How a QoS>=1 completion reaches the role worker. Not cosmetic: an adapter
    # whose coroutine stays suspended for the whole round trip pays a resume per
    # message that one correlating the ack in a callback does not, measured at
    # 11-34% on this bench and growing with load. Five of the six bridged
    # clients are "awaited" because `await client.publish(...)` is the only API
    # their library offers; gmqtt and mqttium expose a cheaper path and take it.
    # Rule: every adapter uses the cheapest mechanism its library exposes, and
    # records which one, so a reader can see who was forced onto the slow path.
    completion_mechanism: str = "sync"  # sync | callback | awaited
    # Whether the role worker can drive this client on its own asyncio loop,
    # with no thread between the measurement and the library.
    #
    # The bench used to drive every client through a sync facade so that all of
    # them met the same outstanding-window gate. That equalised the *mechanism*
    # and not the *cost*: the cross-thread handoff is a fixed 18.5 us per
    # message, so it taxes a fast client far harder than a slow one — measured
    # at 46% of the period of a 25,000 msgs/s client against 11% at 6,000, which
    # is enough to reorder the field. Comparability comes from an identical
    # contract, not an identical driving mechanism; each client is now driven by
    # the fastest path its own API offers.
    native_async: bool = False
    # Within the async path: True when the library admits a publish
    # synchronously on the loop, so the role never allocates a coroutine per
    # message. False for libraries whose only API is `await publish(...)`, which
    # is then their fastest path and so the honest one to measure.
    publish_sync_on_loop: bool = False
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
        if point.get("outage_s") is not None and not self.reconnect:
            missing.append("reconnect")
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


@runtime_checkable
class AsyncMqttClientAdapter(Protocol):
    """Adapter driven directly on the role worker's own asyncio loop.

    Same completion contract as the sync facade — QoS 0 at the transport, QoS 1
    at PUBACK, QoS 2 at PUBCOMP — and the same outstanding-window semantics. The
    difference is only that nothing crosses a thread: the role's loop *is* the
    client's loop, so a publish is a call and a completion is a callback on that
    same loop.

    Two publish shapes, chosen once by `publish_sync_on_loop` and never branched
    on per message:

    - ``publish_nowait`` for libraries that admit synchronously on the loop
      (mqttium, gmqtt). Returns the synthetic mid; completion arrives later
      through ``on_publish``.
    - ``publish`` for libraries whose only API is awaitable (aiomqtt, amqtt,
      zmqtt). Awaiting is their fastest path, so awaiting is what is measured.

    Implementations must fire ``on_publish(mid, reason_code)`` on the loop.
    """

    @classmethod
    def capabilities(cls) -> AdapterCapabilities: ...

    @classmethod
    def identity(cls) -> dict: ...

    async def connect(self, host: str, port: int, keepalive: int = 60) -> None: ...

    async def disconnect(self) -> None: ...

    def publish_nowait(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> Optional[int]:
        """Admit one publish on the loop; return its synthetic mid."""

    async def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> Optional[int]:
        """Await one publish to completion; return its synthetic mid."""

    async def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult: ...

    def build_publish_properties(self, profile: str) -> Any: ...

    on_connect: Optional[MessageCallback]
    on_publish: Optional[MessageCallback]
    on_message: Optional[MessageCallback]
