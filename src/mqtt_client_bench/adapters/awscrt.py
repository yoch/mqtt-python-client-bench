"""AWS Common Runtime (awscrt) MQTT adapter — native C engine, generic broker."""

from __future__ import annotations

import threading
from concurrent.futures import Future
from pathlib import Path
from typing import Any, Optional

from mqtt_client_bench.adapters.base import (
    AdapterCapabilities,
    PublishResult,
    SubscribeResult,
)


class AwscrtAdapter:
    """Sync facade over awscrt.mqtt (v3.1.1) and awscrt.mqtt5 (v5).

    Declared as native / crt_event_loop so results are never presented as pure Python.
    """

    MQTT_ERR_SUCCESS = 0

    def __init__(self) -> None:
        self.on_connect = None
        self.on_publish = None
        self.on_message = None
        self.on_subscribe = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None
        self._conn: Any = None
        self._mqtt5: Any = None
        self._connected = threading.Event()
        self._topic_callbacks: dict[str, Any] = {}
        self._userdata = None
        self._mid_lock = threading.Lock()
        self._next_mid = 1

    def _alloc_mid(self) -> int:
        """Unique synthetic mid for paths where CRT exposes no packet id.

        mqtt3 QoS0 publishes all report packet_id=0 and the mqtt5 client hides
        packet ids entirely; hash-based or constant mids would collide inside
        the publisher's outstanding window and be miscounted as failures.
        """
        with self._mid_lock:
            mid = self._next_mid
            self._next_mid = 1 if self._next_mid >= 65535 else self._next_mid + 1
            return mid

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="awscrt",
            sync_api=True,
            async_bridged=False,
            mqtt_v311=True,
            mqtt_v5=True,
            qos2=False,  # awscrt mqtt3 QoS2 against Mosquitto currently fails PUBLISH completion
            tls=True,
            max_inflight=False,
            max_queued=False,
            message_callback_add=True,
            native_message_callback_add=False,
            v5_publish_properties=True,
            stability="stable",
            io_model="crt_event_loop",
            implementation_language="native",
            # mqtt3 QoS0 and all mqtt5 publishes use counter-allocated mids.
            synthetic_mids=True,
            # aws-c-io exposes no TCP_NODELAY knob and hides the fd; RTT
            # ping-pong would measure an ~84 ms Nagle plateau, so refuse it.
            tcp_nodelay=False,
            notes=(
                "AWS Common Runtime mqtt/mqtt5 clients (aws-c-mqtt). "
                "Native engine — not pure Python."
            ),
        )

    @classmethod
    def identity(cls) -> dict:
        import awscrt

        caps = cls.capabilities()
        return {
            "client": "awscrt",
            "adapter": "awscrt",
            "client_module": str(Path(awscrt.__file__).resolve()),
            "client_version": getattr(awscrt, "__version__", None),
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
    ) -> "AwscrtAdapter":
        try:
            import awscrt  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "awscrt is not installed. Install with: pip install 'mqtt-client-bench[awscrt]'"
            ) from exc
        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        return adapter

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        if self._protocol == "MQTTv5":
            self._connect_mqtt5(host, port, keepalive)
        else:
            self._connect_mqtt311(host, port, keepalive)

    def _tls_ctx(self):
        if not self._tls_ca_certs:
            return None
        from awscrt import io

        tls_opts = io.TlsContextOptions()
        tls_opts.override_default_trust_store_from_path(None, self._tls_ca_certs)
        return io.ClientTlsContext(tls_opts)

    def _connect_mqtt311(self, host: str, port: int, keepalive: int) -> None:
        from awscrt import mqtt
        from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup

        elg = EventLoopGroup(1)
        resolver = DefaultHostResolver(elg)
        bootstrap = ClientBootstrap(elg, resolver)
        tls = self._tls_ctx()
        client = mqtt.Client(bootstrap, tls)
        self._connected.clear()

        def on_success(connection, callback_data=None):
            self._connected.set()
            cb = self.on_connect
            if cb:
                cb(self, self._userdata, {}, 0, None)

        def on_failure(connection, error_code, callback_data=None):
            self._connected.set()

        self._conn = mqtt.Connection(
            client=client,
            host_name=host,
            port=port,
            client_id=self._client_id,
            clean_session=self._clean_session,
            keep_alive_secs=keepalive,
            on_connection_success=on_success,
            on_connection_failure=on_failure,
        )
        future = self._conn.connect()
        future.result(timeout=30)
        if not self._connected.wait(timeout=30):
            raise TimeoutError("awscrt mqtt connect timeout")

    def _connect_mqtt5(self, host: str, port: int, keepalive: int) -> None:
        from awscrt import mqtt5

        self._connected.clear()

        def on_success(data):
            self._connected.set()
            cb = self.on_connect
            if cb:
                cb(self, self._userdata, {}, 0, None)

        def on_failure(data):
            self._connected.set()

        def on_publish(data):
            packet = data.publish_packet
            msg = _Msg(str(packet.topic), packet.payload or b"", int(packet.qos), bool(packet.retain))
            self._dispatch(msg)

        opts = mqtt5.ClientOptions(
            host_name=host,
            port=port,
            tls_ctx=self._tls_ctx(),
            connect_options=mqtt5.ConnectPacket(
                client_id=self._client_id,
                keep_alive_interval_sec=keepalive,
            ),
            session_behavior=(
                mqtt5.ClientSessionBehaviorType.CLEAN
                if self._clean_session
                else mqtt5.ClientSessionBehaviorType.REJOIN_POST_SUCCESS
            ),
            on_lifecycle_event_connection_success_fn=on_success,
            on_lifecycle_event_connection_failure_fn=on_failure,
            on_publish_callback_fn=on_publish,
        )
        self._mqtt5 = mqtt5.Client(opts)
        self._mqtt5.start()
        if not self._connected.wait(timeout=30):
            raise TimeoutError("awscrt mqtt5 connect timeout")

    def disconnect(self) -> None:
        if self._conn is not None:
            try:
                self._conn.disconnect().result(timeout=10)
            except Exception:  # noqa: BLE001
                pass
            self._conn = None
        if self._mqtt5 is not None:
            try:
                self._mqtt5.stop()
            except Exception:  # noqa: BLE001
                pass
            self._mqtt5 = None

    def loop_start(self) -> None:
        return None

    def loop_stop(self) -> None:
        return None

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult:
        if self._mqtt5 is not None:
            return self._publish_mqtt5(topic, payload, qos, retain, properties)
        return self._publish_mqtt311(topic, payload, qos, retain)

    def _publish_mqtt311(self, topic, payload, qos, retain) -> PublishResult:
        from awscrt import mqtt

        assert self._conn is not None
        future, packet_id = self._conn.publish(topic, payload or b"", mqtt.QoS(qos), retain)
        # QoS0 has no packet id (CRT reports 0 for every message).
        mid = int(packet_id) if packet_id else self._alloc_mid()

        def _done(fut: Future):
            try:
                fut.result()
                rc = 0
            except Exception:  # noqa: BLE001
                rc = 128
            cb = self.on_publish
            if cb:
                cb(self, self._userdata, mid, rc, None)

        future.add_done_callback(_done)
        return PublishResult(rc=0, mid=mid)

    def _publish_mqtt5(self, topic, payload, qos, retain, properties) -> PublishResult:
        from awscrt import mqtt5

        assert self._mqtt5 is not None
        kwargs: dict[str, Any] = {
            "topic": topic,
            "payload": payload or b"",
            "qos": mqtt5.QoS(qos),
            "retain": retain,
        }
        if isinstance(properties, dict):
            if "content_type" in properties:
                kwargs["content_type"] = properties["content_type"]
            if "message_expiry_interval" in properties:
                kwargs["message_expiry_interval_sec"] = properties["message_expiry_interval"]
            if "correlation_data" in properties:
                kwargs["correlation_data"] = properties["correlation_data"]
            if "response_topic" in properties:
                kwargs["response_topic"] = properties["response_topic"]
            if "user_property" in properties:
                kwargs["user_properties"] = [
                    mqtt5.UserProperty(name=k, value=v) for k, v in properties["user_property"]
                ]
            if properties.get("payload_format_indicator"):
                kwargs["payload_format_indicator"] = mqtt5.PayloadFormatIndicator.UTF8
        packet = mqtt5.PublishPacket(**kwargs)
        # The mqtt5 client does not expose packet ids up front; correlate the
        # completion with a unique synthetic mid allocated before publish.
        mid = self._alloc_mid()
        future = self._mqtt5.publish(packet)

        def _done(fut: Future):
            try:
                fut.result()
                rc = 0
            except Exception:  # noqa: BLE001
                rc = 128
            cb = self.on_publish
            if cb:
                cb(self, self._userdata, mid, rc, None)

        future.add_done_callback(_done)
        return PublishResult(rc=0, mid=mid)

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        if self._mqtt5 is not None:
            from awscrt import mqtt5

            packet = mqtt5.SubscribePacket(
                subscriptions=[mqtt5.Subscription(topic_filter=topic, qos=mqtt5.QoS(qos))]
            )
            future = self._mqtt5.subscribe(packet)
            mid = self._alloc_mid()

            def _done(fut: Future):
                try:
                    fut.result()
                    grants = [qos]
                except Exception:  # noqa: BLE001
                    grants = [128]
                cb = self.on_subscribe
                if cb:
                    cb(self, self._userdata, mid, grants, None)

            future.add_done_callback(_done)
            return SubscribeResult(rc=0, mid=mid)

        from awscrt import mqtt

        assert self._conn is not None

        def _cb(topic, payload, dup, qos, retain, **kwargs):
            self._dispatch(_Msg(topic, payload, int(qos), bool(retain)))

        future, packet_id = self._conn.subscribe(topic, mqtt.QoS(qos), callback=_cb)
        mid = int(packet_id)

        def _done(fut: Future):
            try:
                fut.result()
                grants = [qos]
            except Exception:  # noqa: BLE001
                grants = [128]
            cb = self.on_subscribe
            if cb:
                cb(self, self._userdata, mid, grants, None)

        future.add_done_callback(_done)
        return SubscribeResult(rc=0, mid=mid)

    def message_callback_add(self, topic: str, callback) -> None:
        self._topic_callbacks[topic] = callback

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

    def _dispatch(self, msg) -> None:
        from mqtt_client_bench.adapters.async_bridge import topic_matches_sub

        matched = False
        for filt, callback in list(self._topic_callbacks.items()):
            if topic_matches_sub(filt, msg.topic):
                matched = True
                callback(self, self._userdata, msg)
        if not matched and self.on_message is not None:
            self.on_message(self, self._userdata, msg)


class _Msg:
    __slots__ = ("topic", "payload", "qos", "retain")

    def __init__(self, topic, payload, qos=0, retain=False):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
