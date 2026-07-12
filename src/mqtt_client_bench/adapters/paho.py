"""Paho MQTT adapter — full sync implementation of the bench client interface."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from mqtt_client_bench.adapters.base import (
    AdapterCapabilities,
    PublishResult,
    SubscribeResult,
)


def build_paho_publish_properties(profile: str) -> Any:
    """MQTT v5 PUBLISH properties shared by Paho and aiomqtt (paho Properties objects)."""
    if profile in (None, "none"):
        return None
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties

    props = Properties(PacketTypes.PUBLISH)
    if profile == "realistic":
        props.PayloadFormatIndicator = 1
        props.ContentType = "application/json"
        props.MessageExpiryInterval = 60
        props.UserProperty = [("schema", "telemetry.v1"), ("region", "eu-west-1")]
    elif profile == "rich":
        props.PayloadFormatIndicator = 1
        props.ContentType = "application/json"
        props.MessageExpiryInterval = 60
        props.CorrelationData = b"c" * 32
        props.ResponseTopic = "bench/response/" + ("r" * 48)
        props.UserProperty = [(f"k{i:02d}", "v" * 64) for i in range(16)]
    else:
        return None
    return props


class PahoAdapter:
    MQTT_ERR_SUCCESS = 0

    def __init__(self, client: Any, mqtt_mod: Any):
        self._client = client
        self._mqtt = mqtt_mod
        self.MQTT_ERR_SUCCESS = int(getattr(mqtt_mod, "MQTT_ERR_SUCCESS", 0))

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="paho",
            sync_api=True,
            async_bridged=False,
            mqtt_v311=True,
            mqtt_v5=True,
            qos2=True,
            tls=True,
            max_inflight=True,
            max_queued=True,
            message_callback_add=True,
            native_message_callback_add=True,
            v5_publish_properties=True,
            stability="stable",
            io_model="sync",
            implementation_language="python",
            notes="Eclipse Paho MQTT Python (callback API v2).",
        )

    @classmethod
    def identity(cls) -> dict:
        import paho
        import paho.mqtt

        caps = cls.capabilities()
        # The version lives on paho.mqtt (the namespace package has none).
        version = getattr(paho.mqtt, "__version__", None)
        if version is None:
            try:
                from importlib.metadata import version as pkg_version

                version = pkg_version("paho-mqtt")
            except Exception:  # noqa: BLE001
                version = None
        return {
            "client": "paho",
            "adapter": "paho",
            "client_module": str(Path(paho.__file__).resolve()),
            "client_version": version,
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
    ) -> "PahoAdapter":
        import paho.mqtt.client as mqtt

        proto = getattr(mqtt, protocol)
        kwargs: dict[str, Any] = {
            "callback_api_version": mqtt.CallbackAPIVersion.VERSION2,
            "client_id": client_id,
            "protocol": proto,
        }
        if protocol != "MQTTv5":
            kwargs["clean_session"] = clean_session
        client = mqtt.Client(**kwargs)
        client.max_inflight_messages = max_inflight
        client.max_queued_messages = max_queued
        adapter = cls(client, mqtt)
        if tls_ca_certs:
            adapter.tls_set(ca_certs=tls_ca_certs)
        return adapter

    def tls_set(self, ca_certs: str) -> None:
        self._client.tls_set(ca_certs=ca_certs)

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        self._client.connect(host, port, keepalive=keepalive)

    def disconnect(self) -> None:
        self._client.disconnect()

    def loop_start(self) -> None:
        self._client.loop_start()

    def loop_stop(self) -> None:
        self._client.loop_stop()

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult:
        kwargs: dict[str, Any] = {"payload": payload, "qos": qos, "retain": retain}
        if properties is not None:
            kwargs["properties"] = properties
        info = self._client.publish(topic, **kwargs)
        return PublishResult(rc=int(info.rc), mid=getattr(info, "mid", None))

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        result, mid = self._client.subscribe(topic, qos=qos)
        return SubscribeResult(rc=int(result), mid=mid)

    def message_callback_add(self, topic: str, callback) -> None:
        self._client.message_callback_add(topic, callback)

    def build_publish_properties(self, profile: str) -> Any:
        return build_paho_publish_properties(profile)

    @property
    def on_connect(self):
        return self._client.on_connect

    @on_connect.setter
    def on_connect(self, cb) -> None:
        self._client.on_connect = cb

    @property
    def on_publish(self):
        return self._client.on_publish

    @on_publish.setter
    def on_publish(self, cb) -> None:
        self._client.on_publish = cb

    @property
    def on_message(self):
        return self._client.on_message

    @on_message.setter
    def on_message(self, cb) -> None:
        self._client.on_message = cb

    @property
    def on_subscribe(self):
        return self._client.on_subscribe

    @on_subscribe.setter
    def on_subscribe(self, cb) -> None:
        self._client.on_subscribe = cb
