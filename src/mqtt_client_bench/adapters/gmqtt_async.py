"""gmqtt driven on the role worker's own loop.

gmqtt exposes no `on_publish` callback at all, so QoS>=1 completion is taken
from the same private hook the sync adapter uses (`_remove_message_from_query`,
declared in `_PRIVATE_API`). Nothing about *what* is measured changes here; only
the thread the loop runs on does.
"""

from __future__ import annotations

import ssl
from typing import Any, Dict, Optional

from mqtt_client_bench.adapters.async_bridge import IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, SubscribeResult
from mqtt_client_bench.adapters.gmqtt import GmqttAdapter


class GmqttAsyncAdapter:
    def __init__(self) -> None:
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._ssl_context: Any = None
        self._next_mid = 0
        self._real_to_synth: Dict[int, int] = {}
        self.on_connect = None
        self.on_publish = None
        self.on_message = None

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return GmqttAdapter.capabilities()

    @classmethod
    def identity(cls) -> dict:
        return GmqttAdapter.identity()

    @classmethod
    def create(
        cls,
        *,
        client_id: str,
        protocol: str = "MQTTv311",
        clean_session: bool = True,
        max_inflight: int = 20,
        max_queued: int = 200,
        max_queued_bytes: Optional[int] = None,
        tls_ca_certs: Optional[str] = None,
    ) -> "GmqttAsyncAdapter":
        from gmqtt import Client as MQTTClient

        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._client = MQTTClient(client_id, clean_session=clean_session)
        if tls_ca_certs:
            adapter._ssl_context = ssl.create_default_context(cafile=tls_ca_certs)
        adapter._wire()
        return adapter

    def _alloc_mid(self) -> int:
        self._next_mid = 1 if self._next_mid >= 65535 else self._next_mid + 1
        return self._next_mid

    def _wire(self) -> None:
        client = self._client

        def _on_connect(gmqtt_client, session_present, result, properties):
            if self.on_connect is not None:
                self.on_connect(self, None, {"session present": bool(session_present)}, result, properties)

        def _on_message(gmqtt_client, topic, payload, qos, properties):
            if self.on_message is not None:
                self.on_message(
                    self, None,
                    IncomingMessage(topic=topic, payload=payload, qos=int(qos), retain=False),
                )

        client.on_connect = _on_connect
        client.on_message = _on_message

        orig = client._remove_message_from_query

        def _remove_and_ack(mid):
            orig(mid)
            synth = self._real_to_synth.pop(int(mid), None)
            cb = self.on_publish
            if cb is not None:
                cb(self, None, int(mid) if synth is None else synth, 0, None)

        client._remove_message_from_query = _remove_and_ack

    async def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        from gmqtt.mqtt.constants import MQTTv311, MQTTv50

        await self._client.connect(
            host,
            port=port,
            ssl=self._ssl_context if self._ssl_context is not None else False,
            keepalive=keepalive,
            version=MQTTv50 if self._protocol == "MQTTv5" else MQTTv311,
        )

    async def disconnect(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    def _message(self, topic, payload, qos, retain, properties):
        from gmqtt import Message

        kwargs: Dict[str, Any] = {}
        if isinstance(properties, dict):
            kwargs.update(properties)
        return Message(topic, payload, qos=qos, retain=retain, **kwargs)

    def publish_nowait(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> Optional[int]:
        mid = self._alloc_mid()
        client = self._client
        message = self._message(topic, payload, qos, retain, properties)
        cb = self.on_publish
        if int(qos) == 0:
            try:
                client.publish(message)
                if cb is not None:
                    cb(self, None, mid, 0, None)
            except Exception:  # noqa: BLE001
                if cb is not None:
                    cb(self, None, mid, 128, None)
            return mid

        # Mirrors gmqtt's own Client.publish() body: the public call discards the
        # packet id, which is the only handle on the PUBACK. `push_message_nowait`
        # keeps this synchronous; without it the storage push must be awaited, and
        # that older gmqtt goes down the awaited path below.
        real_mid = None
        try:
            real_mid, package = client._connection.publish(message)
            if real_mid is None:
                if cb is not None:
                    cb(self, None, mid, 0, None)
                return mid
            self._real_to_synth[int(real_mid)] = mid
            push = getattr(client._persistent_storage, "push_message_nowait", None)
            if push is None:
                raise RuntimeError("gmqtt persistent storage has no nowait push")
            push(int(real_mid), package)
        except Exception:  # noqa: BLE001
            if real_mid is not None:
                self._real_to_synth.pop(int(real_mid), None)
            if cb is not None:
                cb(self, None, mid, 128, None)
        return mid

    async def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        return self.publish_nowait(topic, payload, qos, retain, properties)

    async def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        mid = self._client.subscribe(topic, qos=qos)
        return SubscribeResult(rc=0, mid=int(getattr(mid, "mid", 0) or 0))

    def build_publish_properties(self, profile: str) -> Any:
        return GmqttAdapter.build_publish_properties(self, profile)
