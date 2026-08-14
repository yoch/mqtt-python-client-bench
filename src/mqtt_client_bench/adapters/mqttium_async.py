"""MQTTium driven on the role worker's own loop — no bridge, no thread crossing.

The sync facade in `mqttium.py` remains for the sync role path during migration.
This one exists because the handoff between the two was measured at 18.5 us per
message: a fixed cost, so it taxed this client (56 us per message natively) far
harder than a slow one, which is enough to reorder a ranking.

``FlowControlError`` from ``publish_nowait`` is mapped to ``mid is None`` (Paho
queue-full), not to ``on_publish`` reason 128. The facade in ``mqttium.py`` still
returns a mid before the bridge call and cannot do that.
"""

from __future__ import annotations

import ssl
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from mqtt_client_bench.adapters.base import AdapterCapabilities, SubscribeResult
from mqtt_client_bench.adapters.mqttium import MqttiumAdapter

try:
    from mqttium.errors import FlowControlError
except ImportError:  # mqttium extra not installed; the adapter is still importable

    class FlowControlError(Exception):
        """Stand-in so the module imports without the mqttium extra."""


class MqttiumAsyncAdapter:
    """Native async adapter; same capabilities and identity as the sync one."""

    def __init__(self) -> None:
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None
        self._max_inflight = 20
        self._max_queued = 200
        self._max_queued_bytes: Optional[int] = None
        self._next_mid = 0
        # Real packet id -> synthetic mids, FIFO because ids are reused. Only
        # the loop touches it, so no lock: admission and delivery are both on it.
        self._real_to_synth: Dict[int, Deque[int]] = {}
        self.on_connect = None
        self.on_publish = None
        self.on_message = None

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return MqttiumAdapter.capabilities()

    @classmethod
    def identity(cls) -> dict:
        return MqttiumAdapter.identity()

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
    ) -> "MqttiumAsyncAdapter":
        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._max_inflight = max_inflight
        adapter._max_queued = max_queued
        adapter._max_queued_bytes = max_queued_bytes
        adapter._tls_ca_certs = tls_ca_certs
        return adapter

    def _alloc_mid(self) -> int:
        self._next_mid = 1 if self._next_mid >= 65535 else self._next_mid + 1
        return self._next_mid

    async def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        from mqttium.api import AsyncClient
        from mqttium.enums import MQTTProtocolVersion

        tls: Any = None
        if self._tls_ca_certs:
            tls = ssl.create_default_context(cafile=self._tls_ca_certs)

        kwargs: Dict[str, Any] = {}
        if self._max_queued_bytes:
            kwargs["max_outbound_bytes"] = max(1 << 20, int(self._max_queued_bytes))
            kwargs["max_pending_outbound_bytes"] = max(64 << 20, int(self._max_queued_bytes))

        self._client = AsyncClient(
            client_id=self._client_id,
            protocol=getattr(MQTTProtocolVersion, self._protocol),
            clean_start=self._clean_session,
            keepalive=keepalive,
            max_outbound_inflight=max(1, int(self._max_inflight)),
            max_pending_outbound_messages=max(0, int(self._max_queued)),
            message_delivery="callback",
            **kwargs,
        )

        if self.on_message is not None:
            def _on_message(msg) -> None:
                self.on_message(self, None, msg)

            self._client.on_message = _on_message

        await self._client.connect(host, port, ssl=tls)
        if self.on_connect is not None:
            self.on_connect(self, None, {}, 0, None)

    async def disconnect(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            await client.disconnect()

    def _arm_completions(self) -> None:
        """Install on_publish on first QoS>=1 use, never before.

        mqttium takes its direct QoS0 transport write only while on_publish is
        None (`_direct_qos0_ready`); arming it up front cost 38% of the QoS0
        rate. QoS is fixed per measurement point, so a QoS0 point never arms it.
        """
        if self._client.on_publish is not None:
            return

        def _on_publish(mid, reason=None) -> None:
            if mid is None:
                return
            pending = self._real_to_synth.get(int(mid))
            if not pending:
                return
            synth = pending.popleft()
            if not pending:
                self._real_to_synth.pop(int(mid), None)
            cb = self.on_publish
            if cb is not None:
                cb(self, None, synth, 0 if reason is None else 128, None)

        self._client.on_publish = _on_publish

    def publish_nowait(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> Optional[int]:
        """Admit one publish, or return None if the library refused.

        ``FlowControlError`` means the write pump / pending window is full, not
        that a packet failed. Returning None (no ``on_publish``) matches Paho's
        queue-full ``mid is None`` contract so the role counts backpressure
        instead of ``protocol_failed``. Other exceptions propagate.
        """
        data = b"" if payload is None else payload
        if isinstance(data, str):
            data = data.encode("utf-8")
        if int(qos) == 0:
            try:
                self._client.publish_nowait(topic, data, qos=0, retain=retain, properties=properties)
            except FlowControlError:
                return None
            mid = self._alloc_mid()
            cb = self.on_publish
            if cb is not None:
                cb(self, None, mid, 0, None)
            return mid
        self._arm_completions()
        try:
            receipt = self._client.publish_nowait(
                topic, data, qos=qos, retain=retain, properties=properties
            )
        except FlowControlError:
            return None
        mid = self._alloc_mid()
        if receipt.mid is None:
            cb = self.on_publish
            if cb is not None:
                cb(self, None, mid, 0, None)
            return mid
        self._real_to_synth.setdefault(int(receipt.mid), deque()).append(mid)
        return mid

    async def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        # Present for protocol completeness; this client admits synchronously,
        # so the role uses publish_nowait and never calls this.
        return self.publish_nowait(topic, payload, qos, retain, properties)

    async def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        result = await self._client.subscribe(topic, qos=qos)
        return SubscribeResult(rc=0, mid=int(result.mid))

    def build_publish_properties(self, profile: str) -> Any:
        return MqttiumAdapter.build_publish_properties(self, profile)
