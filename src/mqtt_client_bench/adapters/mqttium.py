"""MQTTium native adapter — AsyncClient via AsyncioBridge (PyPI ≥1.0.0rc11)."""

from __future__ import annotations

import asyncio
import inspect
import ssl
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage
from mqtt_client_bench.adapters.base import AdapterCapabilities, PublishResult, SubscribeResult

try:
    from mqttium.errors import FlowControlError
except ImportError:  # mqttium extra not installed; the adapter is still importable

    class FlowControlError(Exception):
        """Stand-in so the module imports without the mqttium extra."""


def is_flow_control(exc: BaseException) -> bool:
    """True for this process's FlowControlError and for one loaded via client_path.

    Role workers import the adapter before ``configure_client_path`` swaps the
    mqttium package. ``except FlowControlError`` then misses the class the
    checkout actually raises. Matching the name and module keeps both builds.
    """
    if isinstance(exc, FlowControlError):
        return True
    cls = type(exc)
    return cls.__name__ == "FlowControlError" and cls.__module__.startswith("mqttium")


def receipt_already_done(receipt: Any) -> bool:
    """QoS 0 receipts are complete at admission; QoS>=1 wait for PUBACK/PUBCOMP."""
    done = getattr(receipt, "is_done", None)
    return bool(done()) if callable(done) else False


def observe_receipt(receipt: Any, on_complete: Any) -> None:
    """Call ``on_complete(reason_code)`` when a 1.0.0rc15 receipt settles.

    ``PublishReceipt.wait`` is the public observer. It allocates a future only
    when somebody waits, then has to be driven by a Task. A Task per in-flight
    publish is a harness cost 1.0.0rc14 does not pay (one ``on_publish`` for
    every ack). The waiter list is what ``wait`` itself appends to, so one
    future and a done callback is the same observation without that task.
    ``_error`` is where the library records a terminal failure for ``wait`` to
    re-raise; there is no public accessor.
    """
    loop = asyncio.get_running_loop()
    waiter = loop.create_future()
    waiters = receipt._waiters
    if waiters is None:
        receipt._waiters = [waiter]
    else:
        waiters.append(waiter)

    def _done(_fut: asyncio.Future[Any]) -> None:
        on_complete(128 if receipt._error is not None else 0)

    waiter.add_done_callback(_done)


def _private_api() -> Dict[str, str]:
    """Declare whichever completion hook this build actually forces the adapter to use.

    1.0.0rc14 takes the direct QoS 0 write only while ``on_publish`` is unset.
    1.0.0rc15 removed that hook. Completion is still observed through
    ``PublishReceipt._waiters`` and ``_error``, the same fields ``wait()``
    uses, because awaiting ``wait()`` would allocate a task per publish.
    """
    try:
        from mqttium.api import AsyncClient
    except ImportError:
        return {}
    try:
        source = inspect.getsource(AsyncClient.__init__)
    except (OSError, TypeError):
        return {}
    if "self.on_publish" not in source:
        return {
            "PublishReceipt._waiters / PublishReceipt._error": (
                "1.0.0rc15 removed on_publish. QoS>=1 completion is observed by "
                "registering one future on the receipt, which is what "
                "PublishReceipt.wait does, and reading _error when it resolves. "
                "A Task per publish would be a harness tax rc14 does not pay."
            ),
        }
    return {
        "AsyncClient.on_publish is None / _direct_qos0_ready": (
            "direct QoS0 transport write is only taken while the library "
            "on_publish is unset; the adapter fires the bench callback itself "
            "and arms on_publish on the first QoS>=1 publish"
        ),
    }


def uses_on_publish(client: Any) -> bool:
    """1.0.0rc14 and earlier complete QoS>=1 through ``AsyncClient.on_publish``.

    1.0.0rc15 removed that hook. The direct QoS 0 write used to require it to
    stay unset, so the rc14 adapter arms it lazily; rc15 has nothing to arm.
    """
    return hasattr(client, "on_publish")


def outbound_bound_kwargs(
    client_cls: type,
    *,
    max_inflight: int,
    max_queued: int,
    max_queued_bytes: Optional[int],
) -> Dict[str, int]:
    """Map bench inflight/queue knobs onto whichever names this build froze.

    1.0.0rc14: ``max_pending_outbound_messages`` / ``max_pending_outbound_bytes``
    / ``max_outbound_bytes``. 1.0.0rc15: ``max_unacknowledged_messages`` /
    ``max_unacknowledged_bytes`` / ``max_write_queue_bytes``. A build that
    exposes neither is refused rather than run with the library default, which
    would silently change the window being measured.
    """
    names = set(inspect.signature(client_cls.__init__).parameters)
    if "max_outbound_inflight" not in names:
        raise RuntimeError("mqttium AsyncClient no longer accepts max_outbound_inflight")
    queued = max(0, int(max_queued))
    if "max_pending_outbound_messages" in names:
        message_key = "max_pending_outbound_messages"
        pending_bytes_key = "max_pending_outbound_bytes"
        queue_bytes_key = "max_outbound_bytes"
    elif "max_unacknowledged_messages" in names:
        message_key = "max_unacknowledged_messages"
        pending_bytes_key = "max_unacknowledged_bytes"
        queue_bytes_key = "max_write_queue_bytes"
    else:
        raise RuntimeError("mqttium AsyncClient has no known outbound message bound")
    kwargs: Dict[str, int] = {
        "max_outbound_inflight": max(1, int(max_inflight)),
        message_key: queued,
    }
    if max_queued_bytes:
        missing = [key for key in (pending_bytes_key, queue_bytes_key) if key not in names]
        if missing:
            raise RuntimeError(
                "mqttium AsyncClient is missing outbound byte bounds: " + ", ".join(missing)
            )
        # The 1 MiB write-pump default is 16 slots of a 64 KiB payload. Size
        # both byte windows from the requested depth so the message bound is
        # what binds, and never shrink them below the library defaults.
        kwargs[queue_bytes_key] = max(1 << 20, int(max_queued_bytes))
        kwargs[pending_bytes_key] = max(64 << 20, int(max_queued_bytes))
    return kwargs


class MqttiumAdapter(BridgedAdapterBase):
    """Bench the native ``mqttium.api.AsyncClient`` API (not the Paho façade).

    Publishes go through ``publish_nowait()``: loop-bound, non-suspending
    admission. QoS 0 completes when that call returns (handed to the writer).

    Through 1.0.0rc14, QoS>=1 completion is the library ``on_publish`` callback,
    armed on the first QoS>=1 publish and never on a QoS 0 point: the direct
    QoS 0 write runs only while ``on_publish is None``. 1.0.0rc15 removed
    ``on_publish``; QoS>=1 completion registers one future on the receipt
    (what ``PublishReceipt.wait()`` itself does) so admission stays synchronous
    and does not allocate a task per publish. A refused publish raises
    ``FlowControlError`` and is not counted as a completion.
    """

    _NAME = "mqttium"
    _NOTES = (
        "MQTTium AsyncClient (https://pypi.org/project/mqttium/) — async-native MQTT "
        "3.1.1/5; QoS0 via publish_nowait on the owning loop (PyPI ≥1.0.0rc11). "
        "Through 1.0.0rc14, QoS>=1 uses on_publish armed lazily so the direct QoS0 "
        "write stays available. From 1.0.0rc15, QoS>=1 uses PublishReceipt.wait(). "
        "Native message_callback_add."
    )

    def __init__(self) -> None:
        super().__init__()
        self._client: Any = None
        self._client_id = ""
        self._protocol = "MQTTv311"
        self._clean_session = True
        self._tls_ca_certs: Optional[str] = None
        self._max_inflight = 20
        self._max_queued = 200
        self._max_queued_bytes: Optional[int] = None
        # Real packet id -> the synthetic mids the role worker is waiting on,
        # FIFO because mqttium reuses ids. Written and read on the loop thread
        # only: publish_nowait admits on that thread and on_publish is delivered
        # on it, so no lock is needed on the hot path.
        self._real_to_synth: Dict[int, Deque[int]] = {}
        self._on_publish_cb: Any = None
        # Filters registered before connect(); flushed onto AsyncClient once it exists.
        self._pending_filters: List[Tuple[str, Any]] = []

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="mqttium",
            sync_api=False,
            async_bridged=True,
            mqtt_v311=True,
            mqtt_v5=True,
            qos2=True,
            tls=True,
            max_inflight=True,
            max_queued=True,
            max_queued_bytes=True,
            message_callback_add=True,
            native_message_callback_add=True,
            v5_publish_properties=True,
            stability="experimental",
            io_model="asyncio_bridged",
            implementation_language="python",
            completion_mechanism="callback",
            native_async=True,
            publish_sync_on_loop=True,
            synthetic_mids=True,
            tcp_nodelay=True,
            notes=cls._NOTES,
        )

    @classmethod
    def identity(cls) -> dict:
        import mqttium

        caps = cls.capabilities()
        version = getattr(mqttium, "__version__", None)
        if version is None:
            try:
                from importlib.metadata import version as pkg_version

                version = pkg_version("mqttium")
            except Exception:  # noqa: BLE001
                version = None
        return {
            "client": "mqttium",
            "adapter": "mqttium",
            "client_module": str(Path(mqttium.__file__).resolve()),
            "client_version": version,
            "stability": caps.stability,
            "io_model": caps.io_model,
            "implementation_language": caps.implementation_language,
            "completion_mechanism": caps.completion_mechanism,
            "synthetic_mids": caps.synthetic_mids,
            "display_note": caps.notes,
            # QoS0 fires after publish_nowait admits to the write pump, not after
            # the socket write completes (Paho's boundary). Declared so rankings
            # are not read as if the contracts matched.
            "qos0_boundary": "queue",
            "private_api": _private_api(),
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
        max_queued_bytes: Optional[int] = None,
        tls_ca_certs: Optional[str] = None,
    ) -> "MqttiumAdapter":
        try:
            import mqttium  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "mqttium is not installed. Install with: pip install 'mqtt-client-bench[mqttium]'"
            ) from exc
        adapter = cls()
        adapter._client_id = client_id
        adapter._protocol = protocol
        adapter._clean_session = clean_session
        adapter._tls_ca_certs = tls_ca_certs
        adapter._max_inflight = max_inflight
        adapter._max_queued = max_queued
        adapter._max_queued_bytes = max_queued_bytes
        return adapter

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        from mqttium.api import AsyncClient
        from mqttium.enums import MQTTProtocolVersion

        self._ensure_bridge()
        self._stopping = False
        proto = getattr(MQTTProtocolVersion, self._protocol)
        tls: Any = None
        if self._tls_ca_certs:
            tls = ssl.create_default_context(cafile=self._tls_ca_certs)

        async def _connect():
            # a2+ removed EngineConfig.max_queued. rc14 names the admission
            # window max_pending_outbound_*; rc15 names it max_unacknowledged_*
            # and the writer window max_write_queue_*. Both still raise
            # FlowControlError when either the message or the byte bound is
            # full, and the 1 MiB writer default is 16 slots of a 64 KiB
            # payload — a 76-98% refusal rate on the payload sweep unless the
            # byte window is sized from bench max_queued_bytes.
            self._client = AsyncClient(
                client_id=self._client_id,
                protocol=proto,
                clean_start=self._clean_session,
                keepalive=keepalive,
                message_delivery="callback",
                **outbound_bound_kwargs(
                    AsyncClient,
                    max_inflight=self._max_inflight,
                    max_queued=self._max_queued,
                    max_queued_bytes=self._max_queued_bytes,
                ),
            )

            def _on_publish(mid, reason=None) -> None:
                # PUBLISH_COMPLETE is emitted at PUBACK for QoS1 and at PUBCOMP
                # for QoS2 (protocol/outbound.py:535 and :610), so this honours
                # the bench's completion contract exactly as awaiting the
                # receipt did.
                if mid is None:
                    return
                pending = self._real_to_synth.get(int(mid))
                if not pending:
                    return
                synth = pending.popleft()
                if not pending:
                    self._real_to_synth.pop(int(mid), None)
                self._fire_on_publish(synth, reason_code=0 if reason is None else 128)

            # Installed lazily by the first QoS>=1 publish, never at connect:
            # mqttium takes its direct QoS0 transport write only while
            # on_publish is None (_direct_qos0_ready), so installing it here
            # cost 38% of the QoS0 rate — 39,118 msgs/s down to 24,039. QoS is
            # fixed per measurement point, so a QoS0 point never installs it.
            self._on_publish_cb = _on_publish

            def _on_message(msg) -> None:
                self._dispatch_message(
                    IncomingMessage(
                        topic=str(msg.topic),
                        payload=msg.payload,
                        qos=int(msg.qos),
                        retain=bool(msg.retain),
                    )
                )

            self._client.on_message = _on_message
            for filt, wrapped in self._pending_filters:
                self._client.message_callback_add(filt, wrapped)
            self._pending_filters.clear()
            await self._client.connect(host, port, ssl=tls)
            self._connected = True
            self._fire_on_connect(flags={}, reason_code=0, properties=None)

        self._bridge.run(_connect())

    async def _message_pump(self) -> None:
        return None

    def disconnect(self) -> None:
        if self._client is None or not self._connected:
            return
        self._ensure_bridge()

        async def _disconnect():
            client = self._client
            self._client = None
            self._connected = False
            self._stopping = True
            if client is not None:
                await client.disconnect()

        try:
            self._bridge.run(_disconnect(), timeout=10.0)
        except Exception:  # noqa: BLE001
            self._connected = False

    def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> PublishResult:
        mid = self.alloc_mid()
        client = self._client
        if client is None or not self._connected:
            return PublishResult(rc=1, mid=None)

        data = b"" if payload is None else payload
        if isinstance(data, str):
            data = data.encode("utf-8")

        # QoS0 contract: on_publish = handed to transport. A sync loop-thread
        # callback (no asyncio.Task per message) via schedule_call.
        if int(qos) == 0:

            def _publish_qos0() -> None:
                try:
                    client.publish_nowait(
                        topic, data, qos=0, retain=retain, properties=properties
                    )
                    self._fire_on_publish(mid, reason_code=0)
                except Exception:  # noqa: BLE001
                    self._fire_on_publish(mid, reason_code=128)

            self.schedule_call(_publish_qos0)
            return PublishResult(rc=0, mid=mid)

        # Correlate the ack instead of suspending a coroutine for the whole
        # round trip. publish_nowait is synchronous on the loop thread, so
        # submission and registration happen in one call. Through rc14 the
        # completion arrives later through on_publish. rc15 removed that hook;
        # one future on the receipt (what wait() registers) resolves at
        # PUBACK/PUBCOMP without a Task per publish. Registering after
        # submission is race-free: both run on the loop thread. The façade
        # has already handed the role a mid, so a refusal is reason 128 here;
        # the native path returns None instead.
        def _publish_qosn() -> None:
            try:
                if uses_on_publish(client):
                    if client.on_publish is None:
                        # Same loop thread that will later deliver the ack, so the
                        # callback is in place before any completion can arrive.
                        client.on_publish = self._on_publish_cb
                    receipt = client.publish_nowait(
                        topic, data, qos=qos, retain=retain, properties=properties
                    )
                    if receipt.mid is None:
                        self._fire_on_publish(mid, reason_code=0)
                        return
                    self._real_to_synth.setdefault(int(receipt.mid), deque()).append(mid)
                    return
                receipt = client.publish_nowait(
                    topic, data, qos=qos, retain=retain, properties=properties
                )
            except Exception:  # noqa: BLE001
                self._fire_on_publish(mid, reason_code=128)
                return
            if receipt_already_done(receipt):
                self._fire_on_publish(mid, reason_code=0)
                return
            observe_receipt(
                receipt,
                lambda reason, synth=mid: self._fire_on_publish(synth, reason_code=reason),
            )

        self.schedule_call(_publish_qosn)
        return PublishResult(rc=0, mid=mid)

    def _wrap_native_message_cb(self, callback: Any) -> Any:
        def _cb(msg: Any) -> None:
            callback(
                self,
                None,
                IncomingMessage(
                    topic=str(msg.topic),
                    payload=msg.payload,
                    qos=int(msg.qos),
                    retain=bool(msg.retain),
                ),
            )

        return _cb

    def message_callback_add(self, topic: str, callback: Any) -> None:
        """Library matcher, not the harness emulator in BridgedAdapterBase."""
        wrapped = self._wrap_native_message_cb(callback)
        if self._client is None:
            self._pending_filters.append((topic, wrapped))
            return
        client = self._client
        self.schedule_call(lambda: client.message_callback_add(topic, wrapped))

    def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        mid = self.alloc_mid()
        client = self._client
        if client is None or not self._connected:
            return SubscribeResult(rc=1, mid=None)

        async def _subscribe():
            try:
                result = await client.subscribe(topic, qos=qos)
                grants = list(result.reason_codes)
                self._fire_on_subscribe(mid, grants, None)
            except Exception:  # noqa: BLE001
                self._fire_on_subscribe(mid, [128], None)

        self.schedule_coro(_subscribe())
        return SubscribeResult(rc=0, mid=mid)

    def build_publish_properties(self, profile: str) -> Any:
        if profile in (None, "none"):
            return None
        from mqttium.types import Properties

        if profile == "realistic":
            values: Dict[str, Any] = {
                "payload_format_indicator": 1,
                "content_type": "application/json",
                "message_expiry_interval": 60,
                "user_property": [("schema", "telemetry.v1"), ("region", "eu-west-1")],
            }
        elif profile == "rich":
            values = {
                "payload_format_indicator": 1,
                "content_type": "application/json",
                "message_expiry_interval": 60,
                "correlation_data": b"c" * 32,
                "response_topic": "bench/response/" + ("r" * 48),
                "user_property": [(f"k{i:02d}", "v" * 64) for i in range(16)],
            }
        else:
            return None
        # rc14 Properties is a mutable bag with set(); rc15 freezes the mapping
        # passed to the constructor and has no setter.
        if hasattr(Properties, "set"):
            props = Properties()
            for name, value in values.items():
                props.set(name, value)
            return props
        return Properties(values)
