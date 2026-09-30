"""Adapter contract tests: registry, drive shapes, bridged facades, mqttium.

These pin the private-API shapes adapters depend on, so a library release
that moves them fails here instead of drifting silently. They need every
client library but aiomqtt3 installed in one environment (see the tests
workflow) and are skipped otherwise.
"""

from __future__ import annotations

import asyncio
import inspect
import time
import unittest
from importlib.metadata import version as pkg_version

from mqtt_client_bench.adapters.async_bridge import (
    AsyncioBridge,
    BridgedAdapterBase,
    IncomingMessage,
    topic_matches_sub,
)
from mqtt_client_bench.adapters.gmqtt import GmqttAdapter
from mqtt_client_bench.adapters.mqttium import MqttiumAdapter, outbound_bound_kwargs
from mqtt_client_bench.adapters.mqttium_async import FlowControlError, MqttiumAsyncAdapter
from mqtt_client_bench.adapters.paho import build_paho_publish_properties
from mqtt_client_bench.adapters.registry import (
    _ADAPTERS,
    EXPERIMENTAL_CLIENTS,
    STABLE_CLIENTS,
    adapter_identity,
    create_adapter,
    get_adapter_class,
    list_clients,
)
from mqtt_client_bench.bench.worker import drive_shape

try:
    from gmqtt import Client as GmqttClient
    from mqttium.api import AsyncClient
    from mqttium.api.models import PublishReceipt
    from mqttium.types import Properties
except ImportError as exc:  # pragma: no cover - depends on the environment
    raise unittest.SkipTest(f"client libraries not installed: {exc}")


class AdapterRegistryTests(unittest.TestCase):
    def test_list_clients(self):
        names = {row["name"] for row in list_clients()}
        self.assertEqual(names, {"paho", "gmqtt", "aiomqtt", "aiomqtt3", "amqtt", "awscrt", "zmqtt", "mqttium"})
        self.assertEqual(set(STABLE_CLIENTS), {"paho", "gmqtt", "aiomqtt", "amqtt", "awscrt", "mqttium"})
        self.assertEqual(set(EXPERIMENTAL_CLIENTS), {"zmqtt", "aiomqtt3"})

    def test_protocol_declarations(self):
        self.assertFalse(get_adapter_class("amqtt").capabilities().mqtt_v5)
        caps = get_adapter_class("aiomqtt3").capabilities()
        self.assertTrue(caps.mqtt_v5)
        self.assertFalse(caps.mqtt_v311)
        for name in ("paho", "gmqtt", "aiomqtt", "awscrt", "zmqtt", "mqttium"):
            caps = get_adapter_class(name).capabilities()
            self.assertTrue(caps.mqtt_v5 and caps.mqtt_v311, name)

    def test_extended_feature_declarations(self):
        """Who runs the extended feature points; everyone else is refused."""
        expected = {
            # qos2, native filters, publish properties, topic alias, receive maximum
            "paho": (True, True, True, True, True),
            "gmqtt": (False, False, True, True, True),
            "aiomqtt": (True, False, True, True, True),
            "aiomqtt3": (False, False, False, False, True),
            "amqtt": (True, False, False, False, False),
            "awscrt": (False, False, True, False, False),
            "zmqtt": (True, False, True, False, True),
            "mqttium": (True, True, True, True, True),
        }
        self.assertEqual(set(expected), set(_ADAPTERS))
        for name, flags in expected.items():
            caps = get_adapter_class(name).capabilities()
            got = (caps.qos2, caps.native_message_callback_add, caps.v5_publish_properties, caps.v5_topic_alias, caps.v5_receive_maximum)
            self.assertEqual(got, flags, name)
            self.assertTrue(caps.tls, name)

    def test_alias_properties_carry_alias_one(self):
        self.assertEqual(build_paho_publish_properties("alias").TopicAlias, 1)
        self.assertEqual(GmqttAdapter().build_publish_properties("alias"), {"topic_alias": 1})

    def test_receive_maximum_reaches_the_library(self):
        paho = create_adapter("paho", client_id="rm", protocol="MQTTv5", receive_maximum=16)
        self.assertEqual(paho._connect_properties.ReceiveMaximum, 16)
        with self.assertRaises(ValueError):
            create_adapter("awscrt", client_id="rm", protocol="MQTTv5", receive_maximum=16)

    def test_drive_shapes(self):
        """Which drive loop a client gets is decided by its capabilities, once.

        A library that can admit a publish on the loop must not be driven by
        awaiting, which would pay a coroutine resume per message the others
        do not; an await-only library must not be serialised to one in flight.
        """
        expected = {
            "paho": "sync",
            "awscrt": "sync",
            "gmqtt": "nowait",
            "mqttium": "nowait",
            "aiomqtt": "awaited",
            "aiomqtt3": "awaited",
            "amqtt": "awaited",
            "zmqtt": "awaited",
        }
        self.assertEqual(set(expected), set(_ADAPTERS))
        for name, shape in expected.items():
            self.assertEqual(drive_shape(name), shape, name)

    def test_awscrt_identity_native(self):
        caps = get_adapter_class("awscrt").capabilities()
        self.assertEqual(caps.implementation_language, "native")
        self.assertEqual(caps.io_model, "crt_event_loop")
        info = adapter_identity("awscrt")
        self.assertEqual(info["client"], "awscrt")
        self.assertEqual(info["implementation_language"], "native")

    def test_client_identities_stable(self):
        for name in ("paho", "gmqtt", "aiomqtt", "amqtt", "awscrt", "zmqtt"):
            caps = get_adapter_class(name).capabilities()
            self.assertEqual(caps.unimplemented, [], name)
            info = adapter_identity(name)
            self.assertEqual(info["client"], name)
            self.assertIsNotNone(info.get("client_module"), name)

    def test_adapters_declare_their_private_api_use(self):
        # Reaching into a library's internals changes what is being measured, so
        # every such dependency must be declared and visible in the result JSON.
        for name in ("gmqtt", "aiomqtt", "amqtt"):
            declared = adapter_identity(name).get("private_api") or {}
            self.assertTrue(declared, f"{name} must declare its private API use")
            for attr, reason in declared.items():
                self.assertTrue(reason.strip(), f"{name}:{attr} needs a reason")

    def test_gmqtt_private_api_shape(self):
        # gmqtt's public publish() drops the packet id, so QoS>=1 mirrors it via
        # internals. If a gmqtt release moves them, fail here rather than let the
        # adapter silently measure something else.
        source = inspect.getsource(GmqttClient.publish)
        self.assertIn("self._connection.publish(message)", source)
        self.assertIn("push_message_nowait", source)
        self.assertNotIn("return ", source)
        client = GmqttClient("shape-probe")
        for attr in ("_connection", "_persistent_storage", "_remove_message_from_query"):
            self.assertTrue(hasattr(client, attr), f"gmqtt no longer exposes {attr}")
        self.assertEqual(pkg_version("gmqtt"), "0.7.0")

    def test_every_adapter_declares_how_completions_reach_the_worker(self):
        expected = {
            "paho": "sync",
            "gmqtt": "callback",
            "awscrt": "callback",
            "mqttium": "callback",
            "aiomqtt": "awaited",
            "amqtt": "awaited",
            "zmqtt": "awaited",
            "aiomqtt3": "awaited",
        }
        self.assertEqual(set(expected), set(_ADAPTERS), "a client gained or lost a declaration")
        for name, want in expected.items():
            self.assertEqual(get_adapter_class(name).capabilities().completion_mechanism, want, name)
        self.assertEqual(adapter_identity("gmqtt")["completion_mechanism"], "callback")

    def test_mqttium_keeps_the_qos0_fast_path_unarmed(self):
        """mqttium takes its direct QoS0 transport write only while on_publish
        is None; installing it at connect cost 38% of the QoS0 rate. The
        callback is installed by the first QoS>=1 publish and a QoS0 point must
        never arm it. Asserted rather than inferred from a rate, which run-to-run
        noise would hide."""

        class StubReceipt:
            mid = 7

        class StubClient:
            def __init__(self):
                self.on_publish = None
                self.published = 0

            def publish_nowait(self, *a, **k):
                self.published += 1
                return StubReceipt()

        for qos, armed in ((0, False), (1, True), (2, True)):
            with self.subTest(qos=qos):
                adapter = MqttiumAdapter()
                stub = StubClient()
                adapter._client = stub
                adapter._connected = True
                adapter._on_publish_cb = lambda mid, reason=None: None
                adapter.schedule_call = lambda fn: fn()
                adapter.schedule_coro = lambda coro: coro.close()
                adapter.publish("t", b"x" * 64, qos=qos)
                self.assertEqual(stub.published, 1, "the publish must reach the client")
                self.assertEqual(stub.on_publish is not None, armed)

    def test_mqttium_public_api_shape(self):
        # 1.x completes QoS>=1 through the public receipt and has no façade.
        async_client = AsyncClient(client_id="shape-probe")
        for attr in ("publish_nowait", "message_callback_add", "message_callback_remove"):
            self.assertTrue(hasattr(async_client, attr), f"mqttium no longer exposes {attr}")
        params = inspect.signature(AsyncClient.__init__).parameters
        for name in (
            "max_outbound_inflight",
            "message_delivery",
            "max_unacknowledged_messages",
            "max_unacknowledged_bytes",
            "max_write_queue_bytes",
        ):
            self.assertIn(name, params)
        self.assertNotIn("on_publish", params)
        declared = getattr(PublishReceipt, "__dataclass_fields__", None) or set(PublishReceipt.__slots__)
        self.assertIn("_waiters", declared)
        self.assertIn("_error", declared)
        props = Properties({"content_type": "application/json"})
        self.assertEqual(props.get("content_type"), "application/json")

    def test_mqttium_queues_native_filters_until_connect(self):
        adapter = MqttiumAdapter.create(client_id="cb-probe")
        adapter.message_callback_add("bench/+/x", lambda *_a: None)
        self.assertEqual(len(adapter._pending_filters), 1)
        self.assertIsNone(adapter._client)

    def test_realistic_properties_do_not_claim_utf8(self):
        g = GmqttAdapter().build_publish_properties("realistic")
        self.assertNotIn("payload_format_indicator", g)
        self.assertEqual(g["content_type"], "application/octet-stream")
        p = build_paho_publish_properties("realistic")
        self.assertFalse(hasattr(p, "PayloadFormatIndicator"))
        self.assertEqual(p.ContentType, "application/octet-stream")


class BridgedAdapterTests(unittest.TestCase):
    def test_topic_matches_sub(self):

        self.assertTrue(topic_matches_sub("a/b", "a/b"))
        self.assertTrue(topic_matches_sub("a/+", "a/b"))
        self.assertTrue(topic_matches_sub("a/#", "a/b/c"))
        self.assertTrue(topic_matches_sub("#", "a/b"))
        self.assertFalse(topic_matches_sub("a/b", "a/c"))
        self.assertFalse(topic_matches_sub("a/+", "a/b/c"))
        self.assertFalse(topic_matches_sub("a/#", "b/c"))

    def test_dispatch_prefers_topic_callback(self):

        adapter = BridgedAdapterBase()
        seen = {"topic": 0, "global": 0}

        def on_topic(client, userdata, msg):
            seen["topic"] += 1

        def on_message(client, userdata, msg):
            seen["global"] += 1

        adapter.on_message = on_message
        adapter.message_callback_add("bench/+/data", on_topic)
        adapter._dispatch_message(IncomingMessage(topic="bench/x/data", payload=b"1"))
        self.assertEqual(seen["topic"], 1)
        self.assertEqual(seen["global"], 0)
        adapter._dispatch_message(IncomingMessage(topic="other", payload=b"2"))
        self.assertEqual(seen["global"], 1)

    def test_bridge_start_stop_and_callbacks(self):

        adapter = BridgedAdapterBase()
        connected = []
        published = []
        subscribed = []

        adapter.on_connect = lambda *a, **k: connected.append(a)
        adapter.on_publish = lambda *a, **k: published.append(a)
        adapter.on_subscribe = lambda *a, **k: subscribed.append(a)

        adapter.loop_start()
        self.assertTrue(adapter._bridge.running)
        adapter._fire_on_connect(reason_code=0)
        adapter._fire_on_publish(7, reason_code=0)
        adapter._fire_on_subscribe(3, [0])
        adapter.loop_stop()
        self.assertFalse(adapter._bridge.running)
        self.assertEqual(len(connected), 1)
        self.assertEqual(published[0][2], 7)
        self.assertEqual(subscribed[0][2], 3)

    def test_schedule_coro_coalesces_wake(self):

        bridge = AsyncioBridge()
        bridge.start()
        done = []
        wakes = {"n": 0}
        original = bridge._drain_pending

        def counting_drain():
            wakes["n"] += 1
            original()

        bridge._drain_pending = counting_drain  # type: ignore[method-assign]

        async def _work(i):
            done.append(i)

        for i in range(32):
            bridge.schedule_coro(_work(i))
        deadline = time.time() + 2.0
        while len(done) < 32 and time.time() < deadline:
            time.sleep(0.01)
        bridge.stop()
        self.assertEqual(sorted(done), list(range(32)))
        # One coalesced wake for the burst (may be 1; allow a few if the drain
        # races a second append before clearing the flag).
        self.assertGreaterEqual(wakes["n"], 1)
        self.assertLessEqual(wakes["n"], 8)

    def test_schedule_coro_reuses_workers(self):
        # await-only publish APIs must not pay one asyncio.Task per message:
        # that is a harness tax schedule_call clients never pay.

        bridge = AsyncioBridge()
        bridge.start()
        done = []

        async def _work(i):
            done.append(i)

        for i in range(500):
            bridge.schedule_coro(_work(i))
            # Let the loop drain so a single worker can be handed the next item.
            time.sleep(0.0005)
        deadline = time.time() + 5.0
        while len(done) < 500 and time.time() < deadline:
            time.sleep(0.01)
        workers = len(bridge._workers)
        bridge.stop()
        self.assertEqual(len(done), 500)
        # 500 messages served by a handful of reused workers, not 500 tasks.
        self.assertLessEqual(workers, 16)

    def test_schedule_coro_keeps_concurrency_for_awaiting_publishes(self):
        # Workers are created on demand, so overlapping in-flight publishes (QoS>=1
        # awaiting a PUBACK) are not serialised by the pool.

        bridge = AsyncioBridge()
        bridge.start()
        gate = {"release": None}
        started = []
        finished = []

        async def _blocked(i):
            started.append(i)
            await gate["release"]
            finished.append(i)

        async def _make_gate():
            gate["release"] = asyncio.get_running_loop().create_future()

        bridge.run(_make_gate())
        for i in range(64):
            bridge.schedule_coro(_blocked(i))
        deadline = time.time() + 5.0
        while len(started) < 64 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(started), 64, "pool serialised concurrent publishes")
        self.assertEqual(finished, [])
        bridge._loop.call_soon_threadsafe(gate["release"].set_result, None)
        deadline = time.time() + 5.0
        while len(finished) < 64 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(finished), 64)
        bridge.stop()

    def test_schedule_call_defers_on_loop_thread(self):
        """Loop-thread schedule_call must not run inline (RTT echo from on_message)."""

        bridge = AsyncioBridge()
        bridge.start()
        depth = {"n": 0}
        ran = []

        def _nested():
            depth["n"] += 1
            ran.append(depth["n"])
            depth["n"] -= 1

        async def _on_loop():
            bridge.schedule_call(_nested)
            # Inline execution would append before this line.
            self.assertEqual(ran, [])

        bridge.run(_on_loop())
        deadline = time.time() + 2.0
        while not ran and time.time() < deadline:
            time.sleep(0.01)
        bridge.stop()
        self.assertEqual(ran, [1])

    def test_bridge_stop_drops_queued_work_cleanly(self):

        bridge = AsyncioBridge()
        bridge.start()
        ran = []

        async def _work():
            ran.append(1)

        # Queue without letting the loop drain, then tear down.
        with bridge._pending_lock:
            bridge._pending.append(_work())
        bridge.stop()
        self.assertFalse(bridge.running)

    def test_alloc_mid_cycles(self):

        adapter = BridgedAdapterBase()
        mids = [adapter.alloc_mid() for _ in range(5)]
        self.assertEqual(mids, [1, 2, 3, 4, 5])
        adapter._next_mid = 65535
        self.assertEqual(adapter.alloc_mid(), 65535)
        self.assertEqual(adapter.alloc_mid(), 1)

    def test_create_adapters(self):

        for name in ("gmqtt", "aiomqtt", "amqtt", "zmqtt", "awscrt"):
            adapter = create_adapter(name, client_id=f"test-{name}")
            self.assertEqual(adapter.MQTT_ERR_SUCCESS, 0)
            self.assertTrue(hasattr(adapter, "publish"))
            self.assertTrue(hasattr(adapter, "subscribe"))
            self.assertIsNone(adapter.build_publish_properties("none"))

class MqttiumNativeNowaitTests(unittest.TestCase):
    """FlowControlError is queue-full, not a completed failure."""

    def test_qos0_flow_control_returns_none_without_on_publish(self):
        adapter = MqttiumAsyncAdapter()
        fired = []
        adapter.on_publish = lambda *args: fired.append(args)

        class _Client:
            def publish_nowait(self, *args, **kwargs):
                raise FlowControlError("write pump full")

        adapter._client = _Client()
        self.assertIsNone(adapter.publish_nowait("t", b"x", qos=0))
        self.assertEqual(fired, [])

    def test_qos1_flow_control_returns_none_without_on_publish(self):
        adapter = MqttiumAsyncAdapter()
        fired = []
        adapter.on_publish = lambda *args: fired.append(args)

        class _Client:
            on_publish = None

            def publish_nowait(self, *args, **kwargs):
                raise FlowControlError("pending outbound full")

        adapter._client = _Client()
        self.assertIsNone(adapter.publish_nowait("t", b"x", qos=1))
        self.assertEqual(fired, [])

    def test_qos0_success_still_completes_inline(self):
        adapter = MqttiumAsyncAdapter()
        fired = []
        adapter.on_publish = lambda *args: fired.append(args[2])

        class _Client:
            def publish_nowait(self, *args, **kwargs):
                return None

        adapter._client = _Client()
        mid = adapter.publish_nowait("t", b"x", qos=0)
        self.assertIsNotNone(mid)
        self.assertEqual(fired, [mid])

    def test_rc15_qos1_receipt_relays_puback(self):
        adapter = MqttiumAsyncAdapter()
        fired = []
        adapter.on_publish = lambda *args: fired.append((args[2], args[3]))

        class _Receipt:
            def __init__(self):
                self.mid = 3
                self._settled = False
                self._error = None
                self._waiters = None

            def is_done(self):
                return self._settled

            def settle(self, error=None):
                self._error = error
                self._settled = True
                waiters = self._waiters
                self._waiters = None
                if not waiters:
                    return
                for waiter in waiters:
                    if not waiter.done():
                        waiter.set_result(None)

        receipt = _Receipt()

        class _Client:
            def publish_nowait(self, *args, **kwargs):
                return receipt

        adapter._client = _Client()

        async def drive():
            mid = adapter.publish_nowait("t", b"x", qos=1)
            self.assertEqual(fired, [])
            receipt.settle()
            return mid

        mid = asyncio.run(drive())
        self.assertEqual(fired, [(mid, 0)])

    def test_rc15_flow_control_returns_none(self):
        adapter = MqttiumAsyncAdapter()
        fired = []
        adapter.on_publish = lambda *args: fired.append(args)

        class _Client:
            def publish_nowait(self, *args, **kwargs):
                raise FlowControlError("writer full")

        adapter._client = _Client()
        self.assertIsNone(adapter.publish_nowait("t", b"x", qos=1))
        self.assertEqual(fired, [])

    def test_outbound_bounds_follow_the_frozen_vocabulary(self):

        class Rc14:
            def __init__(
                self,
                max_outbound_inflight=20,
                max_pending_outbound_messages=200,
                max_pending_outbound_bytes=None,
                max_outbound_bytes=None,
            ):
                del (
                    max_outbound_inflight,
                    max_pending_outbound_messages,
                    max_pending_outbound_bytes,
                    max_outbound_bytes,
                )

        class Rc15:
            def __init__(
                self,
                max_outbound_inflight=20,
                max_unacknowledged_messages=200,
                max_unacknowledged_bytes=None,
                max_write_queue_bytes=None,
            ):
                del (
                    max_outbound_inflight,
                    max_unacknowledged_messages,
                    max_unacknowledged_bytes,
                    max_write_queue_bytes,
                )

        rc14 = outbound_bound_kwargs(Rc14, max_inflight=64, max_queued=200, max_queued_bytes=8 << 20)
        self.assertEqual(rc14["max_outbound_inflight"], 64)
        self.assertEqual(rc14["max_pending_outbound_messages"], 200)
        self.assertEqual(rc14["max_pending_outbound_bytes"], 64 << 20)
        self.assertEqual(rc14["max_outbound_bytes"], 8 << 20)
        rc15 = outbound_bound_kwargs(Rc15, max_inflight=64, max_queued=200, max_queued_bytes=8 << 20)
        self.assertEqual(rc15["max_unacknowledged_messages"], 200)
        self.assertEqual(rc15["max_unacknowledged_bytes"], 64 << 20)
        self.assertEqual(rc15["max_write_queue_bytes"], 8 << 20)
        self.assertNotIn("max_pending_outbound_messages", rc15)

    def test_other_errors_still_propagate(self):
        adapter = MqttiumAsyncAdapter()

        class _Client:
            def publish_nowait(self, *args, **kwargs):
                raise RuntimeError("not on the owning loop")

        adapter._client = _Client()
        with self.assertRaises(RuntimeError):
            adapter.publish_nowait("t", b"x", qos=0)



if __name__ == "__main__":
    unittest.main()
