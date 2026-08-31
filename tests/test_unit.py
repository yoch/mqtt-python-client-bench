"""Unit tests for MQTT client benchmark helpers."""

from __future__ import annotations

import asyncio
import collections
import json
import os
import re
import threading
import time
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mqtt_client_bench.adapters import registry  # noqa: E402
from mqtt_client_bench.adapters.base import AdapterCapabilities  # noqa: E402
from mqtt_client_bench.adapters.native import NativeAsyncAdapter  # noqa: E402
from mqtt_client_bench.adapters.mqttium import MqttiumAdapter  # noqa: E402
from mqtt_client_bench.adapters.mqttium_async import FlowControlError, MqttiumAsyncAdapter  # noqa: E402
from mqtt_client_bench.roles import publisher  # noqa: E402
from mqtt_client_bench.adapters.registry import (  # noqa: E402
    EXPERIMENTAL_CLIENTS,
    STABLE_CLIENTS,
    list_clients,
    unsupported_for_client,
)
from mqtt_client_bench.harness import (  # noqa: E402
    DEFAULT_INGRESS_OFFER_MSGS_PER_S,
    capacity_from_qos_sweep,
    capacity_from_scenario,
    reconcile_ingress_loadgen,
    resolve_ingress_offer,
    unsupported_features,
)
from mqtt_client_bench.loadgen import (  # noqa: E402
    EMQTT_MAX_OFFER_MSGS_PER_S,
    HAMMER_MAX_RATE_MSGS_PER_S,
    HAMMER_PUB_CLIENTS,
    UNPACED_PUB_CLIENTS,
    LoadgenSpec,
    build_hammer_cmd,
    build_pub_args,
    clamp_emqtt_offer,
    clamp_hammer_rate,
    enrich_loadgen_stats,
    resolve_hammer_pub_clients,
    interval_for_rate,
    nominal_rate,
    observed_pub_rate,
    parse_emqtt_output,
    parse_hammer_output,
    select_loadgen_engine,
    topic_is_templated,
)
from mqtt_client_bench.metrics import (  # noqa: E402
    abba_block_ratios,
    abba_order,
    compare_verdict,
    compare_verdict_from_block_ratios,
    integrity_counts,
    latency_summary,
    median,
    percentile,
    sanitize_number,
    summarize_valid_runs,
)
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, estimate_suite, expand_scenario, list_scenarios  # noqa: E402
from mqtt_client_bench.sampling import (  # noqa: E402
    DEFAULT_COMPLETION_LOG_LIMIT,
    DEFAULT_METRIC_SAMPLE_LIMIT,
    CompletionLog,
)
from mqtt_client_bench.sampling import (  # noqa: E402
    ReservoirSampler,
    SequenceTracker,
    bound_payload_backlog,
    integrity_from_summaries,
)
from mqtt_client_bench.workloads import (  # noqa: E402
    build_payload,
    callback_match_loadgen_topic,
    callback_match_topics,
    decode_header,
    encode_header,
    overlapping_match_filters,
    payload_len_for_remaining_length,
    remaining_length_size,
    rl_boundary_payloads,
    single_topic,
)


class MetricsTests(unittest.TestCase):
    def test_sanitize(self):
        self.assertIsNone(sanitize_number(float("nan")))
        self.assertIsNone(sanitize_number(float("inf")))
        self.assertEqual(sanitize_number(1.5), 1.5)

    def test_percentile_and_median(self):
        values = [1, 2, 3, 4, 5]
        self.assertEqual(median(values), 3)
        self.assertEqual(percentile(values, 100), 5)
        self.assertIsNone(percentile([], 50))

    def test_latency_p99_gate(self):
        samples = list(range(100))
        summary = latency_summary(samples, min_for_p99=10_000)
        self.assertFalse(summary["p99_published"])
        self.assertIsNone(summary["p99_ms"])
        big = list(range(10_000))
        summary2 = latency_summary(big, min_for_p99=10_000)
        self.assertTrue(summary2["p99_published"])
        self.assertIsNotNone(summary2["p99_ms"])

    def test_abba_order(self):
        self.assertEqual(abba_order(1), ["A", "B", "B", "A"])
        self.assertEqual(len(abba_order(4)), 16)
        self.assertEqual(abba_order(4).count("A"), 8)
        self.assertEqual(abba_order(4).count("B"), 8)

    def test_abba_block_ratios_deterministic(self):
        order = abba_order(2)
        # A=100, B=110, B=110, A=100  => ratio 1.1 twice
        rates = [100.0, 110.0, 110.0, 100.0, 100.0, 110.0, 110.0, 100.0]
        ratios = abba_block_ratios(order, rates)
        self.assertEqual(ratios, [1.1, 1.1])
        verdict = compare_verdict_from_block_ratios(ratios, min_effect_pct=3.0, seed=1)
        self.assertEqual(verdict["verdict"], "improvement")
        # Incomplete block with None is dropped.
        self.assertEqual(abba_block_ratios(order, [100.0, None, 110.0, 100.0] + rates[4:]), [1.1])

    def test_compare_inconclusive_on_noise(self):
        baseline = [100.0] * 8
        candidate = [101.0] * 8
        verdict = compare_verdict(baseline, candidate, min_effect_pct=3.0)
        self.assertEqual(verdict["verdict"], "inconclusive")

    def test_summarize_valid_runs_filters(self):
        runs = [
            {"status": "valid", "primary_msgs_per_s": 10.0, "non_comparable": False},
            {"status": "inconclusive", "primary_msgs_per_s": 999.0, "non_comparable": False},
            {"status": "valid", "primary_msgs_per_s": 20.0, "non_comparable": True},
        ]
        summary = summarize_valid_runs(runs)
        self.assertEqual(summary["n"], 1)
        self.assertEqual(summary["median"], 10.0)
        self.assertEqual(summary["inconclusive_n"], 1)

    def test_capacity_from_qos_sweep_uses_smoke_rates(self):
        result = {
            "results": [
                {
                    "point": {"qos_publish": 0},
                    "summary": {"median": None},
                    "runs": [
                        {
                            "status": "valid",
                            "primary_msgs_per_s": 9000.0,
                            "non_comparable": True,
                        }
                    ],
                },
                {
                    "point": {"qos_publish": 1},
                    "summary": {"median": None},
                    "runs": [
                        {
                            "status": "valid",
                            "primary_msgs_per_s": 4000.0,
                            "non_comparable": True,
                        }
                    ],
                },
            ]
        }
        self.assertEqual(capacity_from_qos_sweep(result), 4000.0)

    def test_capacity_from_scenario_median(self):
        result = {
            "results": [
                {
                    "point": {"cadence": "capacity"},
                    "summary": {"median": 1200.0},
                    "runs": [],
                }
            ]
        }
        self.assertEqual(capacity_from_scenario(result), 1200.0)

    def test_rtt_capacity_scenario_is_closed_loop(self):
        from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario

        scenario = SCENARIO_BY_NAME["rtt_capacity_qos1"]
        points = expand_scenario(scenario, "smoke")
        self.assertEqual(len(points), 2)  # dual_protocol: MQTTv311 + MQTTv5
        for point in points:
            self.assertEqual(point["cadence"], "capacity")
            self.assertNotIn("load_fraction", point)
            self.assertEqual(point["topology"], "application_rtt")
            self.assertIn(point["protocol"], ("MQTTv311", "MQTTv5"))

    def test_integrity(self):
        expected = range(1, 6)
        received = [1, 2, 2, 4, 3, 5]
        counts = integrity_counts(expected, received)
        self.assertEqual(counts["unique"], 5)
        self.assertEqual(counts["duplicates"], 1)
        self.assertEqual(counts["missing"], 0)
        self.assertGreaterEqual(counts["out_of_order"], 1)


class BoundedSamplingTests(unittest.TestCase):
    def test_high_throughput_samples_remain_bounded(self):
        import tracemalloc

        tracemalloc.start()
        sampler = ReservoirSampler(1024, seed=7)
        sequences = SequenceTracker(1024)
        for value in range(250_000):
            sampler.add(value)
            sequences.add(value)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.assertEqual(sampler.seen, 250_000)
        self.assertEqual(len(sampler.snapshot()), 1024)
        self.assertEqual(sampler.metadata()["retained"], 1024)
        self.assertIsNone(sequences.exact_values())
        self.assertLess(current, 512 * 1024)
        self.assertLess(peak, 1024 * 1024)

    def test_sequence_integrity_fingerprint_releases_exact_prefix_at_limit(self):
        expected = SequenceTracker(1024)
        received = SequenceTracker(1024)
        for sequence in range(1, 100_001):
            expected.add(sequence)
            received.add(sequence)

        self.assertIsNone(expected.exact_values())
        self.assertIsNone(received.exact_values())
        integrity = integrity_from_summaries(expected.summary(), received.summary())
        self.assertTrue(integrity["digest_match"])
        self.assertEqual(integrity["missing"], 0)
        self.assertEqual(integrity["duplicates"], 0)

        received.add(100_000)
        mismatch = integrity_from_summaries(expected.summary(), received.summary())
        self.assertFalse(mismatch["digest_match"])
        self.assertEqual(mismatch["count_delta"], 1)

    def test_large_payload_backlog_is_explicitly_capped(self):
        backlog = bound_payload_backlog(64, 8 * 1024 * 1024, 64 * 1024 * 1024)
        self.assertEqual(backlog["effective_outstanding"], 8)
        self.assertEqual(backlog["maximum_bytes"], 64 * 1024 * 1024)

        oversized = bound_payload_backlog(64, 128 * 1024 * 1024, 64 * 1024 * 1024)
        self.assertEqual(oversized["effective_outstanding"], 1)
        self.assertEqual(oversized["maximum_bytes"], 128 * 1024 * 1024)

    def test_process_memory_reports_rss_uss_pss_and_exit_signal(self):
        from unittest.mock import patch

        from mqtt_client_bench.telemetry import (
            process_exit_metadata,
            process_memory_peaks,
            process_stats,
        )

        def fake_read(path):
            if path.endswith("/status"):
                return "VmRSS:\t120 kB\nVmHWM:\t160 kB\n"
            if path.endswith("/smaps_rollup"):
                return (
                    "Pss:\t90 kB\nPrivate_Clean:\t20 kB\n"
                    "Private_Dirty:\t50 kB\nPrivate_Hugetlb:\t4 kB\n"
                )
            return None

        with patch("mqtt_client_bench.telemetry._read_text", side_effect=fake_read):
            stats = process_stats(123)
        self.assertEqual(stats["rss_kb"], 120)
        self.assertEqual(stats["rss_hwm_kb"], 160)
        self.assertEqual(stats["uss_kb"], 74)
        self.assertEqual(stats["pss_kb"], 90)

        peaks = process_memory_peaks([{"processes": {"publisher": stats}}])
        self.assertEqual(peaks["publisher"]["peak_uss_kb"], 74)
        self.assertTrue(process_exit_metadata(-9)["possible_oom_or_sigkill"])
        self.assertFalse(process_exit_metadata(0)["possible_oom_or_sigkill"])


class WorkloadTests(unittest.TestCase):
    def test_payload_sizes(self):
        self.assertEqual(build_payload("empty0"), b"")
        self.assertEqual(len(build_payload("binary64")), 64)
        self.assertEqual(len(build_payload("telemetry256")), 256)
        self.assertIsInstance(build_payload("telemetry256_str"), str)

    def test_header_roundtrip(self):
        header = encode_header(b"abcd1234", 7, 99, 99, 123456789)
        decoded = decode_header(header + b"extra")
        self.assertEqual(decoded["publisher_id"], 7)
        self.assertEqual(decoded["sequence"], 99)
        self.assertEqual(decoded["send_ns"], 123456789)

    def test_remaining_length_boundaries(self):
        topic = single_topic("abcd1234")
        for target in (126, 127, 128, 16383, 16384):
            payload_len = payload_len_for_remaining_length(topic, 0, target)
            self.assertEqual(remaining_length_size(topic, 0, payload_len), target)
        sizes = rl_boundary_payloads(topic, qos=0)
        self.assertIn("rl_127", sizes)
        self.assertIn("rl_128", sizes)

    def test_unsupported_features_guard(self):
        self.assertEqual(unsupported_features({"payload": "telemetry256", "qos_publish": 0}), [])
        # receive_maximum / retained_count / submit_count are executable now.
        self.assertEqual(unsupported_features({"receive_maximum": 10}), [])
        self.assertEqual(unsupported_features({"retained_count": 10_000}), [])
        self.assertEqual(unsupported_features({"submit_count": 150}), [])
        # outage_s is implemented (graceful disconnect/reconnect), so it must no
        # longer be refused; it only requires an adapter that can reconnect and
        # an outage short enough to leave traffic on both sides of the gap.
        self.assertEqual(unsupported_features({"outage_s": 2.0, "duration_s": 12.0}), [])
        self.assertIn("properties_profile:topic_alias", unsupported_features({"properties_profile": "topic_alias"}))
        self.assertIn("connect_mode:tcp_concurrent", unsupported_features({"connect_mode": "tcp_concurrent"}))
        self.assertIn("topic_topology:fleet4k_zipf", unsupported_features({"topic_topology": "fleet4k_zipf"}))
        self.assertIn("network:wan_cut", unsupported_features({"network": "wan_cut"}))
        self.assertEqual(unsupported_features({"properties_profile": "realistic", "connect_mode": "tcp_serial"}), [])


class AdapterRegistryTests(unittest.TestCase):
    def test_list_clients(self):
        names = {row["name"] for row in list_clients()}
        self.assertTrue({"paho", "gmqtt", "aiomqtt", "amqtt", "awscrt"}.issubset(names))
        self.assertIn("zmqtt", names)
        self.assertIn("aiomqtt3", names)
        self.assertIn("paho", STABLE_CLIENTS)
        self.assertIn("awscrt", STABLE_CLIENTS)
        self.assertIn("zmqtt", EXPERIMENTAL_CLIENTS)
        self.assertIn("aiomqtt3", EXPERIMENTAL_CLIENTS)
        self.assertIn("mqttium", EXPERIMENTAL_CLIENTS)
        self.assertIn("mqttium-compat", EXPERIMENTAL_CLIENTS)
        self.assertNotIn("mqttium", STABLE_CLIENTS)
        self.assertNotIn("mqttium-compat", STABLE_CLIENTS)

    def test_implemented_clients_accept_core_points(self):
        point = {"payload": "telemetry256", "qos_publish": 0, "protocol": "MQTTv311"}
        for name in ("paho", "gmqtt", "aiomqtt", "amqtt", "awscrt", "zmqtt"):
            missing = unsupported_for_client(name, point)
            self.assertEqual(missing, [], name)

    def test_callback_matching_native_clients(self):
        point = {"callback_filters": 64, "qos_subscribe": 0}
        from mqtt_client_bench.adapters.registry import _ADAPTERS

        native = {"paho", "mqttium", "mqttium-compat"}
        for name in native:
            self.assertEqual(unsupported_for_client(name, point), [], name)
        for name in _ADAPTERS:
            if name in native:
                continue
            try:
                missing = unsupported_for_client(name, point)
            except Exception:
                continue  # optional dep not installed (aiomqtt3)
            self.assertIn(
                "native_message_callback_add",
                missing,
                f"{name} must refuse callback_filters without native matching",
            )

    def test_amqtt_refuses_mqtt_v5(self):
        point = {"protocol": "MQTTv5", "qos_publish": 0}
        self.assertIn("mqtt_v5", unsupported_for_client("amqtt", point))

    def test_gmqtt_refuses_qos2(self):
        point = {"protocol": "MQTTv311", "qos_publish": 2}
        self.assertIn("qos2", unsupported_for_client("gmqtt", point))
        self.assertEqual(unsupported_for_client("paho", point), [])

    def test_aiomqtt3_refuses_qos2(self):
        point = {"protocol": "MQTTv5", "qos_publish": 2}
        self.assertIn("qos2", unsupported_for_client("aiomqtt3", point))
        self.assertEqual(
            unsupported_for_client("aiomqtt3", {"protocol": "MQTTv5", "qos_publish": 1}),
            [],
        )

    def test_inflight_control_required(self):
        point = {"protocol": "MQTTv311", "qos_publish": 1, "require_max_inflight": True, "inflight": 20}
        self.assertEqual(unsupported_for_client("paho", point), [])
        self.assertIn("max_inflight", unsupported_for_client("gmqtt", point))
        self.assertIn("max_inflight", unsupported_for_client("amqtt", point))

    def test_fleet_refused_for_async_bridged(self):
        point = {"topology": "fleet", "fleet_size": 32}
        self.assertEqual(unsupported_for_client("paho", point), [])
        self.assertIn("fleet_async_bridged", unsupported_for_client("gmqtt", point))

    def test_aiomqtt3_mqtt5_only(self):
        self.assertIn("mqtt_v311", unsupported_for_client("aiomqtt3", {"protocol": "MQTTv311"}))
        self.assertEqual(unsupported_for_client("aiomqtt3", {"protocol": "MQTTv5", "qos_publish": 0}), [])

    def test_aiomqtt3_extra_pulls_paho_for_sys_probe(self):
        # SysCountersProbe imports paho-mqtt. The isolated aiomqtt3 extra must
        # install it or every publisher_only point is fail-closed as
        # publisher_completions_unconfirmed (sys_probe_start_failed).
        text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        extra = re.search(r"^aiomqtt3\s*=\s*\[([^\]]*)\]", text, re.M)
        self.assertIsNotNone(extra, "aiomqtt3 extra missing from pyproject.toml")
        self.assertIn("paho-mqtt", extra.group(1))

    def test_awscrt_identity_native(self):
        from mqtt_client_bench.adapters.registry import adapter_identity, get_adapter_class

        caps = get_adapter_class("awscrt").capabilities()
        self.assertEqual(caps.implementation_language, "native")
        self.assertEqual(caps.io_model, "crt_event_loop")
        info = adapter_identity("awscrt")
        self.assertEqual(info["client"], "awscrt")
        self.assertEqual(info["implementation_language"], "native")

    def test_client_identities_stable(self):
        from mqtt_client_bench.adapters.registry import adapter_identity, get_adapter_class

        for name in ("paho", "gmqtt", "aiomqtt", "amqtt", "awscrt", "zmqtt"):
            caps = get_adapter_class(name).capabilities()
            self.assertEqual(caps.unimplemented, [], name)
            info = adapter_identity(name)
            self.assertEqual(info["client"], name)
            self.assertIsNotNone(info.get("client_module"), name)

    def test_adapters_declare_their_private_api_use(self):
        # Reaching into a library's internals changes what is being measured, so
        # every such dependency must be declared and visible in the result JSON.
        from mqtt_client_bench.adapters.registry import adapter_identity

        for name in ("gmqtt", "aiomqtt", "mqttium-compat", "amqtt", "mqttium"):
            info = adapter_identity(name)
            declared = info.get("private_api")
            self.assertTrue(declared, f"{name} must declare its private API use")
            for attr, reason in declared.items():
                self.assertTrue(reason.strip(), f"{name}:{attr} needs a reason")
        # aiomqtt3 lives in a separate venv; skip when not installed.
        info = adapter_identity("aiomqtt3")
        if info.get("error") or not info.get("client_module"):
            return
        declared = info.get("private_api")
        self.assertTrue(declared, "aiomqtt3 must declare its private API use")

    def test_gmqtt_private_api_shape(self):
        # gmqtt's public publish() drops the packet id, so QoS>=1 mirrors it via
        # internals. If a gmqtt release moves them, fail here rather than let the
        # adapter silently measure something else.
        import inspect

        from gmqtt import Client

        source = inspect.getsource(Client.publish)
        self.assertIn("self._connection.publish(message)", source)
        self.assertIn("push_message_nowait", source)
        # Still returns nothing: that is why QoS>=1 cannot use the public API.
        self.assertNotIn("return ", source)
        client = Client("shape-probe")
        for attr in ("_connection", "_persistent_storage", "_remove_message_from_query"):
            self.assertTrue(hasattr(client, attr), f"gmqtt no longer exposes {attr}")

    def test_every_adapter_declares_how_completions_reach_the_worker(self):
        """An adapter that suspends a coroutine per publish pays a resume the
        others do not — measured at 11-34% here, growing with load. Five of six
        bridged clients are forced onto that path by their library's API; the
        rule is that anyone with a cheaper path takes it, and that everyone
        records which path they are on, so a ranking can be read honestly."""
        from mqtt_client_bench.adapters.registry import _ADAPTERS, adapter_identity, get_adapter_class

        expected = {
            "paho": "sync", "mqttium-compat": "sync",
            "gmqtt": "callback", "awscrt": "callback", "mqttium": "callback",
            "aiomqtt": "awaited", "amqtt": "awaited", "zmqtt": "awaited",
            "aiomqtt3": "awaited",
        }
        self.assertEqual(set(expected), set(_ADAPTERS), "a client gained or lost a declaration")
        for name, want in expected.items():
            caps = get_adapter_class(name).capabilities()
            self.assertEqual(caps.completion_mechanism, want, name)
            self.assertIn(caps.completion_mechanism, ("sync", "callback", "awaited"), name)

        # And it must reach the result document, or a reader cannot see it.
        self.assertEqual(adapter_identity("gmqtt")["completion_mechanism"], "callback")

    def test_max_queued_bytes_only_reaches_declaring_adapters(self):
        # A byte-bounded outbound queue silently becomes the binding window at
        # large payloads. The knob equalises that, so it must reach every
        # adapter that declares the bound and none that would ignore it.
        import inspect

        from mqtt_client_bench.adapters.registry import _ADAPTERS, get_adapter_class

        declaring = {
            name
            for name in _ADAPTERS
            if get_adapter_class(name).capabilities().max_queued_bytes
        }
        self.assertEqual(declaring, {"mqttium", "mqttium-compat"})
        for name in declaring:
            params = inspect.signature(get_adapter_class(name).create).parameters
            self.assertIn("max_queued_bytes", params, name)
        for name in set(_ADAPTERS) - declaring:
            params = inspect.signature(get_adapter_class(name).create).parameters
            self.assertNotIn("max_queued_bytes", params, name)

    def test_mqttium_keeps_the_qos0_fast_path_unarmed(self):
        """mqttium takes its direct QoS0 transport write only while on_publish
        is None. The adapter needs that callback to correlate QoS>=1 acks, and
        installing it at connect cost 38% of the QoS0 rate — 39,118 msgs/s down
        to 24,039 — because it disarmed the fast path for every point. QoS is
        fixed per measurement point, so the callback is installed by the first
        QoS>=1 publish and a QoS0 point must never arm it. Asserted rather than
        inferred from a rate, which run-to-run noise would hide."""
        from mqtt_client_bench.adapters.mqttium import MqttiumAdapter

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
                self.assertEqual(
                    stub.on_publish is not None, armed,
                    f"qos={qos}: on_publish {'must' if armed else 'must not'} be armed",
                )

    def test_mqttium_private_api_shape(self):
        # mqttium moves fast and the compat adapter still reaches into the
        # façade for TLS, SUBACK delivery, and write-pump sizing. If a release
        # moves any of that, fail here rather than let the adapter silently
        # measure something else.
        import inspect

        from mqttium.api import AsyncClient
        from mqttium.compat import paho as mqtt

        async_client = AsyncClient(client_id="shape-probe")
        for attr in (
            "publish_nowait",
            "message_callback_add",
            "message_callback_remove",
            "_engine",
            "_engine_lock",
            "_sub_futs",
            "_collect_effects_locked",
            "_drain_effects",
            "_reconfigure",
            "_max_outbound_bytes",
        ):
            self.assertTrue(hasattr(async_client, attr), f"mqttium no longer exposes {attr}")
        # No on_subscribe hook is why the compat adapter mirrors the SUBACK
        # future registration by hand.
        self.assertFalse(hasattr(async_client, "on_subscribe"))

        facade = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="shape-probe")
        for attr in ("_async", "_loop", "_submit", "_run_loop_mutation"):
            self.assertTrue(hasattr(facade, attr), f"mqttium façade no longer exposes {attr}")
        # rc6 added the ctor parameter the adapter used to rebuild the inner
        # client to set. Keep asserting attach-time-only so a later release
        # that makes it runtime-mutable can drop the ctor-only comment.
        self.assertIn(
            "max_outbound_inflight", inspect.signature(mqtt.Client.__init__).parameters
        )
        with self.assertRaises(AttributeError):
            facade._async._reconfigure(max_outbound_inflight=64)

    def test_mqttium_compat_passes_inflight_and_write_pump_on_create(self):
        from mqtt_client_bench.adapters.mqttium_compat import MqttiumCompatAdapter

        adapter = MqttiumCompatAdapter.create(
            client_id="inflight-probe",
            max_inflight=64,
            max_queued=200,
            max_queued_bytes=8 << 20,
        )
        inner = adapter._client._async
        self.assertEqual(inner._engine.config.max_outbound_inflight, 64)
        self.assertEqual(inner._engine.config.max_pending_outbound_messages, 200)
        self.assertEqual(inner._engine.config.max_pending_outbound_bytes, 64 << 20)
        self.assertEqual(inner._max_outbound_bytes, 8 << 20)

    def test_mqttium_queues_native_filters_until_connect(self):
        adapter = MqttiumAdapter.create(client_id="cb-probe")
        adapter.message_callback_add("bench/+/x", lambda *_a: None)
        self.assertEqual(len(adapter._pending_filters), 1)
        self.assertIsNone(adapter._client)

    def test_gmqtt_v5_properties_align_payload_format(self):
        from mqtt_client_bench.adapters.gmqtt import GmqttAdapter
        from mqtt_client_bench.adapters.paho import build_paho_publish_properties

        g = GmqttAdapter().build_publish_properties("realistic")
        self.assertEqual(g["payload_format_indicator"], 1)
        p = build_paho_publish_properties("realistic")
        self.assertEqual(getattr(p, "PayloadFormatIndicator"), 1)


class BridgedAdapterTests(unittest.TestCase):
    def test_topic_matches_sub(self):
        from mqtt_client_bench.adapters.async_bridge import topic_matches_sub

        self.assertTrue(topic_matches_sub("a/b", "a/b"))
        self.assertTrue(topic_matches_sub("a/+", "a/b"))
        self.assertTrue(topic_matches_sub("a/#", "a/b/c"))
        self.assertTrue(topic_matches_sub("#", "a/b"))
        self.assertFalse(topic_matches_sub("a/b", "a/c"))
        self.assertFalse(topic_matches_sub("a/+", "a/b/c"))
        self.assertFalse(topic_matches_sub("a/#", "b/c"))

    def test_dispatch_prefers_topic_callback(self):
        from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase, IncomingMessage

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
        from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase

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
        import time

        from mqtt_client_bench.adapters.async_bridge import AsyncioBridge

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
        import time

        from mqtt_client_bench.adapters.async_bridge import AsyncioBridge

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
        import asyncio
        import time

        from mqtt_client_bench.adapters.async_bridge import AsyncioBridge

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

    def test_bridge_stop_drops_queued_work_cleanly(self):
        from mqtt_client_bench.adapters.async_bridge import AsyncioBridge

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
        from mqtt_client_bench.adapters.async_bridge import BridgedAdapterBase

        adapter = BridgedAdapterBase()
        mids = [adapter.alloc_mid() for _ in range(5)]
        self.assertEqual(mids, [1, 2, 3, 4, 5])
        adapter._next_mid = 65535
        self.assertEqual(adapter.alloc_mid(), 65535)
        self.assertEqual(adapter.alloc_mid(), 1)

    def test_create_adapters(self):
        from mqtt_client_bench.adapters.registry import create_adapter

        for name in ("gmqtt", "aiomqtt", "amqtt", "zmqtt", "awscrt"):
            adapter = create_adapter(name, client_id=f"test-{name}")
            self.assertEqual(adapter.MQTT_ERR_SUCCESS, 0)
            self.assertTrue(hasattr(adapter, "publish"))
            self.assertTrue(hasattr(adapter, "subscribe"))
            self.assertIsNone(adapter.build_publish_properties("none"))


class PublisherContractTests(unittest.TestCase):
    def test_early_ack_tracker(self):
        from mqtt_client_bench.roles import publisher as pub_mod

        state = {
            "mid_send_ns": {},
            "early_acks": {},
            "seen_mids_inflight": {7},
            "inflight_local": 1,
            "completed_success": 0,
            "completed_failed": 0,
            "protocol_completed": 0,
            "protocol_failed": 0,
            "socket_completed_qos0": 0,
            "completed_in_window": 0,
            "completed_during_drain": 0,
            "latencies_ns": ReservoirSampler(10),
            "phase": "measure",
            "lock": __import__("threading").Lock(),
            "overflow_success": 0, "overflow_failed": 0,
            "overflow_in_window": 0, "overflow_during_drain": 0,
            "fold_pending": False,
        }
        state["completions"] = CompletionLog(64, sampler=state["latencies_ns"])
        # Simulate callback before registration.
        now = 1000
        with state["lock"]:
            state["early_acks"][7] = (now, False)
            early = state["early_acks"].pop(7, None)
            self.assertIsNotNone(early)
            pub_mod._consume_completion_locked(state, 1, 500, early[0], early[1], mid=7)
        # The counters are derived from the log, not maintained live.
        tally = state["completions"].summary(1)
        self.assertEqual(tally["completed_success"], 1)
        self.assertEqual(tally["completed_in_window"], 1)
        self.assertEqual(state["latencies_ns"].seen, 1, "the latency must survive the fold")
        self.assertNotIn(7, state["seen_mids_inflight"])

    def test_mid_freed_on_completion_allows_reuse(self):
        from mqtt_client_bench.roles import publisher as pub_mod

        state = {
            "mid_send_ns": {3: 100},
            "early_acks": {},
            "seen_mids_inflight": {3},
            "inflight_local": 1,
            "completed_success": 0,
            "completed_failed": 0,
            "protocol_completed": 0,
            "protocol_failed": 0,
            "socket_completed_qos0": 0,
            "completed_in_window": 0,
            "completed_during_drain": 0,
            "latencies_ns": ReservoirSampler(10),
            "phase": "measure",
            "lock": __import__("threading").Lock(),
            "overflow_success": 0, "overflow_failed": 0,
            "overflow_in_window": 0, "overflow_during_drain": 0,
            "fold_pending": False,
        }
        state["completions"] = CompletionLog(64, sampler=state["latencies_ns"])
        with state["lock"]:
            send_ns = state["mid_send_ns"].pop(3)
            pub_mod._consume_completion_locked(state, 1, send_ns, 200, False, mid=3)
        self.assertNotIn(3, state["seen_mids_inflight"])
        # Same mid may be issued again without a false collision.
        self.assertNotIn(3, state["seen_mids_inflight"])

    def test_open_loop_backpressure_counter_logic(self):
        # outstanding gate must count misses rather than unbounded growth.
        outstanding = 2
        inflight_local = 2
        missed = 0
        if inflight_local >= outstanding:
            missed += 1
        self.assertEqual(missed, 1)

    def _drive_publish_loop(self, *, ack_mode: str, outstanding: int = 8, until_s: float = 0.05):
        """Run the real publish loop against a fake adapter.

        ``ack_mode``: "sync" fires on_publish inside publish() (before it returns,
        the early-ack race), "deferred" fires it after publish() returns, "never"
        leaves the mid outstanding.
        """
        import threading
        import time as _time

        from mqtt_client_bench.adapters.base import PublishResult
        from mqtt_client_bench.roles import publisher as pub_mod

        state = {
            "offered": 0,
            "submitted": 0,
            "sync_rejected": 0,
            "completed_success": 0,
            "completed_failed": 0,
            "missed_due_to_backpressure": 0,
            "publish_calls": 0,
            "publish_accepted": 0,
            "publish_rejected": 0,
            "protocol_completed": 0,
            "protocol_failed": 0,
            "socket_completed_qos0": 0,
            "completed_in_window": 0,
            "completed_during_drain": 0,
            # The worker bounds these upstream now, so the loop calls .add() on a
            # reservoir rather than appending to a list.
            "latencies_ns": ReservoirSampler(1000, seed=11),
            "scheduler_lags_ns": ReservoirSampler(1000, seed=29),
            "lock": threading.Lock(),
            "inflight_local": 0,
            "phase": "measure",
            "mid_send_ns": {},
            "early_acks": {},
            "seen_mids_inflight": set(),
            "overflow_success": 0,
            "overflow_failed": 0,
            "overflow_in_window": 0,
            "overflow_during_drain": 0,
            "fold_pending": False,
        }
        # Completions are logged and tallied after the window, so a state built
        # by hand needs the log the same way the role worker builds it.
        state["completions"] = CompletionLog(4096, sampler=state["latencies_ns"])

        def on_publish(client, userdata, mid, reason_code=None, properties=None):
            now = _time.perf_counter_ns()
            with state["lock"]:
                send_ns = state["mid_send_ns"].pop(mid, None)
                if send_ns is None:
                    state["early_acks"][mid] = (now, False)
                    return
                pub_mod._consume_completion_locked(state, 1, send_ns, now, False, mid=mid)

        class FakeAdapter:
            def __init__(self):
                self.next_mid = 0
                self.pending = []

            def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
                # Wrap like a real 16-bit packet id so mid reuse is exercised.
                self.next_mid = 1 if self.next_mid >= 32 else self.next_mid + 1
                mid = self.next_mid
                if ack_mode == "sync":
                    on_publish(self, None, mid)
                elif ack_mode == "deferred":
                    self.pending.append(mid)
                    # Ack in batches so the loop keeps going and mids get reused,
                    # exercising completion-after-publish-returns with wrapping.
                    if len(self.pending) >= max(1, outstanding // 2):
                        self.flush()
                return PublishResult(rc=0, mid=mid)

            def flush(self):
                for mid in self.pending:
                    on_publish(self, None, mid)
                self.pending.clear()

        adapter = FakeAdapter()
        pub_mod._run_publish_loop(
            adapter,
            state,
            topic="t",
            qos=1,
            body=b"x" * 8,
            corpus=[],
            run_id=b"abcdefgh",
            outstanding=outstanding,
            cadence="capacity",
            until=_time.perf_counter() + until_s,
            target_rate=None,
            properties_builder=lambda: None,
        )
        adapter.flush()
        return state

    def test_publish_loop_counters_survive_being_hoisted(self):
        # The loop keeps its single-writer counters in locals and flushes them to
        # `state` on the way out, which is worth a per-message dict lookup but
        # only if no exit path can drop them. These identities catch a missed
        # flush immediately: every offer is a call, and every call either lands
        # or is rejected.
        for ack_mode in ("sync", "deferred"):
            with self.subTest(ack_mode=ack_mode):
                state = self._drive_publish_loop(ack_mode=ack_mode, until_s=0.05)
                self.assertGreater(state["publish_calls"], 0)
                self.assertEqual(state["offered"], state["publish_calls"])
                self.assertEqual(state["submitted"], state["publish_accepted"])
                self.assertEqual(state["sync_rejected"], state["publish_rejected"])
                self.assertEqual(
                    state["publish_calls"],
                    state["submitted"] + state["sync_rejected"] + state["missed_due_to_backpressure"],
                )

    def test_publish_loop_tracker_matches_outstanding_mids(self):
        # seen_mids_inflight is maintained incrementally (no per-message rebuild):
        # it must stay exactly the set of submitted-but-uncompleted mids.
        for mode in ("sync", "deferred"):
            with self.subTest(ack_mode=mode):
                state = self._drive_publish_loop(ack_mode=mode)
                self.assertGreater(state["offered"], 0)
                self.assertEqual(
                    state["seen_mids_inflight"],
                    set(state["mid_send_ns"]) | set(state["early_acks"]),
                )
                tally = state["completions"].summary(1)
                # The log is deliberately small here so the fold path runs: the
                # few completions between a full buffer and the fold are counted
                # live, so the total is the sum of the two.
                completed = tally["completed_success"] + state["overflow_success"]
                self.assertEqual(completed, state["submitted"])
                # A wrapping mid that was already freed must not count as a collision.
                self.assertEqual(tally["completed_failed"] + state["overflow_failed"], 0)
                self.assertEqual(state["inflight_local"], 0)

    def test_publish_loop_gate_blocks_at_outstanding_without_acks(self):
        # No completions: the outstanding gate must bound submissions and the
        # tracker must hold exactly those still-open mids.
        state = self._drive_publish_loop(ack_mode="never", outstanding=8)
        self.assertEqual(state["inflight_local"], 8)
        self.assertEqual(state["submitted"], 8)
        self.assertEqual(len(state["seen_mids_inflight"]), 8)
        self.assertEqual(state["seen_mids_inflight"], set(state["mid_send_ns"]))

    def test_aiomqtt3_refuses_v5_property_profiles(self):
        missing = unsupported_for_client(
            "aiomqtt3", {"protocol": "MQTTv5", "properties_profile": "realistic"}
        )
        self.assertTrue(any("properties" in m for m in missing), missing)


class ScenarioTests(unittest.TestCase):
    def test_core_catalogue(self):
        core = list_scenarios("core")
        self.assertGreaterEqual(len(core), 5)
        names = {s.name for s in core}
        self.assertIn("pub_qos_sweep_telemetry", names)

    def test_removed_executable_variants(self):
        hier = expand_scenario(SCENARIO_BY_NAME["sub_hierarchy_telemetry"], "standard")
        self.assertFalse(any(p.get("topic_topology") == "fleet4k_zipf" for p in hier))
        stress = expand_scenario(SCENARIO_BY_NAME["topic_stress"], "standard")
        self.assertFalse(any(p.get("topic_topology") == "fleet100k" for p in stress))
        net = expand_scenario(SCENARIO_BY_NAME["network_matrix"], "standard")
        self.assertFalse(any(p.get("network") == "wan_cut" for p in net))
        # session_resume_qos1 is executable now: a plain DISCONNECT is enough of an
        # outage, since MQTT keeps session state whenever Clean Session = 0.
        session = SCENARIO_BY_NAME["session_resume_qos1"]
        self.assertNotIn("planned", session.tags)

    def test_rtt_requires_tcp_nodelay(self):
        # Without TCP_NODELAY the RTT loop measures a ~40 ms/hop Nagle plateau.
        from mqtt_client_bench.adapters.awscrt import AwscrtAdapter
        from mqtt_client_bench.adapters.paho import PahoAdapter

        point = {"topology": "application_rtt", "qos_publish": 1, "qos_subscribe": 1}
        self.assertIn("tcp_nodelay", AwscrtAdapter.capabilities().missing_for_point(point))
        self.assertNotIn("tcp_nodelay", PahoAdapter.capabilities().missing_for_point(point))
        # Non-RTT topologies stay unaffected for awscrt.
        self.assertNotIn(
            "tcp_nodelay",
            AwscrtAdapter.capabilities().missing_for_point({"topology": "publisher_only"}),
        )

    def test_former_planned_scenarios_are_executable(self):
        for name in ("mqttv5_flow_control", "queue_rejection", "retained_bootstrap"):
            scenario = SCENARIO_BY_NAME[name]
            self.assertNotIn("planned", scenario.tags, name)
            self.assertEqual(scenario.suite, "full", name)
            for point in expand_scenario(scenario, "standard"):
                missing = [m for m in unsupported_features(point) if m == "planned_scenario"]
                self.assertEqual(missing, [], name)

    def test_inflight_variant_marks_requirement(self):
        points = expand_scenario(SCENARIO_BY_NAME["pub_qos1_inflight"], "standard")
        self.assertTrue(all(p.get("require_max_inflight") for p in points))

    def test_expand_smoke_shorter(self):
        scenario = SCENARIO_BY_NAME["pub_qos_sweep_telemetry"]
        smoke = expand_scenario(scenario, "smoke")
        standard = expand_scenario(scenario, "standard")
        self.assertTrue(all(p.get("non_comparable") for p in smoke))
        self.assertGreater(standard[0]["duration_s"], smoke[0]["duration_s"])
        self.assertEqual(standard[0]["duration_s"], 12.0)
        self.assertEqual(standard[0]["warmup_s"], 3.0)
        self.assertEqual(standard[0]["drain_s"], 6.0)
        self.assertEqual(smoke[0]["duration_s"], 3.0)

    def test_estimate(self):
        from mqtt_client_bench.scenarios import default_runs

        est = estimate_suite("core", "smoke", 1)
        self.assertGreater(est["points"], 0)
        self.assertGreater(est["estimated_minutes"], 0)
        std = estimate_suite("core", "standard", default_runs("standard"))
        self.assertEqual(std["runs_per_point"], 3)
        # core×1 client must stay night-sized, not multi-day.
        self.assertLess(std["estimated_minutes"], 120.0)
        self.assertGreater(std["estimated_minutes"], 20.0)

    def test_experimental_suite_matches_core_contracts(self):
        core = {s.name for s in list_scenarios("core")}
        experimental = {s.name for s in list_scenarios("experimental")}
        self.assertEqual(core, experimental)

    def test_experimental_clients_refused_from_core_suite(self):
        from mqtt_client_bench.harness import run_suite

        with self.assertRaises(ValueError):
            run_suite("core", client="zmqtt", profile="smoke", runs=1)


class CliDefaultsTests(unittest.TestCase):
    def test_profile_defaults_standard(self):
        from mqtt_client_bench.run import build_parser

        parser = build_parser()
        for cmd in ("run", "calibrate", "compare"):
            args = parser.parse_args([cmd] + (["--output", "x"] if cmd == "calibrate" else [])
                                     + (["--clients", "paho,gmqtt", "--scenario", "pub_qos_sweep_telemetry"] if cmd == "compare" else [])
                                     + (["--scenario", "pub_qos_sweep_telemetry"] if cmd == "run" else []))
            self.assertEqual(args.profile, "standard", cmd)


class BarrierTests(unittest.TestCase):
    def test_two_phase_barrier(self):
        import tempfile
        import threading

        from mqtt_client_bench.control import BarrierServer, barrier_client_session

        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "b.sock")
            server = BarrierServer(path)
            results = []

            def worker():
                s = barrier_client_session(path, timeout_s=5)
                results.append(s.wait("T0"))
                s.ack("WARMUP_DRAINED")
                results.append(s.wait("T_MEASURE"))
                s.close()

            t = threading.Thread(target=worker)
            t.start()
            server.accept_n(1, timeout_s=5)
            self.assertEqual(server.broadcast("T0"), 0)
            server.wait_for_acks("WARMUP_DRAINED", 1, timeout_s=5)
            self.assertEqual(server.broadcast("T_MEASURE"), 0)
            t.join(timeout=5)
            server.close()
            self.assertEqual(results, ["T0", "T_MEASURE"])


class LoadgenTests(unittest.TestCase):
    def test_parse_fixture(self):
        sample = (ROOT / "fixtures" / "emqtt_bench_sample.txt").read_text(encoding="utf-8")
        stats = parse_emqtt_output(sample)
        self.assertGreaterEqual(stats["samples"], 2)
        self.assertEqual(stats["last_rate"], 99725)
        self.assertEqual(stats["last_total"], 2102563)
        self.assertEqual(stats["rates"][0], 39.92)

    def test_nominal_rate(self):
        self.assertEqual(nominal_rate(20, 100), 200.0)
        self.assertEqual(interval_for_rate(20, 20000), 1)
        self.assertEqual(nominal_rate(32, 1), 32000.0)
        self.assertEqual(nominal_rate(64, 1), 64000.0)
        self.assertEqual(nominal_rate(128, 1), 128000.0)

    def test_qos0_effective_offer_not_parsed_rate(self):
        """QoS0 pub rates from emqtt-bench are ~2×; offer reference is nominal."""
        spec = LoadgenSpec(clients=32, interval_ms=1, qos=0, mode="pub", engine="emqtt")
        parsed = {
            "median_rate": 64000.0,
            "last_rate": 64000.0,
            "last_total": 64000,
            "samples": 1,
            "rates": [64000.0],
            "totals": [64000],
            "kinds": ["pub"],
        }
        stats = enrich_loadgen_stats(spec, parsed)
        self.assertEqual(stats["effective_offer_msgs_per_s"], 32000.0)
        self.assertEqual(stats["nominal_rate"], 32000.0)
        self.assertTrue(stats["qos0_pub_counter_double_count"])
        self.assertEqual(stats["emitted_msgs"], 32000)
        self.assertEqual(stats["parsed_pub_rate_raw"], 64000.0)
        self.assertEqual(stats["observed_pub_rate"], 32000.0)
        self.assertEqual(observed_pub_rate(parsed, qos=0), 32000.0)
        self.assertEqual(observed_pub_rate(parsed, qos=1), 64000.0)

    def test_mqtt_version_helper(self):
        from mqtt_client_bench.harness import effective_loadgen_mqtt_version, mqtt_version_for_point

        self.assertEqual(mqtt_version_for_point({"protocol": "MQTTv5"}), 5)
        self.assertEqual(mqtt_version_for_point({"protocol": "MQTTv311"}), 4)
        self.assertEqual(mqtt_version_for_point({"protocol": "MQTTv31"}), 3)
        self.assertEqual(effective_loadgen_mqtt_version(4), 5)
        self.assertEqual(effective_loadgen_mqtt_version(5), 5)

    def test_loadgen_shortids_for_v311(self):
        args = build_pub_args(LoadgenSpec(mqtt_version=4))
        self.assertIn("--shortids", args)
        self.assertIn("-w", args)
        self.assertNotIn("--shortids", build_pub_args(LoadgenSpec(mqtt_version=5)))

    def test_unpaced_firehose_uses_observed_offer(self):
        spec = LoadgenSpec(clients=8, interval_ms=0, qos=0, mode="pub", engine="hammer")
        parsed = {
            "median_rate": 220000.0,
            "last_rate": 220000.0,
            "samples": 1,
            "rates": [220000.0],
            "totals": [220000],
            "kinds": ["pub"],
        }
        stats = enrich_loadgen_stats(spec, parsed)
        self.assertIsNone(stats["nominal_rate"])
        self.assertEqual(stats["effective_offer_msgs_per_s"], 220000.0)
        self.assertEqual(stats["observed_pub_rate"], 220000.0)
        self.assertFalse(stats["qos0_pub_counter_double_count"])
        self.assertEqual(stats["engine"], "hammer")
        self.assertFalse(stats["paced"])

    def test_paced_hammer_offer_is_configured_rate(self):
        spec = LoadgenSpec(
            clients=2, interval_ms=0, qos=0, mode="pub", engine="hammer", rate_msgs_per_s=200000
        )
        parsed = {
            "median_rate": 189777.0,
            "last_rate": 189777.0,
            "samples": 1,
            "rates": [189777.0],
            "totals": [1897770],
            "kinds": ["pub"],
        }
        stats = enrich_loadgen_stats(spec, parsed)
        self.assertEqual(stats["nominal_rate"], 200000.0)
        self.assertEqual(stats["effective_offer_msgs_per_s"], 200000.0)
        self.assertTrue(stats["paced"])
        self.assertEqual(clamp_hammer_rate(500000), HAMMER_MAX_RATE_MSGS_PER_S)
        self.assertEqual(clamp_hammer_rate(200000), 200000)
        cmd = build_hammer_cmd(spec)
        self.assertIn("--rate", cmd)
        self.assertEqual(cmd[cmd.index("--rate") + 1], "200000")
        self.assertNotIn("--interval-us", cmd)

    def test_select_hammer_for_qos0_exact_topic(self):
        spec = LoadgenSpec(topic="bench/t", qos=0, mode="pub")
        with patch.dict(os.environ, {"MQTT_BENCH_LOADGEN": "auto"}, clear=False):
            self.assertEqual(select_loadgen_engine(spec), "hammer")
        with patch.dict(os.environ, {"MQTT_BENCH_LOADGEN": "emqtt"}, clear=False):
            self.assertEqual(select_loadgen_engine(spec), "emqtt")
        with patch.dict(os.environ, {"MQTT_BENCH_LOADGEN": "nope"}, clear=False):
            self.assertEqual(select_loadgen_engine(spec), "emqtt")
        self.assertEqual(
            select_loadgen_engine(LoadgenSpec(topic="bench/t", qos=0, mode="pub", engine="emqtt")),
            "emqtt",
        )
        self.assertTrue(topic_is_templated("bench/%i/data"))
        templated = LoadgenSpec(topic="bench/%i/data", qos=0, mode="pub")
        with patch.dict(os.environ, {"MQTT_BENCH_LOADGEN": "auto"}, clear=False):
            self.assertEqual(select_loadgen_engine(templated), "emqtt")
        qos1 = LoadgenSpec(topic="bench/t", qos=1, mode="pub")
        self.assertEqual(select_loadgen_engine(qos1), "emqtt")
        self.assertEqual(HAMMER_PUB_CLIENTS, 2)
        self.assertEqual(UNPACED_PUB_CLIENTS, 2)
        self.assertEqual(resolve_hammer_pub_clients({}, 32), HAMMER_PUB_CLIENTS)
        self.assertEqual(resolve_hammer_pub_clients({"fanin_mode": "constant_aggregate"}, 128), 128)
        self.assertEqual(resolve_hammer_pub_clients({"fanin_mode": "per_publisher"}, 1), 1)
        self.assertEqual(HAMMER_MAX_RATE_MSGS_PER_S, 200000)
        clients, target = clamp_emqtt_offer(150, 200000)
        self.assertEqual(clients, 100)
        self.assertEqual(target, float(EMQTT_MAX_OFFER_MSGS_PER_S))
        # Ranking default with the catalogue's 32 loadgen_clients must not
        # stay at 32k: I=1 of 32 is 32k, so publishers have to rise to 100.
        clients, target = clamp_emqtt_offer(32, DEFAULT_INGRESS_OFFER_MSGS_PER_S)
        self.assertEqual(clients, 100)
        self.assertEqual(target, float(EMQTT_MAX_OFFER_MSGS_PER_S))
        self.assertEqual(interval_for_rate(clients, target), 1)
        self.assertEqual(nominal_rate(clients, 1), float(EMQTT_MAX_OFFER_MSGS_PER_S))
        # Explicit I=1 grid / burst-shaped targets keep their client count.
        self.assertEqual(clamp_emqtt_offer(32, 32000), (32, 32000.0))
        self.assertEqual(clamp_emqtt_offer(64, 64000), (64, 64000.0))

    def test_parse_hammer_json(self):
        text = 'noise\n{"role":"pub","clients":8,"msgs":1000,"seconds":1.0,"msgs_per_s":12345.6}\n'
        parsed = parse_hammer_output(text)
        self.assertEqual(parsed["last_total"], 1000)
        self.assertAlmostEqual(parsed["median_rate"], 12345.6)

    def test_callback_match_helpers(self):
        run_id = "abcd1234"
        topics = callback_match_topics(run_id, 3)
        self.assertEqual(
            topics,
            [
                "bench/abcd1234/org/acme/cb/0/data",
                "bench/abcd1234/org/acme/cb/1/data",
                "bench/abcd1234/org/acme/cb/2/data",
            ],
        )
        self.assertEqual(callback_match_loadgen_topic(run_id), "bench/abcd1234/org/acme/cb/%i/data")
        self.assertEqual(len(overlapping_match_filters(run_id, 8)), 8)


class CeilingProbeTests(unittest.TestCase):
    def test_ceiling_scenario_expansion(self):
        broker = SCENARIO_BY_NAME["broker_ceiling_ingress"]
        client = SCENARIO_BY_NAME["client_ceiling_ingress"]
        self.assertEqual(broker.topology, "broker_ceiling")
        self.assertEqual(client.topology, "subscriber_ingress")
        b_points = expand_scenario(broker, "smoke")
        c_points = expand_scenario(client, "smoke")
        self.assertEqual(len(b_points), 3)
        self.assertEqual(len(c_points), 3)
        self.assertEqual([p["loadgen_clients"] for p in b_points], [32, 64, 128])
        self.assertEqual([p["ingress_target_msgs_per_s"] for p in b_points], [32000, 64000, 128000])
        for p in b_points + c_points:
            self.assertTrue(p["non_comparable"])
            self.assertIn("diagnostic", p["tags"])
            # I=1 quantization: clients * 1000 / target == 1
            self.assertEqual(interval_for_rate(p["loadgen_clients"], p["ingress_target_msgs_per_s"]), 1)
            self.assertEqual(nominal_rate(p["loadgen_clients"], 1), float(p["ingress_target_msgs_per_s"]))

    def test_resolve_ingress_offer(self):
        self.assertEqual(resolve_ingress_offer({}, 32), DEFAULT_INGRESS_OFFER_MSGS_PER_S)
        self.assertEqual(resolve_ingress_offer({"ingress_target_msgs_per_s": 64000}, 64), 64000.0)
        self.assertEqual(resolve_ingress_offer({"fanin_mode": "per_publisher"}, 16), 16000.0)
        self.assertEqual(DEFAULT_INGRESS_OFFER_MSGS_PER_S, 200000.0)

    def test_ingress_offer_override_marks_non_comparable(self):
        with patch.dict(os.environ, {"MQTT_BENCH_INGRESS_OFFER": "240000"}, clear=False):
            point = {}
            self.assertEqual(resolve_ingress_offer(point, 32), 240000.0)
            self.assertTrue(point["non_comparable"])
            self.assertTrue(point["ingress_offer_overridden"])
            # Explicit ceiling-grid targets and per_publisher offers stay untouched.
            explicit = {"ingress_target_msgs_per_s": 64000}
            self.assertEqual(resolve_ingress_offer(explicit, 64), 64000.0)
            self.assertNotIn("ingress_offer_overridden", explicit)
            fanin = {"fanin_mode": "per_publisher"}
            self.assertEqual(resolve_ingress_offer(fanin, 16), 16000.0)
            self.assertNotIn("ingress_offer_overridden", fanin)
            # The hammer clamp lifts to the override instead of truncating it.
            self.assertEqual(clamp_hammer_rate(240000), 240000)
            self.assertEqual(clamp_hammer_rate(500000), 240000)
        with patch.dict(os.environ, {"MQTT_BENCH_INGRESS_OFFER": "garbage"}, clear=False):
            with self.assertRaises(ValueError):
                resolve_ingress_offer({}, 32)
        with patch.dict(os.environ, {"MQTT_BENCH_INGRESS_OFFER": "-1"}, clear=False):
            with self.assertRaises(ValueError):
                resolve_ingress_offer({}, 32)
        # Without the env var nothing changes: default offer, conservative clamp.
        clean = {k: v for k, v in os.environ.items() if k != "MQTT_BENCH_INGRESS_OFFER"}
        with patch.dict(os.environ, clean, clear=True):
            point = {}
            self.assertEqual(resolve_ingress_offer(point, 32), DEFAULT_INGRESS_OFFER_MSGS_PER_S)
            self.assertNotIn("non_comparable", point)
            self.assertEqual(clamp_hammer_rate(500000), HAMMER_MAX_RATE_MSGS_PER_S)

    def test_sub_exact_offer_is_200k(self):
        points = expand_scenario(SCENARIO_BY_NAME["sub_exact_telemetry"], "smoke")
        self.assertGreaterEqual(len(points), 1)
        for p in points:
            self.assertEqual(resolve_ingress_offer(p, p["loadgen_clients"]), DEFAULT_INGRESS_OFFER_MSGS_PER_S)

    def test_templated_emqtt_offer_raises_clients_to_hold_100k(self):
        """Catalogue loadgen_clients=32 must not keep a 32k I=1 offer after 200k default."""
        clients = 32
        target = resolve_ingress_offer({"callback_filters": 1}, clients)
        self.assertEqual(target, DEFAULT_INGRESS_OFFER_MSGS_PER_S)
        clients, target = clamp_emqtt_offer(clients, target)
        interval = interval_for_rate(clients, target)
        self.assertEqual(clients, 100)
        self.assertEqual(target, float(EMQTT_MAX_OFFER_MSGS_PER_S))
        self.assertEqual(interval, 1)
        self.assertEqual(nominal_rate(clients, interval), float(EMQTT_MAX_OFFER_MSGS_PER_S))
        # Filter sweep: 1 / 16 / 256 must share that offer once clamped.
        for filters in (1, 16, 256):
            n = max(32, min(filters, 256))
            n, tgt = clamp_emqtt_offer(n, resolve_ingress_offer({}, n))
            self.assertEqual(nominal_rate(n, interval_for_rate(n, tgt)), float(EMQTT_MAX_OFFER_MSGS_PER_S))

    def test_validate_run_uses_effective_offer_not_raw_qos0(self):
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress",
            "cadence": "capacity",
            "duration_s": 20.0,
            "tags": ["representative"],
        }
        workers = [{"ok": True, "role": "subscriber", "msgs_per_s": 30000.0, "subscriber_delivered": 600000}]
        # Raw last_rate looks like 64k but effective offer is 32k — must NOT flag loadgen_below_half.
        loadgen = {
            "nominal_rate": 32000.0,
            "effective_offer_msgs_per_s": 32000.0,
            "observed_pub_rate": 31000.0,
            "qos0_pub_counter_double_count": True,
            "parsed": {"last_rate": 64000.0, "last_total": 1280000, "median_rate": 64000.0},
        }
        # last_total 1_280_000 with QoS0 double-count → 640_000 decoded PUBLISHes.
        sys_counters = {"dropped_delta": 0, "publish_received_delta": 640_000}
        validity = validate_run(point, workers, loadgen, [], sys_counters=sys_counters)
        self.assertNotIn("loadgen_below_half_nominal", validity["reasons"])
        self.assertEqual(validity["bottleneck"], "offer_limited")
        self.assertAlmostEqual(validity["delivery_offer_ratio"], 30000.0 / 32000.0)

    def test_validate_run_sys_drops_broker_limited(self):
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "broker_ceiling",
            "cadence": "capacity",
            "duration_s": 20.0,
            "tags": ["diagnostic"],
        }
        loadgen = {
            "nominal_rate": 64000.0,
            "effective_offer_msgs_per_s": 64000.0,
            "observed_pub_rate": 60000.0,
            "qos0_pub_counter_double_count": True,
            "parsed": {"last_rate": 120000.0, "last_total": 2400000, "median_rate": 120000.0},
        }
        ref_sub = {
            "observed_recv_rate": 28000.0,
            "parsed": {"last_rate": 28000.0, "last_total": 560000, "median_rate": 28000.0},
        }
        # > 1% of offer*duration (64000*20*0.01 = 12800).
        # last_total 2_400_000 with QoS0 double-count → 1_200_000 decoded PUBLISHes.
        sys_counters = {"dropped_delta": 20000, "publish_received_delta": 1_200_000}
        validity = validate_run(point, [], loadgen, [], sys_counters=sys_counters, loadgen_ref_sub=ref_sub)
        self.assertEqual(validity["bottleneck"], "broker_limited")
        self.assertEqual(validity["status"], "inconclusive")
        self.assertTrue(
            any(str(r).startswith("sys_publish_dropped") for r in validity["reasons"]),
            validity["reasons"],
        )
        self.assertIn("delivery_below_half_offer", validity["reasons"])

    def test_validate_run_sys_drops_fail_closed_subscriber_ingress(self):
        """Ingress drops mean the SUT did not drain; the delivery count is still a score."""
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress",
            "cadence": "capacity",
            "duration_s": 20.0,
        }
        workers = [
            {
                "ok": True,
                "role": "subscriber",
                "msgs_per_s": 16000.0,
                "subscriber_delivered": 320000,
            }
        ]
        loadgen = {
            "nominal_rate": 200000.0,
            "effective_offer_msgs_per_s": 200000.0,
            "observed_pub_rate": 180000.0,
            "parsed": {"last_rate": 180000.0, "last_total": 3600000, "median_rate": 180000.0},
        }
        sys_counters = {"dropped_delta": 42699, "publish_received_delta": 3_600_000}
        validity = validate_run(point, workers, loadgen, [], sys_counters=sys_counters)
        self.assertEqual(validity["status"], "valid")
        self.assertFalse(
            any(str(r).startswith("sys_publish_dropped:") for r in validity["reasons"]),
            validity["reasons"],
        )
        self.assertEqual(validity["bottleneck"], "sut_limited")

    def test_validate_run_delivery_below_half_subscriber_ingress(self):
        """Core capacity: a slow SUT well below the offer is still a score."""
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress",
            "cadence": "capacity",
            "duration_s": 20.0,
        }
        workers = [
            {
                "ok": True,
                "role": "subscriber",
                "msgs_per_s": 14000.0,
                "subscriber_delivered": 280000,
            }
        ]
        loadgen = {
            "engine": "hammer",
            "nominal_rate": 200000.0,
            "effective_offer_msgs_per_s": 200000.0,
            "observed_pub_rate": 199000.0,
            "target_requested": 200000.0,
            "rate_msgs_per_s": 200000.0,
            "interval_ms": 0,
            "paced": True,
            "parsed": {"last_rate": 199000.0, "last_total": 3980000, "median_rate": 199000.0},
        }
        validity = validate_run(
            point, workers, loadgen, [],
            sys_counters={"dropped_delta": 0, "publish_received_delta": 3_980_000},
        )
        self.assertEqual(validity["status"], "valid")
        self.assertNotIn("delivery_below_half_offer", validity["reasons"])
        self.assertEqual(validity["bottleneck"], "sut_limited")

    def test_validate_run_ingress_ranking_ignores_pegged_broker_cpu(self):
        """A 200k offer pegs Mosquitto; ranking sub_* still scores deliveries."""
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress",
            "cadence": "capacity",
            "duration_s": 20.0,
        }
        workers = [
            {
                "ok": True,
                "role": "subscriber",
                "msgs_per_s": 45000.0,
                "subscriber_delivered": 900000,
            }
        ]
        loadgen = {
            "engine": "hammer",
            "nominal_rate": 200000.0,
            "effective_offer_msgs_per_s": 200000.0,
            "observed_pub_rate": 199000.0,
            "target_requested": 200000.0,
            "rate_msgs_per_s": 200000.0,
            "interval_ms": 0,
            "paced": True,
            "parsed": {"last_rate": 199000.0, "last_total": 3980000, "median_rate": 199000.0},
        }
        samples = [{"containers": {"mosquitto": {"cpu_pct": 100.0}}}]
        sys_counters = {"dropped_delta": 50000, "publish_received_delta": 3_980_000}
        validity = validate_run(point, workers, loadgen, samples, sys_counters=sys_counters)
        self.assertEqual(validity["status"], "valid")
        self.assertEqual(validity["bottleneck"], "sut_limited")
        self.assertAlmostEqual(validity["broker_cpu_max_pct"], 100.0)
        self.assertFalse(
            any(str(r).startswith("container_cpu_high:") for r in validity["reasons"]),
            validity["reasons"],
        )
        self.assertFalse(
            any(str(r).startswith("broker_headroom_low:") for r in validity["reasons"]),
            validity["reasons"],
        )

        diagnostic = dict(point)
        diagnostic["tags"] = ["diagnostic"]
        refused = validate_run(diagnostic, workers, loadgen, samples, sys_counters=sys_counters)
        self.assertEqual(refused["status"], "inconclusive")
        self.assertTrue(
            any(str(r).startswith("container_cpu_high:") for r in refused["reasons"]),
            refused["reasons"],
        )
        self.assertEqual(refused["bottleneck"], "broker_limited")

    def test_validate_run_unpaced_slow_client_is_valid_sut(self):
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress",
            "cadence": "capacity",
            "duration_s": 20.0,
        }
        workers = [
            {
                "ok": True,
                "role": "subscriber",
                "msgs_per_s": 15000.0,
                "subscriber_delivered": 300000,
            }
        ]
        loadgen = {
            "engine": "hammer",
            "nominal_rate": None,
            "effective_offer_msgs_per_s": 220000.0,
            "observed_pub_rate": 220000.0,
            "target_requested": 100000.0,
            "interval_ms": 0,
            "paced": False,
            "parsed": {"last_rate": 220000.0, "last_total": 4400000, "median_rate": 220000.0},
        }
        validity = validate_run(
            point, workers, loadgen, [],
            sys_counters={"dropped_delta": 0, "publish_received_delta": 4_400_000},
        )
        self.assertEqual(validity["status"], "valid")
        self.assertNotIn("delivery_below_half_offer", validity["reasons"])
        self.assertEqual(validity["bottleneck"], "sut_limited")

    def test_validate_run_diagnostic_delivery_below_half(self):
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress",
            "cadence": "capacity",
            "duration_s": 20.0,
            "tags": ["diagnostic"],
        }
        workers = [
            {
                "ok": True,
                "role": "subscriber",
                "msgs_per_s": 15000.0,
                "subscriber_delivered": 300000,
            }
        ]
        loadgen = {
            "engine": "hammer",
            "nominal_rate": 200000.0,
            "effective_offer_msgs_per_s": 200000.0,
            "observed_pub_rate": 199000.0,
            "target_requested": 200000.0,
            "rate_msgs_per_s": 200000.0,
            "interval_ms": 0,
            "paced": True,
            "parsed": {"last_rate": 199000.0, "last_total": 3980000, "median_rate": 199000.0},
        }
        validity = validate_run(
            point, workers, loadgen, [],
            sys_counters={"dropped_delta": 0, "publish_received_delta": 3_980_000},
        )
        self.assertEqual(validity["status"], "inconclusive")
        self.assertIn("delivery_below_half_offer", validity["reasons"])
        self.assertEqual(validity["bottleneck"], "sut_limited")

    def test_ingress_reconciliation_matches_sys_received(self):
        point = {"topology": "subscriber_ingress", "cadence": "capacity"}
        loadgen = {
            "mode": "pub",
            "qos0_pub_counter_double_count": False,
            "parsed": {"last_total": 1_897_882},
        }
        out = reconcile_ingress_loadgen(
            point, loadgen, {"publish_received_delta": 1_897_882}
        )
        self.assertTrue(out["applicable"])
        self.assertIsNone(out["reason"])
        self.assertAlmostEqual(out["ratio"], 1.0)

    def test_ingress_reconciliation_rejects_write_fiction(self):
        point = {"topology": "subscriber_ingress", "cadence": "capacity", "duration_s": 12.0}
        loadgen = {
            "mode": "pub",
            "qos0_pub_counter_double_count": False,
            "parsed": {"last_total": 2_000_000},
        }
        out = reconcile_ingress_loadgen(
            point, loadgen, {"publish_received_delta": 80_000}
        )
        self.assertTrue(out["applicable"])
        self.assertTrue(str(out["reason"]).startswith("loadgen_unconfirmed_by_broker"))

    def test_ingress_reconciliation_fails_closed_without_sys(self):
        point = {"topology": "subscriber_ingress", "cadence": "capacity"}
        loadgen = {
            "mode": "pub",
            "qos0_pub_counter_double_count": False,
            "parsed": {"last_total": 1_897_882},
        }
        for sys_counters in (None, {}, {"error": "probe_failed"}, {"dropped_delta": 0}):
            with self.subTest(sys_counters=sys_counters):
                out = reconcile_ingress_loadgen(point, loadgen, sys_counters)
                self.assertTrue(out["applicable"])
                self.assertEqual(out["reason"], "loadgen_unconfirmed_by_broker")

    def test_ingress_reconciliation_fails_closed_without_loadgen_total(self):
        point = {"topology": "subscriber_ingress", "cadence": "capacity"}
        loadgen = {"mode": "pub", "parsed": {}}
        out = reconcile_ingress_loadgen(
            point, loadgen, {"publish_received_delta": 1_897_882}
        )
        self.assertTrue(out["applicable"])
        self.assertEqual(out["reason"], "loadgen_unconfirmed_by_broker")
        self.assertIsNone(out["loadgen_msgs"])

    def test_ingress_reconciliation_uses_corrected_emitted_msgs(self):
        point = {"topology": "subscriber_ingress", "cadence": "capacity"}
        loadgen = {
            "mode": "pub",
            "qos0_pub_counter_double_count": True,
            "parsed": {"last_total": 1_280_000},
        }
        out = reconcile_ingress_loadgen(
            point, loadgen, {"publish_received_delta": 640_000}
        )
        self.assertTrue(out["applicable"])
        self.assertIsNone(out["reason"])
        self.assertEqual(out["loadgen_msgs"], 640_000)

    def test_sys_counters_delta(self):
        from mqtt_client_bench.sys_probe import sys_counters_delta

        before = {"dropped": 10, "publish_sent": 100, "publish_received": 100}
        after = {"dropped": 25, "publish_sent": 500, "publish_received": 480}
        delta = sys_counters_delta(before, after)
        self.assertEqual(delta["dropped_delta"], 15)
        self.assertEqual(delta["publish_sent_delta"], 400)
        self.assertEqual(delta["publish_received_delta"], 380)


class FairnessGateTests(unittest.TestCase):
    def _pub_worker(self, completed):
        return {"ok": True, "role": "publisher", "completed_success": completed}

    def test_reconciliation_accepts_broker_confirmed_run(self):
        from mqtt_client_bench.harness import reconcile_broker_publishes

        out = reconcile_broker_publishes(
            {"topology": "publisher_only"},
            [self._pub_worker(100_000)],
            {"publish_received_delta": 99_000},
        )
        self.assertTrue(out["applicable"])
        self.assertIsNone(out["reason"])
        self.assertAlmostEqual(out["ratio"], 0.99, places=2)

    def test_reconciliation_rejects_a_broker_it_did_not_have_to_itself(self):
        # The check used to bound only the low side, so a broker that received
        # far more than this run published was accepted in silence. That is the
        # dangerous direction: foreign traffic inflates the counter and masks a
        # genuine drop, and it also invalidates the broker CPU, headroom and
        # drop figures the same run reports. Measured over 714 reconciled runs
        # the honest ratio never left 0.96-1.07.
        from mqtt_client_bench.harness import reconcile_broker_publishes

        out = reconcile_broker_publishes(
            {"topology": "publisher_only"},
            [self._pub_worker(100_000)],
            {"publish_received_delta": 300_000},
        )
        self.assertTrue(out["reason"].startswith("broker_received_above_completed:"), out)

        # And the band still accepts what the $SYS window's extra width produces.
        for received in (96_000, 100_000, 107_000):
            ok = reconcile_broker_publishes(
                {"topology": "publisher_only"},
                [self._pub_worker(100_000)],
                {"publish_received_delta": received},
            )
            self.assertIsNone(ok["reason"], f"received={received}")

    def test_reconciliation_rejects_completions_the_broker_never_saw(self):
        # The QoS0 failure mode: an adapter counts a publish at an in-process
        # queue, so the broker never receives most of them.
        from mqtt_client_bench.harness import reconcile_broker_publishes

        out = reconcile_broker_publishes(
            {"topology": "publisher_only"},
            [self._pub_worker(100_000)],
            {"publish_received_delta": 40_000},
        )
        self.assertTrue(out["reason"].startswith("broker_received_below_completed:"))

    def test_reconciliation_flags_missing_probe(self):
        from mqtt_client_bench.harness import reconcile_broker_publishes

        out = reconcile_broker_publishes(
            {"topology": "publisher_only"}, [self._pub_worker(10)], None
        )
        self.assertEqual(out["reason"], "publisher_completions_unconfirmed")

    def test_reconciliation_skips_topologies_with_other_publishers(self):
        # emqtt-bench publishes here too, so the broker counter mixes sources.
        from mqtt_client_bench.harness import reconcile_broker_publishes

        for topology in ("subscriber_ingress", "duplex_gateway", "broker_ceiling", "application_rtt"):
            out = reconcile_broker_publishes(
                {"topology": topology}, [self._pub_worker(100)], {"publish_received_delta": 0}
            )
            self.assertFalse(out["applicable"], topology)
            self.assertIsNone(out["reason"], topology)

    def test_validate_run_invalidates_unconfirmed_publisher_run(self):
        from mqtt_client_bench.harness import validate_run

        out = validate_run(
            {"topology": "publisher_only", "duration_s": 12.0},
            [self._pub_worker(100_000)],
            None,
            [],
            sys_counters={"publish_received_delta": 10_000},
        )
        self.assertEqual(out["status"], "inconclusive")
        self.assertEqual(out["bottleneck"], "broker_unconfirmed")

    def test_broker_headroom_gate(self):
        from mqtt_client_bench.harness import validate_run

        def run_with_cpu(pct):
            samples = [{"containers": {"mosquitto": {"cpu_pct": pct}}}]
            return validate_run(
                {"topology": "publisher_only", "duration_s": 12.0},
                [self._pub_worker(1000)],
                None,
                samples,
                sys_counters={"publish_received_delta": 1000},
            )

        quiet = run_with_cpu(40.0)
        self.assertEqual(quiet["status"], "valid")
        self.assertEqual(quiet["broker_cpu_max_pct"], 40.0)

        tight = run_with_cpu(75.0)
        self.assertEqual(tight["status"], "inconclusive")
        self.assertEqual(tight["bottleneck"], "broker_limited")
        self.assertTrue(any(r.startswith("broker_headroom_low:") for r in tight["reasons"]))

        saturated = run_with_cpu(95.0)
        self.assertEqual(saturated["bottleneck"], "broker_limited")
        self.assertTrue(any(r.startswith("container_cpu_high:") for r in saturated["reasons"]))

    def test_worker_error_reported_once(self):
        from mqtt_client_bench.harness import validate_run

        out = validate_run(
            {"topology": "publisher_only", "duration_s": 12.0},
            [{"ok": False, "role": "publisher", "error": "warmup_drain_timeout"}],
            None,
            [],
        )
        self.assertEqual(
            [r for r in out["reasons"] if "warmup_drain_timeout" in r],
            ["worker_error:warmup_drain_timeout"],
        )

    def test_host_state_reasons(self):
        from mqtt_client_bench.harness import host_state_reasons

        self.assertEqual(host_state_reasons({"scaling_governor": "performance", "loadavg": [1.0], "cpu_count": 8}), [])
        self.assertIn(
            "cpu_governor_not_performance:powersave",
            host_state_reasons({"scaling_governor": "powersave", "loadavg": [1.0], "cpu_count": 8}),
        )
        busy = host_state_reasons({"scaling_governor": "performance", "loadavg": [20.0], "cpu_count": 8})
        self.assertTrue(any(r.startswith("host_busy_at_start:") for r in busy))
        # Threshold scales with the machine: same load on a big box is fine.
        self.assertEqual(
            host_state_reasons({"scaling_governor": "performance", "loadavg": [20.0], "cpu_count": 64}), []
        )

    def test_unreadable_governor_fails_closed(self):
        # A host with no cpufreq sysfs (container, VM) reports None. Treating
        # that as "fine" let a full campaign measured off the reference host
        # come back `valid`; an unknown frequency policy is not a comparable one.
        from mqtt_client_bench.harness import host_state_reasons

        self.assertEqual(
            host_state_reasons({"scaling_governor": None, "loadavg": [1.0], "cpu_count": 8}),
            ["cpu_governor_unknown"],
        )
        # Key absent entirely is the same claim: no evidence of a pinned clock.
        self.assertEqual(
            host_state_reasons({"loadavg": [1.0], "cpu_count": 8}), ["cpu_governor_unknown"]
        )

    def test_host_state_reasons_reach_the_environment_banner(self):
        # The gate is only useful if the report says why the run is unusable.
        from mqtt_client_bench.report import _reason_kind

        self.assertEqual(_reason_kind("cpu_governor_unknown"), "environment")
        self.assertEqual(_reason_kind("cpu_governor_not_performance:powersave"), "environment")
        self.assertEqual(_reason_kind("host_busy_at_start:20.0"), "environment")

    def test_inflight_window_is_equalised_except_in_the_sweep(self):
        # Clients that expose max_inflight must not run a narrower window than
        # clients that ignore it and are bounded only by `outstanding`.
        from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario

        for name, scenario in SCENARIO_BY_NAME.items():
            for point in expand_scenario(scenario, "standard"):
                if point.get("require_max_inflight"):
                    continue  # pub_qos1_inflight sweeps the window on purpose
                self.assertEqual(
                    point["inflight"],
                    point["outstanding"],
                    f"{name}: in-flight window differs from the outstanding gate",
                )
                self.assertGreaterEqual(point["max_queued"], point["inflight"])

    def test_new_scenarios_are_additive(self):
        # The new coverage lives in `full` so it does not force a re-run of the
        # published `core` campaign.
        from mqtt_client_bench.scenarios import SCENARIO_BY_NAME

        for name in ("sub_delivery_latency", "pubcomp_latency_qos2", "cost_per_message"):
            self.assertIn(name, SCENARIO_BY_NAME)
            self.assertEqual(SCENARIO_BY_NAME[name].suite, "full", name)
            self.assertNotIn("planned", SCENARIO_BY_NAME[name].tags, name)

    def test_session_resume_scenarios_are_executable(self):
        from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario

        for name in ("session_resume_qos1", "reconnect_ordering"):
            scenario = SCENARIO_BY_NAME[name]
            self.assertNotIn("planned", scenario.tags, name)
            self.assertEqual(scenario.suite, "full", name)
            for point in expand_scenario(scenario, "standard"):
                self.assertTrue(point.get("session_persistent"), name)
                self.assertGreater(float(point["outage_s"]), 0.0, name)
                # The outage must fit inside the measure window with room on
                # both sides, otherwise there is no backlog to replay.
                self.assertLess(float(point["outage_s"]), float(point["duration_s"]), name)

    def test_mqttv5_flow_control_pins_receive_maximum(self):
        points = expand_scenario(SCENARIO_BY_NAME["mqttv5_flow_control"], "standard")
        self.assertEqual(len(points), 2)
        rms = sorted(int(p["receive_maximum"]) for p in points)
        self.assertEqual(rms, [10, 100])
        for point in points:
            self.assertEqual(point["protocol"], "MQTTv5")
            self.assertEqual(point["qos_publish"], 1)
            self.assertEqual(point["inflight"], 100)
            self.assertTrue(point.get("require_max_inflight"))
            self.assertNotIn("planned_scenario", unsupported_features(point))

    def test_queue_rejection_keeps_the_pinned_window(self):
        points = expand_scenario(SCENARIO_BY_NAME["queue_rejection"], "standard")
        self.assertEqual(len(points), 1)
        point = points[0]
        self.assertEqual(point["submit_count"], 150)
        self.assertEqual(point["inflight"], 1)
        self.assertEqual(point["max_queued"], 100)
        self.assertTrue(point.get("require_max_inflight"))
        self.assertTrue(point.get("require_max_queued"))
        self.assertEqual(point["expected_accepts"], 100)
        self.assertEqual(point["expected_rejects"], 50)

    def test_retained_bootstrap_defers_subscribe_and_is_non_comparable(self):
        standard = expand_scenario(SCENARIO_BY_NAME["retained_bootstrap"], "standard")
        self.assertEqual([p["retained_count"] for p in standard], [10_000, 100_000])
        for point in standard:
            self.assertTrue(point.get("non_comparable"))
            self.assertTrue(point.get("defer_subscribe"))
            self.assertEqual(point["subscription"], "retained")
        smoke = expand_scenario(SCENARIO_BY_NAME["retained_bootstrap"], "smoke")
        self.assertTrue(all(int(p["retained_count"]) <= 200 for p in smoke))

    def test_queue_accounting_reasons(self):
        from mqtt_client_bench.harness import queue_accounting_reasons

        point = {
            "submit_count": 150,
            "expected_accepts": 100,
            "expected_rejects": 50,
        }
        ok = [{"role": "publisher", "offered": 150, "publish_accepted": 100, "sync_rejected": 50}]
        self.assertEqual(queue_accounting_reasons(point, ok), [])
        never = [{"role": "publisher", "offered": 150, "publish_accepted": 150, "sync_rejected": 0}]
        self.assertIn("queue_never_rejected", queue_accounting_reasons(point, never))
        mismatch = [{"role": "publisher", "offered": 150, "publish_accepted": 80, "sync_rejected": 70}]
        reasons = queue_accounting_reasons(point, mismatch)
        self.assertTrue(any(r.startswith("queue_accepts_mismatch") for r in reasons), reasons)

    def test_validate_run_retained_snapshot_empty(self):
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress",
            "cadence": "capacity",
            "retained_count": 10_000,
            "duration_s": 12.0,
            "tags": ["stress", "functional"],
        }
        empty = validate_run(point, [{"role": "subscriber", "ok": True, "subscriber_delivered": 0}], None, [])
        self.assertIn("retained_snapshot_empty", empty["reasons"])
        ok = validate_run(point, [{"role": "subscriber", "ok": True, "subscriber_delivered": 10_000}], None, [])
        self.assertNotIn("retained_snapshot_empty", ok["reasons"])
        self.assertNotIn("loadgen_emitted_nothing", ok["reasons"])

    def test_retained_packet_roundtrip_shape(self):
        from mqtt_client_bench.broker import encode_remaining_length
        from mqtt_client_bench.retained import (
            encode_connect,
            encode_disconnect,
            encode_publish_qos0_retain,
            retained_payload,
        )
        from mqtt_client_bench.workloads import HEADER_SIZE, decode_header_fields, retained_topics

        topics = retained_topics("abcd1234", 3)
        self.assertEqual(topics[0], "bench/abcd1234/retained/000000")
        self.assertEqual(len(topics), 3)
        payload = retained_payload(b"abcd1234", 7, 256)
        self.assertEqual(len(payload), 256)
        _pub, sequence, _corr, _send = decode_header_fields(payload)
        self.assertEqual(sequence, 7)
        packet = encode_publish_qos0_retain(topics[0], payload)
        self.assertEqual(packet[0], 0x31)
        self.assertGreater(len(packet), HEADER_SIZE)
        self.assertEqual(encode_disconnect(), b"\xe0\x00")
        connect = encode_connect("retseed")
        self.assertEqual(connect[0], 0x10)
        # Remaining length of the CONNECT is encoded, not a placeholder.
        self.assertEqual(encode_remaining_length(0), b"\x00")

    def test_subscription_filters_retained(self):
        from mqtt_client_bench.roles.subscriber import _subscription_filters
        from mqtt_client_bench.workloads import retained_wildcard

        self.assertEqual(
            _subscription_filters({"subscription": "retained"}, "abcd1234"),
            [retained_wildcard("abcd1234")],
        )

    def test_receive_maximum_overlay_text(self):
        from mqtt_client_bench.broker import receive_maximum_overlay_text, write_receive_maximum_overlay
        from mqtt_client_bench.paths import RECEIVE_MAXIMUM_OVERRIDE

        self.assertEqual(receive_maximum_overlay_text(10), "max_inflight_messages 10\n")
        path = write_receive_maximum_overlay(100)
        try:
            self.assertEqual(path, RECEIVE_MAXIMUM_OVERRIDE)
            self.assertEqual(path.read_text(encoding="utf-8"), "max_inflight_messages 100\n")
        finally:
            if path.exists():
                path.unlink()

    def test_submit_burst_counts_rejects(self):
        from mqtt_client_bench.adapters.base import PublishResult
        from mqtt_client_bench.roles.publisher import _run_submit_burst
        from mqtt_client_bench.sampling import CompletionLog, ReservoirSampler

        class FakeAdapter:
            def __init__(self):
                self.calls = 0

            def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
                del topic, payload, qos, retain, properties
                self.calls += 1
                if self.calls <= 100:
                    return PublishResult(rc=0, mid=self.calls)
                return PublishResult(rc=4, mid=None)

        state = {
            "offered": 0,
            "publish_calls": 0,
            "submitted": 0,
            "sync_rejected": 0,
            "publish_accepted": 0,
            "publish_rejected": 0,
            "completed_failed": 0,
            "protocol_failed": 0,
            "inflight_local": 0,
            "seen_mids_inflight": set(),
            "early_acks": {},
            "mid_send_ns": {},
            "lock": threading.Lock(),
            "completions": CompletionLog(16, sampler=ReservoirSampler(8, seed=1)),
            "overflow_success": 0,
            "overflow_failed": 0,
            "overflow_in_window": 0,
            "overflow_during_drain": 0,
            "phase": "measure",
        }
        adapter = FakeAdapter()
        _run_submit_burst(
            adapter,
            state,
            topic="bench/t",
            qos=1,
            body=b"x" * 16,
            corpus=[],
            run_id=b"abcd1234",
            submit_count=150,
            properties_builder=lambda: None,
            force_header=False,
        )
        self.assertEqual(adapter.calls, 150)
        self.assertEqual(state["offered"], 150)
        self.assertEqual(state["publish_accepted"], 100)
        self.assertEqual(state["sync_rejected"], 50)

    def test_memory_guard_trips_and_invalidates_the_run(self):
        # A QoS0 path completing at an in-process queue leaves the outstanding
        # gate with nothing to hold back; one worker was observed at 11.5 GB on
        # 1 MiB payloads. The guard must stop it AND the run must not be
        # published, since its measure window is truncated.
        from mqtt_client_bench.harness import validate_run
        from mqtt_client_bench.telemetry import MemoryGuard, self_rss_kb

        rss = self_rss_kb()
        self.assertIsNotNone(rss, "RSS must be readable for the guard to work")

        # Limit far below current RSS: must trip once enough calls elapse.
        guard = MemoryGuard(limit_mb=0.001, check_every=4)
        self.assertFalse(guard.exceeded(), "sampled 1 in N, so not on the first call")
        tripped = any(guard.exceeded() for _ in range(16))
        self.assertTrue(tripped)
        self.assertIsNotNone(guard.tripped_at_kb)
        self.assertTrue(guard.exceeded(), "stays tripped once it fires")

        # Generous limit: never trips.
        calm = MemoryGuard(limit_mb=1024 * 1024, check_every=1)
        self.assertFalse(any(calm.exceeded() for _ in range(32)))

        out = validate_run(
            {"topology": "publisher_only", "duration_s": 12.0},
            [{"ok": True, "role": "publisher", "completed_success": 10,
              "memory_guard_tripped_kb": 11_466 * 1024}],
            None,
            [],
            sys_counters={"publish_received_delta": 10},
        )
        self.assertEqual(out["status"], "inconclusive")
        self.assertTrue(any(r.startswith("memory_guard_tripped:") for r in out["reasons"]), out["reasons"])

    def test_outage_must_fit_inside_the_measure_window(self):
        # A 3 s outage in a 3 s smoke window leaves no traffic to replay: that is
        # a degenerate measurement, so it must be refused, not published.
        from mqtt_client_bench.harness import unsupported_features

        degenerate = unsupported_features({"outage_s": 3.0, "duration_s": 3.0})
        self.assertTrue(any(r.startswith("outage_exceeds_window") for r in degenerate), degenerate)
        # Half the window or less is fine, so short smoke runs stay usable.
        self.assertEqual(unsupported_features({"outage_s": 3.0, "duration_s": 12.0}), [])
        self.assertEqual(unsupported_features({"outage_s": 1.0, "duration_s": 3.0}), [])

    def test_outage_requires_a_reconnecting_adapter(self):
        from mqtt_client_bench.adapters.base import AdapterCapabilities

        caps = AdapterCapabilities(name="x", reconnect=False)
        self.assertIn("reconnect", caps.missing_for_point({"outage_s": 2.0}))
        self.assertEqual(
            AdapterCapabilities(name="x").missing_for_point({"outage_s": 2.0}), []
        )

    def test_session_present_flag_shapes(self):
        # Adapters report CONNACK flags in three different shapes.
        from mqtt_client_bench.roles.subscriber import _session_present

        class PahoStyle:
            session_present = True

        self.assertTrue(_session_present(PahoStyle()))
        self.assertTrue(_session_present({"session_present": True}))
        self.assertTrue(_session_present({"session present": True}))  # gmqtt / paho v1
        self.assertFalse(_session_present({"session_present": False}))
        self.assertIsNone(_session_present({}), "unreported must stay unknown, not False")

    def test_resume_scenarios_stay_out_of_the_throughput_chart(self):
        # Their throughput is pinned by the cadence; the substance is integrity.
        from mqtt_client_bench.report import _CHART_EXCLUDED_SCENARIOS

        self.assertIn("session_resume_qos1", _CHART_EXCLUDED_SCENARIOS)
        self.assertIn("reconnect_ordering", _CHART_EXCLUDED_SCENARIOS)
        self.assertIn("queue_rejection", _CHART_EXCLUDED_SCENARIOS)
        self.assertIn("retained_bootstrap", _CHART_EXCLUDED_SCENARIOS)
        self.assertIn("mqttv5_flow_control", _CHART_EXCLUDED_SCENARIOS)
        # Fraction-of-own-capacity latency is an intra-client question.
        self.assertIn("puback_latency_qos1", _CHART_EXCLUDED_SCENARIOS)
        self.assertIn("application_rtt_qos1", _CHART_EXCLUDED_SCENARIOS)

    def test_cost_per_message_uses_worker_window_cpu(self):
        # CPU must come from the workers' own measure window. Telemetry samples
        # span warmup and drain too, so using them would divide out-of-window CPU
        # by in-window messages.
        from mqtt_client_bench.harness import cost_per_message

        samples = [
            {"processes": {"w0": {"cpu_ticks": 1000, "rss_kb": 40_000}}},
            {"processes": {"w0": {"cpu_ticks": 9999, "rss_kb": 52_000}}},
        ]
        workers = [
            {"role": "publisher", "completed_in_window": 120_000, "cpu_ns_in_window": 6_000_000_000}
        ]
        out = cost_per_message(workers, samples)
        self.assertEqual(out["messages"], 120_000)
        self.assertEqual(out["cpu_ns_in_window"], 6_000_000_000)
        self.assertEqual(out["rss_peak_kb"], 52_000)
        self.assertAlmostEqual(out["cpu_us_per_message"], 50.0)

    def test_cost_per_message_does_not_double_count_pub_and_sub(self):
        # In a pub+sub topology each logical message is published once and
        # delivered once; summing both would halve the reported cost.
        from mqtt_client_bench.harness import cost_per_message

        workers = [
            {"role": "publisher", "completed_in_window": 1000, "cpu_ns_in_window": 1_000_000_000},
            {"role": "subscriber", "subscriber_delivered": 1000, "cpu_ns_in_window": 1_000_000_000},
        ]
        out = cost_per_message(workers, [])
        self.assertEqual(out["messages"], 1000, "denominator must be logical messages")
        self.assertEqual(out["published"], 1000)
        self.assertEqual(out["delivered"], 1000)
        # Both processes' CPU over 1000 messages: 2 s / 1000 = 2000 us.
        self.assertAlmostEqual(out["cpu_us_per_message"], 2000.0)

    def test_cost_per_message_needs_cpu_and_traffic(self):
        from mqtt_client_bench.harness import cost_per_message

        self.assertIsNone(cost_per_message([], []))
        # No worker CPU recorded (older result files) -> no figure at all.
        self.assertIsNone(cost_per_message([{"role": "publisher", "completed_in_window": 10}], []))
        # CPU but no traffic.
        self.assertIsNone(
            cost_per_message(
                [{"role": "publisher", "completed_in_window": 0, "cpu_ns_in_window": 5}], []
            )
        )

    def test_broker_cpu_judged_on_the_measure_window(self):
        # A warmup ramp spike must not invalidate a run whose measured window was
        # quiet; without the window filter this run would be broker_limited.
        from mqtt_client_bench.harness import validate_run

        samples = [
            {"ts": 100.0, "containers": {"mosquitto": {"cpu_pct": 95.0}}},  # warmup ramp
            {"ts": 200.0, "containers": {"mosquitto": {"cpu_pct": 30.0}}},  # measure
            {"ts": 201.0, "containers": {"mosquitto": {"cpu_pct": 32.0}}},  # measure
            {"ts": 300.0, "containers": {"mosquitto": {"cpu_pct": 91.0}}},  # drain
        ]
        point = {"topology": "publisher_only", "duration_s": 12.0}
        workers = [{"ok": True, "role": "publisher", "completed_success": 1000}]
        sys_counters = {"publish_received_delta": 1000}

        scoped = validate_run(point, workers, None, samples, sys_counters=sys_counters,
                              measure_window=(199.0, 202.0))
        self.assertEqual(scoped["status"], "valid")
        self.assertEqual(scoped["broker_cpu_max_pct"], 32.0)

        unscoped = validate_run(point, workers, None, samples, sys_counters=sys_counters)
        self.assertEqual(unscoped["broker_cpu_max_pct"], 95.0)
        self.assertEqual(unscoped["bottleneck"], "broker_limited")

    def test_measure_window_filter_falls_back_when_empty(self):
        from mqtt_client_bench.harness import _samples_in_window

        samples = [{"ts": 10.0}, {"ts": 11.0}]
        self.assertEqual(_samples_in_window(samples, None), samples)
        self.assertEqual(_samples_in_window(samples, (10.5, 10.9)), samples, "no samples inside -> keep all")
        self.assertEqual(_samples_in_window(samples, (9.0, 10.5)), [{"ts": 10.0}])

    def test_inflight_sweep_still_sweeps(self):
        from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario

        points = expand_scenario(SCENARIO_BY_NAME["pub_qos1_inflight"], "standard")
        self.assertEqual(sorted(p["inflight"] for p in points), [1, 20, 100])
        self.assertTrue(all(p["require_max_inflight"] for p in points))


class MatrixRunnerTests(unittest.TestCase):
    def test_rotation_counterbalances_slot_positions(self):
        # Position within a point is itself a condition (the first client runs on
        # a freshly idle machine), so it must rotate across repetitions.
        clients = ["paho", "gmqtt", "aiomqtt"]
        seen = {c: set() for c in clients}
        for run_idx in range(len(clients)):
            rotation = clients[run_idx % len(clients):] + clients[: run_idx % len(clients)]
            for slot, client in enumerate(rotation):
                seen[client].add(slot)
        for client, slots in seen.items():
            self.assertEqual(slots, {0, 1, 2}, f"{client} never occupied every slot")

    def test_matrix_requires_two_clients(self):
        from mqtt_client_bench.harness import run_matrix

        with self.assertRaises(ValueError):
            run_matrix("pub_qos_sweep_telemetry", ["paho"])

    def test_matrix_checkpoints_mark_partial_scenarios(self):
        # Points are written as they finish so an interruption costs one point,
        # not the whole scenario. A resuming campaign must be able to tell a
        # half-written file from a finished one.
        import tempfile

        from mqtt_client_bench.harness import _write_matrix_documents

        with tempfile.TemporaryDirectory() as tmp:
            clients = ["paho", "gmqtt"]
            per_client = {c: [{"point": {}, "runs": [], "summary": {}}] for c in clients}
            docs = _write_matrix_documents(
                clients, per_client, name="pub_qos_sweep_telemetry", profile="standard",
                runs=3, seed=42, client_paths={}, meta={}, cpusets={},
                output_dir=tmp, points_expected=6,
            )
            self.assertEqual(docs["paho"]["points_expected"], 6)
            self.assertFalse(docs["paho"]["points_complete"], "1 of 6 points is partial")
            self.assertTrue(Path(tmp, "paho-pub_qos_sweep_telemetry.json").exists())

            for c in clients:
                per_client[c] = [{"point": {}, "runs": [], "summary": {}} for _ in range(6)]
            docs = _write_matrix_documents(
                clients, per_client, name="pub_qos_sweep_telemetry", profile="standard",
                runs=3, seed=42, client_paths={}, meta={}, cpusets={},
                output_dir=tmp, points_expected=6,
            )
            self.assertTrue(docs["gmqtt"]["points_complete"])

    def test_resume_check_rejects_partial_scenario_files(self):
        # Mirrors the completeness test in scripts/run_campaign_5h.sh: a file
        # whose runs carry provenance but whose points are incomplete must be
        # re-run, not skipped.
        def complete(blocks, expected):
            runs = [r for b in blocks for r in (b.get("runs") or [])]
            if not runs or not all("started_at" in r for r in runs):
                return False
            return len(blocks) >= expected

        run = {"started_at": "2026-08-06T00:00:00+00:00"}
        self.assertFalse(complete([{"runs": [run]}], 6), "partial must re-run")
        self.assertTrue(complete([{"runs": [run]}] * 6, 6))
        # Pre-fix results carry no provenance, so they never count as done.
        self.assertFalse(complete([{"runs": [{}]}] * 6, 6))

    def test_matrix_documents_keep_scenario_shape(self):
        # The report reads <client>-<scenario>.json; the interleaved runner must
        # emit exactly that shape, only with extra provenance.
        from mqtt_client_bench.harness import _scenario_payload

        doc = _scenario_payload(
            name="pub_qos_sweep_telemetry",
            profile="standard",
            runs=3,
            seed=42,
            client="paho",
            client_path=None,
            meta={"managed_broker": True},
            all_results=[],
            cpusets={"sut": "0,4"},
            extra={"interleaved_with": ["gmqtt"]},
        )
        for key in ("schema_version", "scenario", "profile", "runs", "client", "client_identity", "results"):
            self.assertIn(key, doc)
        self.assertEqual(doc["interleaved_with"], ["gmqtt"])


class TelemetryTests(unittest.TestCase):
    def _fake_cgroup(self, usage_usec: int, mem_bytes: int = 1024):
        import tempfile

        d = tempfile.mkdtemp(prefix="cgroup-")
        Path(d, "cpu.stat").write_text(
            f"usage_usec {usage_usec}\nuser_usec 1\nsystem_usec 2\n", encoding="utf-8"
        )
        Path(d, "memory.current").write_text(f"{mem_bytes}\n", encoding="utf-8")
        return d

    def test_cgroup_readers(self):
        from mqtt_client_bench.telemetry import cgroup_cpu_usec, cgroup_memory_bytes

        d = self._fake_cgroup(123456, mem_bytes=4096)
        self.assertEqual(cgroup_cpu_usec(d), 123456)
        self.assertEqual(cgroup_memory_bytes(d), 4096)
        self.assertIsNone(cgroup_cpu_usec("/nonexistent-cgroup"))
        self.assertIsNone(cgroup_memory_bytes("/nonexistent-cgroup"))

    def test_container_sampler_cpu_percent_matches_docker_convention(self):
        # 100% == one saturated core, same as `docker stats`, so the existing
        # >=85% broker-saturation rule keeps its meaning.
        from mqtt_client_bench.telemetry import ContainerSampler

        sampler = ContainerSampler.__new__(ContainerSampler)
        sampler.name = "fake"
        sampler._last = None
        sampler.cgroup_path = self._fake_cgroup(1_000_000)
        first = sampler.sample()
        self.assertIsNone(first["cpu_pct"], "first sample has no delta yet")
        self.assertEqual(first["source"], "cgroup")

        # Pretend one wall second elapsed and one CPU-second was consumed.
        sampler._last = (sampler._last[0] - 1.0, 1_000_000)
        Path(sampler.cgroup_path, "cpu.stat").write_text(
            "usage_usec 2000000\n", encoding="utf-8"
        )
        second = sampler.sample()
        self.assertAlmostEqual(second["cpu_pct"], 100.0, delta=1.0)

    def test_pin_current_process_rejects_bad_input(self):
        from mqtt_client_bench.telemetry import pin_current_process

        self.assertIsNone(pin_current_process(None))
        self.assertIsNone(pin_current_process(""))
        self.assertIsNone(pin_current_process("not-a-cpu"))

    def test_temporarily_pinned_restores_affinity(self):
        import os

        from mqtt_client_bench.telemetry import allocate_cpuset, temporarily_pinned

        if not hasattr(os, "sched_getaffinity"):
            self.skipTest("sched_getaffinity unavailable")
        before = os.sched_getaffinity(0)
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")
        with temporarily_pinned(cpusets["sut"]):
            inside = os.sched_getaffinity(0)
        self.assertEqual(os.sched_getaffinity(0), before)
        self.assertEqual(inside, {int(c) for c in cpusets["sut"].split(",")})


class HostCalibrationTests(unittest.TestCase):
    """The host profile is what makes a number readable on another machine."""

    def test_harness_cost_probe_returns_a_floor_and_its_spread(self):
        from mqtt_client_bench.hostcal import measure_harness_cost_ns

        # Two passes keep the test fast; the statistic is what is under test,
        # not the value. The floor must be the smallest sample, and the spread
        # must describe the samples rather than being assumed quiet.
        result = measure_harness_cost_ns(passes=2)
        self.assertGreater(result["ns_per_message"], 0.0)
        self.assertLessEqual(result["ns_per_message"], result["median_ns_per_message"])
        self.assertLessEqual(result["median_ns_per_message"], result["max_ns_per_message"])
        self.assertEqual(result["passes"], 2)
        self.assertGreaterEqual(result["spread_pct"], 0.0)

    def test_frequency_policy_names_an_unreadable_governor(self):
        from mqtt_client_bench.hostcal import frequency_policy

        # A container reports None. It must get a name of its own, not inherit
        # the reference host's posture by silence — that silence is exactly what
        # let a container campaign come back valid.
        self.assertEqual(frequency_policy({"scaling_governor": None}), "unpinned")
        self.assertEqual(frequency_policy({"scaling_governor": "performance"}), "performance")
        self.assertEqual(frequency_policy({"scaling_governor": "powersave"}), "governed:powersave")

    def test_fingerprint_tracks_identity_and_ceilings_only(self):
        from mqtt_client_bench.hostcal import host_fingerprint

        base = {
            "host": {
                "cpu_model": "Test CPU",
                "cpu_count": 8,
                "physical_groups": 4,
                "threads_per_group": 2,
                "frequency_policy": "performance",
                "hostname": "a",
            },
            "ceilings": {
                "harness_cost_ns_per_message": 3500.0,
                "broker_paced_ceiling_msgs_per_s": 231000.0,
                "broker_fanout_msgs_per_s": 73000.0,
            },
        }
        fp = host_fingerprint(base)

        # Hostname is identity for a human, not for a measurement.
        renamed = json.loads(json.dumps(base))
        renamed["host"]["hostname"] = "b"
        self.assertEqual(host_fingerprint(renamed), fp)

        # Measurement noise must not churn the digest. Real numbers, from two
        # calibrations of this workstation: an absolute rounding put these on
        # different fingerprints for one machine, which makes every result that
        # references it look stale for no reason.
        for key, first, second in (
            ("harness_cost_ns_per_message", 3325.9, 3283.9),
            ("broker_paced_ceiling_msgs_per_s", 599256.5, 599438.0),
            ("broker_fanout_msgs_per_s", 74819.1, 74828.4),
        ):
            one = json.loads(json.dumps(base)); one["ceilings"][key] = first
            two = json.loads(json.dumps(base)); two["ceilings"][key] = second
            self.assertEqual(host_fingerprint(one), host_fingerprint(two),
                             f"{key} churned the fingerprint on noise")

        # A real change still moves it: 600k and 60k are different machines.
        big = json.loads(json.dumps(base))
        big["ceilings"]["broker_paced_ceiling_msgs_per_s"] = 60000.0
        self.assertNotEqual(host_fingerprint(big), host_fingerprint(base))

        # A different machine, or a real change in what it can do, must.
        for mutation in (
            ("host", "threads_per_group", 1),
            ("host", "frequency_policy", "unpinned"),
            ("ceilings", "broker_paced_ceiling_msgs_per_s", 60000.0),
        ):
            section, key, value = mutation
            changed = json.loads(json.dumps(base))
            changed[section][key] = value
            self.assertNotEqual(host_fingerprint(changed), fp, f"{key} did not move the fingerprint")

    def test_cpu_utilisation_decides_when_loadavg_looks_fine(self):
        from mqtt_client_bench import hostcal

        # The case that motivated the second signal: on this workstation with an
        # editor, a browser and a chat client running, loadavg read 0.92 against
        # a 1.60 gate and passed, while the CPUs were 22% busy. Load average
        # counts queued tasks; interactive load burns cycles in bursts that are
        # rarely queued at the instant it is sampled.
        # cpu_count is pinned: the loadavg threshold scales with it, so on a
        # 2-core CI runner 0.92 would trip the loadavg reason this case is
        # asserting stays silent.
        with patch.object(hostcal, "busy_pct_over", return_value=22.2):
            with patch("os.getloadavg", return_value=(0.92, 0.9, 0.9)):
                with patch("os.cpu_count", return_value=8):
                    state = hostcal.check_host_idle()
        self.assertFalse(state["idle"])
        self.assertTrue(any("cpu=" in r for r in state["reasons"]))
        self.assertFalse(any("loadavg=" in r for r in state["reasons"]))

        # And a genuinely quiet machine passes on both.
        with patch.object(hostcal, "busy_pct_over", return_value=1.5):
            with patch("os.getloadavg", return_value=(0.1, 0.1, 0.1)):
                with patch("os.cpu_count", return_value=8):
                    state = hostcal.check_host_idle()
        self.assertTrue(state["idle"], state["reasons"])

    def test_fanout_ceiling_is_named_as_a_diagnostic_not_an_offer(self):
        from mqtt_client_bench.hostcal import _FINGERPRINT_CEILINGS

        # The core sub_* offer is an over-offer: its job is to make the SUT
        # client the bottleneck. Deriving it from what the broker can sustain
        # would collapse it to the fan-out rate - measured here at 73k against
        # a loadgen that emits 637k - and make the fastest clients neighbours
        # of the constraint instead of its subject.
        self.assertIn("broker_fanout_msgs_per_s", _FINGERPRINT_CEILINGS)
        self.assertNotIn("broker_ceiling_msgs_per_s", _FINGERPRINT_CEILINGS)

    def test_budget_splits_across_the_probes(self):
        from mqtt_client_bench.hostcal import probe_durations

        # One knob, because a host is calibrated once: the operator should set
        # how long they are willing to wait, not four durations.
        plan = probe_durations(300.0)
        self.assertEqual(plan["budget_s"], 300.0)
        self.assertGreaterEqual(plan["harness_passes"], 30)
        # Each share is divided by the runs that probe actually makes, so the
        # total lands near the budget instead of several times over it: the
        # fan-out probe became a grid of 15 runs, and a per-run duration would
        # have spent 450 s of a 300 s budget on it alone.
        total = (
            plan["harness_passes"] * 0.5
            + plan["sweep_runs"] * plan["sweep_step_s"]
            + plan["fanout_runs"] * plan["fanout_s"]
        )
        self.assertLessEqual(total, plan["budget_s"] * 1.1)
        self.assertGreaterEqual(total, plan["budget_s"] * 0.7)
        # The floor keeps a mistyped budget from producing a meaningless probe.
        tiny = probe_durations(1.0)
        self.assertGreaterEqual(tiny["budget_s"], 30.0)
        self.assertGreaterEqual(tiny["harness_passes"], 5)

    def test_only_positive_evidence_of_another_host_excludes_a_result(self):
        from mqtt_client_bench.hostcal import matches_reference, result_host_key

        ref = {
            "host_fingerprint": "abc123",
            "host": {"hostname": "yoch-HP", "cpu_model": "i7-3770"},
        }

        # Fingerprinted, same machine.
        mine = result_host_key({"host_profile": {"fingerprint": "abc123"}})
        self.assertTrue(matches_reference(mine, ref))

        # Fingerprinted, another machine: the case the mechanism exists for.
        other = result_host_key({"host_profile": {"fingerprint": "def456"}})
        self.assertFalse(matches_reference(other, ref))

        # Legacy but identifiable as the reference host: the committed corpus.
        legacy = result_host_key(
            {"environment": {"hostname": "yoch-HP", "cpu_model": "i7-3770"}}
        )
        self.assertTrue(legacy["legacy"])
        self.assertTrue(matches_reference(legacy, ref))

        # Legacy and identifiable as something else.
        foreign = result_host_key(
            {"environment": {"hostname": "cursor", "cpu_model": "Xeon"}}
        )
        self.assertFalse(matches_reference(foreign, ref))

        # No evidence either way. Dropping these would empty the site for any
        # corpus predating the environment block, which is a worse failure than
        # publishing a document whose provenance is merely unrecorded.
        blank = result_host_key({})
        self.assertTrue(matches_reference(blank, ref))

        # And with no reference profile committed, nothing is filtered at all.
        self.assertTrue(matches_reference(foreign, None))

    def test_report_counts_what_it_skipped_by_host(self):
        import json as _json
        import tempfile
        from pathlib import Path as _Path

        from mqtt_client_bench.report import load_results_with_skips

        def doc(hostname, cpu):
            return {
                "schema_version": 1,
                "scenario": "pub_qos_sweep_telemetry",
                "client": "paho",
                "profile": "standard",
                "runs": 1,
                "environment": {"hostname": hostname, "cpu_model": cpu},
                "results": [],
            }

        ref = {"host_fingerprint": "abc", "host": {"hostname": "keep", "cpu_model": "cpu-a"}}
        with tempfile.TemporaryDirectory() as tmp:
            root = _Path(tmp)
            (root / "a.json").write_text(_json.dumps(doc("keep", "cpu-a")))
            (root / "b.json").write_text(_json.dumps(doc("elsewhere", "cpu-b")))
            (root / "c.json").write_text(_json.dumps(doc("elsewhere", "cpu-b")))
            docs, skipped = load_results_with_skips(root, reference=ref)
        self.assertEqual(len(docs), 1)
        # Counted and named: a reader who expected a document and cannot find
        # it needs to know which machine it came from.
        self.assertEqual(skipped, {"elsewhere": 2})

    def test_declared_unpinned_clock_is_the_only_thing_that_excuses_it(self):
        from mqtt_client_bench.harness import host_state_reasons

        xeon = {"scaling_governor": None, "loadavg": [0.1], "cpu_count": 4}

        # No profile: still fails closed. Absent evidence is not evidence of a
        # pinned clock, and this is the case that let a container campaign come
        # back valid in the first place.
        self.assertEqual(host_state_reasons(xeon), ["cpu_governor_unknown"])

        # A written, versioned, fingerprinted declaration excuses it.
        self.assertEqual(host_state_reasons(xeon, declared_policy="unpinned"), [])

        # A profile claiming a pinned clock on a machine that has none does not.
        self.assertEqual(
            host_state_reasons(xeon, declared_policy="performance"),
            ["cpu_governor_unknown"],
        )

        # And `unpinned` excuses an *unreadable* governor, never a readable one
        # set to something else: those are different claims.
        governed = {"scaling_governor": "powersave", "loadavg": [0.1], "cpu_count": 4}
        self.assertEqual(
            host_state_reasons(governed, declared_policy="unpinned"),
            ["cpu_governor_not_performance:powersave"],
        )

    def test_python_and_broker_are_part_of_the_fingerprint(self):
        from mqtt_client_bench.hostcal import host_fingerprint

        # The harness cost is an interpreter cost before it is anything else and
        # the fan-out ceiling is a property of one Mosquitto build, so a change
        # in either makes the recorded ceilings mean something different.
        base = {
            "host": {
                "cpu_model": "Test CPU", "cpu_count": 8, "physical_groups": 4,
                "threads_per_group": 2, "frequency_policy": "performance",
                "python": "3.12.3", "kernel": "Linux-6.8.0-137",
            },
            "broker": {"image": "m:2.1.2", "image_digest": "sha256:aaa", "config_hash": "c1"},
            "ceilings": {"harness_cost_ns_per_message": 3300.0},
        }
        fp = host_fingerprint(base)
        for section, key, value in (
            ("host", "python", "3.13.0"),
            ("broker", "image_digest", "sha256:bbb"),
            ("broker", "config_hash", "c2"),
        ):
            changed = json.loads(json.dumps(base))
            changed[section][key] = value
            self.assertNotEqual(host_fingerprint(changed), fp, f"{key} did not move it")

        # The kernel stays out: a point release moves it without moving any
        # measurement, and a digest that churns for nothing gets ignored.
        same = json.loads(json.dumps(base))
        same["host"]["kernel"] = "Linux-6.8.0-999"
        self.assertEqual(host_fingerprint(same), fp)

    def test_only_the_reference_host_writes_into_the_published_corpus(self):
        from mqtt_client_bench.hostcal import results_dir_for

        # Campaign files are named <client>-<scenario>.json, so a runner writing
        # to `results/` would overwrite the published corpus file by file. This
        # is why the default depends on the host rather than on a flag someone
        # has to remember on a machine they rarely log into.
        reference = {"role": "reference", "host": {"hostname": "yoch-HP"}, "host_fingerprint": "abc"}
        self.assertEqual(results_dir_for(reference), "results")

        runner = {"role": "runner", "host": {"hostname": "xeon"}, "host_fingerprint": "def456"}
        self.assertEqual(results_dir_for(runner), "results/xeon-def456")

        # An uncalibrated host has no business there either, and cannot even say
        # which machine it is.
        self.assertEqual(results_dir_for(None), "results/uncalibrated")

        # A hostname is not a path component until it is made one.
        odd = {"role": "runner", "host": {"hostname": "host/../etc"}, "host_fingerprint": "f"}
        self.assertEqual(results_dir_for(odd), "results/host----etc-f")

    def test_a_runner_directory_is_invisible_to_the_default_build(self):
        import json as _json
        import tempfile
        from pathlib import Path as _Path

        from mqtt_client_bench.report import load_results_with_skips

        def doc(hostname):
            return {
                "schema_version": 1, "scenario": "pub_qos_sweep_telemetry",
                "client": "paho", "profile": "standard", "runs": 1,
                "environment": {"hostname": hostname, "cpu_model": "cpu-a"},
                "results": [],
            }

        with tempfile.TemporaryDirectory() as tmp:
            root = _Path(tmp)
            (root / "a.json").write_text(_json.dumps(doc("keep")))
            runner = root / "xeon-def456"
            runner.mkdir()
            (runner / "a.json").write_text(_json.dumps(doc("xeon")))

            ref = {"host_fingerprint": "abc", "host": {"hostname": "keep", "cpu_model": "cpu-a"}}
            docs, skipped = load_results_with_skips(root, reference=ref)
            # Not skipped-and-counted: never looked at. The build globs the top
            # level only, so a runner campaign cannot perturb the published site
            # even before the host filter gets a say.
            self.assertEqual(len(docs), 1)
            self.assertEqual(skipped, {})

            # And it reads on its own with the filter turned off.
            own, _ = load_results_with_skips(runner, reference=None)
            self.assertEqual(len(own), 1)

    def test_a_zero_ceiling_is_a_failure_not_a_measurement(self):
        from mqtt_client_bench import hostcal

        # Measured for real: with no broker listening, every hammer step
        # returned 0.0 and the profile was still written, marked idle-verified
        # and signed. It would have become the committed reference whose
        # ceilings drive every campaign offer, and its harness cost - which
        # needs no broker - would have made it look half right.
        idle = {"idle": True, "reasons": [], "loadavg_before": 0.1, "busy_pct": 1.0}
        with patch.object(hostcal, "check_host_idle", return_value=idle):
            with patch.object(hostcal, "measure_harness_cost_ns",
                              return_value={"ns_per_message": 3300.0, "spread_pct": 1.0,
                                            "median_ns_per_message": 3310.0,
                                            "max_ns_per_message": 3320.0, "passes": 5}):
                with patch.object(hostcal, "measure_broker_paced_ceiling",
                                  return_value={"msgs_per_s": 0.0, "steps": []}):
                    with patch.object(hostcal, "measure_broker_fanout_ceiling",
                                      return_value={"msgs_per_s": 0.0, "subscriber_msgs_per_s": None}):
                        with patch("mqtt_client_bench.broker.broker_running", return_value=True):
                            with self.assertRaises(hostcal.CalibrationFailed) as caught:
                                hostcal.calibrate_host(budget_s=30.0)
        self.assertIn("broker_paced_ceiling_msgs_per_s", str(caught.exception))

    def test_paced_sweep_stops_at_the_knee(self):
        from mqtt_client_bench import hostcal

        # Below the knee a paced hammer delivers what it was asked for; above it
        # delivery flattens at the broker's rate. Measured on the reference
        # host: 100k -> 99985, 200k -> 199099, 400k -> 230023.
        def _fake(mode, *, clients, rate, **kw):
            return {"msgs_per_s": min(float(rate), 620000.0)}

        with patch.object(hostcal, "_run_hammer", side_effect=_fake):
            result = hostcal.measure_broker_paced_ceiling(cpuset=None, seconds=1)

        # 600k held; 700k delivered 620k, which is 88% of what was asked and so
        # below the 97% bar. The ceiling is the last *delivered* offer, and the
        # walk stops there rather than reading the plateau as a higher one.
        self.assertEqual(result["msgs_per_s"], 600000.0)
        self.assertEqual([s["offer_msgs_per_s"] for s in result["steps"]],
                         [100000, 200000, 300000, 400000, 500000, 600000, 700000])
        self.assertEqual([s["held"] for s in result["steps"]],
                         [True, True, True, True, True, True, False])

        # An offer sits below the knee, not on it: at the knee it lands above on
        # about half the runs, and above it delivery flattens at whatever the
        # broker does rather than at what was asked for.
        self.assertLess(result["recommended_offer_msgs_per_s"], result["msgs_per_s"])
        self.assertEqual(result["recommended_offer_msgs_per_s"], 510000.0)

    def test_a_sweep_that_delivers_nothing_fails(self):
        from mqtt_client_bench import hostcal

        # The unpaced probe once recorded 0.0 as a ceiling on a host with no
        # broker listening. Zero is not a slow host.
        with patch.object(hostcal, "_run_hammer", return_value={"msgs_per_s": 0.0}):
            with self.assertRaises(hostcal.CalibrationFailed):
                hostcal.measure_broker_paced_ceiling(cpuset=None, seconds=1)

    def test_calibration_refuses_a_busy_machine(self):
        from mqtt_client_bench import hostcal

        # A contended run is re-run; a contended calibration is committed and
        # then governs every campaign after it. So this one refuses rather
        # than annotating.
        busy = {"idle": False, "reasons": ["host_busy:loadavg=9.0 over 1.6"], "loadavg_before": 9.0}
        with patch.object(hostcal, "check_host_idle", return_value=busy):
            with self.assertRaises(hostcal.HostNotIdle):
                hostcal.calibrate_host(skip_ceilings=True, budget_s=30.0)

    def test_host_profile_must_match_the_machine_it_runs_on(self):
        from mqtt_client_bench.harness import _validate_host_profile
        from mqtt_client_bench.hostcal import host_identity

        _validate_host_profile({"host": host_identity(), "role": "runner"})

        foreign = {"host": dict(host_identity(), cpu_model="Some Other CPU"), "role": "runner"}
        with self.assertRaises(ValueError):
            _validate_host_profile(foreign)

        # The reference host is the one that gets published, so its profile
        # carries the stricter requirement.
        unverified = {"host": host_identity(), "role": "reference", "idle": {"verified": False}}
        with self.assertRaises(ValueError):
            _validate_host_profile(unverified)


class ReceiveMaximumOverlayTests(unittest.TestCase):
    def test_an_overlay_that_did_not_take_fails_the_point_closed(self):
        from mqtt_client_bench import harness

        # Mosquitto does not apply max_inflight_messages on SIGHUP. Measured on
        # 2.1.2: the advertised value moved on the first application after a
        # container start and never again - apply(10) reached the wire,
        # apply(100) and the restore both stayed at 10. Two points asking for 10
        # and 100 therefore measured the same broker twice and came back
        # `valid`, which is worse than coming back with nothing.
        point = {"receive_maximum": 100, "topology": "publisher_only"}
        overlay = {"receive_maximum": 100, "advertised": 10, "applied": False, "overlay": "x"}
        with patch.object(harness, "apply_receive_maximum", return_value=overlay):
            with patch.object(harness, "restore_receive_maximum"):
                result = harness.run_point(
                    point, client="paho", host="127.0.0.1", port=1, tls_port=2,
                    profile="smoke", work_dir=Path("/tmp"), cpusets={},
                )
        self.assertEqual(result["status"], "inconclusive")
        self.assertTrue(
            any(r.startswith("receive_maximum_not_applied:") for r in result["reasons"]),
            result["reasons"],
        )
        # And the reason carries both numbers, so a reader does not have to
        # reproduce the run to learn what the broker actually advertised.
        self.assertIn("asked=100", result["reasons"][0])
        self.assertIn("advertised=10", result["reasons"][0])


class HostCappedOfferTests(unittest.TestCase):
    """The host profile caps the ingress offer; it does not set it."""

    @staticmethod
    def _profile(recommended):
        return {"ceilings": {"recommended_offer_msgs_per_s": recommended}}

    def test_a_capable_host_keeps_the_reference_offer(self):
        from mqtt_client_bench.harness import (
            DEFAULT_INGRESS_OFFER_MSGS_PER_S, resolve_ingress_offer,
        )

        # This workstation: min(200k, 509k) = 200k, so nothing changes here -
        # which is the point. Measured on the two fastest clients, raising the
        # offer to 594k moved delivery by under 2% and pushed every mqttium run
        # to inconclusive, so a higher offer buys nothing and costs headroom.
        point = {}
        offer = resolve_ingress_offer(point, 32, self._profile(509368.0))
        self.assertEqual(offer, DEFAULT_INGRESS_OFFER_MSGS_PER_S)
        self.assertNotIn("ingress_offer_capped_by_host", point)

    def test_a_weaker_host_gets_what_it_can_emit(self):
        from mqtt_client_bench.harness import resolve_ingress_offer

        # The failure this removes: a smaller machine recording an offer of
        # 200k it never produced, which is what the withdrawn container
        # campaign did.
        point = {}
        offer = resolve_ingress_offer(point, 32, self._profile(60000.0))
        self.assertEqual(offer, 60000.0)
        self.assertEqual(point["ingress_offer_capped_by_host"], 60000.0)

    def test_no_profile_falls_back_to_the_constant(self):
        from mqtt_client_bench.harness import (
            DEFAULT_INGRESS_OFFER_MSGS_PER_S, resolve_ingress_offer,
        )

        self.assertEqual(
            resolve_ingress_offer({}, 32, None), DEFAULT_INGRESS_OFFER_MSGS_PER_S
        )

    def test_an_explicit_target_and_the_override_both_win(self):
        from mqtt_client_bench.harness import resolve_ingress_offer

        # Ceiling-grid points pin their own offer and must not be capped into
        # a different grid.
        self.assertEqual(
            resolve_ingress_offer({"ingress_target_msgs_per_s": 128000}, 128,
                                  self._profile(60000.0)),
            128000.0,
        )
        # The diagnostic override sits above both, and marks the point.
        with patch.dict(os.environ, {"MQTT_BENCH_INGRESS_OFFER": "300000"}, clear=False):
            point = {}
            self.assertEqual(resolve_ingress_offer(point, 32, self._profile(60000.0)),
                             300000.0)
            self.assertTrue(point["non_comparable"])

    def test_the_hammer_clamp_takes_the_host_ceiling(self):
        from mqtt_client_bench.loadgen import HAMMER_MAX_RATE_MSGS_PER_S, clamp_hammer_rate

        # Without a profile, the constant: what this workstation sustains.
        self.assertEqual(clamp_hammer_rate(500000), HAMMER_MAX_RATE_MSGS_PER_S)
        # With one, the machine's own number.
        self.assertEqual(clamp_hammer_rate(500000, 90000.0), 90000)
        # A ceiling above the constant does not raise it.
        self.assertEqual(clamp_hammer_rate(500000, 900000.0), HAMMER_MAX_RATE_MSGS_PER_S)


class BrokerFanoutLimitTests(unittest.TestCase):
    """A subscribe point at the host's fan-out ceiling measures the broker."""

    @staticmethod
    def _run(delivered, *, subscribers=1, ceiling=76000.0):
        from mqtt_client_bench.harness import validate_run

        point = {
            "topology": "subscriber_ingress", "subscribers": subscribers,
            "duration_s": 12.0, "qos_publish": 0,
        }
        workers = [{"role": "subscriber", "ok": True, "msgs_per_s": delivered,
                    "subscriber_delivered": int(delivered * 12), "duration_s": 12.0}]
        offer = 200000.0
        loadgen = {
            "mode": "pub", "effective_offer_msgs_per_s": offer,
            "observed_pub_rate": offer, "emitted_msgs": int(offer * 12),
            "parsed": {"last_total": int(offer * 12)}, "paced": True,
        }
        sys_counters = {"publish_received_delta": int(offer * 12), "dropped_delta": 0}
        return validate_run(point, workers, loadgen, [], sys_counters=sys_counters,
                            fanout_ceiling_msgs_per_s=ceiling)

    def test_delivery_at_the_fanout_ceiling_is_attributed_to_the_broker(self):
        # mqttium came back at 76,796 msgs/s against a measured one-subscriber
        # fan-out ceiling of 75-77k. A Python client landing exactly on the rate
        # a C subscriber sustains is the broker's number wearing the client's
        # name, and the reader had no way to tell.
        v = self._run(76796.0)
        self.assertEqual(v["bottleneck"], "broker_limited")
        self.assertTrue(any(r.startswith("broker_fanout_limited:") for r in v["reasons"]),
                        v["reasons"])
        # The delivery is real and core subscribe does not invalidate on a
        # pegged broker, so the run keeps its number.
        self.assertEqual(v["status"], "valid")

    def test_a_client_below_the_ceiling_keeps_its_own_score(self):
        # paho at 25k against the same ceiling is measuring paho.
        v = self._run(25304.0)
        self.assertNotEqual(v["bottleneck"], "broker_limited")
        self.assertFalse(any(r.startswith("broker_fanout_limited") for r in v["reasons"]))

    def test_the_mark_does_not_generalise_past_one_subscriber(self):
        # The broker reads once and writes once per subscriber, so a ceiling
        # measured at one says nothing about fanout_scaling at 8 or 32.
        v = self._run(76796.0, subscribers=8)
        self.assertFalse(any(r.startswith("broker_fanout_limited") for r in v["reasons"]))

    def test_no_profile_means_no_mark(self):
        # A host nobody calibrated has no ceiling to compare against, and
        # inventing one would be worse than staying quiet.
        v = self._run(76796.0, ceiling=None)
        self.assertFalse(any(r.startswith("broker_fanout_limited") for r in v["reasons"]))


class QueueRejectionPathTests(unittest.TestCase):
    """Which path answers the queue-rejection question, and why."""

    def test_sync_on_loop_clients_keep_their_native_path_for_a_burst(self):
        from mqtt_client_bench.adapters.registry import get_async_adapter_class

        # A burst used to force every client onto the sync facade, on the
        # grounds that an awaited native path would drain the queue between
        # submissions. That holds for await-only libraries and inverts for the
        # ones that admit a publish on the loop: for those the facade is the
        # expensive path, because its engine is on another thread and admission
        # costs a round trip per call.
        on_loop = {"mqttium", "gmqtt"}
        awaited = {"aiomqtt", "aiomqtt3", "amqtt", "zmqtt"}
        for name in on_loop:
            self.assertTrue(
                get_async_adapter_class(name).capabilities().publish_sync_on_loop,
                f"{name} should submit on the loop",
            )
        for name in awaited:
            self.assertFalse(
                get_async_adapter_class(name).capabilities().publish_sync_on_loop,
                f"{name} awaits, so a burst must stay on the facade",
            )

    def test_burst_on_loop_scores_admission_and_never_yields(self):
        import asyncio as _asyncio
        import inspect

        from mqtt_client_bench.roles import publisher

        # The streaming loop yields after a refusal so the write pump can
        # drain. Here draining is the thing being measured against, so a yield
        # would turn refusals back into accepts.
        source = inspect.getsource(publisher._run_submit_burst_on_loop)
        body = source.split('"""', 2)[-1]
        self.assertNotIn("await ", body, "the burst must not yield to the loop")

        class _BoundedQueue:
            """Admits `bound` publishes, then refuses like FlowControlError."""

            def __init__(self, bound):
                self.bound = bound
                self.admitted = 0
                self.on_publish = None

            def publish_nowait(self, topic, payload, qos, retain, props):
                if self.admitted >= self.bound:
                    return None
                self.admitted += 1
                return self.admitted

        adapter = _BoundedQueue(100)
        state = publisher.new_publisher_state()
        state["phase"] = "measure"
        _asyncio.new_event_loop().run_until_complete(
            publisher._run_submit_burst_on_loop(
                adapter, state, topic="t", qos=1, body=b"x" * 8, corpus=[],
                run_id=b"testrun1", submit_count=150,
                properties_builder=lambda: None, track_sequences=False,
            )
        )
        self.assertEqual(state["publish_accepted"], 100)
        self.assertEqual(state["publish_rejected"], 50)
        self.assertEqual(state["offered"], 150)


class SchemaTests(unittest.TestCase):
    def test_schema_file_exists_and_parses(self):
        import json

        schema_path = SRC / "mqtt_client_bench" / "result.schema.json"
        data = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertEqual(data["properties"]["schema_version"]["const"], 1)
        self.assertIn("client", data["properties"])
        self.assertIn("awscrt", data["properties"]["client"]["enum"])
        self.assertIn("mqttium", data["properties"]["client"]["enum"])
        self.assertIn("mqttium-compat", data["properties"]["client"]["enum"])
        self.assertIn("yoch/mqtt-python-client-bench", data["$id"])


class ReportTests(unittest.TestCase):
    def test_qos0_latency_boundary_flagged_on_detail_page(self):
        """QoS0 completion latency from a non-socket boundary must be marked.

        A 'queue' boundary times admission to the client's write path, not the
        socket write; showing those percentiles bare invites a cross-contract
        latency comparison the methodology page only warns about globally.
        """
        from mqtt_client_bench.report import classify_payload, render_detail

        def sample(qos0_boundary, qos):
            return {
                "schema_version": 1,
                "scenario": "pub_qos_sweep_telemetry",
                "profile": "standard",
                "runs": 1,
                "client": "mqttium",
                "client_identity": {"client": "mqttium", "qos0_boundary": qos0_boundary},
                "results": [
                    {
                        "point": {"payload": "telemetry256", "qos_publish": qos},
                        "runs": [
                            {
                                "status": "valid",
                                "primary_msgs_per_s": 100000.0,
                                "workers": [
                                    {
                                        "role": "publisher",
                                        "ok": True,
                                        "latency_summary": {
                                            "p50_ms": 0.1,
                                            "p95_ms": 0.2,
                                            "p99_ms": 0.3,
                                            "p99_published": True,
                                        },
                                    }
                                ],
                            }
                        ],
                        "summary": {"median": 100000.0, "min": 100000.0, "max": 100000.0},
                    }
                ],
            }

        doc = classify_payload(sample("queue", 0), "mqttium-x.json")
        self.assertEqual(doc.points[0].latency_boundary, "queue")
        html = render_detail(doc, "now")
        self.assertIn("†", html)
        self.assertIn("qos0_boundary", html)

        # Socket boundary (paho contract) and QoS>=1 points stay unmarked:
        # PUBACK/PUBCOMP are the same boundary for every client.
        self.assertIsNone(classify_payload(sample("socket", 0), "x.json").points[0].latency_boundary)
        self.assertIsNone(classify_payload(sample(None, 0), "x.json").points[0].latency_boundary)
        self.assertIsNone(classify_payload(sample("queue", 1), "x.json").points[0].latency_boundary)

    def test_build_site_from_scenario_json(self):
        # reference=None: this case is about rendering, and its fixture is a
        # synthetic "bench-host". Left to the default it would depend on which
        # host profile happens to be committed, which is not what it tests.
        import json
        import tempfile

        from mqtt_client_bench.report import build_site, load_results

        sample = {
            "schema_version": 1,
            "scenario": "pub_qos_sweep_telemetry",
            "profile": "smoke",
            "runs": 1,
            "seed": 42,
            "client": "paho",
            "client_path": None,
            "client_identity": {"name": "paho", "version": "2.1.0"},
            "broker": {"host": "127.0.0.1", "port": 11883},
            "environment": {"hostname": "bench-host", "python": "3.12"},
            "results": [
                {
                    "point": {"payload": "telemetry256", "qos_publish": 0, "non_comparable": True},
                    "runs": [
                        {
                            "schema_version": 1,
                            "run_id": "abcd1234",
                            "status": "valid",
                            "primary_msgs_per_s": 12000.5,
                            "non_comparable": True,
                            "workers": [
                                {
                                    "role": "publisher",
                                    "ok": True,
                                    "latency_summary": {
                                        "p50_ms": 0.4,
                                        "p95_ms": 0.8,
                                        "p99_ms": 1.2,
                                        "p99_published": True,
                                    },
                                    "integrity": {
                                        "expected": 10,
                                        "received": 10,
                                        "unique": 10,
                                        "missing": 0,
                                        "duplicates": 0,
                                        "out_of_order": 0,
                                        "unexpected": 0,
                                    },
                                }
                            ],
                        }
                    ],
                    "summary": {
                        "n": 0,
                        "median": None,
                        "mad": None,
                        "min": None,
                        "max": None,
                        "mean": None,
                        "inconclusive_n": 0,
                        "total_runs": 1,
                    },
                }
            ],
        }
        suite = {
            "suite": "core",
            "estimate": {"scenarios": 1, "points": 1},
            "scenarios": [sample],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            results = root / "results"
            site = root / "site"
            results.mkdir()
            (results / "paho-pub-qos.json").write_text(json.dumps(sample), encoding="utf-8")
            (results / "suite-core.json").write_text(json.dumps(suite), encoding="utf-8")
            # Ephemeral local artefacts must not be loaded into the site.
            (results / "_scratch.json").write_text(json.dumps(sample), encoding="utf-8")
            (results / "probe-smoke.json").write_text(json.dumps(sample), encoding="utf-8")
            docs = load_results(results, reference=None)
            self.assertEqual(len(docs), 2)
            kinds = {d.kind for d in docs}
            self.assertEqual(kinds, {"scenario", "suite"})
            summary = build_site(results, site, reference=None)
            self.assertEqual(summary["results"], 2)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("pub_qos_sweep_telemetry", index)
            self.assertIn("paho", index)
            scenario_html = next(
                p.read_text(encoding="utf-8")
                for p in (site / "runs").glob("*.html")
                if "suite" not in p.name
            )
            self.assertIn("non-comparable", scenario_html)
            suite_html = next(p for p in (site / "runs").glob("*.html") if "suite" in p.name).read_text(
                encoding="utf-8"
            )
            self.assertIn('href="paho-pub-qos.html"', suite_html)
            self.assertIn("pub_qos_sweep_telemetry", suite_html)
            self.assertFalse(any(site.rglob("*.json")))

    def test_performance_matrix_on_index(self):
        import json
        import tempfile

        from mqtt_client_bench.report import build_site

        def sample(client: str, scenario: str, rate: float) -> dict:
            return {
                "schema_version": 1,
                "scenario": scenario,
                "profile": "standard",
                "runs": 1,
                "client": client,
                "results": [
                    {
                        "point": {"qos_publish": 1},
                        "runs": [
                            {
                                "status": "valid",
                                "primary_msgs_per_s": rate,
                                "non_comparable": False,
                                "workers": [{"role": "publisher", "ok": True}],
                            }
                        ],
                        "summary": {"n": 1, "median": rate, "total_runs": 1},
                    }
                ],
            }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            results = root / "results"
            site = root / "site"
            results.mkdir()
            (results / "paho-pub.json").write_text(
                json.dumps(sample("paho", "pub_qos_sweep_telemetry", 7000.0)), encoding="utf-8"
            )
            (results / "gmqtt-pub.json").write_text(
                json.dumps(sample("gmqtt", "pub_qos_sweep_telemetry", 8000.0)), encoding="utf-8"
            )
            # Same peer group as gmqtt (both asyncio_bridged), so "best" is a
            # meaningful comparison; paho sits in the sync group on its own.
            (results / "aiomqtt-pub.json").write_text(
                json.dumps(sample("aiomqtt", "pub_qos_sweep_telemetry", 5000.0)), encoding="utf-8"
            )
            (results / "paho-duplex.json").write_text(
                json.dumps(sample("paho", "duplex_gateway", 200.0)), encoding="utf-8"
            )
            (results / "paho-e2e.json").write_text(
                json.dumps(sample("paho", "e2e_integrity", 1000.0)), encoding="utf-8"
            )
            (results / "paho-callback.json").write_text(
                json.dumps(sample("paho", "sub_callback_matching", 12000.0)), encoding="utf-8"
            )
            (results / "paho-rl.json").write_text(
                json.dumps(sample("paho", "remaining_length_boundaries", 6500.0)), encoding="utf-8"
            )
            (results / "compare-paho-gmqtt.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "scenario": "pub_qos_sweep_telemetry",
                        "profile": "standard",
                        "baseline_client": "paho",
                        "candidate_client": "gmqtt",
                        "order": "ABBA",
                        "verdict": {"verdict": "inconclusive"},
                        "points": [],
                    }
                ),
                encoding="utf-8",
            )
            build_site(results, site, reference=None)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("Performance matrix", index)
            self.assertIn('class="matrix"', index)
            self.assertIn("8,000.0", index)
            # "Best" is scoped to a peer group: gmqtt beats aiomqtt among the
            # bridged clients, and paho — alone in the sync group — is never
            # crowned just for having no one to compare against.
            matrix_only = index[index.index('class="matrix"') : index.index("All results")]
            best_cells = re.findall(
                r'<td class="[^"]*\bbest\b[^"]*"[^>]*>([\d,.]+)</td>', matrix_only
            )
            self.assertEqual(best_cells, ["8,000.0"])
            # Alone in its group, paho is shown but explicitly not ranked, and the
            # group boundary is drawn on the cells so a row can be read at all.
            solo_cells = re.findall(
                r'<td class="[^"]*\bsolo\b[^"]*"[^>]*>([\d,.]+)</td>', matrix_only
            )
            self.assertIn("7,000.0", solo_cells)
            self.assertIn("alone in the sync group", matrix_only)
            self.assertIn("group-start", matrix_only)
            # Charts are server-rendered SVG: no CDN and no canvas. The site
            # does ship a small same-origin app.js, but it only adds hover,
            # sorting and the theme switch on top of markup that is already
            # complete, so nothing it does can be asserted from the HTML.
            self.assertIn('<svg class="chart-svg"', index)
            self.assertNotIn("<canvas", index)
            # Rate-capped / niche checks stay in the matrix (at the end) but leave the chart.
            self.assertIn("duplex_gateway", index)
            self.assertIn("e2e_integrity", index)
            self.assertIn("sub_callback_matching", index)
            self.assertIn("remaining_length_boundaries", index)
            charts = [
                part.split("</svg>", 1)[0]
                for part in index.split('<svg class="chart-svg"')[1:]
            ]
            self.assertGreater(len(charts), 1, "overview is split per peer group")
            for chart in charts:
                for excluded in (
                    "duplex_gateway",
                    "e2e_integrity",
                    "sub_callback_matching",
                    "remaining_length_boundaries",
                ):
                    self.assertNotIn(excluded, chart)
            # Every client still appears, but never beside one from another peer
            # group: a chart that mixes them invites the comparison the matrix
            # refuses to make.
            for client in ("gmqtt", "paho"):
                self.assertTrue(any(client in c for c in charts), client)
            self.assertFalse(
                any("paho" in c and "gmqtt" in c for c in charts),
                "sync and asyncio_bridged clients must not share a chart",
            )
            self.assertTrue(
                any("pub_qos_sweep_telemetry · MQTTv311" in c for c in charts)
            )
            matrix_body = index[index.index('class="matrix"') :]
            self.assertLess(
                matrix_body.index("pub_qos_sweep_telemetry · MQTTv311"),
                matrix_body.index("duplex_gateway · MQTTv311"),
            )
            self.assertLess(matrix_body.index("duplex_gateway · MQTTv311"), matrix_body.index("e2e_integrity · MQTTv311"))
            self.assertLess(
                matrix_body.index("e2e_integrity · MQTTv311"),
                matrix_body.index("sub_callback_matching · MQTTv311"),
            )
            self.assertLess(
                matrix_body.index("sub_callback_matching · MQTTv311"),
                matrix_body.index("remaining_length_boundaries · MQTTv311"),
            )
            # Columns are grouped by peer group, so sync (paho) precedes the
            # asyncio_bridged block (gmqtt, aiomqtt) regardless of client name.
            self.assertLess(matrix_body.index("paho"), matrix_body.index("gmqtt"))
            self.assertIn("asyncio_bridged", matrix_body)
            self.assertIn("sync", matrix_body)
            self.assertRegex(matrix_body, r'class="group-head\b')
            # Compare docs must not inflate the Clients hero stat.
            self.assertRegex(index, r'stat-label">Clients</p>\s*<p class="stat-value">3</p>')

    def test_client_load_signals_surface_on_index(self):
        import json
        import tempfile

        from mqtt_client_bench.report import build_site

        payload = {
            "schema_version": 1,
            "scenario": "puback_latency_qos1",
            "profile": "standard",
            "runs": 3,
            "client": "awscrt",
            "results": [
                {
                    "point": {"qos_publish": 1, "load_fraction": 0.9},
                    "runs": [
                        {
                            "status": "inconclusive",
                            "primary_msgs_per_s": None,
                            "non_comparable": False,
                            "reasons": ["open_loop_rate_out_of_tolerance"],
                            "workers": [],
                        },
                        {
                            "status": "valid",
                            "primary_msgs_per_s": 5000.0,
                            "non_comparable": False,
                            "reasons": [],
                            "workers": [{"role": "publisher", "ok": True}],
                        },
                        {
                            "status": "inconclusive",
                            "primary_msgs_per_s": None,
                            "non_comparable": False,
                            "reasons": ["open_loop_rate_out_of_tolerance"],
                            "workers": [],
                        },
                    ],
                    "summary": {"n": 1, "median": 5000.0, "total_runs": 3},
                }
            ],
        }
        refused = {
            "schema_version": 1,
            "scenario": "application_rtt_qos1",
            "profile": "standard",
            "runs": 1,
            "client": "awscrt",
            "results": [
                {
                    "point": {"topology": "application_rtt"},
                    "runs": [
                        {
                            "status": "inconclusive",
                            "primary_msgs_per_s": None,
                            "non_comparable": False,
                            "reasons": ["not_implemented:tcp_nodelay"],
                            "workers": [],
                        }
                    ],
                    "summary": {"n": 0, "median": None, "total_runs": 1},
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            results = root / "results"
            site = root / "site"
            results.mkdir()
            (results / "awscrt-puback.json").write_text(json.dumps(payload), encoding="utf-8")
            (results / "awscrt-rtt.json").write_text(json.dumps(refused), encoding="utf-8")
            build_site(results, site, reference=None)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("Client issues", index)
            self.assertIn("under load", index)
            self.assertIn("open_loop_rate_out_of_tolerance", index)
            self.assertIn("capability", index)
            self.assertIn("tcp_nodelay", index)
            # The matrix stays numeric — a refusal is never rendered as a value —
            # but an empty cell now says *why* it is empty instead of showing a
            # bare em-dash that could equally mean "never run".
            matrix_body = index[index.index('class="matrix"') : index.index("Client issues")]
            self.assertRegex(matrix_body, r'class="num empty empty-refused\b')
            self.assertIn("⊘", matrix_body)
            self.assertNotIn("<td class=\"num\">tcp_nodelay", matrix_body)
            # The reason appears only as a tooltip, never as a cell value.
            self.assertIn('title="refused — client lacks the capability: tcp_nodelay"', matrix_body)

    def test_stability_orders_a_peer_group_without_splitting_it(self):
        """A pre-release client competes with released ones of the same I/O model.

        Stability used to split the peer groups, which put libraries doing
        identical work in separate charts and meant a client graduating from
        experimental to stable would silently change who it was compared with.
        It now orders a group instead: stable first, pre-release badged.
        """
        from mqtt_client_bench.report.aggregate import peer_groups
        from mqtt_client_bench.report.model import ClientMeta, _sort_clients

        meta = {
            "paho": ClientMeta("paho", io_model="sync", stability="stable"),
            "gmqtt": ClientMeta("gmqtt", io_model="asyncio_bridged", stability="stable"),
            "zmqtt": ClientMeta("zmqtt", io_model="asyncio_bridged", stability="experimental"),
            "awscrt": ClientMeta("awscrt", io_model="crt_event_loop", stability="stable"),
        }
        self.assertEqual(meta["zmqtt"].peer_group, meta["gmqtt"].peer_group)

        ordered = _sort_clients(list(meta), meta)
        groups = peer_groups(ordered, meta)
        self.assertEqual(
            [group for group, _ in groups],
            ["sync", "asyncio_bridged", "crt_event_loop"],
        )
        bridged = dict(groups)["asyncio_bridged"]
        self.assertEqual(bridged, ["gmqtt", "zmqtt"], "stable sorts before experimental")

    def test_integrity_aggregates_all_runs(self):
        from mqtt_client_bench.report import _collect_integrity

        runs = [
            {
                "status": "valid",
                "non_comparable": False,
                "workers": [
                    {"integrity": {"expected": 10, "received": 9, "unique": 9, "missing": 1, "duplicates": 0, "out_of_order": 0, "unexpected": 0}}
                ]
            },
            {
                "status": "valid",
                "non_comparable": False,
                "workers": [
                    {"integrity": {"expected": 10, "received": 8, "unique": 8, "missing": 2, "duplicates": 1, "out_of_order": 0, "unexpected": 0}}
                ]
            },
            {
                # Inconclusive must not poison the aggregate (fc61949-era fail-open).
                "status": "inconclusive",
                "non_comparable": False,
                "workers": [
                    {"integrity": {"expected": 10, "received": 0, "unique": 0, "missing": 10, "duplicates": 0, "out_of_order": 0, "unexpected": 0}}
                ]
            },
        ]
        integ = _collect_integrity(runs)
        self.assertEqual(integ["missing"], 3)
        self.assertEqual(integ["worst_missing"], 2)
        self.assertEqual(integ["duplicates"], 1)


class DualProtocolTests(unittest.TestCase):
    def test_dual_expand_qos_sweep_and_sub_exact(self):
        qos_points = expand_scenario(SCENARIO_BY_NAME["pub_qos_sweep_telemetry"], "standard")
        self.assertEqual(len(qos_points), 6)  # 3 qos × 2 protocols
        protos = {(p["qos_publish"], p["protocol"]) for p in qos_points}
        self.assertEqual(
            protos,
            {(0, "MQTTv311"), (0, "MQTTv5"), (1, "MQTTv311"), (1, "MQTTv5"), (2, "MQTTv311"), (2, "MQTTv5")},
        )
        sub_points = expand_scenario(SCENARIO_BY_NAME["sub_exact_telemetry"], "standard")
        self.assertEqual(len(sub_points), 2)
        self.assertEqual({p["protocol"] for p in sub_points}, {"MQTTv311", "MQTTv5"})

    def test_open_loop_fractions_and_dual(self):
        for name in ("puback_latency_qos1", "application_rtt_qos1"):
            points = expand_scenario(SCENARIO_BY_NAME[name], "standard")
            fracs = sorted({float(p["load_fraction"]) for p in points})
            self.assertEqual(fracs, [0.5, 0.75, 0.9, 1.0], name)
            self.assertEqual(len(points), 8, name)  # 4 fractions × 2 protocols
            self.assertEqual({p["protocol"] for p in points}, {"MQTTv311", "MQTTv5"}, name)
            self.assertEqual(
                {(p["protocol"], float(p["load_fraction"])) for p in points},
                {
                    (protocol, fraction)
                    for protocol in ("MQTTv311", "MQTTv5")
                    for fraction in (0.5, 0.75, 0.9, 1.0)
                },
                name,
            )

    def test_payload_sweep_stays_v311_only(self):
        points = expand_scenario(SCENARIO_BY_NAME["pub_payload_sweep_qos0"], "standard")
        self.assertTrue(all(p.get("protocol", "MQTTv311") == "MQTTv311" for p in points))
        self.assertEqual(len(points), 7)

    def test_protocols_for_client(self):
        from mqtt_client_bench.harness import protocols_for_client

        self.assertEqual(protocols_for_client("paho"), ["MQTTv311", "MQTTv5"])
        self.assertEqual(protocols_for_client("gmqtt"), ["MQTTv311", "MQTTv5"])
        self.assertEqual(protocols_for_client("aiomqtt3"), ["MQTTv5"])
        self.assertEqual(protocols_for_client("amqtt"), ["MQTTv311"])

    def test_gmqtt_calibration_populates_both_protocol_buckets(self):
        import tempfile
        from unittest.mock import patch

        from mqtt_client_bench.harness import calibrate

        calls = []

        def fake_run_scenario(name, **kwargs):
            point_filter = kwargs["point_filter"]
            protocol = next(
                protocol
                for protocol in ("MQTTv311", "MQTTv5")
                if point_filter({"qos_publish": 1, "protocol": protocol})
            )
            calls.append((name, protocol))
            base = 10_000.0 if protocol == "MQTTv311" else 11_000.0
            rate = base if name == "pub_qos_sweep_telemetry" else base / 2
            return {
                "results": [
                    {
                        "point": {"qos_publish": 1, "protocol": protocol},
                        "summary": {"median": rate},
                        "runs": [],
                    }
                ],
                "broker": {"image_digest": "sha256:test"},
                "environment": {"runner": "test"},
            }

        with tempfile.TemporaryDirectory() as tmp:
            output = str(Path(tmp) / "gmqtt-calibration.json")
            with (
                patch("mqtt_client_bench.harness.run_scenario", side_effect=fake_run_scenario),
                patch(
                    "mqtt_client_bench.harness.adapter_identity",
                    return_value={"client": "gmqtt", "client_version": "0.7.0"},
                ),
            ):
                profile = calibrate(output, client="gmqtt", profile="smoke")

        self.assertEqual(
            calls,
            [
                ("pub_qos_sweep_telemetry", "MQTTv311"),
                ("rtt_capacity_qos1", "MQTTv311"),
                ("pub_qos_sweep_telemetry", "MQTTv5"),
                ("rtt_capacity_qos1", "MQTTv5"),
            ],
        )
        buckets = profile["protocol_capacities"]
        self.assertEqual(buckets["MQTTv311"]["capacity_msgs_per_s"], 10_000.0)
        self.assertEqual(buckets["MQTTv5"]["capacity_msgs_per_s"], 11_000.0)
        self.assertEqual(buckets["MQTTv5"]["rtt_capacity_msgs_per_s"], 5_500.0)
        self.assertEqual(buckets["MQTTv5"]["fractions"]["1.00"], 11_000.0)

    def test_capacity_from_load_profile_protocol_buckets(self):
        from mqtt_client_bench.harness import capacity_from_load_profile

        profile = {
            "protocol_capacities": {
                "MQTTv5": {"capacity_msgs_per_s": 1000.0, "rtt_capacity_msgs_per_s": 500.0},
            }
        }
        self.assertEqual(
            capacity_from_load_profile(profile, protocol="MQTTv5", kind="publish"),
            1000.0,
        )
        with self.assertRaises(ValueError) as ctx:
            capacity_from_load_profile(profile, protocol="MQTTv311", kind="publish")
        self.assertIn("load_profile_missing_protocol:MQTTv311", str(ctx.exception))

    def test_legacy_load_profile_v311_only(self):
        from mqtt_client_bench.harness import capacity_from_load_profile

        legacy = {"capacity_msgs_per_s": 2000.0, "rtt_capacity_msgs_per_s": 800.0}
        self.assertEqual(capacity_from_load_profile(legacy, protocol="MQTTv311", kind="publish"), 2000.0)
        with self.assertRaises(ValueError):
            capacity_from_load_profile(legacy, protocol="MQTTv5", kind="publish")

    def test_lean_header_decode_agrees_with_the_dict_one(self):
        # Two roles decode a header on every message and read one or two of its
        # fields. The lean decoder exists to skip the dict and the slice; if it
        # ever disagreed with decode_header, integrity and latency would both be
        # measuring something other than what was sent.
        from mqtt_client_bench.workloads import (
            HEADER_SIZE,
            decode_header,
            decode_header_fields,
            encode_header,
        )

        for pub_id, seq, corr, send_ns in (
            (1, 0, 0, 0),
            (7, 42, 4242, 123456789),
            (0xFFFFFFFF, (1 << 64) - 1, (1 << 64) - 1, (1 << 64) - 1),
        ):
            payload = encode_header(b"abcdefgh", pub_id, seq, corr, send_ns) + b"tail"
            want = decode_header(payload)
            self.assertEqual(
                decode_header_fields(payload),
                (want["publisher_id"], want["sequence"], want["correlation"], want["send_ns"]),
            )
        # Same refusals, so the callers' except clauses keep working.
        with self.assertRaises(ValueError):
            decode_header_fields(b"short")
        with self.assertRaises(ValueError):
            decode_header_fields(b"XXXX" + bytes(HEADER_SIZE))

    def test_precomputed_tail_stamps_the_same_bytes(self):
        # The publisher stopped calling wrap_with_header per message and now
        # concatenates a tail cut once per run. The wire payload must be
        # identical, or every integrity check and decode is measuring a
        # different message than before.
        from mqtt_client_bench.workloads import (
            HEADER_SIZE,
            encode_header,
            payload_tail,
            wrap_with_header,
        )

        header = encode_header(b"abcdefgh", 1, 42, 42, 123456789)
        for size in (0, 1, HEADER_SIZE - 1, HEADER_SIZE, HEADER_SIZE + 1, 256, 65536):
            body = bytes(range(256)) * (size // 256) + b"z" * (size % 256)
            self.assertEqual(len(body), size)
            if len(body) >= HEADER_SIZE:
                tail = payload_tail(body)
                self.assertEqual(header + tail, wrap_with_header(body, header), f"size={size}")
                self.assertEqual(len(header + tail), size, f"size={size}")

    def test_reservoir_keeps_a_uniform_sample(self):
        # The percentiles the report publishes are only as good as this sample.
        # Pin the property, not the algorithm, so a future attempt to make `add`
        # cheaper cannot quietly bias which part of the run is retained — an
        # Algorithm-L rewrite was tried here and reverted for measuring no
        # faster at this bench's n/k, but the next one should be free to try.
        import statistics

        from mqtt_client_bench.sampling import ReservoirSampler

        n, k, trials = 2_000, 20, 400
        counts = [0] * n
        for seed in range(trials):
            s = ReservoirSampler(k, seed=seed)
            for i in range(n):
                s.add(i)
            self.assertEqual(s.seen, n)
            self.assertEqual(len(s.snapshot()), k)
            for v in s.snapshot():
                counts[v] += 1

        expected = trials * k / n  # 4.0
        # Split the stream in five and compare occupancy: a reservoir that
        # favours early or late items shows up immediately here.
        chunk = n // 5
        buckets = [sum(counts[i * chunk:(i + 1) * chunk]) / chunk for i in range(5)]
        for b in buckets:
            self.assertGreater(b, expected * 0.55, f"bucket occupancy {buckets}")
            self.assertLess(b, expected * 1.45, f"bucket occupancy {buckets}")
        self.assertAlmostEqual(statistics.mean(buckets), expected, delta=expected * 0.1)

    def test_sequences_are_not_fingerprinted_when_nobody_reads_them(self):
        # The fingerprints cost 1.2 us per message and are only ever compared
        # against a subscriber's. A publisher_only point has none.
        from mqtt_client_bench.sampling import SequenceTracker, sequence_tracker

        live = sequence_tracker(100, enabled=True)
        self.assertIsInstance(live, SequenceTracker)
        live.add(7)
        self.assertEqual(live.summary()["count"], 1)

        off = sequence_tracker(100, enabled=False)
        off.add(7)
        self.assertEqual(off.exact_values(), [])
        self.assertFalse(off.summary()["tracked"])
        self.assertEqual(off.summary()["count"], 0)
        # Same keys as the real one: the worker reads them by name, and a
        # shorter dict crashed every publisher_only run with KeyError('first').
        self.assertEqual(set(off.summary()), set(live.summary()))

    def test_harness_fingerprint_tracks_code_not_prose(self):
        """A result is comparable only with one from the same measurement path.

        The client-version gate cannot see the harness moving under a client
        that sat still, which is how a campaign came to publish a matrix mixing
        two harness generations whose per-message cost differed by 40%. The
        fingerprint closes that, but only if it reacts to code and ignores
        prose — otherwise a docstring fix silently invalidates twelve hours.
        """
        import tempfile

        from mqtt_client_bench.provenance import (
            MEASUREMENT_PATH,
            _file_digest,
            _measurement_file,
            _structural_digest,
            harness_fingerprint,
        )

        base = harness_fingerprint()
        self.assertEqual(base, harness_fingerprint(), "must be deterministic")
        self.assertRegex(base, r"^[0-9a-f]{16}$")

        # Every module the fingerprint covers must exist, or it silently
        # degrades to hashing the word "missing".
        self.assertIn("scripts/mqtt_hammer.c", MEASUREMENT_PATH)
        for rel in MEASUREMENT_PATH:
            self.assertTrue(_measurement_file(rel).exists(), f"{rel} is not in the tree")

        with tempfile.TemporaryDirectory() as tmp:
            mod = Path(tmp) / "m.py"
            mod.write_text('"""Doc."""\n\n\ndef f():\n    return 1\n')
            a = _structural_digest(mod)
            mod.write_text('"""Different prose entirely."""\n# and a comment\n\n\ndef f():\n    return 1\n')
            self.assertEqual(a, _structural_digest(mod), "prose must not invalidate a campaign")
            mod.write_text('"""Doc."""\n\n\ndef f():\n    return 2\n')
            self.assertNotEqual(a, _structural_digest(mod), "changed code must invalidate")

            hammer = Path(tmp) / "mqtt_hammer.c"
            hammer.write_text("static int g_rate = 200000;\n")
            hashed = _file_digest(hammer)
            hammer.write_text("/* comment only */\nstatic int g_rate = 200000;\n")
            self.assertNotEqual(hashed, _file_digest(hammer), "C comment changes must invalidate")
            hammer.write_text("static int g_rate = 100000;\n")
            self.assertNotEqual(hashed, _file_digest(hammer), "C code changes must invalidate")

    def test_write_json_never_leaves_a_partial_document(self):
        # A campaign is paused with SIGINT, which can land in the middle of a
        # checkpoint write. Truncate-then-write cost a whole scenario once; the
        # destination must only ever hold a complete document.
        import json
        import tempfile
        from unittest import mock

        from mqtt_client_bench.control import write_json

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "checkpoint.json"
            write_json(str(target), {"results": ["first"]})

            # Interrupt the replacement exactly as a SIGINT would.
            with mock.patch("mqtt_client_bench.control.os.replace", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    write_json(str(target), {"results": ["second"]})

            # The previous document survives intact, and no temp file is left.
            self.assertEqual(json.loads(target.read_text())["results"], ["first"])
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["checkpoint.json"])

            write_json(str(target), {"results": ["second"]})
            self.assertEqual(json.loads(target.read_text())["results"], ["second"])

    def test_archive_drops_raw_samples_but_never_a_statistic(self):
        # Raw sample vectors are 97% of a result file and are read only once, to
        # produce statistics that are stored beside them. Archiving them must
        # leave every one of those statistics behind, and say how many samples
        # were moved so a summary is never mistaken for a complete record.
        import gzip
        import json
        import tempfile

        from mqtt_client_bench.archive import ARCHIVED_KEY, archive_results, slim_document

        doc = {
            "results": [
                {
                    "runs": [
                        {
                            "workers": [
                                {
                                    "role": "publisher",
                                    "latencies_ns": [1_000_000, 2_000_000, 3_000_000],
                                    "scheduler_lags_ns": [10, 20],
                                    "sent_sequences": [1, 2, 3, 4],
                                    "integrity": {"missing": 0},
                                },
                                {
                                    "role": "subscriber",
                                    "sequences": [1, 2, 3],
                                    "latency_summary": {"p50_ms": 1.5},
                                    "latencies_ns": [1_500_000],
                                },
                            ]
                        }
                    ]
                }
            ]
        }
        slim, dropped = slim_document(doc)
        self.assertEqual(dropped["latencies_ns"], 4)
        self.assertEqual(dropped["sent_sequences"], 4)
        pub, sub = slim["results"][0]["runs"][0]["workers"]
        for key in ("latencies_ns", "scheduler_lags_ns", "sent_sequences"):
            self.assertNotIn(key, pub)
        self.assertNotIn("sequences", sub)
        self.assertEqual(pub[ARCHIVED_KEY]["latencies_ns"], 3)
        # Statistics survive: the pre-existing one untouched, the missing one derived.
        self.assertEqual(sub["latency_summary"]["p50_ms"], 1.5)
        self.assertIn("p50_ms", pub["latency_summary"])
        self.assertEqual(pub["integrity"], {"missing": 0})
        # The original is not mutated.
        self.assertEqual(len(doc["results"][0]["runs"][0]["workers"][0]["latencies_ns"]), 3)

        with tempfile.TemporaryDirectory() as tmp:
            results, archive = Path(tmp) / "results", Path(tmp) / "archive"
            results.mkdir()
            (results / "paho-x.json").write_text(json.dumps(doc), encoding="utf-8")
            first = archive_results(results, archive)
            self.assertEqual(first["files"], 1)
            with gzip.open(archive / "paho-x.json.gz", "rb") as fh:
                restored = json.loads(fh.read())
            # The archive is the document as measured, not the summary.
            self.assertEqual(
                restored["results"][0]["runs"][0]["workers"][0]["latencies_ns"],
                [1_000_000, 2_000_000, 3_000_000],
            )
            # Re-running finds nothing left to archive and cannot clobber it.
            second = archive_results(results, archive)
            self.assertEqual(second["files"], 0)
            with gzip.open(archive / "paho-x.json.gz", "rb") as fh:
                self.assertIn("latencies_ns", json.loads(fh.read())["results"][0]["runs"][0]["workers"][0])

    def test_report_renders_multi_point_compare_verdicts(self):
        # A compare over a multi-point scenario has no top-level ratio or CI:
        # the aggregate verdict is the string "multi_point" and the statistics
        # live on each point. The page used to render an empty verdict panel and
        # "Points 0" for every campaign A/B, since pub_qos_sweep_telemetry always
        # expands to MQTTv311/v5 x QoS 0/1/2.
        import json
        import tempfile

        from mqtt_client_bench.report import build_site

        def point(index, protocol, qos, verdict, ratio, effect, lo, hi):
            return {
                "point_index": index,
                "point": {
                    "name": "pub_qos_sweep_telemetry",
                    "protocol": protocol,
                    "qos_publish": qos,
                },
                "verdict": {
                    "verdict": verdict,
                    "median_ratio": ratio,
                    "absolute_effect_pct": effect,
                    "ci_low": lo,
                    "ci_high": hi,
                    "n_blocks": 4,
                },
            }

        with tempfile.TemporaryDirectory() as tmp:
            results, site = Path(tmp) / "results", Path(tmp) / "site"
            results.mkdir()
            (results / "compare-paho-awscrt.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "scenario": "pub_qos_sweep_telemetry",
                        "profile": "standard",
                        "baseline_client": "paho",
                        "candidate_client": "awscrt",
                        "order": ["A", "B", "B", "A"],
                        "verdict": {
                            "verdict": "multi_point",
                            "points": [
                                {"index": 0, "verdict": "improvement"},
                                {"index": 1, "verdict": "inconclusive"},
                            ],
                        },
                        "points": [
                            point(0, "MQTTv311", 0, "improvement", 1.2078, 20.78, 0.1355, 0.2533),
                            point(1, "MQTTv5", 2, "inconclusive", None, None, None, None),
                        ],
                    }
                ),
                encoding="utf-8",
            )
            build_site(results, site, reference=None)
            page = (site / "runs" / "compare-paho-awscrt.html").read_text(encoding="utf-8")
            self.assertIn("Per-point verdicts", page)
            # Both points are listed, with the statistics that only exist per point.
            self.assertIn("MQTTv311 qos=0", page)
            self.assertIn("MQTTv5 qos=2", page)
            self.assertIn("1.208", page)
            self.assertIn("20.78", page)
            self.assertIn("0.136", page)
            # And the point count reflects the compare payload, not doc.points.
            self.assertNotIn(
                '<p class="stat-label">Points</p>\n          <p class="stat-value">0</p>', page
            )

    def test_report_splits_dual_protocol_rows(self):
        import json
        import tempfile

        from mqtt_client_bench.report import build_site

        sample = {
            "schema_version": 1,
            "scenario": "pub_qos_sweep_telemetry",
            "profile": "standard",
            "runs": 1,
            "client": "paho",
            "results": [
                {
                    "point": {"qos_publish": 1, "protocol": "MQTTv311"},
                    "runs": [
                        {
                            "status": "valid",
                            "primary_msgs_per_s": 7000.0,
                            "non_comparable": False,
                            "workers": [{"role": "publisher", "ok": True}],
                        }
                    ],
                    "summary": {"n": 1, "median": 7000.0, "total_runs": 1},
                },
                {
                    "point": {"qos_publish": 1, "protocol": "MQTTv5"},
                    "runs": [
                        {
                            "status": "valid",
                            "primary_msgs_per_s": 6500.0,
                            "non_comparable": False,
                            "workers": [{"role": "publisher", "ok": True}],
                        }
                    ],
                    "summary": {"n": 1, "median": 6500.0, "total_runs": 1},
                },
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            results = root / "results"
            site = root / "site"
            results.mkdir()
            (results / "paho-pub-dual.json").write_text(json.dumps(sample), encoding="utf-8")
            build_site(results, site, reference=None)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("pub_qos_sweep_telemetry · MQTTv311", index)
            self.assertIn("pub_qos_sweep_telemetry · MQTTv5", index)
            self.assertIn("comparable only within the same protocol", index)




class _FakeSyncOnLoopAdapter:
    """A client that admits a publish on the loop and acknowledges it later.

    Stands in for mqttium and gmqtt so the loop can be measured with no library
    and no socket in the way.
    """

    def __init__(self, complete_after: int = 1):
        self._complete_after = max(1, complete_after)
        self._pending: list[int] = []
        self._mid = 0
        self.on_connect = None
        self.on_publish = None
        self.on_message = None

    @classmethod
    def capabilities(cls):
        return AdapterCapabilities(name="fake-sync-on-loop", native_async=True, publish_sync_on_loop=True)

    def publish_nowait(self, topic, payload=None, qos=0, retain=False, properties=None):
        self._mid = 1 if self._mid >= 65535 else self._mid + 1
        mid = self._mid
        self._pending.append(mid)
        if len(self._pending) >= self._complete_after:
            for done in self._pending:
                if self.on_publish is not None:
                    self.on_publish(self, None, done, 0, None)
            self._pending.clear()
        return mid


class _FakeDeferredAdapter:
    """Admits on the loop and acknowledges *later*, from the loop itself.

    The other sync-on-loop fake acknowledges inside the publish call, so the
    publish loop never parks on the outstanding gate. Only this one exercises
    the park-and-wake path - which is how a QoS 1 client actually behaves, and
    where a missing wake-up leaves the loop asleep after one full window.
    """

    def __init__(self):
        self._mid = 0
        self.on_publish = None
        self.on_connect = None
        self.on_message = None

    @classmethod
    def capabilities(cls):
        return AdapterCapabilities(name="fake-deferred", native_async=True, publish_sync_on_loop=True)

    def publish_nowait(self, topic, payload=None, qos=0, retain=False, properties=None):
        self._mid = 1 if self._mid >= 65535 else self._mid + 1
        mid = self._mid
        # call_later, not call_soon: with call_soon the acknowledgements land at
        # the loop's very next yield, which happens before the window is full,
        # so the loop never parks and the test proves nothing. A delay longer
        # than the fill time is what forces the park.
        asyncio.get_running_loop().call_later(0.002, self._ack, mid)
        return mid

    def _ack(self, mid):
        if self.on_publish is not None:
            self.on_publish(self, None, mid, 0, None)


class _FakeAwaitedAdapter:
    """A client whose only publish API is awaitable, with a fixed service time."""

    def __init__(self, delay_s: float = 0.001):
        self._delay = delay_s
        self._mid = 0
        self.concurrent = 0
        self.max_concurrent = 0
        self.on_connect = None
        self.on_publish = None
        self.on_message = None

    @classmethod
    def capabilities(cls):
        return AdapterCapabilities(name="fake-awaited", native_async=True, publish_sync_on_loop=False)

    async def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        self.concurrent += 1
        self.max_concurrent = max(self.max_concurrent, self.concurrent)
        try:
            await asyncio.sleep(self._delay)
        finally:
            self.concurrent -= 1
        self._mid = 1 if self._mid >= 65535 else self._mid + 1
        return self._mid


class _FakeRefusingAdapter:
    """publish_nowait returns None until ``refuse`` admits are skipped.

    Stands in for mqttium FlowControlError mapped to mid is None.
    """

    def __init__(self, refuse: int = 0, always: bool = False):
        self.refuse_left = refuse
        self.always = always
        self.calls = 0
        self._mid = 0
        self.on_publish = None
        self.on_connect = None
        self.on_message = None

    @classmethod
    def capabilities(cls):
        return AdapterCapabilities(
            name="fake-refuse", native_async=True, publish_sync_on_loop=True
        )

    def publish_nowait(self, topic, payload=None, qos=0, retain=False, properties=None):
        self.calls += 1
        if self.always or self.refuse_left > 0:
            if self.refuse_left > 0:
                self.refuse_left -= 1
            return None
        self._mid = 1 if self._mid >= 65535 else self._mid + 1
        if self.on_publish is not None:
            self.on_publish(self, None, self._mid, 0, None)
        return self._mid


class CampaignCompletenessTests(unittest.TestCase):
    """One rule decides what a campaign re-measures, and what status reports.

    They were two rules once, in two shell heredocs, and they disagreed: status
    said "11/11 scenarios complete" about a set of results the gate was about to
    re-measure in full, because status never looked at the harness fingerprint it
    claimed to filter on.
    """

    def _write(self, tmp, client, scenario, *, points, fingerprint, version="1.0", started=True):
        blocks = []
        for i in range(points):
            run = {"status": "valid", "harness_fingerprint": fingerprint}
            if started:
                run["started_at"] = "2026-01-01T00:00:00+00:00"
            blocks.append({"point": {"i": i}, "runs": [run]})
        doc = {"results": blocks, "client_identity": {"client_version": version}}
        (tmp / f"{client}-{scenario}.json").write_text(json.dumps(doc))

    def test_the_gate_and_the_display_read_the_same_rule(self):
        import tempfile

        from mqtt_client_bench import campaign
        from mqtt_client_bench.adapters.registry import adapter_identity
        from mqtt_client_bench.harness import HARNESS_FINGERPRINT

        scenario = "pub_qos_sweep_telemetry"
        points = len(expand_scenario(SCENARIO_BY_NAME[scenario], "standard"))
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            installed = adapter_identity("paho").get("client_version")

            # Complete, current: both agree it is done.
            self._write(tmp, "paho", scenario, points=points,
                        fingerprint=HARNESS_FINGERPRINT, version=installed)
            self.assertTrue(campaign.scenario_complete(scenario, ["paho"], tmp))
            self.assertEqual(campaign.scenario_state(scenario, ["paho"], tmp)["state"], "done")

            # Same file, measured by a different harness: stale, not done. This
            # is the case the old display got wrong.
            self._write(tmp, "paho", scenario, points=points,
                        fingerprint="0" * 16, version=installed)
            self.assertFalse(campaign.scenario_complete(scenario, ["paho"], tmp))
            state, why, _ = campaign.scenario_state(scenario, ["paho"], tmp)["clients"]["paho"]
            self.assertEqual(state, "stale")
            self.assertIn("harness", why)

            # Unstamped results predate the fingerprint entirely.
            self._write(tmp, "paho", scenario, points=points,
                        fingerprint=None, version=installed)
            self.assertFalse(campaign.scenario_complete(scenario, ["paho"], tmp))

            # Half the points measured: partial, and it says how far.
            self._write(tmp, "paho", scenario, points=max(1, points // 2),
                        fingerprint=HARNESS_FINGERPRINT, version=installed)
            st = campaign.scenario_state(scenario, ["paho"], tmp)
            self.assertEqual(st["clients"]["paho"][0], "partial")

            # A client the campaign runs but never measured is not "done".
            self._write(tmp, "paho", scenario, points=points,
                        fingerprint=HARNESS_FINGERPRINT, version=installed)
            self.assertFalse(campaign.scenario_complete(scenario, ["paho", "gmqtt"], tmp))


class NativeAsyncPathTests(unittest.TestCase):
    """The publish path that runs on the role worker's own loop.

    These guard the property the path exists for: the harness must not charge a
    client for work the harness did, and it must not quietly shrink a client's
    in-flight window. Both failures look like a slow library.
    """

    @staticmethod
    def _state(log_limit=4096, sample_limit=1000):
        """A publisher state.

        The small defaults deliberately force the log to fold, so the tests
        exercise that path. The budget test passes production limits instead,
        because that is the configuration whose cost the budget is about.
        """
        st = {
            "offered": 0, "submitted": 0, "sync_rejected": 0,
            "completed_success": 0, "completed_failed": 0,
            "missed_due_to_backpressure": 0,
            "publish_calls": 0, "publish_accepted": 0, "publish_rejected": 0,
            "protocol_completed": 0, "protocol_failed": 0,
            "socket_completed_qos0": 0,
            "completed_in_window": 0, "completed_during_drain": 0,
            "latencies_ns": ReservoirSampler(sample_limit, seed=11),
            "scheduler_lags_ns": ReservoirSampler(sample_limit, seed=29),
            "inflight_local": 0,
            "phase": "measure",
            "mid_send_ns": {}, "early_acks": {},
            "warmup_drain_ok": True, "seen_mids_inflight": set(),
            "gate_waiter": None, "loop_expired": False,
            "pending_send_ns": None, "completed_inline": None,
            "overflow_success": 0, "overflow_failed": 0,
            "overflow_in_window": 0, "overflow_during_drain": 0,
            "fold_pending": False,
        }
        st["completions"] = CompletionLog(log_limit, sampler=st["latencies_ns"])
        return st

    @staticmethod
    def _completed(state, qos=1):
        """Counters are derived from the log now, not maintained live."""
        tally = state["completions"].summary(qos)
        return tally["completed_success"] + state["overflow_success"]

    @staticmethod
    def _loop_kwargs(**over):
        kwargs = dict(
            topic="bench/t", qos=1, body=b"x" * 64, corpus=[],
            run_id=b"testrun1", outstanding=8, cadence="capacity",
            until=time.perf_counter() + 0.25, target_rate=None,
            properties_builder=lambda: None, track_sequences=False,
        )
        kwargs.update(over)
        return kwargs

    def test_registry_and_capabilities_agree(self):
        """A client is driven natively only if it declares it can be.

        The two lists drifting apart is how a half-migrated client would end up
        on an untested path without anyone noticing.
        """
        for name, cls in registry._ASYNC_ADAPTERS.items():
            self.assertTrue(
                cls.capabilities().native_async,
                f"{name} has a native adapter but does not declare native_async",
            )
        for name, cls in registry._ADAPTERS.items():
            if cls.capabilities().native_async:
                self.assertIn(
                    name, registry._ASYNC_ADAPTERS,
                    f"{name} declares native_async with no native adapter registered",
                )

    def test_every_bridged_adapter_exposes_its_coroutines(self):
        """One call site per library, so the two drive modes cannot diverge."""
        for name, cls in registry._ADAPTERS.items():
            if not cls.capabilities().async_bridged:
                continue
            native = registry._ASYNC_ADAPTERS.get(name)
            if native is None or not issubclass(native, NativeAsyncAdapter):
                continue  # hand-written native adapter, checked by its own tests
            for method in ("aconnect", "apublish", "asubscribe", "adisconnect"):
                self.assertTrue(
                    callable(getattr(cls, method, None)),
                    f"{name} is driven natively through the generic wrapper but "
                    f"does not expose {method}()",
                )

    def test_sync_on_loop_respects_the_outstanding_window(self):
        """The gate is the scenario's, not the loop's convenience."""
        outstanding = 8
        adapter = _FakeSyncOnLoopAdapter(complete_after=4)
        state = self._state()
        adapter.on_publish = publisher._make_on_publish(state, 1, lock=None)
        peak = []

        async def drive():
            task = asyncio.ensure_future(
                publisher._run_publish_loop_async(
                    adapter, state, **self._loop_kwargs(outstanding=outstanding)
                )
            )
            while not task.done():
                peak.append(state["inflight_local"])
                await asyncio.sleep(0.005)
            return await task

        asyncio.new_event_loop().run_until_complete(drive())
        self.assertLessEqual(max(peak, default=0), outstanding)
        self.assertGreater(self._completed(state), 0)
        self.assertEqual(state["offered"], state["submitted"])

    def test_deferred_completion_wakes_the_parked_loop(self):
        """A completion that frees a slot must wake the loop waiting on it.

        With the wake-up missing, the loop filled the window once and then slept
        until the deadline: mqttium QoS 1 offered exactly 64 messages in a 3 s
        window, 21 msgs/s instead of ~15,000. QoS 0 looked perfect throughout,
        because a completion delivered inside the publish call never parks.
        """
        outstanding = 8
        adapter = _FakeDeferredAdapter()
        state = self._state()
        adapter.on_publish = publisher._make_on_publish(state, 1, lock=None)

        async def drive():
            return await publisher._run_publish_loop_async(
                adapter, state, **self._loop_kwargs(outstanding=outstanding,
                                                    until=time.perf_counter() + 0.3)
            )

        asyncio.new_event_loop().run_until_complete(drive())
        completed = self._completed(state)
        self.assertGreater(
            completed, outstanding * 10,
            f"only {completed} completions: the loop parked on the gate and was never woken",
        )

    def test_awaited_shape_keeps_the_window_full(self):
        """An await-only API still gets `outstanding` publishes in flight.

        Awaiting one publish before starting the next would pin the window at 1
        and report round-trip time as capacity - a harness artefact that would
        read as a library being four times slower than it is.
        """
        outstanding = 8
        adapter = _FakeAwaitedAdapter(delay_s=0.002)
        state = self._state()

        async def drive():
            return await publisher._run_publish_loop_async(
                adapter, state, **self._loop_kwargs(outstanding=outstanding)
            )

        asyncio.new_event_loop().run_until_complete(drive())
        self.assertEqual(adapter.max_concurrent, outstanding)
        self.assertGreater(self._completed(state), 0)

    def test_awaited_shape_releases_every_slot(self):
        """A cancelled publish must not leave its slot held.

        It did, once: the workers were cancelled mid-await at the end of the
        window, the window never drained, and the run was failed as
        `warmup_drain_timeout` - the harness's fault, reported as the client's.
        """
        adapter = _FakeAwaitedAdapter(delay_s=5.0)  # never completes in time
        state = self._state()

        async def drive():
            return await publisher._run_publish_loop_async(
                adapter, state, **self._loop_kwargs(until=time.perf_counter() + 0.1)
            )

        asyncio.new_event_loop().run_until_complete(drive())
        self.assertEqual(state["inflight_local"], 0)

    def _achieved_open_loop_rate(self, adapter, target, qos=1):
        state = self._state()
        # The sync-on-loop shape completes through this callback; the awaited
        # shape accounts its own completions and simply never fires it.
        adapter.on_publish = publisher._make_on_publish(state, qos, lock=None)

        async def drive():
            started = time.perf_counter()
            await publisher._run_publish_loop_async(
                adapter, state,
                **self._loop_kwargs(qos=qos, outstanding=32, cadence="steady50",
                                    target_rate=target, until=started + 0.5)
            )
            return time.perf_counter() - started

        elapsed = asyncio.new_event_loop().run_until_complete(drive())
        # Offer rate, not completion rate: completions may drain after T1.
        return state["offered"] / elapsed

    def test_awaited_shape_holds_the_open_loop_target_rate(self):
        """An open-loop run must actually offer the rate it was asked for.

        It did not: the awaited path used to jump its pacing cursor forward when
        it found itself late and charge the skipped slots as backpressure. Since
        asyncio.sleep resolves to about a millisecond against intervals of tens
        of microseconds, it was late on every iteration, so the cursor ran away
        and the achieved rate sat permanently under target. Every open-loop run
        on an await-only client came back open_loop_rate_out_of_tolerance -
        aiomqtt went from 24/24 valid runs to 0/21 - and the harness was right
        to refuse them. The unit suite passed throughout (9e1eab5).
        """
        target = 2000.0
        rate = self._achieved_open_loop_rate(_FakeAwaitedAdapter(delay_s=0.00005), target)
        self.assertGreater(
            rate, target * 0.98,
            f"awaited open loop offered {rate:.0f} msgs/s against a {target:.0f} target",
        )
        self.assertLess(rate, target * 1.05)

    def test_sync_on_loop_shape_holds_the_open_loop_target_rate(self):
        """The same guarantee for the other publish shape, as a control."""
        target = 2000.0
        rate = self._achieved_open_loop_rate(_FakeSyncOnLoopAdapter(complete_after=1), target)
        self.assertGreater(
            rate, target * 0.98,
            f"sync-on-loop open loop offered {rate:.0f} msgs/s against a {target:.0f} target",
        )
        self.assertLess(rate, target * 1.05)

    def test_harness_cost_per_message_stays_under_budget(self):
        """The harness's own per-message cost, measured against a null client.

        The cost matters because it is *fixed* per message, so it does not tax
        clients equally: it inflates a fast client's period by a larger fraction
        than a slow one's, which compresses - and can reorder - a ranking.

        Measured on this loop with no library in the way, the floor sits near
        3.3 us against 18.5 us through the bridge this path replaced. The
        completion counters left the window (CompletionLog), the reservoir left
        the hot path with them, and the header stamper resolves the payload
        shape once per run instead of once per message.

        The bound is deliberately loose. It catches a gross regression on any
        machine CI happens to run on; it does not certify the target, which is
        5% of the fastest measured client's period - on the reference host,
        gmqtt at 37,014 msgs/s, so 1.35 us, and the floor is about 2.5x that.
        The real invariant needs both terms measured on the same machine, which
        is what the host profile is for; asserting it here against a constant
        is what let this test drift into 3.3x the rule it stood for.

        The statistic is the *minimum* over several passes, not one wall-clock
        shot. A single shot ranged 3700-7650 ns on this host depending on what
        else was running, and failed roughly one run in four while the code had
        not moved at all - verified by measuring the commit that set the bound
        and finding it identical. The floor over N is stable to about 2%.
        """
        from mqtt_client_bench.hostcal import measure_harness_cost_ns

        result = measure_harness_cost_ns(passes=7)
        self.assertLess(
            result["ns_per_message"], 8000.0,
            f"harness floor is {result['ns_per_message']:.0f} ns/message over "
            f"{result['passes']} passes; the bridged path it replaced cost "
            "18500 ns, so this is a regression toward it",
        )

    def test_closed_loop_nowait_refusal_is_backpressure_not_protocol_failed(self):
        """A full write pump must not be recorded as a failed MQTT completion.

        mqttium native used to fire on_publish rc=128 on FlowControlError, so
        one refused nowait invalidated the whole payload-sweep run.
        """
        adapter = _FakeRefusingAdapter(refuse=20)
        state = self._state()
        adapter.on_publish = publisher._make_on_publish(state, 0, lock=None)

        async def drive():
            return await publisher._run_publish_loop_async(
                adapter, state,
                **self._loop_kwargs(qos=0, outstanding=8, until=time.perf_counter() + 0.15),
            )

        asyncio.new_event_loop().run_until_complete(drive())
        self.assertGreaterEqual(state["sync_rejected"], 20)
        self.assertEqual(state["protocol_failed"], 0)
        tally = state["completions"].summary(0)
        self.assertEqual(tally["completed_failed"] + state["overflow_failed"], 0)
        self.assertGreater(self._completed(state, qos=0), 0)
        self.assertEqual(
            state["offered"],
            state["submitted"] + state["sync_rejected"] + state["missed_due_to_backpressure"],
        )

    def test_open_loop_nowait_refusal_misses_the_slot_instead_of_retrying(self):
        """Retrying a refused nowait in the same tick would exceed target_rate."""
        adapter = _FakeRefusingAdapter(always=True)
        state = self._state()
        adapter.on_publish = publisher._make_on_publish(state, 0, lock=None)
        target = 200.0
        window = 0.25

        async def drive():
            started = time.perf_counter()
            await publisher._run_publish_loop_async(
                adapter, state,
                **self._loop_kwargs(
                    qos=0, outstanding=8, cadence="steady50",
                    target_rate=target, until=started + window,
                ),
            )

        asyncio.new_event_loop().run_until_complete(drive())
        self.assertEqual(state["submitted"], 0)
        self.assertEqual(state["sync_rejected"], 0)
        self.assertEqual(state["missed_due_to_backpressure"], state["offered"])
        self.assertLess(
            state["offered"], target * window * 2,
            "refused nowait retried inside the tick and inflated the offer",
        )
        self.assertGreater(state["offered"], target * window * 0.5)

    def test_closed_loop_nowait_refusal_yields_so_the_writer_can_run(self):
        """Without an await, a QoS0 refuse loop starves the write pump."""
        adapter = _FakeRefusingAdapter(always=True)
        state = self._state()
        adapter.on_publish = publisher._make_on_publish(state, 0, lock=None)
        writer_ticks = []

        async def drive():
            async def writer():
                while True:
                    writer_ticks.append(time.perf_counter())
                    await asyncio.sleep(0)

            task = asyncio.create_task(writer())
            started = time.perf_counter()
            try:
                await publisher._run_publish_loop_async(
                    adapter, state,
                    **self._loop_kwargs(qos=0, outstanding=8, until=started + 0.05),
                )
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            return started

        started = asyncio.new_event_loop().run_until_complete(drive())
        self.assertTrue(
            any(tick < started + 0.05 for tick in writer_ticks),
            "nowait refuse loop never yielded; a real write pump would stall",
        )


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

    def test_other_errors_still_propagate(self):
        adapter = MqttiumAsyncAdapter()

        class _Client:
            def publish_nowait(self, *args, **kwargs):
                raise RuntimeError("not on the owning loop")

        adapter._client = _Client()
        with self.assertRaises(RuntimeError):
            adapter.publish_nowait("t", b"x", qos=0)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
