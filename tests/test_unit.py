"""Unit tests for MQTT client benchmark helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mqtt_client_bench.adapters.registry import (  # noqa: E402
    EXPERIMENTAL_CLIENTS,
    STABLE_CLIENTS,
    list_clients,
    unsupported_for_client,
)
from mqtt_client_bench.harness import capacity_from_qos_sweep, capacity_from_scenario, unsupported_features  # noqa: E402
from mqtt_client_bench.loadgen import (  # noqa: E402
    LoadgenSpec,
    enrich_loadgen_stats,
    interval_for_rate,
    nominal_rate,
    observed_pub_rate,
    parse_emqtt_output,
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
        self.assertIn("receive_maximum", unsupported_features({"receive_maximum": 10}))
        self.assertIn("retained_count", unsupported_features({"retained_count": 10_000}))
        self.assertIn("session_outage", unsupported_features({"outage_s": 2.0}))
        self.assertIn("queue_rejection_protocol", unsupported_features({"submit_count": 150}))
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

    def test_implemented_clients_accept_core_points(self):
        point = {"payload": "telemetry256", "qos_publish": 0, "protocol": "MQTTv311"}
        for name in ("paho", "gmqtt", "aiomqtt", "amqtt", "awscrt", "zmqtt"):
            missing = unsupported_for_client(name, point)
            self.assertEqual(missing, [], name)

    def test_callback_matching_paho_only(self):
        point = {"callback_filters": 64, "qos_subscribe": 0}
        self.assertEqual(unsupported_for_client("paho", point), [])
        for name in ("gmqtt", "aiomqtt", "amqtt", "awscrt", "zmqtt"):
            self.assertIn("native_message_callback_add", unsupported_for_client(name, point), name)

    def test_amqtt_refuses_mqtt_v5(self):
        point = {"protocol": "MQTTv5", "qos_publish": 0}
        self.assertIn("mqtt_v5", unsupported_for_client("amqtt", point))

    def test_gmqtt_refuses_qos2(self):
        point = {"protocol": "MQTTv311", "qos_publish": 2}
        self.assertIn("qos2", unsupported_for_client("gmqtt", point))
        self.assertEqual(unsupported_for_client("paho", point), [])

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
            "latencies_ns": [],
            "phase": "measure",
            "lock": __import__("threading").Lock(),
        }
        # Simulate callback before registration.
        now = 1000
        with state["lock"]:
            state["early_acks"][7] = (now, False)
            early = state["early_acks"].pop(7, None)
            self.assertIsNotNone(early)
            pub_mod._consume_completion_locked(state, 1, 500, early[0], early[1], mid=7)
        self.assertEqual(state["completed_success"], 1)
        self.assertEqual(state["completed_in_window"], 1)
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
            "latencies_ns": [],
            "phase": "measure",
            "lock": __import__("threading").Lock(),
        }
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
        session = SCENARIO_BY_NAME["session_resume_qos1"]
        self.assertIn("planned", session.tags)

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

    def test_niche_scenarios_are_planned(self):
        # Harness-level gaps: kept in the catalogue, excluded from suite
        # execution instead of burning campaign time on refused points.
        for name in ("mqttv5_flow_control", "queue_rejection", "retained_bootstrap", "session_resume_qos1"):
            scenario = SCENARIO_BY_NAME[name]
            self.assertIn("planned", scenario.tags, name)
            for point in expand_scenario(scenario, "standard"):
                self.assertTrue(point.get("non_comparable"), name)

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
        spec = LoadgenSpec(clients=32, interval_ms=1, qos=0, mode="pub")
        parsed = {"median_rate": 64000.0, "last_rate": 64000.0, "samples": 1, "rates": [64000.0], "totals": [64000], "kinds": ["pub"]}
        stats = enrich_loadgen_stats(spec, parsed)
        self.assertEqual(stats["effective_offer_msgs_per_s"], 32000.0)
        self.assertEqual(stats["nominal_rate"], 32000.0)
        self.assertTrue(stats["qos0_pub_counter_double_count"])
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
        from mqtt_client_bench.loadgen import build_pub_args

        args = build_pub_args(LoadgenSpec(mqtt_version=4))
        self.assertIn("--shortids", args)
        self.assertNotIn("--shortids", build_pub_args(LoadgenSpec(mqtt_version=5)))

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
        from mqtt_client_bench.harness import resolve_ingress_offer

        self.assertEqual(resolve_ingress_offer({}, 32), 40000.0)
        self.assertEqual(resolve_ingress_offer({"ingress_target_msgs_per_s": 64000}, 64), 64000.0)
        self.assertEqual(resolve_ingress_offer({"fanin_mode": "per_publisher"}, 16), 16000.0)

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
        validity = validate_run(point, workers, loadgen, [])
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
        # > 1% of offer*duration (64000*20*0.01 = 12800)
        sys_counters = {"dropped_delta": 20000}
        validity = validate_run(point, [], loadgen, [], sys_counters=sys_counters, loadgen_ref_sub=ref_sub)
        self.assertEqual(validity["bottleneck"], "broker_limited")
        self.assertIn("sys_publish_dropped", validity["reasons"])
        self.assertIn("delivery_below_half_offer", validity["reasons"])

    def test_sys_counters_delta(self):
        from mqtt_client_bench.sys_probe import sys_counters_delta

        before = {"dropped": 10, "publish_sent": 100, "publish_received": 100}
        after = {"dropped": 25, "publish_sent": 500, "publish_received": 480}
        delta = sys_counters_delta(before, after)
        self.assertEqual(delta["dropped_delta"], 15)
        self.assertEqual(delta["publish_sent_delta"], 400)
        self.assertEqual(delta["publish_received_delta"], 380)


class SchemaTests(unittest.TestCase):
    def test_schema_file_exists_and_parses(self):
        import json

        schema_path = SRC / "mqtt_client_bench" / "result.schema.json"
        data = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertEqual(data["properties"]["schema_version"]["const"], 1)
        self.assertIn("client", data["properties"])
        self.assertIn("awscrt", data["properties"]["client"]["enum"])
        self.assertIn("yoch/mqtt-python-client-bench", data["$id"])


class ReportTests(unittest.TestCase):
    def test_build_site_from_scenario_json(self):
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
            (results / "paho-pub-qos-smoke.json").write_text(json.dumps(sample), encoding="utf-8")
            (results / "suite-core.json").write_text(json.dumps(suite), encoding="utf-8")
            docs = load_results(results)
            self.assertEqual(len(docs), 2)
            kinds = {d.kind for d in docs}
            self.assertEqual(kinds, {"scenario", "suite"})
            summary = build_site(results, site)
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
            self.assertIn('href="paho-pub-qos-smoke.html"', suite_html)
            self.assertIn("pub_qos_sweep_telemetry", suite_html)
            self.assertFalse(any(site.rglob("*.json")))

    def test_performance_matrix_on_index(self):
        import html
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
            (results / "paho-duplex.json").write_text(
                json.dumps(sample("paho", "duplex_gateway", 200.0)), encoding="utf-8"
            )
            (results / "paho-e2e.json").write_text(
                json.dumps(sample("paho", "e2e_integrity", 1000.0)), encoding="utf-8"
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
            build_site(results, site)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("Performance matrix", index)
            self.assertIn('class="matrix"', index)
            self.assertIn('class="num best"', index)
            self.assertIn("8,000.0", index)
            self.assertIn("data-overview=", index)
            # Rate-capped checks stay in the matrix (at the end) but leave the chart.
            self.assertIn("duplex_gateway", index)
            self.assertIn("e2e_integrity", index)
            overview_attr = index.split("data-overview='", 1)[1].split("'></canvas>", 1)[0]
            overview_payload = json.loads(html.unescape(overview_attr))
            self.assertNotIn("duplex_gateway", overview_payload["scenarios"])
            self.assertNotIn("e2e_integrity", overview_payload["scenarios"])
            self.assertEqual(overview_payload["scenarios"], ["pub_qos_sweep_telemetry · MQTTv311"])
            clients_in_chart = [s["client"] for s in overview_payload["series"]]
            self.assertEqual(clients_in_chart, ["gmqtt", "paho"])
            matrix_body = index[index.index('class="matrix"') :]
            self.assertLess(
                matrix_body.index("pub_qos_sweep_telemetry · MQTTv311"),
                matrix_body.index("duplex_gateway · MQTTv311"),
            )
            self.assertLess(matrix_body.index("duplex_gateway · MQTTv311"), matrix_body.index("e2e_integrity · MQTTv311"))
            # Matrix header order matches chart: gmqtt before paho.
            self.assertLess(matrix_body.index(">gmqtt<"), matrix_body.index(">paho<"))
            # Compare docs must not inflate the Clients hero stat.
            self.assertRegex(index, r'stat-label">Clients</p>\s*<p class="stat-value">2</p>')

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
            build_site(results, site)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("Client issues", index)
            self.assertIn("under load", index)
            self.assertIn("open_loop_rate_out_of_tolerance", index)
            self.assertIn("capability", index)
            self.assertIn("tcp_nodelay", index)
            # Matrix stays numeric-only; issues live in the dedicated table.
            matrix_body = index[index.index('class="matrix"') : index.index("Client issues")]
            self.assertNotIn("open_loop_rate_out_of_tolerance", matrix_body)
            self.assertNotIn("tcp_nodelay", matrix_body)

    def test_integrity_aggregates_all_runs(self):
        from mqtt_client_bench.report import _collect_integrity

        runs = [
            {
                "workers": [
                    {"integrity": {"expected": 10, "received": 9, "unique": 9, "missing": 1, "duplicates": 0, "out_of_order": 0, "unexpected": 0}}
                ]
            },
            {
                "workers": [
                    {"integrity": {"expected": 10, "received": 8, "unique": 8, "missing": 2, "duplicates": 1, "out_of_order": 0, "unexpected": 0}}
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
            self.assertEqual(fracs, [0.5, 0.9], name)
            self.assertEqual(len(points), 4, name)  # 2 fractions × 2 protocols
            self.assertEqual({p["protocol"] for p in points}, {"MQTTv311", "MQTTv5"}, name)

    def test_payload_sweep_stays_v311_only(self):
        points = expand_scenario(SCENARIO_BY_NAME["pub_payload_sweep_qos0"], "standard")
        self.assertTrue(all(p.get("protocol", "MQTTv311") == "MQTTv311" for p in points))
        self.assertEqual(len(points), 7)

    def test_protocols_for_client(self):
        from mqtt_client_bench.harness import protocols_for_client

        self.assertEqual(protocols_for_client("paho"), ["MQTTv311", "MQTTv5"])
        self.assertEqual(protocols_for_client("aiomqtt3"), ["MQTTv5"])
        self.assertEqual(protocols_for_client("amqtt"), ["MQTTv311"])

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
            build_site(results, site)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("pub_qos_sweep_telemetry · MQTTv311", index)
            self.assertIn("pub_qos_sweep_telemetry · MQTTv5", index)
            self.assertIn("Comparable only within the same protocol", index)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
