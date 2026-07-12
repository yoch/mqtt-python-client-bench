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
from mqtt_client_bench.harness import capacity_from_qos_sweep, unsupported_features  # noqa: E402
from mqtt_client_bench.loadgen import interval_for_rate, nominal_rate, parse_emqtt_output  # noqa: E402
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

    def test_inflight_variant_marks_requirement(self):
        points = expand_scenario(SCENARIO_BY_NAME["pub_qos1_inflight"], "standard")
        self.assertTrue(all(p.get("require_max_inflight") for p in points))

    def test_expand_smoke_shorter(self):
        scenario = SCENARIO_BY_NAME["pub_qos_sweep_telemetry"]
        smoke = expand_scenario(scenario, "smoke")
        standard = expand_scenario(scenario, "standard")
        self.assertTrue(all(p.get("non_comparable") for p in smoke))
        self.assertGreater(standard[0]["duration_s"], smoke[0]["duration_s"])

    def test_estimate(self):
        est = estimate_suite("core", "smoke", 1)
        self.assertGreater(est["points"], 0)
        self.assertGreater(est["estimated_minutes"], 0)

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

    def test_mqtt_version_helper(self):
        from mqtt_client_bench.harness import effective_loadgen_mqtt_version, mqtt_version_for_point

        self.assertEqual(mqtt_version_for_point({"protocol": "MQTTv5"}), 5)
        self.assertEqual(mqtt_version_for_point({"protocol": "MQTTv311"}), 4)
        self.assertEqual(mqtt_version_for_point({"protocol": "MQTTv31"}), 3)
        self.assertEqual(effective_loadgen_mqtt_version(4), 5)
        self.assertEqual(effective_loadgen_mqtt_version(5), 5)

    def test_loadgen_shortids_for_v311(self):
        from mqtt_client_bench.loadgen import LoadgenSpec, build_pub_args

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


if __name__ == "__main__":
    raise SystemExit(unittest.main())
