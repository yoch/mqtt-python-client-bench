"""Unit tests for MQTT client benchmark helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mqtt_client_bench.adapters.registry import list_clients, unsupported_for_client  # noqa: E402
from mqtt_client_bench.harness import unsupported_features  # noqa: E402
from mqtt_client_bench.loadgen import interval_for_rate, nominal_rate, parse_emqtt_output  # noqa: E402
from mqtt_client_bench.metrics import (  # noqa: E402
    abba_order,
    compare_verdict,
    integrity_counts,
    latency_summary,
    median,
    percentile,
    sanitize_number,
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

    def test_compare_inconclusive_on_noise(self):
        baseline = [100.0] * 8
        candidate = [101.0] * 8
        verdict = compare_verdict(baseline, candidate, min_effect_pct=3.0)
        self.assertEqual(verdict["verdict"], "inconclusive")

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
        # Supported values must not be flagged for paho.
        self.assertEqual(unsupported_features({"properties_profile": "realistic", "connect_mode": "tcp_serial"}), [])


class AdapterRegistryTests(unittest.TestCase):
    def test_list_clients(self):
        names = {row["name"] for row in list_clients()}
        self.assertEqual(names, {"paho", "gmqtt", "aiomqtt", "amqtt"})

    def test_implemented_clients_accept_core_points(self):
        point = {"payload": "telemetry256", "qos_publish": 0, "protocol": "MQTTv311"}
        for name in ("paho", "gmqtt", "aiomqtt", "amqtt"):
            missing = unsupported_for_client(name, point)
            self.assertEqual(missing, [], name)

    def test_callback_matching_capability(self):
        point = {"callback_filters": 64, "qos_subscribe": 0}
        for name in ("paho", "gmqtt", "aiomqtt", "amqtt"):
            self.assertEqual(unsupported_for_client(name, point), [], name)

    def test_amqtt_refuses_v5_properties_profile(self):
        point = {"protocol": "MQTTv5", "properties_profile": "realistic", "qos_publish": 0}
        missing = unsupported_for_client("amqtt", point)
        self.assertIn("properties_profile:realistic", missing)
        self.assertEqual(unsupported_for_client("gmqtt", point), [])
        self.assertEqual(unsupported_for_client("aiomqtt", point), [])

    def test_client_identities(self):
        from mqtt_client_bench.adapters.registry import adapter_identity, get_adapter_class

        for name in ("paho", "gmqtt", "aiomqtt", "amqtt"):
            caps = get_adapter_class(name).capabilities()
            self.assertEqual(caps.unimplemented, [], name)
            info = adapter_identity(name)
            self.assertEqual(info["client"], name)
            self.assertIsNotNone(info.get("client_module"), name)
            self.assertNotEqual(info.get("status"), "stub", name)


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

        for name in ("gmqtt", "aiomqtt", "amqtt"):
            adapter = create_adapter(name, client_id=f"test-{name}")
            self.assertEqual(adapter.MQTT_ERR_SUCCESS, 0)
            self.assertTrue(hasattr(adapter, "publish"))
            self.assertTrue(hasattr(adapter, "subscribe"))
            self.assertIsNone(adapter.build_publish_properties("none"))


class ScenarioTests(unittest.TestCase):
    def test_core_catalogue(self):
        core = list_scenarios("core")
        self.assertGreaterEqual(len(core), 5)
        names = {s.name for s in core}
        self.assertIn("pub_qos_sweep_telemetry", names)

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
                                }
                            ],
                        }
                    ],
                    "summary": {"n": 1, "median": 12000.5, "mad": 0, "min": 12000.5, "max": 12000.5, "mean": 12000.5},
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            results = root / "results"
            site = root / "site"
            results.mkdir()
            (results / "paho-pub-qos-smoke.json").write_text(json.dumps(sample), encoding="utf-8")
            docs = load_results(results)
            self.assertEqual(len(docs), 1)
            self.assertEqual(docs[0].kind, "scenario")
            self.assertEqual(docs[0].status, "valid")
            summary = build_site(results, site)
            self.assertEqual(summary["results"], 1)
            index = (site / "index.html").read_text(encoding="utf-8")
            self.assertIn("pub_qos_sweep_telemetry", index)
            self.assertIn("paho", index)
            detail = next((site / "runs").glob("*.html")).read_text(encoding="utf-8")
            self.assertIn("12000.5", detail.replace(",", ""))
            self.assertFalse(any(site.rglob("*.json")))


if __name__ == "__main__":
    raise SystemExit(unittest.main())
