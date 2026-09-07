from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mqtt_client_bench.version_compare import arm_path, compare_versions, source_provenance


class VersionCompareTests(unittest.TestCase):
    def test_arm_path_never_collapses_same_client_sources(self):
        self.assertEqual(arm_path("A", "/old", "/new"), "/old")
        self.assertEqual(arm_path("B", "/old", "/new"), "/new")
        with self.assertRaises(ValueError):
            arm_path("X", "/old", "/new")

    def test_source_provenance_rejects_missing_directory(self):
        with self.assertRaises(ValueError):
            source_provenance("/definitely/not/a/checkout")

    def test_compare_versions_interleaves_distinct_paths_and_never_attaches_aa(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = str(Path(tmp) / "old")
            new = str(Path(tmp) / "new")
            Path(old).mkdir()
            Path(new).mkdir()
            seen_paths = []

            def fake_run_point(point, *, client, client_path, **kwargs):
                seen_paths.append(client_path)
                # A small deterministic version effect. comparison_value is
                # patched below so this test exercises orchestration, not RTT parsing.
                return {
                    "status": "valid",
                    "non_comparable": False,
                    "point": dict(point),
                    "client": client,
                    "client_path": client_path,
                    "synthetic_value": 1.0 if client_path == old else 0.9,
                    "pacing": {
                        "mode": "external",
                        "tokens_scheduled": 1000,
                        "tokens_emitted": 1000,
                        "tokens_received": 1000,
                        "tokens_expected_in_measure_window": 1000,
                        "sequence_gaps": 0,
                        "token_send_failures": 0,
                        "stimulus_valid": True,
                        "stimulus_invalid_reasons": [],
                        "catch_up": {"events": 0},
                    },
                }

            def fake_observed(result, scenario):
                return {
                    "comparison_metric": "latency_p50_ms",
                    "comparison_direction": "lower_is_better",
                    "value": result["synthetic_value"],
                    "p95_ms": result["synthetic_value"] * 1.1,
                    "p99_ms": result["synthetic_value"] * 1.2,
                }

            sources = {
                "baseline": {
                    "path": old,
                    "git_sha": "a" * 40,
                    "git_dirty": False,
                    "adapter_identity": {"client": "mqttium", "client_version": "old"},
                },
                "candidate": {
                    "path": new,
                    "git_sha": "b" * 40,
                    "git_dirty": False,
                    "adapter_identity": {"client": "mqttium", "client_version": "new"},
                },
            }
            with (
                patch("mqtt_client_bench.version_compare.validate_version_arms", return_value=sources),
                patch("mqtt_client_bench.version_compare.allocate_cpuset", return_value={"sut": "0", "broker": "1", "loadgen": "2", "orch": "3"}),
                patch("mqtt_client_bench.version_compare.pin_current_process"),
                patch("mqtt_client_bench.version_compare.parse_broker_endpoint", return_value=("127.0.0.1", 1883)),
                patch("mqtt_client_bench.version_compare.wait_for_broker"),
                patch("mqtt_client_bench.version_compare.resolve_external_broker_pid", return_value=123),
                patch("mqtt_client_bench.version_compare.resolve_host_profile", return_value=None),
                patch("mqtt_client_bench.version_compare.run_point", side_effect=fake_run_point),
                patch("mqtt_client_bench.version_compare.comparison_value", side_effect=fake_observed),
                patch("mqtt_client_bench.version_compare.time.sleep"),
            ):
                payload = compare_versions(
                    client="mqttium",
                    baseline_path=old,
                    candidate_path=new,
                    scenario="application_rtt_fixed_rate",
                    target_rate=3942,
                    broker="127.0.0.1:1883",
                    broker_pid=123,
                    blocks=2,
                    profile="standard",
                    variant_index=0,
                    pacer_mode="external",
                )

            self.assertEqual(payload["comparison_kind"], "same_client_version_ab")
            self.assertEqual(payload["target_rate_frozen"], 3942.0)
            self.assertIsNone(payload["aa_control"])
            self.assertIn("separate_same-source_aa", payload["aa_control_reason"])
            self.assertEqual(len(seen_paths), 8)
            self.assertEqual(seen_paths[:4], [old, new, new, old])
            self.assertEqual(seen_paths[4:], [new, old, old, new])
            self.assertEqual(payload["baseline_source"]["git_sha"], "a" * 40)
            self.assertEqual(payload["candidate_source"]["git_sha"], "b" * 40)
            self.assertTrue(all(run["version_source"]["path"] == run["client_path"] for run in payload["runs"]))


if __name__ == "__main__":
    unittest.main()
