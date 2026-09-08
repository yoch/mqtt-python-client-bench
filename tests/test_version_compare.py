from __future__ import annotations

import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from mqtt_client_bench.version_compare import (
    arm_path,
    compare_versions,
    retryable_run_reasons,
    source_provenance,
)


class VersionCompareTests(unittest.TestCase):
    def test_arm_path_never_collapses_same_client_sources(self):
        self.assertEqual(arm_path("A", "/old", "/new"), "/old")
        self.assertEqual(arm_path("B", "/old", "/new"), "/new")
        with self.assertRaises(ValueError):
            arm_path("X", "/old", "/new")

    def test_source_provenance_rejects_missing_directory(self):
        with self.assertRaises(ValueError):
            source_provenance("/definitely/not/a/checkout")

    def _sources(self, old: str, new: str) -> dict:
        return {
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

    @staticmethod
    def _pacing(*, catch_up: int = 0, n: int = 1000) -> dict:
        return {
            "mode": "external",
            "tokens_scheduled": n,
            "tokens_emitted": n,
            "tokens_received": n,
            "tokens_expected_in_measure_window": n,
            "sequence_gaps": 0,
            "token_send_failures": 0,
            "stimulus_valid": True,
            "stimulus_invalid_reasons": [],
            "catch_up": {"events": catch_up},
        }

    @staticmethod
    def _observed(result, scenario):
        return {
            "comparison_metric": "latency_p50_ms",
            "comparison_direction": "lower_is_better",
            "value": result["synthetic_value"],
            "p95_ms": result["synthetic_value"] * 1.1,
            "p99_ms": result["synthetic_value"] * 1.2,
        }

    def _patch_runtime(self, *, sources, run_point):
        stack = ExitStack()
        stack.enter_context(
            patch("mqtt_client_bench.version_compare.validate_version_arms", return_value=sources)
        )
        stack.enter_context(
            patch(
                "mqtt_client_bench.version_compare.allocate_cpuset",
                return_value={"sut": "0", "broker": "1", "loadgen": "2", "orch": "3"},
            )
        )
        stack.enter_context(patch("mqtt_client_bench.version_compare.pin_current_process"))
        stack.enter_context(
            patch(
                "mqtt_client_bench.version_compare.parse_broker_endpoint",
                return_value=("127.0.0.1", 1883),
            )
        )
        stack.enter_context(patch("mqtt_client_bench.version_compare.wait_for_broker"))
        stack.enter_context(
            patch("mqtt_client_bench.version_compare.resolve_external_broker_pid", return_value=123)
        )
        stack.enter_context(
            patch("mqtt_client_bench.version_compare.resolve_host_profile", return_value=None)
        )
        stack.enter_context(
            patch("mqtt_client_bench.version_compare.run_point", side_effect=run_point)
        )
        stack.enter_context(
            patch("mqtt_client_bench.version_compare.comparison_value", side_effect=self._observed)
        )
        stack.enter_context(patch("mqtt_client_bench.version_compare.time.sleep"))
        return stack

    def _compare(self, old: str, new: str, *, sources: dict, run_point, max_block_retries: int = 1):
        with self._patch_runtime(sources=sources, run_point=run_point):
            return compare_versions(
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
                max_block_retries=max_block_retries,
            )

    def test_compare_versions_interleaves_distinct_paths_and_never_attaches_aa(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = str(Path(tmp) / "old")
            new = str(Path(tmp) / "new")
            Path(old).mkdir()
            Path(new).mkdir()
            seen_paths = []

            def fake_run_point(point, *, client, client_path, **kwargs):
                seen_paths.append(client_path)
                return {
                    "status": "valid",
                    "non_comparable": False,
                    "reasons": [],
                    "point": dict(point),
                    "client": client,
                    "client_path": client_path,
                    "synthetic_value": 1.0 if client_path == old else 0.9,
                    "pacing": self._pacing(),
                }

            payload = self._compare(
                old,
                new,
                sources=self._sources(old, new),
                run_point=fake_run_point,
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
            self.assertTrue(
                all(run["version_source"]["path"] == run["client_path"] for run in payload["runs"])
            )
            self.assertTrue(payload["qualification"]["ok"])
            self.assertTrue(payload["verdict"]["version_ab_qualified"])

    def test_excessive_catchup_retries_whole_block_and_retains_first_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = str(Path(tmp) / "old")
            new = str(Path(tmp) / "new")
            Path(old).mkdir()
            Path(new).mkdir()
            calls = 0

            def fake_run_point(point, *, client, client_path, **kwargs):
                nonlocal calls
                call = calls
                calls += 1
                # Only slot 1 of the first attempt is temporally invalid:
                # 5/1000 = 0.5% > the declared 0.2% catch-up budget.
                catch_up = 5 if call == 1 else 0
                return {
                    "status": "valid",
                    "non_comparable": False,
                    "reasons": [],
                    "point": dict(point),
                    "client": client,
                    "client_path": client_path,
                    "synthetic_value": 1.0 if client_path == old else 0.9,
                    "pacing": self._pacing(catch_up=catch_up),
                }

            payload = self._compare(
                old,
                new,
                sources=self._sources(old, new),
                run_point=fake_run_point,
            )

            # 4-slot failed attempt + 4-slot replacement + second 4-slot block.
            self.assertEqual(calls, 12)
            self.assertEqual(len(payload["slot_rates"]), 8)
            self.assertEqual(len(payload["runs"]), 12)
            first_attempt = payload["block_attempts"][0]
            replacement = payload["block_attempts"][1]
            self.assertTrue(first_attempt["retried"])
            self.assertFalse(first_attempt["selected_for_verdict"])
            self.assertTrue(any("catch_up_fraction" in r for r in first_attempt["retryable_reasons"]))
            self.assertFalse(replacement["retried"])
            self.assertTrue(replacement["selected_for_verdict"])
            self.assertTrue(all(not run["active_for_verdict"] for run in payload["runs"][:4]))
            self.assertTrue(all(run["active_for_verdict"] for run in payload["runs"][4:]))
            self.assertTrue(payload["qualification"]["ok"])
            self.assertEqual(payload["qualification"]["retry_exhausted_blocks"], [])

    def test_client_performance_failure_never_requests_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = str(Path(tmp) / "old")
            new = str(Path(tmp) / "new")
            Path(old).mkdir()
            Path(new).mkdir()
            calls = 0

            def fake_run_point(point, *, client, client_path, **kwargs):
                nonlocal calls
                call = calls
                calls += 1
                failed = call == 1
                return {
                    "status": "inconclusive" if failed else "valid",
                    "non_comparable": False,
                    "reasons": ["open_loop_backpressure_misses"] if failed else [],
                    "point": dict(point),
                    "client": client,
                    "client_path": client_path,
                    "synthetic_value": 1.0 if client_path == old else 0.9,
                    "pacing": self._pacing(),
                }

            payload = self._compare(
                old,
                new,
                sources=self._sources(old, new),
                run_point=fake_run_point,
            )

            self.assertEqual(calls, 8)  # no retry at all
            self.assertFalse(payload["qualification"]["ok"])
            self.assertEqual(payload["verdict"]["verdict"], "inconclusive")
            self.assertEqual(payload["block_attempts"][0]["retryable_reasons"], [])
            self.assertFalse(payload["block_attempts"][0]["retried"])

    def test_retryable_reasons_are_mechanism_only(self):
        result = {
            "status": "inconclusive",
            "reasons": ["open_loop_backpressure_misses", "host_busy_at_start:5.2"],
            "pacing": self._pacing(),
        }
        reasons = retryable_run_reasons(result)
        self.assertIn("host_busy_at_start:5.2", reasons)
        self.assertNotIn("open_loop_backpressure_misses", reasons)


if __name__ == "__main__":
    unittest.main()
