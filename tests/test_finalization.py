from __future__ import annotations

import unittest
from pathlib import Path

from mqtt_client_bench.metrics import abba_block_records, median, percentile


ROOT = Path(__file__).resolve().parents[1]


class FinalReducerTests(unittest.TestCase):
    def test_even_sample_median_is_conventional(self) -> None:
        self.assertEqual(median([1.0, 3.0]), 2.0)
        self.assertEqual(median([1.0, 2.0, 3.0, 4.0]), 2.5)

    def test_latency_percentile_remains_nearest_rank(self) -> None:
        # Per-message latency p50 semantics are intentionally unchanged.
        self.assertEqual(percentile([1.0, 3.0], 50.0), 1.0)

    def test_abba_block_uses_both_observations_per_arm(self) -> None:
        # ABBA: A=[100,100], B=[90,130].  The old nearest-rank median reduced
        # B to 90 and produced a false -10% effect.  The conventional median
        # is 110, so the block ratio is +10% and both B observations matter.
        records = abba_block_records(
            ["A", "B", "B", "A"],
            [100.0, 90.0, 130.0, 100.0],
        )
        self.assertEqual(len(records), 1)
        self.assertAlmostEqual(records[0]["a_median"], 100.0)
        self.assertAlmostEqual(records[0]["b_median"], 110.0)
        self.assertAlmostEqual(records[0]["ratio"], 1.1)

    def test_baab_block_uses_both_observations_per_arm(self) -> None:
        records = abba_block_records(
            ["B", "A", "A", "B"],
            [90.0, 100.0, 100.0, 130.0],
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["design"], "BAAB")
        self.assertAlmostEqual(records[0]["ratio"], 1.1)


class OfficialPairwisePacingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.script = (ROOT / "scripts" / "run_pairwise_rtt_campaign.sh").read_text(
            encoding="utf-8"
        )

    def test_standard_defaults_to_external_pacer(self) -> None:
        self.assertIn('PACER_MODE="${PACER_MODE:-external}"', self.script)
        self.assertIn(
            'standard pairwise RTT requires PACER_MODE=external', self.script
        )

    def test_standard_skips_nonpaired_matched_load_matrix(self) -> None:
        # Match the actual standard guard, not the earlier AA default branch
        # which has the same shell condition.
        self.assertIn(
            'if [ "$PROFILE" = "standard" ]; then\n'
            '  if [ "$PACER_MODE" != "external" ]; then',
            self.script,
        )
        self.assertIn(
            '  RUN_LOAD_MATRIX=0\nfi\n\nif [ "${CLIENTS:-}"', self.script
        )
        self.assertIn("rtt_capacity_qos1", self.script)

    def test_every_official_compare_receives_explicit_pacer_mode(self) -> None:
        compare_calls = self.script.count("python -m mqtt_client_bench.run compare")
        pacer_args = self.script.count('--pacer-mode "$PACER_MODE"')
        self.assertGreaterEqual(compare_calls, 4)
        self.assertEqual(pacer_args, compare_calls)


if __name__ == "__main__":
    unittest.main()
