from __future__ import annotations

import unittest

from mqtt_client_bench.metrics import abba_block_records, median, percentile


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


if __name__ == "__main__":
    unittest.main()
