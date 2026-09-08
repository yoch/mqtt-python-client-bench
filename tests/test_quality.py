from __future__ import annotations

import unittest

from mqtt_client_bench.quality import (
    DEFAULT_MAX_CATCH_UP_FRACTION,
    absolute_stationarity,
    classify_aa_quality,
    comparative_quality,
    run_temporal_quality,
)


def _run(slot: int, value: float, *, catch_up: int = 0, status: str = "valid") -> dict:
    return {
        "slot": slot,
        "status": status,
        "non_comparable": False,
        "comparison_value": value,
        "point": {"pacer_mode": "external"},
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
            "catch_up": {"events": catch_up},
            "pacer_lateness": {"p95": 3000.0},
        },
    }


def _doc(block_values: list[list[float]], *, bias: float = 0.0, stability: float = 0.0) -> dict:
    runs = []
    slot = 0
    for values in block_values:
        for value in values:
            runs.append(_run(slot, value))
            slot += 1
    blocks = len(block_values)
    return {
        "baseline_client": "mqttium",
        "candidate_client": "mqttium",
        "blocks_requested": blocks,
        "aa_blocks_requested": blocks,
        "aa_n_blocks": blocks,
        "aa_bias_pct": bias,
        "aa_stability_pct": stability,
        "aa_variant": {"protocol": "MQTTv311", "shared_load_fraction": 0.25},
        "points": [
            {
                "runs": runs,
                "verdict": {
                    "n_blocks": blocks,
                    "absolute_effect_pct": bias,
                    "pair_units": [1.0 for _ in range(max(1, blocks // 2))],
                },
            }
        ],
    }


class QualityContractTests(unittest.TestCase):
    def test_steady_same_client_control_is_usable_for_absolute_and_paired(self):
        doc = _doc(
            [
                [1.00, 1.01, 0.99, 1.00],
                [1.01, 1.00, 1.00, 0.99],
            ],
            bias=0.2,
            stability=0.5,
        )
        quality = classify_aa_quality(doc)
        self.assertTrue(quality["comparative"]["ok"])
        self.assertTrue(quality["stimulus"]["ok"])
        self.assertTrue(quality["absolute"]["ok"])
        self.assertIn("paired_ranking_control", quality["usable_for"])
        self.assertIn("absolute_baseline", quality["usable_for"])

    def test_common_mode_regime_shift_is_not_hidden_by_neutral_pair_units(self):
        # An ABBA ratio can be perfectly neutral when all four slots in block 2
        # move together.  That is usable evidence about a paired estimator only
        # if the stimulus is clean; it is not an absolute baseline.
        doc = _doc(
            [
                [1.0, 1.0, 1.0, 1.0],
                [1.2, 1.2, 1.2, 1.2],
            ],
            bias=0.0,
            stability=0.0,
        )
        quality = classify_aa_quality(doc)
        self.assertTrue(quality["comparative"]["ok"])
        self.assertTrue(quality["stimulus"]["ok"])
        self.assertFalse(quality["absolute"]["ok"])
        self.assertGreater(quality["absolute"]["max_log_deviation_pct"], 9.0)
        self.assertIn("paired_ranking_control", quality["usable_for"])
        self.assertNotIn("absolute_baseline", quality["usable_for"])

    def test_catch_up_over_budget_invalidates_temporal_shape_even_with_all_tokens(self):
        run = _run(0, 1.0, catch_up=3)
        quality = run_temporal_quality(run)
        self.assertFalse(quality["ok"])
        self.assertGreater(quality["catch_up_fraction"], DEFAULT_MAX_CATCH_UP_FRACTION)
        self.assertTrue(any("catch_up_fraction" in reason for reason in quality["reasons"]))

    def test_catch_up_at_budget_is_accepted(self):
        run = _run(0, 1.0, catch_up=2)
        quality = run_temporal_quality(run)
        self.assertTrue(quality["ok"])

    def test_token_integrity_failure_remains_fail_closed(self):
        run = _run(0, 1.0)
        run["pacing"]["tokens_received"] = 999
        run["pacing"]["stimulus_valid"] = False
        run["pacing"]["stimulus_invalid_reasons"] = [
            "pacer_stimulus_invalid:suffix_loss"
        ]
        quality = run_temporal_quality(run)
        self.assertFalse(quality["ok"])
        self.assertIn("pacer_stimulus_invalid:suffix_loss", quality["reasons"])

    def test_temporally_bad_run_makes_absolute_block_incomplete(self):
        doc = _doc(
            [
                [1.0, 1.0, 1.0, 1.0],
                [1.0, 1.0, 1.0, 1.0],
            ]
        )
        doc["points"][0]["runs"][5]["pacing"]["catch_up"]["events"] = 10
        absolute = absolute_stationarity(doc)
        self.assertFalse(absolute["ok"])
        self.assertEqual(absolute["complete_blocks"], 1)
        self.assertEqual(absolute["points"][0]["incomplete_blocks"], [1])

    def test_comparative_quality_separates_neutrality_from_completeness(self):
        doc = _doc(
            [[1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]],
            bias=0.5,
            stability=1.0,
        )
        doc["aa_n_blocks"] = 1
        comparative = comparative_quality(doc)
        self.assertTrue(comparative["neutrality_ok"])
        self.assertFalse(comparative["complete"])
        self.assertFalse(comparative["ok"])
        self.assertIn("incomplete_blocks:1/2", comparative["reasons"])

    def test_pair_stability_over_budget_fails_even_when_absolute_is_steady(self):
        doc = _doc(
            [[1.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]],
            bias=0.0,
            stability=4.0,
        )
        quality = classify_aa_quality(doc)
        self.assertFalse(quality["comparative"]["ok"])
        self.assertTrue(quality["absolute"]["ok"])
        self.assertNotIn("paired_ranking_control", quality["usable_for"])
        self.assertNotIn("absolute_baseline", quality["usable_for"])


if __name__ == "__main__":
    unittest.main()
