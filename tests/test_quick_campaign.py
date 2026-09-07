from pathlib import Path
import unittest


class QuickCampaignTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.script = Path("scripts/run_pairwise_rtt_quick.sh").read_text(encoding="utf-8")

    def test_quick_campaign_keeps_external_pacer(self) -> None:
        self.assertIn('export PACER_MODE="${PACER_MODE:-external}"', self.script)
        self.assertIn('if [ "$PACER_MODE" != "external" ]; then', self.script)

    def test_quick_campaign_has_small_replication_budget(self) -> None:
        self.assertIn('export MATRIX_RUNS="${MATRIX_RUNS:-2}"', self.script)
        self.assertIn('export AA_BLOCKS="${AA_BLOCKS:-4}"', self.script)
        self.assertIn('export ABBA_BLOCKS="${ABBA_BLOCKS:-2}"', self.script)
        self.assertIn('export AA_VARIANT_INDEXES="${AA_VARIANT_INDEXES:-4}"', self.script)

    def test_quick_aa_is_opt_in_and_never_a_publication_gate(self) -> None:
        self.assertIn('export RUN_AA="${RUN_AA:-0}"', self.script)
        self.assertIn('export AA_CONTROL_ENFORCE="${AA_CONTROL_ENFORCE:-0}"', self.script)
        self.assertIn('export PROFILE="${PROFILE:-smoke}"', self.script)

    def test_quick_campaign_runs_both_pairs(self) -> None:
        self.assertIn('export RUN_ASYNCIO_PAIR="${RUN_ASYNCIO_PAIR:-1}"', self.script)
        self.assertIn('export RUN_SYNC_REFERENCE_PAIR="${RUN_SYNC_REFERENCE_PAIR:-1}"', self.script)
        self.assertIn('export RUN_ABBA="${RUN_ABBA:-1}"', self.script)
        self.assertIn('export RUN_LOAD_MATRIX="${RUN_LOAD_MATRIX:-0}"', self.script)

    def test_quick_defaults_to_25_and_50_percent_for_both_protocols(self) -> None:
        self.assertIn('export ABBA_VARIANT_INDEXES="${ABBA_VARIANT_INDEXES:-0,1,2,3}"', self.script)


if __name__ == "__main__":
    unittest.main()
