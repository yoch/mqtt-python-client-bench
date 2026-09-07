from pathlib import Path
import unittest


class DailyCampaignTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.script = Path("scripts/run_pairwise_rtt_daily.sh").read_text(encoding="utf-8")

    def test_daily_keeps_external_pacer(self) -> None:
        self.assertIn('export PACER_MODE="${PACER_MODE:-external}"', self.script)

    def test_daily_skips_aa_by_default(self) -> None:
        self.assertIn('export RUN_AA="${RUN_AA:-0}"', self.script)

    def test_daily_uses_two_balanced_blocks(self) -> None:
        self.assertIn('export ABBA_BLOCKS="${ABBA_BLOCKS:-2}"', self.script)

    def test_daily_only_samples_25_and_75_v311(self) -> None:
        self.assertIn('export ABBA_VARIANT_INDEXES="${ABBA_VARIANT_INDEXES:-0,4}"', self.script)

    def test_daily_delegates_to_quick_campaign(self) -> None:
        self.assertIn('run_pairwise_rtt_quick.sh', self.script)


if __name__ == "__main__":
    unittest.main()
