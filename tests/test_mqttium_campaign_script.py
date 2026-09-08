from pathlib import Path
import unittest


class MqttiumCampaignScriptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.script = Path("scripts/run_mqttium_campaign.sh").read_text(encoding="utf-8")

    def test_accepts_exact_git_sha_and_client_path(self) -> None:
        self.assertIn('MQTTIUM_GIT_SHA', self.script)
        self.assertIn('--client-path', self.script)
        self.assertIn('git -C "$SRC_DIR" checkout --quiet "${MQTTIUM_GIT_SHA}"', self.script)

    def test_labels_isolate_results_and_calibration(self) -> None:
        self.assertIn('MQTTIUM_RUN_LABEL', self.script)
        self.assertIn('calibrations/${MQTTIUM_RUN_LABEL}-mqttium-load.json', self.script)

    def test_compat_is_opt_in(self) -> None:
        self.assertIn('MQTTIUM_COMPAT:-0', self.script)
        self.assertIn('CLIENTS=(mqttium)', self.script)

    def test_non_reference_hosts_use_host_specific_results_dir(self) -> None:
        self.assertIn('results_dir_for(resolve_host_profile())', self.script)


if __name__ == "__main__":
    unittest.main()
