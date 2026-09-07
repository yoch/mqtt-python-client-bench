from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mqtt_client_bench import aslr_runner


class AslrRunnerTests(unittest.TestCase):
    def test_output_path_accepts_split_and_equals_forms(self) -> None:
        self.assertEqual(aslr_runner._output_path(["--output", "result.json"]), Path("result.json"))
        self.assertEqual(aslr_runner._output_path(["--output=result.json"]), Path("result.json"))
        self.assertIsNone(aslr_runner._output_path(["--blocks", "2"]))

    def test_disabled_personality_requires_addr_no_randomize(self) -> None:
        completed = subprocess.CompletedProcess([], 0, stdout="00040000\n", stderr="")
        with patch.object(aslr_runner, "_setarch", return_value="/usr/bin/setarch"), patch.object(
            aslr_runner.subprocess, "run", return_value=completed
        ) as run:
            personality = aslr_runner._disabled_personality()
        self.assertEqual(personality, aslr_runner.ADDR_NO_RANDOMIZE)
        command = run.call_args.args[0]
        self.assertEqual(command[:3], ["/usr/bin/setarch", aslr_runner.platform.machine(), "-R"])

    def test_disabled_personality_fails_closed_when_flag_is_missing(self) -> None:
        completed = subprocess.CompletedProcess([], 0, stdout="00000000\n", stderr="")
        with patch.object(aslr_runner, "_setarch", return_value="/usr/bin/setarch"), patch.object(
            aslr_runner.subprocess, "run", return_value=completed
        ):
            with self.assertRaisesRegex(RuntimeError, "ADDR_NO_RANDOMIZE"):
                aslr_runner._disabled_personality()

    def test_annotation_records_worker_only_control(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "result.json"
            path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
            aslr_runner._annotate_output(path, "disabled", aslr_runner.ADDR_NO_RANDOMIZE)
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["role_process_layout"]["aslr_mode"], "disabled")
        self.assertEqual(payload["role_process_layout"]["orchestrator_aslr"], "unchanged")
        self.assertEqual(payload["role_process_layout"]["verified_worker_personality"], 0x40000)


if __name__ == "__main__":
    unittest.main()
