from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mqtt_client_bench import layout_runner, process_layout


class ProcessLayoutTests(unittest.TestCase):
    def test_disabled_personality_requires_addr_no_randomize(self) -> None:
        completed = subprocess.CompletedProcess([], 0, stdout="00040000\n", stderr="")
        with patch.object(process_layout, "_setarch", return_value="/usr/bin/setarch"), patch.object(
            process_layout.subprocess, "run", return_value=completed
        ) as run:
            personality = process_layout._disabled_personality()
        self.assertEqual(personality, process_layout.ADDR_NO_RANDOMIZE)
        command = run.call_args.args[0]
        self.assertEqual(command[:3], ["/usr/bin/setarch", process_layout.platform.machine(), "-R"])

    def test_disabled_personality_fails_closed_when_flag_is_missing(self) -> None:
        completed = subprocess.CompletedProcess([], 0, stdout="00000000\n", stderr="")
        with patch.object(process_layout, "_setarch", return_value="/usr/bin/setarch"), patch.object(
            process_layout.subprocess, "run", return_value=completed
        ):
            with self.assertRaisesRegex(RuntimeError, "ADDR_NO_RANDOMIZE"):
                process_layout._disabled_personality()

    def test_system_mode_does_not_patch_worker_python(self) -> None:
        from mqtt_client_bench import harness

        original = harness._python
        with process_layout.worker_process_layout("system") as metadata:
            self.assertIs(harness._python, original)
            self.assertEqual(metadata["worker_aslr"], "system")
            self.assertEqual(metadata["orchestrator_aslr"], "unchanged")
            self.assertTrue(metadata["representative_aslr"])
            self.assertTrue(metadata["publication_eligible"])
            self.assertFalse(metadata["experimental_control"])
        self.assertIs(harness._python, original)

    def test_disabled_mode_patches_only_worker_launcher_and_restores(self) -> None:
        from mqtt_client_bench import harness

        original = harness._python
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            process_layout, "_disabled_personality", return_value=process_layout.ADDR_NO_RANDOMIZE
        ), patch.object(
            process_layout, "_write_disabled_python_wrapper", return_value=Path(tmp) / "python-no-aslr"
        ):
            with process_layout.worker_process_layout("disabled") as metadata:
                self.assertIsNot(harness._python, original)
                self.assertTrue(str(harness._python()).endswith("python-no-aslr"))
                self.assertTrue(metadata["addr_no_randomize"])
                self.assertEqual(metadata["scope"], "python_roles_spawned_by_harness._spawn_role")
                self.assertTrue(metadata["experimental_control"])
                self.assertFalse(metadata["representative_aslr"])
                self.assertFalse(metadata["publication_eligible"])
        self.assertIs(harness._python, original)

    def test_disabled_annotation_fails_closed_for_regular_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "scenario": "x",
                        "results": [{"runs": [{"run_id": "12345678", "status": "valid"}]}],
                    }
                ),
                encoding="utf-8",
            )
            metadata = {"worker_aslr": "disabled", "publication_eligible": False}
            self.assertTrue(process_layout.annotate_result(result, metadata))
            payload = json.loads(result.read_text(encoding="utf-8"))
            self.assertFalse(payload["publication_eligible"])
            self.assertTrue(payload["diagnostic_control"])
            run = payload["results"][0]["runs"][0]
            self.assertTrue(run["non_comparable"])
            self.assertIn(process_layout.DIAGNOSTIC_REASON, run["reasons"])

    def test_disabled_annotation_fails_closed_for_version_compare_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "runs": [{"run_id": "12345678", "status": "valid", "reasons": ["x"]}],
                    }
                ),
                encoding="utf-8",
            )
            metadata = {"worker_aslr": "disabled", "publication_eligible": False}
            self.assertTrue(process_layout.annotate_result(result, metadata))
            payload = json.loads(result.read_text(encoding="utf-8"))
            run = payload["runs"][0]
            self.assertTrue(run["non_comparable"])
            self.assertEqual(run["reasons"], ["x", process_layout.DIAGNOSTIC_REASON])

    def test_system_annotation_preserves_comparability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "result.json"
            other = Path(tmp) / "other.json"
            result.write_text(
                json.dumps({"schema_version": 1, "results": [{"runs": [{"status": "valid"}]}]}),
                encoding="utf-8",
            )
            other.write_text(json.dumps({"not_a_result": True}), encoding="utf-8")
            metadata = {"worker_aslr": "system", "publication_eligible": True}
            self.assertTrue(process_layout.annotate_result(result, metadata))
            self.assertFalse(process_layout.annotate_result(other, metadata))
            payload = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(payload["role_process_layout"], metadata)
            self.assertNotIn("diagnostic_control", payload)
            self.assertNotIn("non_comparable", payload["results"][0]["runs"][0])

    def test_layout_runner_only_annotates_changed_output_dir_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old.json"
            new = root / "new.json"
            old.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
            snapshots = layout_runner._snapshot_output_dirs(["--output-dir", str(root)])
            new.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
            count = layout_runner._annotate_outputs(
                ["--output-dir", str(root)],
                {"worker_aslr": "disabled", "publication_eligible": False},
                snapshots,
            )
            self.assertEqual(count, 1)
            self.assertNotIn(
                "role_process_layout", json.loads(old.read_text(encoding="utf-8"))
            )
            new_payload = json.loads(new.read_text(encoding="utf-8"))
            self.assertEqual(new_payload["role_process_layout"]["worker_aslr"], "disabled")
            self.assertFalse(new_payload["publication_eligible"])

    def test_flag_values_accept_split_and_equals_forms(self) -> None:
        self.assertEqual(
            layout_runner._flag_values(
                ["--output", "a.json", "--output-dir=b", "--output=c.json"], "--output"
            ),
            ["a.json", "c.json"],
        )
        self.assertEqual(
            layout_runner._flag_values(["--output-dir=b"], "--output-dir"), ["b"]
        )


if __name__ == "__main__":
    unittest.main()
