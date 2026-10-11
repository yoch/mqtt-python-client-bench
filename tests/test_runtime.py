"""Protect interpreter isolation and campaign reproducibility."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from mqtt_client_bench.bench import campaign, catalog, cli, envs, runtime
from mqtt_client_bench.report import data


def build(version="3.15.0", **changes):
    identity = dict(runtime.local_identity(), version=version)
    identity.update(changes)
    return runtime.Runtime(Path("/test/python"), identity)


class RuntimeTests(unittest.TestCase):
    def test_versions_and_builds_cannot_reuse_environments(self):
        normal = build()
        paths = {
            envs.env_dir("paho", r)
            for r in (
                normal,
                build("3.14.8"),
                build(build_gil_disabled=True),
                build(jit_enabled=True),
            )
        }
        self.assertEqual(len(paths), 4)
        self.assertNotEqual(
            envs.env_dir("aiomqtt", normal), envs.env_dir("aiomqtt3", normal)
        )

    def test_sync_passes_selected_executable_to_uv(self):
        selected = build()
        with (
            patch.object(envs.shutil, "which", return_value="uv"),
            patch.object(envs.subprocess, "run") as run,
        ):
            envs.sync_env("paho", runtime=selected)
        call = run.call_args
        self.assertEqual(
            call.args[0][call.args[0].index("--python") + 1], "/test/python"
        )
        self.assertEqual(
            call.kwargs["env"]["UV_PROJECT_ENVIRONMENT"],
            str(envs.env_dir("paho", selected)),
        )
        self.assertIn("--frozen", call.args[0])

    def test_existing_environment_with_wrong_interpreter_is_rejected(self):
        selected = build()
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(
                envs,
                "inspect_python",
                return_value=dict(
                    build("3.14.8").identity, packages=[["paho-mqtt", "2.1.0", "hash"]]
                ),
            ),
        ):
            with self.assertRaisesRegex(ValueError, "interpreter differs"):
                envs.snapshot(["paho"], selected)

    def test_partial_installation_is_rejected_before_starting_a_worker(self):
        selected = build()
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(
                envs,
                "inspect_python",
                return_value=dict(selected.identity, packages=[]),
            ),
        ):
            with self.assertRaisesRegex(ValueError, "distribution is missing"):
                envs.snapshot(["paho"], selected)

    def test_failed_client_installation_does_not_prevent_other_clients(self):
        args = cli.build_parser().parse_args(
            ["envs", "--sync", "--python", "3.15", "--clients", "paho,awscrt,gmqtt"]
        )
        with (
            patch.object(cli.runtimes, "resolve", return_value=build()),
            patch.object(
                envs,
                "sync_env",
                side_effect=[None, subprocess.CalledProcessError(1, ["uv"]), None],
            ) as sync,
            patch.object(envs, "env_ready", return_value=False),
        ):
            self.assertEqual(cli.cmd_envs(args), 1)
        self.assertEqual(
            [c.args[0] for c in sync.call_args_list], ["paho", "awscrt", "gmqtt"]
        )


class ResumeTests(unittest.TestCase):
    def test_successful_retry_does_not_fail_the_campaign_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = campaign.Store(Path(tmp))
            store.docs["paho"] = {"runs": [
                {"client": "paho", "point": {"name": "pub_qos1_fixed"}, "run_index": 0, "status": "invalid"},
                {"client": "paho", "point": {"name": "pub_qos1_fixed"}, "run_index": 0, "status": "valid"},
            ]}
            args = cli.build_parser().parse_args(["campaign", "--clients", "paho", "--points", "pub_qos1_fixed", "--profile", "smoke", "--output-dir", tmp])
            with patch.object(cli.runtimes, "resolve", return_value=build()), \
                 patch.object(envs, "env_ready", return_value=True), \
                 patch.object(campaign, "run_campaign", return_value=store):
                self.assertEqual(cli.cmd_campaign(args), 0)

    def test_runtime_dependencies_lock_harness_and_sampling_are_frozen(self):
        selected = build()
        profile = catalog.PROFILES["smoke"]
        points = catalog.resolve(["pub_qos1_fixed"])
        frozen = {
            "runtime": selected.as_dict(),
            "environments": {"paho": {"packages": [["paho-mqtt", "2.1.0"]]}},
            "lock_digest": "lock",
            "harness": {"commit": "head", "source_digest": "source"},
        }
        for field in frozen:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest = {
                    "profile": profile.__dict__,
                    "points": [p.as_dict() for p in points],
                    "clients": {"paho": "2.1.0"},
                    "runs_per_point": 1,
                    **copy.deepcopy(frozen),
                }
                manifest[field] = {"changed": True}
                campaign.Store(root).write_manifest(manifest)
                before = (root / "manifest.json").read_bytes()
                with (
                    patch.object(envs, "snapshot", return_value=frozen["environments"]),
                    patch.object(campaign, "lock_digest", return_value="lock"),
                    patch.object(
                        campaign, "harness_revision", return_value=frozen["harness"]
                    ),
                    patch.object(campaign, "open_session") as session,
                ):
                    with self.assertRaisesRegex(ValueError, field):
                        campaign.run_campaign(
                            points, ["paho"], profile, root, runtime=selected
                        )
                    session.assert_not_called()
                self.assertEqual((root / "manifest.json").read_bytes(), before)

    def test_diagnostic_resume_preserves_noncomparable_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            profile = replace(catalog.PROFILES["standard"], comparable=False)
            campaign.Store(root).write_manifest(
                {
                    "profile": profile.__dict__,
                    "points": [],
                    "clients": {},
                    "runs_per_point": 3,
                }
            )
            resumed, *_ = campaign.resume_settings(root)
            self.assertFalse(resumed.comparable)




class ReportRuntimeTests(unittest.TestCase):
    def test_mixed_python_versions_are_refused_before_aggregation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "manifest.json").write_text(
                json.dumps({"points": [], "clients": {"paho": "2.1"}})
            )
            (root / "paho.json").write_text(
                json.dumps(
                    {
                        "client": "paho",
                        "runs": [
                            {"worker": {"python": "3.14.8"}},
                            {"worker": {"python": "3.15.0"}},
                        ],
                    }
                )
            )
            with self.assertRaisesRegex(ValueError, "mixes Python"):
                data.load_campaign(root)



if __name__ == "__main__":
    unittest.main()
