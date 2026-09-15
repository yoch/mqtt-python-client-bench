from pathlib import Path
import importlib.util
import unittest

from mqtt_client_bench.report.model import PointRow, ResultDoc


def _load_summarize():
    path = Path("scripts/summarize_mqttium_gmqtt.py")
    spec = importlib.util.spec_from_file_location("summarize_mqttium_gmqtt", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _point(label: str, rate, *, status="valid", valid=5, total=5, bottleneck="sut_limited"):
    return PointRow(
        label=label,
        median_msgs_per_s=rate,
        status=status,
        valid_runs=valid,
        total_runs=total,
        non_comparable=False,
        bottleneck=bottleneck,
    )


def _doc(client: str, scenario: str, points, *, median):
    return ResultDoc(
        source_name=f"{client}-{scenario}.json",
        slug=f"{client}-{scenario}",
        kind="scenario",
        title=scenario,
        client=client,
        scenario=scenario,
        profile="standard",
        non_comparable=False,
        status="valid",
        median_msgs_per_s=median,
        points=list(points),
        environment={},
        broker={},
        verdict=None,
        raw_meta={},
    )


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


class MqttiumGmqttCompareScriptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.script = Path("scripts/run_mqttium_gmqtt_compare.sh").read_text(encoding="utf-8")

    def test_accepts_exact_git_sha_and_named_client_path(self) -> None:
        self.assertIn("MQTTIUM_GIT_SHA", self.script)
        self.assertIn("MQTTIUM_GIT_REF", self.script)
        self.assertIn('--client-path "mqttium=${MQTTIUM_CLIENT_PATH}"', self.script)
        self.assertIn('git -C "$SRC_DIR" checkout --quiet "${MQTTIUM_GIT_SHA}"', self.script)

    def test_git_installs_isolate_results_and_calibration(self) -> None:
        self.assertIn('MQTTIUM_RUN_LABEL:-mqttium-git', self.script)
        self.assertIn('calibrations/${MQTTIUM_RUN_LABEL}', self.script)
        self.assertIn('${MQTTIUM_RUN_LABEL}/mqttium-gmqtt', self.script)

    def test_pypi_default_stays_on_rc14(self) -> None:
        self.assertIn('MQTTIUM_VER:-1.0.0rc14', self.script)
        self.assertIn("--load-profile-dir \"$CAL_DIR\"", self.script)


class SummarizeMqttiumGmqttAlignmentTests(unittest.TestCase):
    """Headlines must compare the same point, not ResultDoc.median_msgs_per_s."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.summarize = _load_summarize()

    def test_qos_sweep_does_not_ratio_document_medians(self) -> None:
        # Reproduces the PR #460 campaign shape: mqttium's document median is
        # QoS1 v5 (55k), gmqtt's is QoS0 v5 (100k). A document-level ratio is
        # 0.55×; the same-point QoS0 v5 ratio is 1.63×.
        mqttium = _doc(
            "mqttium",
            "pub_qos_sweep_telemetry",
            [
                _point("qos=1, proto=MQTTv5", 55249.5),
                _point("qos=0, proto=MQTTv5", 162684.0),
                _point("qos=1, proto=MQTTv311", 53734.2),
                _point("qos=0, proto=MQTTv311", 170356.3),
            ],
            median=55249.5,
        )
        gmqtt = _doc(
            "gmqtt",
            "pub_qos_sweep_telemetry",
            [
                _point("qos=1, proto=MQTTv5", 53118.6),
                _point("qos=0, proto=MQTTv5", 100229.2),
                _point("qos=1, proto=MQTTv311", 49582.0),
                _point("qos=0, proto=MQTTv311", 109002.7),
            ],
            median=100229.2,
        )
        table = self.summarize.matrix_table_from_docs([mqttium, gmqtt])
        self.assertEqual(table["alignment"], "point_label")
        self.assertEqual(len(table["headlines"]), 4)
        by_label = {row["label"]: row for row in table["headlines"]}
        qos0_v5 = by_label["qos=0, proto=MQTTv5"]
        self.assertAlmostEqual(qos0_v5["mqttium"], 162684.0)
        self.assertAlmostEqual(qos0_v5["gmqtt"], 100229.2)
        self.assertGreater(qos0_v5["mqttium_over_gmqtt"], 1.5)
        qos1_v5 = by_label["qos=1, proto=MQTTv5"]
        self.assertAlmostEqual(qos1_v5["mqttium_over_gmqtt"], 55249.5 / 53118.6)
        # The bug: ratio of document medians is mqttium QoS1 vs gmqtt QoS0.
        bogus = mqttium.median_msgs_per_s / gmqtt.median_msgs_per_s
        self.assertLess(bogus, 0.6)
        self.assertNotAlmostEqual(qos0_v5["mqttium_over_gmqtt"], bogus)
        self.assertAlmostEqual(
            table["qos_sweep_points"]["QoS0 MQTTv5"]["mqttium_over_gmqtt"],
            qos0_v5["mqttium_over_gmqtt"],
        )

    def test_subscribe_splits_protocols(self) -> None:
        mqttium = _doc(
            "mqttium",
            "sub_exact_telemetry",
            [
                _point("proto=MQTTv5", 161182.2, bottleneck="broker_limited"),
                _point("proto=MQTTv311", 199191.4, bottleneck="offer_limited"),
            ],
            median=199191.4,
        )
        gmqtt = _doc(
            "gmqtt",
            "sub_exact_telemetry",
            [
                _point("proto=MQTTv5", 186042.2, bottleneck="offer_limited"),
                _point("proto=MQTTv311", 196622.9, bottleneck="offer_limited"),
            ],
            median=196622.9,
        )
        rows = {
            row["label"]: row
            for row in self.summarize.matrix_table_from_docs([mqttium, gmqtt])["headlines"]
        }
        self.assertAlmostEqual(rows["proto=MQTTv5"]["mqttium_over_gmqtt"], 161182.2 / 186042.2)
        self.assertLess(rows["proto=MQTTv5"]["mqttium_over_gmqtt"], 1.0)
        self.assertGreater(rows["proto=MQTTv311"]["mqttium_over_gmqtt"], 1.0)
        self.assertEqual(rows["proto=MQTTv5"]["mqttium_bottleneck"], "broker_limited")

    def test_campaign_tree_qos0_v5_is_point_aligned(self) -> None:
        root = Path("results/cursor-bce5e86fefabd33d/mqttium-pr460/mqttium-gmqtt")
        if not (root / "mqttium-pub_qos_sweep_telemetry.json").is_file():
            self.skipTest("mqttium-pr460 campaign tree not present")
        table = self.summarize.matrix_table(root)
        qos0_v5 = next(
            row
            for row in table["headlines"]
            if row["scenario"] == "pub_qos_sweep_telemetry" and row["label"] == "qos=0, proto=MQTTv5"
        )
        self.assertGreater(qos0_v5["mqttium_over_gmqtt"], 1.5)
        exact_v5 = next(
            row
            for row in table["headlines"]
            if row["scenario"] == "sub_exact_telemetry" and row["label"] == "proto=MQTTv5"
        )
        self.assertLess(exact_v5["mqttium_over_gmqtt"], 1.0)


if __name__ == "__main__":
    unittest.main()
