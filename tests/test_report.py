"""The static report: what enters a value, what is kept apart, what is linked."""

from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

from mqtt_client_bench import report
from mqtt_client_bench.bench import histogram
from mqtt_client_bench.bench.checks import CHECK_DOCS
from mqtt_client_bench.report.chart import latency_chart
from mqtt_client_bench.report.data import load_campaign, select_campaign

POINTS = [
    {"name": "pub_qos0_max", "kind": "pub", "question": "Publish capacity?", "qos": 0, "payload": 256, "rate": 0, "window": 64, "protocol": "MQTTv311", "suite": "core"},
    {"name": "pub_qos1_fixed", "kind": "pub", "question": "Publish cost?", "qos": 1, "payload": 256, "rate": 2000, "window": 64, "protocol": "MQTTv311", "suite": "core"},
    {"name": "pub_qos1_fixed_v5", "kind": "pub", "question": "Publish cost, v5?", "qos": 1, "payload": 256, "rate": 2000, "window": 64, "protocol": "MQTTv5", "suite": "v5"},
]


def _run(point, index, status="valid", *, rate=None, cpu=None, latencies=None, lags=None, flags=(), attempt=0, failed=None):
    checks = [{"name": "worker_completed", "passed": True, "detail": "ok", "severity": "invalid"}]
    if failed:
        checks.append({"name": failed, "passed": False, "detail": "short", "severity": status})
    metrics = {"window_s": 8.0}
    if rate is not None:
        metrics["msgs_per_s"] = rate
    if cpu is not None:
        metrics["cpu_us_per_msg"] = cpu
    record = {
        "point": point,
        "run_index": index,
        "attempt": attempt,
        "status": status,
        "checks": checks,
        "flags": list(flags),
        "metrics": metrics,
        "worker": {"shape": "sync", "final": {"sent": 10, "done": 10, "received": 0}},
        "peer": {"received_total": 10},
        "broker": {"sys": {"received": 10, "sent": 10, "dropped": 0}},
    }
    if latencies:
        record["latency"] = histogram.from_values(latencies)
    if lags:
        record["lag"] = dict(histogram.from_values(lags), unsent=0)
        record["metrics"]["lag_summary"] = dict(histogram.summary(record["lag"]), unsent=0)
    return record


def _write(root: Path, name: str, docs: dict, *, comparable=True, clients=None) -> Path:
    path = root / name
    path.mkdir(parents=True)
    manifest = {
        "schema": "mqtt-client-bench/2",
        "campaign": name,
        "started_at": "2026-09-30T00:00:00Z",
        "profile": {"name": "standard" if comparable else "smoke", "warmup_s": 2, "measure_s": 8, "drain_s": 2, "runs": 3, "comparable": comparable},
        "points": POINTS,
        "clients": clients or {c: "1.0" for c in docs},
        "runs_per_point": 3,
        "sessions": [{"host": {"hostname": "h", "cpu_model": "cpu"}, "harness_cost": {"ns_per_msg": {"publish_sync": 500.0}}}],
    }
    (path / "manifest.json").write_text(json.dumps(manifest))
    for client, (runs, unsupported) in docs.items():
        doc = {"schema": "mqtt-client-bench/2", "client": client, "version": "1.0", "identity": {"io_model": "sync"}, "runs": runs, "unsupported": unsupported}
        (path / f"{client}.json").write_text(json.dumps(doc))
    return path


def _campaign(root: Path, name="20260930T000000Z", comparable=True) -> Path:
    cap, fixed, v5 = POINTS
    fast = [_run(cap, i, rate=r) for i, r in enumerate((40_000, 41_000, 39_000))]
    fast += [
        _run(fixed, i, cpu=c, latencies=[200_000] * 99 + [900_000], lags=[400_000] * 99 + [7_000_000])
        for i, c in enumerate((50.0, 60.0, 55.0))
    ]
    fast += [_run(v5, i, cpu=70.0) for i in range(3)]
    slow = [_run(cap, i, rate=r) for i, r in enumerate((9_000, 9_500, 9_200))]
    # One run not sustained: it must not pull the median, and must be shown.
    slow += [_run(fixed, 0, cpu=90.0), _run(fixed, 1, cpu=100.0), _run(fixed, 2, "not_sustained", cpu=500.0, failed="offered_rate_held")]
    # An invalid first attempt, retried successfully.
    slow.insert(0, _run(cap, 0, "invalid", rate=1.0, failed="broker_confirms_deliveries"))
    slow[1]["attempt"] = 1
    docs = {
        "fast": (fast, []),
        "slow": (slow, [{"point": "pub_qos1_fixed_v5", "reasons": ["not_implemented:mqtt_v5"]}]),
    }
    return _write(root, name, docs, comparable=comparable)


class ReportDataTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_only_valid_runs_enter_a_value(self):
        c = load_campaign(_campaign(self.root))
        cell = c.cell("pub_qos1_fixed", "slow")
        self.assertEqual(cell.state, "valid")
        self.assertEqual(cell.value("cpu_us_per_msg"), 95.0)
        self.assertEqual(cell.count("not_sustained"), 1)

    def test_retried_attempt_is_kept_apart(self):
        c = load_campaign(_campaign(self.root))
        cell = c.cell("pub_qos0_max", "slow")
        self.assertEqual(len(cell.runs), 3)
        self.assertEqual(len(cell.retried), 1)
        self.assertEqual(cell.value("msgs_per_s"), 9_200)

    def test_latency_is_read_from_merged_histograms(self):
        c = load_campaign(_campaign(self.root))
        cell = c.cell("pub_qos1_fixed", "fast")
        self.assertEqual(cell.latency()["count"], 300)
        self.assertAlmostEqual(cell.value("p50"), 200.0, delta=200.0 * 0.0625)
        self.assertAlmostEqual(cell.value("max"), 900.0)

    def test_schedule_lag_is_kept_apart_from_latency(self):
        c = load_campaign(_campaign(self.root))
        cell = c.cell("pub_qos1_fixed", "fast")
        self.assertEqual(cell.lag()["count"], 300)
        self.assertAlmostEqual(cell.value("lag_p50"), 400.0, delta=400.0 * 0.0625)
        self.assertAlmostEqual(cell.value("lag_max"), 7_000.0)
        self.assertAlmostEqual(cell.value("max"), 900.0)
        self.assertIsNone(c.cell("pub_qos1_fixed", "slow").value("lag_p99"))

    def test_unsupported_is_its_own_state(self):
        c = load_campaign(_campaign(self.root))
        self.assertEqual(c.cell("pub_qos1_fixed_v5", "slow").state, "unsupported")
        self.assertEqual(c.expected_runs(), (3 * 2 - 1) * 3)

    def test_newest_comparable_campaign_is_selected(self):
        _campaign(self.root, "20260901T000000Z")
        _campaign(self.root, "20260915T000000Z")
        _campaign(self.root, "20260920T000000Z-smoke", comparable=False)
        chosen, skipped = select_campaign(self.root)
        self.assertEqual(chosen.name, "20260915T000000Z")
        self.assertEqual(skipped, ["20260920T000000Z-smoke", "20260901T000000Z"])
        chosen, _ = select_campaign(self.root, "20260920T000000Z-smoke")
        self.assertFalse(chosen.comparable)


class ReportSiteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.results = self.root / "results"
        self.site = self.root / "site"

    def tearDown(self):
        self.tmp.cleanup()

    def _html(self, rel: str) -> str:
        return (self.site / rel).read_text(encoding="utf-8")

    def test_site_is_self_contained(self):
        _campaign(self.results)
        report.build(self.results, self.site)
        pages = sorted(self.site.rglob("*.html"))
        self.assertTrue(pages)
        for page in pages:
            text = page.read_text(encoding="utf-8")
            self.assertNotRegex(text, r"(src|href)=\"(https?:)?//", page.name)
            for href in re.findall(r'href="([^"#?]+)', text):
                self.assertTrue((page.parent / href).resolve().exists(), f"{page.name} links to missing {href}")
        self.assertTrue((self.site / "data" / "manifest.json").exists())

    def test_tables_sort_best_first_in_the_metric_direction(self):
        _campaign(self.results)
        report.build(self.results, self.site)
        index = self._html("index.html")
        capacity = index[index.index('id="publish-capacity"'):]
        self.assertLess(capacity.index("client/fast.html"), capacity.index("client/slow.html"))
        cost = index[index.index('id="cpu-fixed"'):]
        self.assertLess(cost.index("client/fast.html"), cost.index("client/slow.html"))

    def test_not_valid_runs_are_listed_in_coverage(self):
        _campaign(self.results)
        report.build(self.results, self.site)
        coverage = self._html("coverage.html")
        self.assertIn("pub_qos1_fixed · slow · not_sustained", coverage)
        self.assertIn("pub_qos0_max · slow · invalid", coverage)
        self.assertIn("+1 retried", coverage)
        self.assertIn("not_implemented:mqtt_v5", coverage)

    def test_methodology_documents_every_check(self):
        _campaign(self.results)
        report.build(self.results, self.site)
        methodology = self._html("methodology.html")
        for name in CHECK_DOCS:
            self.assertIn(f"<code>{name}</code>", methodology)

    def test_no_comparable_campaign_builds_a_placeholder(self):
        _campaign(self.results, "20260920T000000Z-smoke", comparable=False)
        self.assertIsNone(report.build(self.results, self.site))
        self.assertIn("No comparable campaign yet", self._html("index.html"))
        self.assertIn("20260920T000000Z-smoke", self._html("index.html"))

    def test_development_campaign_is_labelled(self):
        _campaign(self.results, "20260920T000000Z-smoke", comparable=False)
        report.build(self.results, self.site, campaign="20260920T000000Z-smoke")
        self.assertIn("Not comparable", self._html("index.html"))

    def test_unknown_campaign_name_is_an_error(self):
        _campaign(self.results)
        with self.assertRaises(FileNotFoundError):
            report.build(self.results, self.site, campaign="nope")

    def test_output_that_is_not_a_built_site_is_never_removed(self):
        _campaign(self.results)
        foreign = self.root / "elsewhere"
        foreign.mkdir()
        (foreign / "keep.txt").write_text("x")
        for out in (self.results, self.results / "site", self.root, foreign):
            with self.subTest(out=out.name), self.assertRaises(ValueError):
                report.build(self.results, out)
        self.assertTrue((foreign / "keep.txt").exists())
        self.assertTrue(any(self.results.iterdir()))
        report.build(self.results, self.site)
        report.build(self.results, self.site)
        self.assertTrue((self.site / "index.html").exists())

    def test_published_data_carries_no_local_paths(self):
        path = _campaign(self.results)
        doc = json.loads((path / "fast.json").read_text())
        doc["identity"]["client_module"] = "/home/someone/proj/.venvs/paho/lib/python3.12/site-packages/paho/__init__.py"
        (path / "fast.json").write_text(json.dumps(doc))
        report.build(self.results, self.site)
        published = json.loads((self.site / "data" / "fast.json").read_text())
        self.assertEqual(published["identity"]["client_module"], "paho/__init__.py")

    def test_names_are_escaped(self):
        path = _campaign(self.results)
        doc = json.loads((path / "fast.json").read_text())
        doc["version"] = "<script>x</script>"
        (path / "fast.json").write_text(json.dumps(doc))
        report.build(self.results, self.site)
        for page in self.site.rglob("*.html"):
            self.assertNotIn("<script>x", page.read_text(encoding="utf-8"), page.name)

    def test_runs_the_manifest_does_not_announce_are_shown(self):
        path = _campaign(self.results)
        extra = dict(POINTS[1], name="pub_extra_fixed")
        (path / "late.json").write_text(
            json.dumps({"client": "late", "version": "1", "identity": {}, "runs": [_run(extra, 0, cpu=10.0)], "unsupported": []})
        )
        report.build(self.results, self.site)
        self.assertTrue((self.site / "client" / "late.html").exists())
        self.assertTrue((self.site / "point" / "pub_extra_fixed.html").exists())

    def test_clients_that_took_the_whole_offer_tie(self):
        cap = POINTS[0]
        docs = {
            "zeta": ([_run(cap, 0, rate=59_000, flags=["offer_bound"])], []),
            "alpha": ([_run(cap, 0, rate=58_000, flags=["offer_bound"])], []),
            "mid": ([_run(cap, 0, rate=30_000)], []),
            "capped": ([_run(cap, 0, rate=62_000, flags=["broker_bound"])], []),
        }
        _write(self.results, "20260930T000000Z", docs)
        report.build(self.results, self.site)
        table = self._html("point/pub_qos0_max.html")
        order = [table.index(f"client/{c}.html") for c in ("alpha", "zeta", "capped", "mid")]
        self.assertEqual(order, sorted(order))
        self.assertIn("≥ 62,000", table)


class ChartTests(unittest.TestCase):
    def test_degenerate_inputs_render(self):
        flat = histogram.from_values([500_000] * 10)
        self.assertEqual(latency_chart({}, {}), "")
        for hists in ({"one": flat}, {"one": flat, "two": flat}, {"zero": histogram.from_values([0, 0])}):
            with self.subTest(n=len(hists)):
                svg = latency_chart(hists, {})
                self.assertTrue(svg.startswith("<svg") and svg.endswith("</svg>"))
                self.assertNotIn("nan", svg)


if __name__ == "__main__":
    unittest.main()
