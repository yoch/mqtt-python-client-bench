"""Unit tests for the v2 core. No broker, no Docker; the C peer is compiled
when a C compiler is available."""

from __future__ import annotations

import asyncio
import copy
import random
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from mqtt_client_bench.adapters.base import PublishResult
from mqtt_client_bench.bench2 import campaign, catalog, checks, drive, harness_cost, histogram, peer, sysprobe


class HistogramTests(unittest.TestCase):
    def test_bucket_bounds_contain_value(self):
        rng = random.Random(7)
        for v in [0, 1, 31, 32, 33, 63, 64, 1000, 10**6, 10**9, 2**40] + [rng.randrange(1, 10**10) for _ in range(500)]:
            low, high = histogram.bucket_bounds(histogram.bucket_of(v))
            self.assertLessEqual(low, v)
            self.assertLess(v, high)

    def test_relative_width_at_most_one_sixteenth(self):
        for i in range(32, 900):
            low, high = histogram.bucket_bounds(i)
            self.assertLessEqual((high - low) / low, 1 / 16 + 1e-12)

    def test_merge_is_addition(self):
        a = histogram.from_values([100, 200, 300])
        b = histogram.from_values([150, -5, 10**6])
        m = histogram.merge([a, b])
        self.assertEqual(m["count"], 5)
        self.assertEqual(m["negative"], 1)
        self.assertEqual(m["min_ns"], 100)
        self.assertEqual(m["max_ns"], 10**6)
        self.assertEqual(m, histogram.from_values([100, 200, 300, 150, -5, 10**6]))

    def test_percentile_within_bucket_error(self):
        values = list(range(1000, 101000, 10))
        h = histogram.from_values(values)
        for q in (0.5, 0.9, 0.99):
            exact = values[int(q * len(values)) - 1]
            self.assertAlmostEqual(histogram.percentile(h, q), exact, delta=exact / 16)
        self.assertEqual(histogram.percentile(h, 1.0), values[-1])

    @unittest.skipUnless(shutil.which("cc") or shutil.which("gcc"), "no C compiler")
    def test_same_indexing_as_c_peer(self):
        rng = random.Random(11)
        values = [0, 1, 31, 32, 47, 48, 64, 65535, 10**9] + [rng.randrange(0, 2**45) for _ in range(300)]
        self.assertEqual(peer.bucket_indices(values), [histogram.bucket_of(v) for v in values])


def _record(kind="pub", qos=1, rate=2000, **over):
    """A run whose counts reconcile exactly; tests perturb one number."""
    window = 10.0
    point = catalog.Point("t", kind, "?", qos=qos, rate=rate).as_dict()
    n_window = int((rate or 20000) * window)
    total = n_window + n_window // 5
    worker = {
        "ok": True,
        "window_ns": int(window * 1e9),
        "connect_ns": 1_000_000,
        "measure": {"sent": 100, "done": 90, "failed": 0, "rejected": 0, "received": 100, "skipped": 0},
        "end": {"sent": 100 + n_window, "done": 90 + n_window, "failed": 0, "rejected": 0, "received": 100 + n_window, "skipped": 0},
        "final": {"sent": total, "done": total, "failed": 0, "rejected": 0, "received": total, "skipped": 0},
    }
    if kind == "pub":
        sysd = {"received": total, "sent": total, "dropped": 0}
        peer_doc = {"error": None, "received_total": total, "received_window": n_window, "latency": histogram.from_values([200_000] * 10)}
    elif kind == "sub":
        sysd = {"received": total, "sent": total, "dropped": 0}
        peer_doc = {"error": None, "sent_total": total, "sent_window": n_window, "acked_total": total if qos else 0}
    else:
        sysd = {"received": 2 * total, "sent": 2 * total, "dropped": 0}
        peer_doc = {"error": None, "received_total": total, "echoed_total": total}
    rec = {
        "client": "x",
        "point": point,
        "schedule": {"measure_s": window},
        "worker": worker,
        "peer": peer_doc,
        "broker": {"sys": sysd, "cpu_cores": 0.3},
        "resources": {
            "client": {"cpu_ns": int(2e9), "cpu_user_s": 1.5, "cpu_sys_s": 0.5, "rss_peak_kb": 30000},
            "host": {"other_cores": 0.1},
        },
    }
    for key, value in over.items():
        rec[key] = value
    return rec


class CheckTests(unittest.TestCase):
    def test_reconciled_runs_are_valid(self):
        for kind in ("pub", "sub", "rtt"):
            with self.subTest(kind=kind):
                out = checks.evaluate(_record(kind))
                self.assertEqual(out["status"], "valid", [c for c in out["checks"] if not c["passed"]])

    def test_broker_disagreeing_with_client_is_invalid(self):
        rec = _record("pub")
        rec["broker"]["sys"]["received"] -= 1000
        out = checks.evaluate(rec)
        self.assertEqual(out["status"], "invalid")
        self.assertIn("broker_confirms_client_publishes", [c["name"] for c in out["checks"] if not c["passed"]])

    def test_client_below_offer_is_not_sustained(self):
        rec = _record("pub")
        rec["worker"]["end"]["sent"] -= 2000
        out = checks.evaluate(rec)
        self.assertEqual(out["status"], "not_sustained")

    def test_peer_below_offer_is_invalid(self):
        rec = _record("sub")
        rec["peer"]["sent_window"] = int(rec["peer"]["sent_window"] * 0.9)
        self.assertEqual(checks.evaluate(rec)["status"], "invalid")

    def test_qos1_loss_is_invalid_on_publish(self):
        rec = _record("pub")
        rec["peer"]["received_total"] -= 500
        rec["broker"]["sys"]["sent"] -= 500
        out = checks.evaluate(rec)
        self.assertIn("no_loss", [c["name"] for c in out["checks"] if not c["passed"]])
        self.assertEqual(out["status"], "invalid")

    def test_slow_receiver_at_capacity_is_valid(self):
        rec = _record("sub", qos=0, rate=0)
        rec["worker"]["final"]["received"] -= 30000
        rec["worker"]["end"]["received"] -= 30000
        out = checks.evaluate(rec)
        self.assertEqual(out["status"], "valid")
        self.assertEqual(out["metrics"]["undelivered_at_stop"], 30000)

    def test_noisy_host(self):
        rec = _record("pub")
        rec["resources"]["host"]["other_cores"] = 2.0
        self.assertEqual(checks.evaluate(rec)["status"], "invalid")
        lax = checks.evaluate(copy.deepcopy(rec), strict=False)
        self.assertEqual(lax["status"], "valid")
        self.assertIn("host_noisy", lax["flags"])

    def test_metrics_are_counts_over_window(self):
        m = checks.evaluate(_record("pub"))["metrics"]
        self.assertEqual(m["msgs_per_s"], 2000)
        self.assertAlmostEqual(m["cpu_us_per_msg"], 100.0)
        self.assertEqual(m["latency_summary"]["count"], 10)

    def test_tolerance(self):
        self.assertEqual(checks.tolerance(0), 5)
        self.assertEqual(checks.tolerance(100_000), 55)


class _FakeSync:
    """Completes every publish inline, like a transport that never blocks."""

    def __init__(self):
        self.on_publish = None
        self.published = 0

    def publish(self, topic, payload, qos):
        self.published += 1
        self.on_publish(None, None, self.published, 0, None)
        return PublishResult(0, self.published)


class _FakeAsync:
    def __init__(self):
        self.on_publish = None
        self.published = 0

    def publish_nowait(self, topic, payload, qos):
        self.published += 1
        asyncio.get_running_loop().call_soon(self.on_publish, None, None, self.published, 0, None)
        return self.published

    async def publish(self, topic, payload, qos):
        self.published += 1
        await asyncio.sleep(0)
        return self.published


class DriveTests(unittest.TestCase):
    def test_fixed_rate_sync_holds_rate(self):
        run = drive.Run()
        t0 = time.monotonic_ns() + 10_000_000
        drive.pub_fixed_sync(_FakeSync(), run, topic="t", qos=1, make=drive.payload_maker(16, True), rate=5000, t_start=t0, t_end=t0 + 400_000_000)
        self.assertAlmostEqual(run.sent + run.skipped, 2000, delta=10)
        self.assertEqual(run.done, run.sent)

    def test_fixed_rate_awaited_holds_rate(self):
        run = drive.Run()
        t0 = time.monotonic_ns() + 10_000_000
        asyncio.run(drive.pub_fixed_awaited(_FakeAsync(), run, topic="t", qos=1, make=drive.payload_maker(16, True), rate=5000, t_start=t0, t_end=t0 + 400_000_000))
        self.assertAlmostEqual(run.sent + run.skipped, 2000, delta=10)
        self.assertEqual(run.done, run.sent)

    def test_capacity_nowait_respects_window_and_stops(self):
        run = drive.Run()
        adapter = _FakeAsync()

        async def main():
            loop = asyncio.get_running_loop()
            loop.call_later(0.1, run.request_stop)
            await drive.pub_capacity_nowait(adapter, run, topic="t", qos=1, make=drive.payload_maker(8, False), window=16, t_start=time.monotonic_ns())

        asyncio.run(main())
        self.assertGreater(run.done, 100)
        self.assertLessEqual(run.sent - run.done, 16)

    def test_stamped_payload_roundtrip(self):
        make = drive.payload_maker(64, True)
        run = drive.Run(latency_capacity=4)
        on_message = drive.message_callback(run, stamped=True)

        class Msg:
            payload = make()

        on_message(None, None, Msg)
        self.assertEqual(run.received, 1)
        self.assertGreaterEqual(run.latencies[0], 0)
        self.assertLess(run.latencies[0], 1_000_000_000)
        self.assertEqual(len(Msg.payload), 64)


class NullClientTests(unittest.TestCase):
    """The harness's share of a message, against a client that does nothing.

    Every figure includes the null adapter's own call and stop check, so the
    true harness share is lower still. 1 us is ~5% of the fastest client's
    period on the reference host (mqttium, ~15 us/msg at QoS 0).
    """

    def test_every_drive_shape_is_within_budget(self):
        costs = harness_cost.measure(n=50_000, repeats=3)
        for shape, ns in costs.items():
            with self.subTest(shape=shape):
                self.assertLessEqual(ns, harness_cost.BUDGET_NS, costs)

    def test_null_client_counts_are_exact(self):
        run = drive.Run()
        adapter = harness_cost.NullSync()
        inner = adapter.publish

        def publish(topic, payload, qos):
            if run.sent >= 999:
                run.stop = True
            return inner(topic, payload, qos)

        adapter.publish = publish
        drive.pub_capacity_sync(adapter, run, topic="t", qos=1, make=drive.payload_maker(8, False), window=4, t_start=0)
        self.assertEqual(run.sent, run.done)
        self.assertEqual(run.failed + run.rejected, 0)


class CatalogTests(unittest.TestCase):
    def test_names_unique(self):
        names = [p.name for suite in catalog.SUITES.values() for p in suite]
        self.assertEqual(len(names), len(set(names)))

    def test_fixed_rates_are_below_slowest_capacity(self):
        for p in catalog.CORE:
            if p.kind in ("pub", "sub") and p.rate:
                self.assertLessEqual(p.rate, catalog.FIXED_RATE)

    def test_v5_mirror_only_changes_protocol(self):
        core = {p.name: p for p in catalog.CORE}
        for p in catalog.V5_MIRROR:
            base = core[p.name[: -len("_v5")]]
            self.assertEqual((p.kind, p.qos, p.rate, p.payload), (base.kind, base.qos, base.rate, base.payload))
            self.assertEqual(p.protocol, "MQTTv5")


class CampaignTests(unittest.TestCase):
    def test_plan_rotates_clients_and_skips_refusals(self):
        points = catalog.resolve(["pub_qos1_fixed", "pub_qos1_fixed_v5"])
        order = campaign.plan(points, ["paho", "gmqtt", "aiomqtt3", "amqtt"], runs=2)
        v311 = [(c, i) for p, c, i in order if p == "pub_qos1_fixed"]
        self.assertEqual(v311, [("paho", 0), ("gmqtt", 0), ("amqtt", 0), ("gmqtt", 1), ("amqtt", 1), ("paho", 1)])
        v5 = {c for p, c, _ in order if p == "pub_qos1_fixed_v5"}
        self.assertEqual(v5, {"paho", "gmqtt", "aiomqtt3"})

    def test_store_retries_invalid_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = campaign.Store(Path(tmp))
            key = ("pub_qos1_fixed", "paho", 0)
            base = {"client": "paho", "point": {"name": "pub_qos1_fixed"}, "run_index": 0, "worker": {}}
            store.docs["paho"] = {"client": "paho", "runs": [], "unsupported": [], "identity": None}
            store.add(dict(base, status="invalid"))
            self.assertFalse(store.done(key))
            store.add(dict(base, status="invalid"))
            self.assertTrue(store.done(key))
            reloaded = campaign.Store(Path(tmp))
            self.assertEqual(len(reloaded.attempts(key)), 2)


class SysProbeTests(unittest.TestCase):
    def test_delta_removes_probe_traffic(self):
        before = {"received": 10, "sent": 100, "dropped": 0, "probe_msgs": 7}
        after = {"received": 1010, "sent": 1105, "dropped": 2, "probe_msgs": 12}
        d = sysprobe.delta(before, after)
        self.assertEqual(d["received"], 1000)
        self.assertEqual(d["sent"], 1000)
        self.assertEqual(d["dropped"], 2)


if __name__ == "__main__":
    unittest.main()
