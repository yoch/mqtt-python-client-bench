"""Unit tests for the v2 core. No broker, no Docker; the C peer is compiled
when a C compiler is available."""

from __future__ import annotations

import asyncio
import copy
import inspect
import random
import re
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from mqtt_client_bench.adapters.base import PublishResult
from mqtt_client_bench.adapters.registry import CLIENT_NAMES
from mqtt_client_bench.bench import campaign, catalog, checks, drive, harness_cost, histogram, peer, runner, sysprobe


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


def _record(kind="pub", qos=1, rate=2000, point_kw=None, **over):
    """A run whose counts reconcile exactly; tests perturb one number."""
    window = 10.0
    point = catalog.Point("t", kind, "?", qos=qos, rate=rate, **(point_kw or {})).as_dict()
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


def _duplex_record(qos=1, rate=1000):
    """Client publishes to a C sink while a C source feeds it, all reconciled."""
    rec = _record("pub", qos=qos, rate=rate)
    rec["point"] = catalog.Point("t", "duplex", "?", qos=qos, rate=rate).as_dict()
    total = rec["worker"]["final"]["sent"]
    n_window = rec["peer"]["received_window"]
    rec["peer_source"] = {"error": None, "sent_total": total, "sent_window": n_window, "acked_total": total}
    rec["broker"]["sys"] = {"received": 2 * total, "sent": 2 * total, "dropped": 0}
    rec["worker"]["latency"] = histogram.from_values([300_000] * 10)
    return rec


class CheckTests(unittest.TestCase):
    def test_every_check_and_flag_is_documented(self):
        # The methodology page is generated from CHECK_DOCS and FLAG_DOCS.
        source = inspect.getsource(checks) + inspect.getsource(runner)
        emitted_checks = set(re.findall(r'checks\.add\(\s*"(\w+)"', source))
        emitted_flags = set(re.findall(r'flags\.append\("(\w+)"\)', source)) | {"non_comparable"}
        self.assertTrue(emitted_checks)
        self.assertEqual(emitted_checks, set(checks.CHECK_DOCS))
        self.assertEqual(emitted_flags, set(checks.FLAG_DOCS))

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

    def test_schedule_lag_is_its_own_metric(self):
        lag = histogram.from_values([50_000] * 9)
        lag["unsent"] = 1
        worker = dict(_record("pub")["worker"], lag=lag)
        m = checks.evaluate(_record("pub", worker=worker))["metrics"]
        self.assertEqual(m["lag_summary"]["count"], 9)
        self.assertEqual(m["lag_summary"]["unsent"], 1)
        self.assertAlmostEqual(m["lag_summary"]["p50_us"], 50, delta=50 / 16)
        self.assertEqual(m["latency_summary"]["count"], 10)

    def test_qos2_counts_like_qos1(self):
        for kind in ("pub", "sub"):
            with self.subTest(kind=kind):
                self.assertEqual(checks.evaluate(_record(kind, qos=2))["status"], checks.VALID)
        rec = _record("pub", qos=2)
        rec["peer"]["received_total"] -= 500
        rec["broker"]["sys"]["sent"] -= 500
        out = checks.evaluate(rec)
        self.assertIn("no_loss", [c["name"] for c in out["checks"] if not c["passed"]])

    def test_duplex_reconciles_both_directions(self):
        out = checks.evaluate(_duplex_record())
        self.assertEqual(out["status"], checks.VALID, [c for c in out["checks"] if not c["passed"]])
        self.assertIn("no_loss_inbound", [c["name"] for c in out["checks"]])
        m = out["metrics"]
        self.assertEqual(m["msgs_per_s"], (m["client_sent"] + m["received"]) / m["window_s"])
        self.assertIn("latency_rx", m)

        rec = _duplex_record()
        rec["worker"]["end"]["received"] -= 1000
        self.assertEqual(checks.evaluate(rec)["status"], checks.NOT_SUSTAINED)

    def test_payload_lengths_are_checked_by_the_sink(self):
        rec = _record("pub")
        rec["peer"]["size_mismatch"] = 0
        self.assertEqual(checks.evaluate(rec)["status"], checks.VALID)
        rec["peer"]["size_mismatch"] = 3
        out = checks.evaluate(rec)
        self.assertEqual(out["status"], checks.INVALID)
        self.assertIn("payloads_intact", [c["name"] for c in out["checks"] if not c["passed"]])

    def test_filter_points_need_every_message_in_a_callback(self):
        rec = _record("sub", point_kw={"topics": 1000, "filters": 100})
        self.assertEqual(checks.evaluate(rec)["status"], checks.VALID)
        rec["worker"]["final"]["unmatched"] = 1
        out = checks.evaluate(rec)
        self.assertIn("callbacks_matched", [c["name"] for c in out["checks"] if not c["passed"]])

    def test_properties_must_reach_the_sink(self):
        rec = _record("pub", point_kw={"protocol": "MQTTv5", "properties": "realistic"})
        rec["peer"]["props_seen"] = rec["peer"]["received_total"]
        self.assertEqual(checks.evaluate(rec)["status"], checks.VALID)
        rec["peer"]["props_seen"] = 0
        self.assertEqual(checks.evaluate(rec)["status"], checks.INVALID)

    def test_topic_alias_is_confirmed_by_broker_bytes(self):
        rec = _record("pub", point_kw={"protocol": "MQTTv5", "topic_alias": True}, data_topic_bytes=200)
        sent = rec["worker"]["final"]["sent"]
        acks = 4 * rec["peer"]["received_total"]
        rec["broker"]["sys"]["bytes_received"] = acks + sent * (256 + 12)
        self.assertEqual(checks.evaluate(rec)["status"], checks.VALID)
        rec["broker"]["sys"]["bytes_received"] = acks + sent * (256 + 200 + 9)
        out = checks.evaluate(rec)
        self.assertIn("topic_alias_used", [c["name"] for c in out["checks"] if not c["passed"]])

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


class _StalledAsync:
    """Every publish takes 200 ms: 256 workers hold only ~1,280 msgs/s."""

    async def publish(self, topic, payload, qos):
        await asyncio.sleep(0.2)
        return 1


def _fixed(rate=5000, duration_ns=400_000_000):
    run = drive.Run(send_capacity=rate * duration_ns // drive.NS + 16)
    t0 = time.monotonic_ns() + 10_000_000
    make = drive.fixed_payload_maker(16, run)
    return run, {"topic": "t", "qos": 1, "make": make, "rate": rate, "t_start": t0, "t_end": t0 + duration_ns}


def _published_lags(run, kw):
    lags, _ = run.window_lags(kw["rate"], kw["t_start"], kw["t_start"], kw["t_end"])
    return lags


class DriveTests(unittest.TestCase):
    def test_fixed_rate_sync_holds_rate(self):
        run, kw = _fixed()
        drive.pub_fixed_sync(_FakeSync(), run, **kw)
        self.assertAlmostEqual(run.sent + run.skipped, 2000, delta=10)
        self.assertEqual(run.done, run.sent)

    def test_fixed_rate_awaited_holds_rate(self):
        run, kw = _fixed()
        asyncio.run(drive.pub_fixed_awaited(_FakeAsync(), run, **kw))
        self.assertAlmostEqual(run.sent + run.skipped, 2000, delta=10)
        self.assertEqual(run.done, run.sent)

    def test_every_fixed_shape_stores_one_send_time_per_publish(self):
        shapes = {
            "sync": lambda run, kw: drive.pub_fixed_sync(_FakeSync(), run, **kw),
            "nowait": lambda run, kw: asyncio.run(drive.pub_fixed_nowait(_FakeAsync(), run, **kw)),
            "awaited": lambda run, kw: asyncio.run(drive.pub_fixed_awaited(_FakeAsync(), run, **kw)),
        }
        for shape, drive_it in shapes.items():
            with self.subTest(shape=shape):
                run, kw = _fixed()
                drive_it(run, kw)
                lags = _published_lags(run, kw)
                self.assertEqual(len(lags), run.sent)
                self.assertEqual(run.send_overflow, 0)
                # Never published before its due time; an idle fake keeps up.
                self.assertGreaterEqual(min(lags), 0)
                self.assertLess(sorted(lags)[len(lags) // 2], 5_000_000)

    def test_lag_exposes_a_client_behind_its_schedule(self):
        run, kw = _fixed(rate=5000, duration_ns=600_000_000)
        asyncio.run(drive.pub_fixed_awaited(_StalledAsync(), run, **kw))
        lags = _published_lags(run, kw)
        # Credits pile up behind busy workers: publishes leave far past due.
        self.assertGreater(max(lags), 100_000_000)
        self.assertGreater(run.skipped, 0)

    def test_window_lags_select_by_due_time(self):
        run = drive.Run(send_capacity=100)
        for i in range(100):
            if i != 30:
                run.sends[i] = drive.due_ns(i, 1000, 0) + i
        lags, unsent = run.window_lags(1000, t_start=0, t_measure=20_000_000, t_end=50_000_000)
        self.assertEqual(lags, [i for i in range(20, 50) if i != 30])
        self.assertEqual(unsent, 1)

    def test_due_time_matches_when_a_message_is_owed(self):
        rate, t_start = 3000, 12_345
        for index in (0, 1, 2, 999, 2999):
            due = drive.due_ns(index, rate, t_start)
            self.assertGreaterEqual(rate * (due - t_start) // drive.NS, index + 1)
            self.assertLess(rate * (due - 1 - t_start) // drive.NS, index + 1)

    def test_capacity_nowait_respects_window_and_stops(self):
        run = drive.Run()
        adapter = _FakeAsync()

        async def main():
            loop = asyncio.get_running_loop()
            loop.call_later(0.1, run.request_stop)
            await drive.pub_capacity_nowait(adapter, run, topic="t", qos=1, make=drive.payload_maker(8), window=16, t_start=time.monotonic_ns())

        asyncio.run(main())
        self.assertGreater(run.done, 100)
        self.assertLessEqual(run.sent - run.done, 16)

    def test_stamped_payload_roundtrip(self):
        run = drive.Run(latency_capacity=4, send_capacity=4)
        make = drive.fixed_payload_maker(64, run)
        on_message = drive.message_callback(run, stamped=True)

        class Msg:
            payload = make(0)

        on_message(None, None, Msg)
        self.assertEqual(run.received, 1)
        self.assertGreaterEqual(run.latencies[0], 0)
        self.assertLess(run.latencies[0], 1_000_000_000)
        self.assertEqual(len(Msg.payload), 64)
        self.assertEqual(run.sends[0], int.from_bytes(Msg.payload[:8], "little"))
        self.assertEqual(run.sends[1], drive.UNSENT)

    def test_fixed_payload_needs_room_for_the_stamp(self):
        with self.assertRaises(ValueError):
            drive.fixed_payload_maker(4, drive.Run())

    def test_cycling_payload_follows_the_index(self):
        run = drive.Run(send_capacity=8)
        make = drive.cycling_payload_maker([10, 200, 30], run)
        self.assertEqual([len(make(i)) for i in range(6)], [10, 200, 30, 10, 200, 30])
        self.assertTrue(all(t != drive.UNSENT for t in run.sends[:6]))

    def test_routed_publish(self):
        calls = []

        def publish(topic, payload, qos, properties=None):
            calls.append((topic, properties))
            return len(calls)

        spread = drive.routed_publish(publish, ["a", "b", "c"], None, alias=False)
        for _ in range(4):
            spread("ignored", b"x", 1)
        self.assertEqual([t for t, _ in calls], ["a", "b", "c", "a"])
        calls.clear()
        alias = drive.routed_publish(publish, ["long/topic"], "P", alias=True)
        for _ in range(3):
            alias("long/topic", b"x", 1)
        self.assertEqual(calls, [("long/topic", "P"), ("", "P"), ("", "P")])
        calls.clear()
        drive.routed_publish(publish, ["t"], "P", alias=False)("t", b"x", 1)
        self.assertEqual(calls, [("t", "P")])


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
        drive.pub_capacity_sync(adapter, run, topic="t", qos=1, make=drive.payload_maker(8), window=4, t_start=0)
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

    def test_full_campaign_fits_the_budget(self):
        profile = catalog.PROFILES["standard"]
        order = campaign.plan(catalog.resolve(suites=list(catalog.SUITES)), list(CLIENT_NAMES), profile.runs)
        budget = catalog.FULL_CAMPAIGN_BUDGET_S * catalog.FULL_CAMPAIGN_MARGIN
        self.assertLessEqual(campaign.estimate_s(order, profile), budget)

    def test_extended_knobs_stay_out_of_plain_records(self):
        self.assertEqual(
            set(catalog.ALL_POINTS["pub_qos1_fixed"].as_dict()),
            {"name", "kind", "question", "qos", "payload", "rate", "window", "protocol", "suite"},
        )
        rl = catalog.ALL_POINTS["pub_rl_boundaries"].as_dict()
        self.assertEqual(rl["remaining_lengths"], list(catalog.RL_BOUNDARIES))

    def test_extended_points_are_well_formed(self):
        for p in catalog.EXTENDED:
            with self.subTest(point=p.name):
                self.assertIn(p.kind, catalog.KINDS)
                self.assertIn(p.properties, catalog.PROPERTY_SETS)
                if p.properties != "none" or p.topic_alias or p.receive_maximum:
                    self.assertEqual(p.protocol, "MQTTv5")
                # gmqtt sizes its outbound packet ids from receive_maximum.
                if p.receive_maximum:
                    self.assertEqual(p.kind, "sub")
                if p.filters:
                    self.assertGreater(p.topics, 1)
                if p.remaining_lengths:
                    self.assertGreater(p.rate, 0)

    def test_refusals_follow_capabilities(self):
        refused = {
            ("gmqtt", "pub_qos2_fixed"): "not_implemented:qos2",
            ("aiomqtt", "sub_filters_fixed"): "not_implemented:native_message_callback_add",
            ("zmqtt", "pub_qos1_fixed_v5_alias"): "not_implemented:v5_topic_alias",
            ("awscrt", "sub_qos1_max_v5_rm16"): "not_implemented:v5_receive_maximum",
            ("aiomqtt3", "pub_qos1_fixed_v5_props"): "not_implemented:v5_publish_properties",
        }
        for (client, point), reason in refused.items():
            with self.subTest(client=client, point=point):
                self.assertIn(reason, runner.refusals(client, catalog.ALL_POINTS[point]))
        for client in ("paho", "mqttium"):
            for p in catalog.EXTENDED:
                if p.protocol == "MQTTv5" or client == "paho":
                    self.assertEqual(runner.refusals(client, p), [], (client, p.name))


class RunnerTests(unittest.TestCase):
    def test_remaining_lengths_become_payload_sizes(self):
        point = catalog.ALL_POINTS["pub_rl_boundaries"]
        topic = runner.run_topics(point, "0123456789ab")["data"]
        sizes = runner.payload_sizes(point, topic)
        overhead = 2 + len(topic) + 2
        self.assertEqual([s + overhead for s in sizes], list(catalog.RL_BOUNDARIES))
        self.assertEqual(runner.payload_sizes(catalog.ALL_POINTS["pub_qos1_fixed"], topic), [256])

    def test_alias_point_publishes_on_a_long_topic(self):
        topics = runner.run_topics(catalog.ALL_POINTS["pub_qos1_fixed_v5_alias"], "0123456789ab")
        self.assertEqual(len(topics["data"]), runner.ALIAS_TOPIC_BYTES)

    @unittest.skipUnless(shutil.which("cc") or shutil.which("gcc"), "no C compiler")
    def test_fanout_topics_match_the_c_peer(self):
        self.assertEqual(runner.fanout_topics("b", 12)[9:], ["b/0/9", "b/1/0", "b/1/1"])
        argv = peer.command("source", host="h", port=1, topic="b", qos=2, protocol="MQTTv5", client_id="c",
                            topics=1000, properties=True)
        self.assertEqual(argv[argv.index("--topics") + 1], "1000")
        self.assertIn("--props", argv)
        argv = peer.command("sink", host="h", port=1, topic="b", qos=1, protocol="MQTTv311", client_id="c", sizes=[1, 2])
        self.assertEqual(argv[argv.index("--sizes") + 1], "1,2")


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

    def _started(self, root: Path, profile, points, clients, runs) -> None:
        campaign.Store(root).write_manifest(
            {
                "profile": profile.__dict__,
                "points": [p.as_dict() for p in points],
                "clients": {c: "1.0" for c in clients},
                "runs_per_point": runs,
                "sessions": [],
            }
        )

    def test_resume_takes_the_settings_it_was_started_with(self):
        points = catalog.resolve(["pub_qos1_fixed", "idle_connect"])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._started(root, catalog.PROFILES["smoke"], points, ["paho", "gmqtt"], 2)
            profile, got, clients, runs = campaign.resume_settings(root)
            self.assertEqual(profile.name, "smoke")
            self.assertEqual([p.name for p in got], ["pub_qos1_fixed", "idle_connect"])
            self.assertEqual((clients, runs), (["paho", "gmqtt"], 2))

    def test_resume_refuses_a_point_the_catalogue_changed(self):
        point = catalog.ALL_POINTS["pub_qos1_fixed"]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._started(root, catalog.PROFILES["smoke"], [point], ["paho"], 1)
            manifest = campaign.Store(root).manifest()
            manifest["points"][0]["rate"] = 1234
            campaign.Store(root).write_manifest(manifest)
            with self.assertRaises(ValueError):
                campaign.resume_settings(root)

    def test_campaign_refuses_to_mix_settings(self):
        points = catalog.resolve(["pub_qos1_fixed"])
        smoke, standard = catalog.PROFILES["smoke"], catalog.PROFILES["standard"]
        mixes = {
            "profile": (standard, points, ["paho"], 1),
            "points": (smoke, catalog.resolve(["idle_connect"]), ["paho"], 1),
            "clients": (smoke, points, ["paho", "gmqtt"], 1),
            "runs": (smoke, points, ["paho"], 3),
        }
        for what, (profile, pts, clients, runs) in mixes.items():
            with self.subTest(what=what), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self._started(root, smoke, points, ["paho"], 1)
                with self.assertRaises(ValueError):
                    campaign.run_campaign(pts, clients, profile, root, runs=runs, log=lambda _m: None)


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
