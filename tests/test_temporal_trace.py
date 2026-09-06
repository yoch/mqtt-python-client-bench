"""Tests for the bounded application-E2E temporal trace sampler."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mqtt_client_bench.temporal_trace import (
    CLOCK_ORDER_SLACK_NS,
    DEFAULT_TEMPORAL_TRACE_POINTS,
    TRACE_METRIC,
    TemporalTraceSampler,
    analyze_trace,
    clock_chain_ok,
    lag1_autocorr,
    load_jsonl,
    round_trip_records,
    trace_stride,
    traces_from_columnar,
    write_trace_artifacts,
)


class StrideTests(unittest.TestCase):
    def test_every_nth_covers_the_window(self):
        self.assertEqual(trace_stride(20_000, 4096), 5)
        self.assertEqual(trace_stride(100, 4096), 1)
        self.assertEqual(trace_stride(0, 4096), 1)


class SamplerTests(unittest.TestCase):
    def test_sequence_association_and_latency_identity(self):
        sampler = TemporalTraceSampler(max_points=8, stride=2)
        accepted = []
        for seq in range(1, 17):
            send = 1_000_000 + seq * 200_000
            recv = send + 240_000 + (seq % 3) * 1000
            if sampler.want(seq):
                ok = sampler.add(sequence=seq, send_ns=send, receive_ns=recv)
                accepted.append(seq)
                self.assertTrue(ok)
        records = sampler.records()
        self.assertEqual([row["sequence"] for row in records], accepted)
        for row in records:
            self.assertEqual(row["latency_ns"], row["receive_ns"] - row["send_ns"])
            self.assertGreaterEqual(row["receive_ns"], row["send_ns"])
        self.assertEqual(TRACE_METRIC, "application_e2e_latency")

    def test_send_receive_ordering_and_invalid_timestamps(self):
        sampler = TemporalTraceSampler(max_points=4, stride=1)
        self.assertFalse(sampler.add(sequence=1, send_ns=0, receive_ns=10))
        self.assertFalse(sampler.add(sequence=2, send_ns=50, receive_ns=40))
        self.assertEqual(sampler.invalid, 2)
        self.assertEqual(len(sampler), 0)
        self.assertTrue(sampler.add(sequence=3, send_ns=10, receive_ns=10))
        self.assertEqual(len(sampler), 1)

    def test_bounded_memory_and_cap(self):
        sampler = TemporalTraceSampler(max_points=4, stride=1)
        for seq in range(20):
            sampler.want(seq)
            sampler.add(sequence=seq, send_ns=100 + seq, receive_ns=200 + seq)
        self.assertEqual(len(sampler), 4)
        self.assertEqual(sampler.memory_bytes(), 4 * 8 * 8)
        self.assertLessEqual(sampler.memory_bytes(), 4 * 8 * 8)
        self.assertEqual(DEFAULT_TEMPORAL_TRACE_POINTS, 4096)
        big = TemporalTraceSampler(max_points=4096, stride=1)
        self.assertEqual(big.memory_bytes(), 4096 * 8 * 8)

    def test_deterministic_downsample_order(self):
        sampler = TemporalTraceSampler(max_points=3, stride=3)
        for seq in (0, 1, 2, 3, 4, 5, 6, 7):
            if sampler.want(seq):
                sampler.add(sequence=seq, send_ns=seq + 1, receive_ns=seq + 2)
        self.assertEqual([row["sequence"] for row in sampler.records()], [0, 3, 6])


class SerializationTests(unittest.TestCase):
    def test_columnar_and_jsonl_round_trip(self):
        rows = [
            {
                "sequence": 4,
                "send_ns": 100,
                "receive_ns": 340,
                "scheduled_deadline_ns": 80,
                "pacer_emission_ns": 90,
                "receiver_token_ns": 95,
                "publish_call_ns": 100,
                "latency_ns": 240,
            },
            {
                "sequence": 8,
                "send_ns": 200,
                "receive_ns": 500,
                "scheduled_deadline_ns": 180,
                "pacer_emission_ns": 190,
                "receiver_token_ns": 195,
                "publish_call_ns": 200,
                "latency_ns": 300,
            },
        ]
        restored = round_trip_records(rows)
        self.assertEqual(len(restored), 2)
        self.assertEqual(restored[0]["sequence"], 4)
        self.assertEqual(restored[0]["latency_ns"], 240)
        sampler = TemporalTraceSampler(max_points=2, stride=1)
        for row in rows:
            sampler.add(
                sequence=row["sequence"],
                send_ns=row["send_ns"],
                receive_ns=row["receive_ns"],
                scheduled_deadline_ns=row["scheduled_deadline_ns"],
                pacer_emission_ns=row["pacer_emission_ns"],
                receiver_token_ns=row["receiver_token_ns"],
                publish_call_ns=row["publish_call_ns"],
            )
        columnar = sampler.to_columnar()
        self.assertEqual(traces_from_columnar(columnar)[1]["latency_ns"], 300)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.jsonl"
            sampler.write_jsonl(str(path))
            loaded = load_jsonl(path)
            self.assertEqual(loaded[0]["sequence"], 4)
            self.assertEqual(json.loads(path.read_text().splitlines()[0])["latency_ns"], 240)


class ClockChainTests(unittest.TestCase):
    def test_expected_order_and_slack(self):
        row = {
            "scheduled_deadline_ns": 1000,
            "pacer_emission_ns": 1010,
            "receiver_token_ns": 1020,
            "publish_call_ns": 1030,
            "send_ns": 1030,
            "receive_ns": 1500,
        }
        self.assertTrue(clock_chain_ok(row))
        inverted = dict(row)
        inverted["receiver_token_ns"] = 900
        self.assertFalse(clock_chain_ok(inverted, slack_ns=0))
        almost = dict(row)
        almost["pacer_emission_ns"] = 1000 - (CLOCK_ORDER_SLACK_NS - 1)
        self.assertTrue(clock_chain_ok(almost))
        missing = {"send_ns": 10, "receive_ns": 20, "scheduled_deadline_ns": 0}
        self.assertTrue(clock_chain_ok(missing))


class AnalysisTests(unittest.TestCase):
    def test_catch_up_conditioning_and_autocorr(self):
        records = []
        interval = 200_000
        for seq in range(20):
            send = 1_000_000 + seq * interval
            catch = seq >= 10
            emission = send + (interval if catch else 0)
            latency = 400_000 if catch else 240_000
            records.append(
                {
                    "sequence": seq,
                    "send_ns": send,
                    "receive_ns": send + latency,
                    "latency_ns": latency,
                    "scheduled_deadline_ns": send,
                    "pacer_emission_ns": emission,
                    "receiver_token_ns": emission + 100,
                    "publish_call_ns": send,
                }
            )
        report = analyze_trace(records, interval_ns=interval)
        self.assertEqual(report["n"], 20)
        self.assertGreater(
            report["conditioned_on_catch_up"]["p50_on_ns"],
            report["conditioned_on_catch_up"]["p50_off_ns"],
        )
        self.assertGreater(report["lag1_autocorr"], 0.5)
        self.assertGreater(lag1_autocorr([1.0, 1.0, 1.0, 9.0, 9.0, 9.0]), 0.0)

    def test_report_writer_emits_html(self):
        doc = {
            "baseline_client": "mqttium",
            "pacer_mode": "in_loop",
            "points": [
                {
                    "point": {"pacer_mode": "in_loop", "target_rate": 5000.0},
                    "runs": [
                        {
                            "run_id": "abcd1234",
                            "client": "mqttium",
                            "slot": 0,
                            "workers": [
                                {
                                    "role": "rtt_initiator",
                                    "pacing": {
                                        "mode": "in_loop",
                                        "target_interval_ns": 200000,
                                    },
                                    "temporal_trace": {},
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        sampler = TemporalTraceSampler(max_points=4, stride=1)
        for seq in range(4):
            sampler.add(sequence=seq, send_ns=100 + seq, receive_ns=200 + seq)
        doc["points"][0]["runs"][0]["workers"][0]["temporal_trace"] = sampler.to_columnar()
        with tempfile.TemporaryDirectory() as tmp:
            summary = write_trace_artifacts(doc, tmp)
            self.assertTrue(Path(summary["html"]).is_file())
            html = Path(summary["html"]).read_text()
            self.assertIn("application_e2e_latency", html)
            self.assertGreater(summary["n_runs"], 0)


if __name__ == "__main__":
    unittest.main()
