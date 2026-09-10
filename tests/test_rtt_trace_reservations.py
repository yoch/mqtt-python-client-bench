"""Check sampling at the initiator's reserve/commit boundary, not just add()."""
from __future__ import annotations

import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mqtt_client_bench.pacing import PaceToken
from mqtt_client_bench.roles import rtt_initiator as rtt
from mqtt_client_bench.temporal_trace import TemporalTraceSampler, trace_stride
from tests.test_rtt_cooperation import _state


class RttTraceReservationTests(unittest.TestCase):
    def test_trace_covers_the_whole_window_and_keeps_pacer_metadata(self):
        count = 3942 * 12
        tracer = TemporalTraceSampler(max_points=4096, stride=trace_stride(count, 4096))
        state = {"temporal_trace": tracer, "trace_pending": {}}
        for seq in range(1, count + 1):
            deadline = 1_000_000_000 + seq * 253_678
            token = PaceToken(seq - 1, deadline, deadline + 2_000)
            rtt._reserve_trace(state, seq, token, deadline + 80_000, deadline + 84_000)
            rtt._commit_trace(state, seq, deadline + 84_000, deadline + 304_000)
        rows = tracer.records()
        self.assertEqual([row["sequence"] for row in rows], list(range(12, count + 1, 12)))
        self.assertEqual(len(rows), 3942)
        self.assertEqual(tracer.seen, count)
        self.assertEqual(tracer.reserved, 3942)
        self.assertEqual(state["trace_pending"], {})
        for row in rows:
            deadline = 1_000_000_000 + row["sequence"] * 253_678
            self.assertEqual(row["scheduled_deadline_ns"], deadline)
            self.assertEqual(row["pacer_emission_ns"], deadline + 2_000)
            self.assertEqual(row["receiver_token_ns"], deadline + 80_000)
            self.assertEqual(row["publish_call_ns"], deadline + 84_000)
            self.assertEqual(row["latency_ns"], 220_000)

    def test_unsampled_and_duplicate_completions_do_not_consume_slots(self):
        tracer = TemporalTraceSampler(max_points=4, stride=2)
        state = {"temporal_trace": tracer, "trace_pending": {}}
        for seq in (2, 4, 6):
            rtt._reserve_trace(state, seq, None, 100, 110)
        for seq in (1, 3, 5):
            rtt._commit_trace(state, seq, 110, 150)
        self.assertEqual(len(tracer), 0)
        self.assertEqual(set(state["trace_pending"]), {2, 4, 6})
        for seq in (6, 2, 6, 4, 4):
            rtt._commit_trace(state, seq, 110, 150)
        self.assertEqual([row["sequence"] for row in tracer.records()], [6, 2, 4])
        self.assertEqual(state["trace_pending"], {})

    def test_missing_pending_mapping_does_not_fabricate_a_trace(self):
        for pending in (None, {}):
            with self.subTest(pending=pending):
                tracer = TemporalTraceSampler(max_points=4)
                state = {"temporal_trace": tracer, "trace_pending": pending}
                rtt._commit_trace(state, 1, 100, 200)
                self.assertEqual(len(tracer), 0)
                self.assertIs(state["trace_pending"], pending)
        tracer = TemporalTraceSampler(max_points=4)
        rtt._commit_trace({"temporal_trace": tracer}, 1, 100, 200)
        self.assertEqual(len(tracer), 0)

    def test_dropped_reservation_leaves_a_hole_not_a_replacement(self):
        tracer = TemporalTraceSampler(max_points=2, stride=2)
        state = {"temporal_trace": tracer, "trace_pending": {}}
        for seq in (2, 4):
            rtt._reserve_trace(state, seq, None, 100, 110)
        rtt._drop_trace(state, 2)
        rtt._commit_trace(state, 2, 110, 150)
        rtt._reserve_trace(state, 6, None, 100, 110)
        rtt._commit_trace(state, 6, 110, 150)
        rtt._commit_trace(state, 4, 110, 150)
        self.assertEqual([row["sequence"] for row in tracer.records()], [4])
        self.assertEqual(tracer.reserved, 2)
        self.assertEqual(state["trace_pending"], {})

    def test_measure_reset_discards_old_reservations(self):
        state = _state()
        tracer = TemporalTraceSampler(max_points=4)
        state["temporal_trace"] = tracer
        rtt._reserve_trace(state, 1, None, 100, 110)
        rtt._reset_measure_counters(state)
        rtt._commit_trace(state, 1, 110, 150)
        self.assertEqual(len(tracer), 0)
        self.assertEqual(state["trace_pending"], {})

    def test_disabled_sampler_does_not_touch_pending(self):
        pending = {1: {"publish_call_ns": 100}}
        state = {"temporal_trace": None, "trace_pending": pending}
        rtt._commit_trace(state, 1, 100, 200)
        self.assertEqual(state["trace_pending"], {1: {"publish_call_ns": 100}})

    def test_immediate_sync_response_commits_only_accepted_sampled_requests(self):
        for refused in (False, True):
            with self.subTest(refused=refused):
                state = _state()
                state["temporal_trace"] = TemporalTraceSampler(max_points=8, stride=2)

                class Adapter:
                    def publish(self, topic, payload, qos, retain):
                        rtt._on_message(state, SimpleNamespace(payload=payload))
                        return SimpleNamespace(rc=int(refused))

                with patch.object(rtt, "_phase_expired", side_effect=[False] * 8 + [True]):
                    rtt._send_loop(
                        Adapter(), state, "test", 1, b"12345678", 1, None,
                        time.perf_counter() + 5,
                    )
                self.assertEqual(state["offered"], 8)
                self.assertEqual(state["sent_in_window"], 0 if refused else 8)
                self.assertEqual(state["completed_in_window"], 0 if refused else 8)
                self.assertEqual(state["trace_pending"], {})
                self.assertEqual(
                    [row["sequence"] for row in state["temporal_trace"].records()],
                    [] if refused else [2, 4, 6, 8],
                )


if __name__ == "__main__":
    unittest.main()
