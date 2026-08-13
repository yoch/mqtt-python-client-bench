"""Focused regressions for the application-RTT initiator."""

from __future__ import annotations

import threading
import time
import unittest
from types import SimpleNamespace

from mqtt_client_bench.roles.rtt_initiator import _send_loop
from mqtt_client_bench.sampling import ReservoirSampler
from mqtt_client_bench.workloads import decode_header_fields


class _ImmediateResponseAdapter:
    """Deliver the response synchronously from inside publish()."""

    def __init__(self, state: dict) -> None:
        self.state = state
        self.calls = 0
        self.responses_saw_registration: list[bool] = []

    def publish(self, topic, payload, qos, retain):
        del topic, qos, retain
        self.calls += 1
        _publisher, _sequence, correlation, _send_ns = decode_header_fields(payload)
        with self.state["lock"]:
            sent = self.state["inflight"].pop(correlation, None)
            self.responses_saw_registration.append(sent is not None)
            # Same path as on_message when publishing_seq matches: stash, do not
            # commit until publish returns rc==0.
            if sent is not None and self.state.get("publishing_seq") == correlation:
                self.state["early_rtt"][correlation] = 1_000
        return SimpleNamespace(rc=0)


class _RefusingAdapter:
    def __init__(self) -> None:
        self.calls = 0

    def publish(self, topic, payload, qos, retain):
        del topic, payload, qos, retain
        self.calls += 1
        return SimpleNamespace(rc=1)


class _FailAfterResponseAdapter:
    """Pop the correlation (as on_message would) then refuse the publish."""

    def __init__(self, state: dict) -> None:
        self.state = state
        self.calls = 0

    def publish(self, topic, payload, qos, retain):
        del topic, qos, retain
        self.calls += 1
        _publisher, _sequence, correlation, _send_ns = decode_header_fields(payload)
        with self.state["lock"]:
            sent = self.state["inflight"].pop(correlation, None)
            if sent is not None and self.state.get("publishing_seq") == correlation:
                self.state["early_rtt"][correlation] = 5_000
                # Deliberately do NOT touch latencies_ns / completed_in_window —
                # that is the send loop's job after rc is known.
        return SimpleNamespace(rc=1)


class _SlowResponseAdapter:
    """Hold every request in flight so the open-loop paceur hits backpressure."""

    def __init__(self) -> None:
        self.calls = 0

    def publish(self, topic, payload, qos, retain):
        del topic, payload, qos, retain
        self.calls += 1
        return SimpleNamespace(rc=0)


class RttInitiatorRaceTests(unittest.TestCase):
    @staticmethod
    def _state() -> dict:
        return {
            "phase": "measure",
            "inflight": {},
            "early_rtt": {},
            "publishing_seq": None,
            "sent_in_window": 0,
            "completed_in_window": 0,
            "offered": 0,
            "missed_due_to_backpressure": 0,
            "retracted_completions": 0,
            "latencies_ns": ReservoirSampler(100, seed=3),
            "lock": threading.Lock(),
        }

    def test_response_can_arrive_before_publish_returns(self) -> None:
        state = self._state()
        adapter = _ImmediateResponseAdapter(state)

        _send_loop(
            adapter,
            state,
            "bench/request",
            1,
            b"deadbeef",
            1,
            None,
            time.perf_counter() + 0.01,
        )

        self.assertGreater(adapter.calls, 1)
        self.assertTrue(all(adapter.responses_saw_registration))
        self.assertEqual(state["inflight"], {})
        self.assertEqual(state["early_rtt"], {})
        self.assertEqual(state["sent_in_window"], adapter.calls)
        self.assertEqual(state["completed_in_window"], adapter.calls)
        self.assertEqual(state["latencies_ns"].seen, adapter.calls)
        self.assertGreater(len(state["latencies_ns"].snapshot()), 0)

    def test_refused_publish_releases_pre_registered_correlation(self) -> None:
        state = self._state()
        adapter = _RefusingAdapter()

        _send_loop(
            adapter,
            state,
            "bench/request",
            1,
            b"deadbeef",
            1,
            None,
            time.perf_counter() + 0.005,
        )

        self.assertGreater(adapter.calls, 0)
        self.assertEqual(state["inflight"], {})
        self.assertEqual(state["sent_in_window"], 0)
        self.assertEqual(state["completed_in_window"], 0)

    def test_refused_publish_after_fast_response_retracts_completion(self) -> None:
        """A sync response that lands before publish returns rc!=0 must not stick.

        Register-before-publish fixed the orphan-response race (74b7673); committing
        the latency only after rc==0 covers the residual where on_message already
        saw the response and publish then fails.
        """
        state = self._state()
        adapter = _FailAfterResponseAdapter(state)

        _send_loop(
            adapter,
            state,
            "bench/request",
            1,
            b"deadbeef",
            1,
            None,
            time.perf_counter() + 0.01,
        )

        self.assertGreater(adapter.calls, 0)
        self.assertEqual(state["inflight"], {})
        self.assertEqual(state["early_rtt"], {})
        self.assertEqual(state["sent_in_window"], 0)
        self.assertEqual(state["completed_in_window"], 0)
        self.assertEqual(state["latencies_ns"].snapshot(), [])
        self.assertGreater(int(state.get("retracted_completions") or 0), 0)

    def test_refused_publish_still_counts_toward_offered(self) -> None:
        """A refused publish consumed a paceur slot and must stay in offered."""
        state = self._state()
        adapter = _RefusingAdapter()
        target = 1000.0
        started = time.perf_counter()
        _send_loop(
            adapter,
            state,
            "bench/request",
            1,
            b"deadbeef",
            outstanding=8,
            target_rate=target,
            until=started + 0.05,
        )
        self.assertGreater(adapter.calls, 0)
        self.assertEqual(state["sent_in_window"], 0)
        self.assertEqual(state["offered"], adapter.calls)
        offer_rate = state["offered"] / max(time.perf_counter() - started, 1e-9)
        self.assertGreater(offer_rate, target * 0.90)

    def test_open_loop_counts_misses_when_inflight_is_full(self) -> None:
        """Paceur must advance and charge misses — same gate as the publisher.

        Sleeping on a full window without advancing next_send left application_rtt
        under-shooting target_rate while the publisher path (which misses) would
        pass validate_run's ±2% offer gate.
        """
        state = self._state()
        adapter = _SlowResponseAdapter()
        target = 2000.0
        started = time.perf_counter()
        _send_loop(
            adapter,
            state,
            "bench/request",
            1,
            b"deadbeef",
            outstanding=1,
            target_rate=target,
            until=started + 0.1,
        )
        elapsed = max(time.perf_counter() - started, 1e-9)
        offered = int(state["offered"])
        missed = int(state["missed_due_to_backpressure"])
        self.assertGreater(missed, 0)
        self.assertEqual(offered, state["sent_in_window"] + missed)
        offer_rate = offered / elapsed
        self.assertGreater(offer_rate, target * 0.90)
        self.assertLess(offer_rate, target * 1.15)


if __name__ == "__main__":
    unittest.main()
