"""Focused regressions for the application-RTT initiator."""

from __future__ import annotations

import threading
import time
import unittest
from types import SimpleNamespace

from mqtt_client_bench.roles.rtt_initiator import _send_loop
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
        return SimpleNamespace(rc=0)


class _RefusingAdapter:
    def __init__(self) -> None:
        self.calls = 0

    def publish(self, topic, payload, qos, retain):
        del topic, payload, qos, retain
        self.calls += 1
        return SimpleNamespace(rc=1)


class RttInitiatorRaceTests(unittest.TestCase):
    @staticmethod
    def _state() -> dict:
        return {
            "phase": "measure",
            "inflight": {},
            "sent_in_window": 0,
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
        self.assertEqual(state["sent_in_window"], adapter.calls)

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


if __name__ == "__main__":
    unittest.main()
