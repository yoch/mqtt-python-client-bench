"""Measurement invariants that the 2026-08 harness regressions proved were missing.

Each test cites the commit that fixed (or introduced) the bug. The unit suite
stayed green while smoke/campaigns failed; these pin the actual property.
"""

from __future__ import annotations

import asyncio
import time
import unittest

from mqtt_client_bench.adapters.base import AdapterCapabilities
from mqtt_client_bench.harness import enrich_worker_integrity, validate_run
from mqtt_client_bench.roles import publisher
from mqtt_client_bench.sampling import (
    CompletionLog,
    ReservoirSampler,
    sequence_tracker,
)


class _FakeSyncOnLoopAdapter:
    def __init__(self, complete_after: int = 1):
        self._complete_after = max(1, complete_after)
        self._pending: list[int] = []
        self._mid = 0
        self.on_publish = None

    @classmethod
    def capabilities(cls):
        return AdapterCapabilities(
            name="fake-sync-on-loop", native_async=True, publish_sync_on_loop=True
        )

    def publish_nowait(self, topic, payload=None, qos=0, retain=False, properties=None):
        self._mid = 1 if self._mid >= 65535 else self._mid + 1
        mid = self._mid
        self._pending.append(mid)
        if len(self._pending) >= self._complete_after:
            for done in self._pending:
                if self.on_publish is not None:
                    self.on_publish(self, None, done, 0, None)
            self._pending.clear()
        return mid


class _FakeAwaitedAdapter:
    def __init__(self, delay_s: float = 0.001):
        self._delay = delay_s
        self.on_publish = None

    @classmethod
    def capabilities(cls):
        return AdapterCapabilities(
            name="fake-awaited", native_async=True, publish_sync_on_loop=False
        )

    async def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        await asyncio.sleep(self._delay)
        return 1


class _FakeDeferredAdapter:
    """Acknowledges later via call_later — forces the outstanding gate to park."""

    def __init__(self):
        self._mid = 0
        self.on_publish = None

    @classmethod
    def capabilities(cls):
        return AdapterCapabilities(
            name="fake-deferred", native_async=True, publish_sync_on_loop=True
        )

    def publish_nowait(self, topic, payload=None, qos=0, retain=False, properties=None):
        self._mid = 1 if self._mid >= 65535 else self._mid + 1
        mid = self._mid
        # call_later, not call_soon: with call_soon the ack lands before the
        # window fills, the loop never parks, and the wake test proves nothing
        # (fc61949).
        asyncio.get_running_loop().call_later(0.002, self._ack, mid)
        return mid

    def _ack(self, mid):
        if self.on_publish is not None:
            self.on_publish(self, None, mid, 0, None)


def _state(log_limit=4096, sample_limit=1000):
    st = {
        "offered": 0,
        "submitted": 0,
        "sync_rejected": 0,
        "completed_success": 0,
        "completed_failed": 0,
        "missed_due_to_backpressure": 0,
        "publish_calls": 0,
        "publish_accepted": 0,
        "publish_rejected": 0,
        "protocol_completed": 0,
        "protocol_failed": 0,
        "socket_completed_qos0": 0,
        "completed_in_window": 0,
        "completed_during_drain": 0,
        "latencies_ns": ReservoirSampler(sample_limit, seed=11),
        "scheduler_lags_ns": ReservoirSampler(sample_limit, seed=29),
        "inflight_local": 0,
        "phase": "measure",
        "mid_send_ns": {},
        "early_acks": {},
        "warmup_drain_ok": True,
        "seen_mids_inflight": set(),
        "gate_waiter": None,
        "loop_expired": False,
        "pending_send_ns": None,
        "completed_inline": None,
        "overflow_success": 0,
        "overflow_failed": 0,
        "overflow_in_window": 0,
        "overflow_during_drain": 0,
        "fold_pending": False,
    }
    st["completions"] = CompletionLog(log_limit, sampler=st["latencies_ns"])
    return st


def _loop_kwargs(**over):
    kwargs = dict(
        topic="bench/t",
        qos=1,
        body=b"x" * 64,
        corpus=[],
        run_id=b"testrun1",
        outstanding=8,
        cadence="capacity",
        until=time.perf_counter() + 0.25,
        target_rate=None,
        properties_builder=lambda: None,
        track_sequences=False,
    )
    kwargs.update(over)
    return kwargs


class NullTrackerShapeTests(unittest.TestCase):
    """18331b2: no-op tracker must expose every key the result assembly reads."""

    def test_null_tracker_summary_has_first(self):
        tracker = sequence_tracker(64, enabled=False)
        summary = tracker.summary()
        for key in ("tracked", "count", "first", "last", "digest_sum64", "digest_xor64"):
            self.assertIn(key, summary)
        # The crash was KeyError('first') on every publisher_only worker.
        self.assertIsNone(summary["first"])
        self.assertFalse(summary["tracked"])


class ValidateRunIntegrityTests(unittest.TestCase):
    """Integrity was enriched after validate_run and never consulted — fail-open."""

    def test_digest_mismatch_is_inconclusive(self):
        workers = [
            {
                "ok": True,
                "role": "publisher",
                "sent_sequence_summary": {
                    "count": 10,
                    "digest_sum64": 1,
                    "digest_xor64": 1,
                },
            },
            {
                "ok": True,
                "role": "subscriber",
                "sequence_summary": {
                    "count": 9,
                    "digest_sum64": 2,
                    "digest_xor64": 2,
                    "out_of_order": 0,
                },
            },
        ]
        enrich_worker_integrity(workers)
        out = validate_run(
            {"topology": "publisher_with_oracle", "cadence": "steady50", "duration_s": 1.0},
            workers,
            None,
            [],
        )
        self.assertEqual(out["status"], "inconclusive")
        self.assertIn("integrity_mismatch", out["reasons"])

    def test_offer_rate_within_tolerance_despite_drain_completions(self):
        """Open-loop gate must check offer, not completions (drain after T1)."""
        workers = [
            {
                "ok": True,
                "role": "publisher",
                "duration_s": 1.0,
                "offered": 1000,
                "missed_due_to_backpressure": 0,
                "completed_success": 1000,
                # Completions lagged into drain — would fail a completion-rate gate.
                "msgs_per_s": 800.0,
                "completed_in_window": 800,
            }
        ]
        out = validate_run(
            {
                "topology": "publisher_only",
                "cadence": "loaded75",
                "target_rate": 1000.0,
                "duration_s": 1.0,
            },
            workers,
            None,
            [],
            sys_counters={"publish_received_delta": 1000, "dropped_delta": 0},
        )
        self.assertEqual(out["status"], "valid", out["reasons"])
        self.assertNotIn("open_loop_rate_out_of_tolerance", out["reasons"])


class OpenLoopShapeTests(unittest.TestCase):
    """9e1eab5 / open-loop unify: both shapes must hold the offer and agree on misses."""

    def _offered_rate(self, adapter, target, qos=1, outstanding=32):
        state = _state()
        if adapter.capabilities().publish_sync_on_loop:
            adapter.on_publish = publisher._make_on_publish(state, qos, lock=None)

        async def drive():
            started = time.perf_counter()
            await publisher._run_publish_loop_async(
                adapter,
                state,
                **_loop_kwargs(
                    qos=qos,
                    outstanding=outstanding,
                    cadence="steady50",
                    target_rate=target,
                    until=started + 0.5,
                ),
            )
            return time.perf_counter() - started

        elapsed = asyncio.new_event_loop().run_until_complete(drive())
        return state["offered"] / elapsed, state["missed_due_to_backpressure"], state["offered"]

    def test_awaited_shape_holds_offer_within_two_percent(self):
        target = 2000.0
        # delay well under the interval so there is no backpressure.
        rate, missed, offered = self._offered_rate(
            _FakeAwaitedAdapter(delay_s=0.00005), target, outstanding=32
        )
        self.assertGreater(offered, 100)
        self.assertLess(missed / max(offered, 1), 0.02)
        self.assertGreater(rate, target * 0.98)
        self.assertLess(rate, target * 1.05)

    def test_sync_on_loop_shape_holds_offer_within_two_percent(self):
        target = 2000.0
        rate, missed, offered = self._offered_rate(
            _FakeSyncOnLoopAdapter(complete_after=1), target, outstanding=32
        )
        self.assertGreater(offered, 100)
        self.assertLess(missed / max(offered, 1), 0.02)
        self.assertGreater(rate, target * 0.98)
        self.assertLess(rate, target * 1.05)

    def test_both_shapes_count_misses_under_backpressure(self):
        """Under load both shapes must charge misses, not burst-catch-up."""
        target = 5000.0
        # Service time longer than the interval → sustained backpressure.
        awaited_rate, awaited_missed, awaited_offered = self._offered_rate(
            _FakeAwaitedAdapter(delay_s=0.005), target, outstanding=4
        )
        sync = _FakeSyncOnLoopAdapter(complete_after=64)  # fill window, complete rarely
        # Deferred-style: complete_after large means window fills and stays full.
        sync_rate, sync_missed, sync_offered = self._offered_rate(
            sync, target, outstanding=4
        )
        self.assertGreater(awaited_missed, 0, "awaited must count backpressure misses")
        self.assertGreater(sync_missed, 0, "sync-on-loop must count backpressure misses")
        # Offered rate (submitted + missed) should still track the target.
        self.assertGreater(awaited_rate, target * 0.90)
        self.assertGreater(sync_rate, target * 0.90)

    def test_awaited_catchup_counts_misses_after_deadline_stop(self):
        """Workers still awaiting at T1 must charge measure-window misses.

        Skipping catch-up when cursor["stop"] was set left offered/missed
        under-counted for the awaited shape under sustained backpressure.
        """
        target = 2000.0
        # delay > window so the only in-flight publish finishes during grace,
        # after stop — the case where gated-on-stop catch-up was a no-op.
        adapter = _FakeAwaitedAdapter(delay_s=0.12)
        state = _state()
        window = 0.05
        until = time.perf_counter() + window

        async def drive():
            await publisher._run_publish_loop_async(
                adapter,
                state,
                **_loop_kwargs(
                    qos=1,
                    outstanding=1,
                    cadence="steady50",
                    target_rate=target,
                    until=until,
                ),
            )

        asyncio.new_event_loop().run_until_complete(drive())
        # 50 ms at 2000/s → ~100 offer slots; one submit + the rest misses.
        self.assertEqual(state["submitted"], 1)
        self.assertGreater(state["missed_due_to_backpressure"], 50)
        self.assertGreater(state["offered"], 80)
        offer_rate = state["offered"] / window
        self.assertGreater(offer_rate, target * 0.85)


class DeferredWakeTests(unittest.TestCase):
    """fc61949: wake must be call_later, not call_soon, or the test is vacuous."""

    def test_deferred_completion_wakes_parked_loop(self):
        outstanding = 8
        adapter = _FakeDeferredAdapter()
        state = _state()
        adapter.on_publish = publisher._make_on_publish(state, 1, lock=None)

        async def drive():
            return await publisher._run_publish_loop_async(
                adapter,
                state,
                **_loop_kwargs(outstanding=outstanding, until=time.perf_counter() + 0.3),
            )

        asyncio.new_event_loop().run_until_complete(drive())
        tally = state["completions"].summary(1)
        completed = tally["completed_success"] + state["overflow_success"]
        self.assertGreater(
            completed,
            outstanding * 10,
            f"only {completed} completions: loop parked and was never woken",
        )

    def test_sync_on_loop_closes_window_at_deadline(self):
        """_expire must close CompletionLog at T1, matching awaited _stop_at_deadline.

        Leaving the window open until main() returns credited same-turn post-
        deadline acks to completed_in_window for publish_sync_on_loop clients.
        """
        adapter = _FakeDeferredAdapter()
        state = _state()
        adapter.on_publish = publisher._make_on_publish(state, 1, lock=None)

        async def drive():
            await publisher._run_publish_loop_async(
                adapter,
                state,
                **_loop_kwargs(outstanding=8, until=time.perf_counter() + 0.05),
            )

        asyncio.new_event_loop().run_until_complete(drive())
        # Contract: closed before run_loop returns, not only in publisher main().
        self.assertTrue(state["completions"].window_closed)
        # A completion that lands after the cut must tally as drain.
        self.assertTrue(state["completions"].add(1_000))
        tally = state["completions"].summary(1)
        self.assertGreaterEqual(tally["completed_during_drain"], 1)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
