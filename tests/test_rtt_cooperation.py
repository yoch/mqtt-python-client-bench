"""Real-ready-socket regressions for the RTT driver's cooperative scheduling.

No broker, library-specific adapter, strace or throughput assertions: these
exercise the production send loop with real socket readiness and queued work.
"""
from __future__ import annotations

import asyncio
import socket
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mqtt_client_bench.pacing import pack_token
from mqtt_client_bench.roles import rtt_initiator as rtt
from mqtt_client_bench.sampling import ReservoirSampler
from mqtt_client_bench.workloads import decode_header_fields


def _state(phase="measure"):
    return {
        "phase": phase, "lock": threading.Lock(), "inflight": {},
        "early_rtt": {}, "publishing_seq": None,
        "latencies_ns": ReservoirSampler(256, seed=71),
        "sent_in_window": 0, "completed_in_window": 0,
        "offered": 0, "missed_due_to_backpressure": 0,
        "retracted_completions": 0,
    }


class _Adapter:
    def __init__(self, deliver):
        self.deliver = deliver
        self.calls = 0

    def publish_nowait(self, topic, payload, qos, retain):
        self.calls += 1
        self.deliver(payload)
        return self.calls

    async def publish(self, topic, payload, qos, retain):
        # An async callable is allowed to finish without suspending too.
        return self.publish_nowait(topic, payload, qos, retain)


class _DueClock:
    """Every in-loop calendar slot is already due, without patching loop time."""
    def __init__(self):
        self.now = 100.0

    def perf_counter(self):
        self.now += 1.0
        return self.now

    def perf_counter_ns(self):
        return int(self.now * 1_000_000_000)


@unittest.skipUnless(hasattr(socket, "AF_UNIX"), "requires Unix datagram sockets")
class RttCooperationTests(unittest.IsolatedAsyncioTestCase):
    def _tokens(self, count):
        rx, tx = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        for sock in (rx, tx):
            sock.setblocking(False)
            self.addCleanup(sock.close)
        start = time.monotonic_ns()
        for index in range(count):
            tx.send(pack_token(index, start + index * 253_678, start + index * 253_678))
        return rx

    async def _drive(self, adapter, state, count, window, sync_on_loop=True):
        rx = self._tokens(count)
        await rtt._send_loop_async(
            adapter, state, "test/request", 1, b"12345678", window, 3942,
            time.perf_counter() + 5, sync_on_loop=sync_on_loop,
            pacer_mode="external", pacer_sock=rx,
            until_ns=time.monotonic_ns() + 5_000_000_000,
            expected_tokens=count,
        )

    async def test_buffered_tokens_allow_reply_io_before_the_send_loop_finishes(self):
        for window in (1, 4, 16):
            for sync_on_loop in (False, True):
                for phase in ("warmup", "measure"):
                    with self.subTest(window=window, sync=sync_on_loop, phase=phase):
                        loop = asyncio.get_running_loop()
                        rx, tx = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
                        rx.setblocking(False)
                        tx.setblocking(False)
                        state = _state(phase)
                        served, heartbeat, depths = [], [], []

                        def receive():
                            while True:
                                try:
                                    payload = rx.recv(64)
                                except BlockingIOError:
                                    break
                                served.append(decode_header_fields(payload)[2])
                                rtt._on_message(state, SimpleNamespace(payload=payload))

                        def send(payload):
                            depths.append(len(state["inflight"]))
                            tx.send(payload)

                        adapter = _Adapter(send)
                        loop.add_reader(rx.fileno(), receive)
                        try:
                            loop.call_soon(heartbeat.append, True)
                            await self._drive(adapter, state, 64, window, sync_on_loop)
                            # A completely buffered pacer must not prevent real
                            # readable response FDs or already-ready tasks running.
                            self.assertTrue(heartbeat)
                            self.assertGreater(len(served), 0)
                            self.assertGreater(adapter.calls, window)
                            self.assertLessEqual(max(depths), window)
                            if phase == "measure":
                                self.assertEqual(state["offered"], 64)
                                self.assertEqual(state["sent_in_window"], adapter.calls)
                                self.assertEqual(
                                    state["offered"],
                                    adapter.calls + state["missed_due_to_backpressure"],
                                )
                                self.assertGreater(state["missed_due_to_backpressure"], 0)
                            else:
                                self.assertEqual(state["offered"], 0)
                                self.assertEqual(state["sent_in_window"], 0)
                                self.assertEqual(state["missed_due_to_backpressure"], 0)
                            self.assertIsNone(state["slot_free"])
                            # Drain separately, not inside the producer's window.
                            state["phase"] = "drain"
                            await asyncio.sleep(0)
                            await asyncio.sleep(0)
                            self.assertEqual(served, list(range(1, adapter.calls + 1)))
                            self.assertEqual(state["inflight"], {})
                        finally:
                            loop.remove_reader(rx.fileno())
                            rx.close()
                            tx.close()

    async def test_full_external_window_counts_misses_without_waiting_for_a_reply(self):
        state = _state()
        state["inflight"][999] = 1
        adapter = _Adapter(lambda payload: self.fail("full window admitted a request"))
        heartbeat, delays = [], []
        real_sleep = asyncio.sleep

        async def observed_sleep(delay):
            delays.append(delay)
            await real_sleep(delay)

        asyncio.get_running_loop().call_soon(heartbeat.append, True)
        with patch.object(rtt.asyncio, "sleep", side_effect=observed_sleep):
            await self._drive(adapter, state, 32, 1)
        self.assertTrue(heartbeat)
        self.assertEqual(delays, [0] * 32)
        self.assertEqual(state["offered"], 32)
        self.assertEqual(state["missed_due_to_backpressure"], 32)
        self.assertEqual(adapter.calls, 0)
        self.assertEqual(state["inflight"], {999: 1})

    async def test_overdue_in_loop_window_yields_after_charging_each_miss(self):
        for phase in ("warmup", "measure"):
            with self.subTest(phase=phase):
                state = _state(phase)
                state["inflight"][999] = 1
                heartbeat, delays = [], []
                adapter = _Adapter(lambda payload: self.fail("full window admitted a request"))
                real_sleep = asyncio.sleep

                async def observed_sleep(delay):
                    delays.append(delay)
                    await real_sleep(delay)

                asyncio.get_running_loop().call_soon(heartbeat.append, True)
                with patch.object(rtt, "time", _DueClock()), patch.object(
                    rtt, "_phase_expired", side_effect=[False] * 32 + [True]
                ), patch.object(rtt.asyncio, "sleep", side_effect=observed_sleep):
                    await rtt._send_loop_async(
                        adapter, state, "test", 1, b"12345678", 1, 3942, 200,
                        sync_on_loop=True,
                    )
                self.assertTrue(heartbeat)
                self.assertEqual(delays, [0] * 32)
                self.assertEqual(state["offered"], 32 if phase == "measure" else 0)
                self.assertEqual(state["missed_due_to_backpressure"], state["offered"])
                self.assertEqual(state["inflight"], {999: 1})

    async def test_full_external_window_can_be_cancelled_before_draining_tokens(self):
        state = _state()
        state["inflight"][999] = 1
        adapter = _Adapter(lambda payload: self.fail("full window admitted a request"))
        task = asyncio.create_task(self._drive(adapter, state, 64, 1))
        asyncio.get_running_loop().call_soon(task.cancel)
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())
        self.assertEqual(adapter.calls, 0)

    async def test_missed_offer_is_not_retried_after_a_reply_frees_the_window(self):
        state = _state()
        state["inflight"][999] = 1
        adapter = _Adapter(lambda payload: self.fail("missed offer was retried"))
        asyncio.get_running_loop().call_soon(state["inflight"].clear)
        await self._drive(adapter, state, 1, 1)
        self.assertEqual(state["inflight"], {})
        self.assertEqual(state["offered"], 1)
        self.assertEqual(state["missed_due_to_backpressure"], 1)
        self.assertEqual(adapter.calls, 0)

    async def test_native_early_response_samples_only_accepted_reserved_requests(self):
        from mqtt_client_bench.temporal_trace import TemporalTraceSampler

        for sync_on_loop in (False, True):
            for refused in (False, True):
                with self.subTest(sync=sync_on_loop, refused=refused):
                    state = _state()
                    state["temporal_trace"] = TemporalTraceSampler(max_points=8, stride=2)

                    class Adapter(_Adapter):
                        def publish_nowait(self, topic, payload, qos, retain):
                            mid = super().publish_nowait(topic, payload, qos, retain)
                            return None if refused else mid

                    adapter = Adapter(lambda payload: rtt._on_message(
                        state, SimpleNamespace(payload=payload)
                    ))
                    await self._drive(adapter, state, 8, 1, sync_on_loop)
                    self.assertEqual(state["offered"], 8)
                    self.assertEqual(state["sent_in_window"], 0 if refused else 8)
                    self.assertEqual(state["completed_in_window"], 0 if refused else 8)
                    self.assertEqual(state["trace_pending"], {})
                    self.assertEqual(
                        [row["sequence"] for row in state["temporal_trace"].records()],
                        [] if refused else [2, 4, 6, 8],
                    )

    async def test_unsaturated_immediate_completion_adds_no_scheduling_hop(self):
        for sync_on_loop in (False, True):
            with self.subTest(sync=sync_on_loop):
                state = _state()
                adapter = _Adapter(lambda payload: rtt._on_message(
                    state, SimpleNamespace(payload=payload)
                ))
                with patch.object(rtt.asyncio, "sleep", side_effect=AssertionError(
                    "uncongested path must not gain an explicit sleep"
                )):
                    await self._drive(adapter, state, 32, 1, sync_on_loop)
                self.assertEqual(state["offered"], 32)
                self.assertEqual(state["sent_in_window"], 32)
                self.assertEqual(state["completed_in_window"], 32)
                self.assertEqual(state["missed_due_to_backpressure"], 0)
                self.assertEqual(state["inflight"], {})


if __name__ == "__main__":
    unittest.main()
