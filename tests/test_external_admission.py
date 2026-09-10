"""Adversarial readiness/ownership tests; no broker or latency thresholds."""
from __future__ import annotations

import asyncio
import socket
import time
import unittest
from unittest.mock import patch

from mqtt_client_bench.external_admission import consume_external_tokens
from mqtt_client_bench.pacing import pack_token


class ExternalAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def sockets(self, count=0):
        rx, tx = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        for sock in (rx, tx):
            sock.setblocking(False)
            self.addCleanup(sock.close)
        now = time.monotonic_ns()
        for seq in range(count):
            tx.send(pack_token(seq, now, now))
        return rx, tx

    async def drive(self, rx, callback, count, asynchronous=False, **kwargs):
        return await consume_external_tokens(
            rx, callback, expected_tokens=count, asynchronous=asynchronous,
            until_ns=kwargs.get("until_ns", time.monotonic_ns() + 2_000_000_000),
        )

    async def test_backlogged_tokens_get_one_callback_per_selector_turn(self):
        loop = asyncio.get_running_loop()
        for asynchronous in (False, True):
            with self.subTest(asynchronous=asynchronous):
                rx, tx = self.sockets(64)
                seen, heartbeats, turns = [], [], []
                turn = 0
                original = loop._selector.select

                def select(timeout=None):
                    nonlocal turn
                    turn += 1
                    return original(timeout)

                def handler(token, receiver_ns):
                    self.assertGreater(receiver_ns, 0)
                    seen.append(token.sequence)
                    turns.append(turn)
                    # Work made ready by the prior token runs before the next.
                    self.assertEqual(len(heartbeats), len(seen) - 1)
                    loop.call_soon(heartbeats.append, token.sequence)

                async def async_handler(token, receiver_ns):
                    handler(token, receiver_ns)  # Deliberately never awaits.

                with patch.object(loop._selector, "select", side_effect=select), patch.object(
                    loop, "create_task", wraps=loop.create_task
                ) as tasks:
                    result = await self.drive(rx, async_handler if asynchronous else handler,
                                              64, asynchronous)
                self.assertEqual(seen, list(range(64)))
                self.assertEqual(len(set(turns)), 64)
                self.assertEqual(result["tokens_received"], 64)
                self.assertEqual(tasks.call_count, int(asynchronous))
                self.assertNotIn(rx.fileno(), loop._selector.get_map())

    async def test_sync_phase_has_one_registration_and_one_timer(self):
        loop = asyncio.get_running_loop()
        rx, tx = self.sockets(32)
        with patch.object(loop, "add_reader", wraps=loop.add_reader) as add, patch.object(
            loop, "call_later", wraps=loop.call_later
        ) as timer:
            result = await self.drive(rx, lambda *_: None, 32)
        self.assertEqual(add.call_count, 1)
        self.assertEqual(timer.call_count, 1)
        self.assertEqual(result["reader_callbacks"], 32)

    async def test_ready_reply_descriptor_progresses_while_tokens_remain(self):
        loop = asyncio.get_running_loop()
        tokens, _ = self.sockets(64)
        reply_rx, reply_tx = self.sockets()
        produced, responses = [], []

        def handle(token, _now):
            produced.append(token.sequence)
            reply_tx.send(bytes([token.sequence]))
            if len(produced) >= 4:
                self.assertGreater(len(responses), 0)

        def reply():
            responses.append(reply_rx.recv(64))

        loop.add_reader(reply_rx, reply)
        try:
            await self.drive(tokens, handle, 64)
            await asyncio.sleep(0)
            self.assertEqual(len(produced), 64)
            self.assertEqual(len(responses), 64)
        finally:
            loop.remove_reader(reply_rx)

    async def test_suspended_async_admission_pauses_reader_without_spinning(self):
        loop = asyncio.get_running_loop()
        rx, _ = self.sockets(8)
        entered, release = asyncio.Event(), asyncio.Event()
        seen = []

        async def handler(token, _now):
            seen.append(token.sequence)
            if token.sequence == 0:
                entered.set()
                await release.wait()

        task = asyncio.create_task(self.drive(rx, handler, 8, True))
        await entered.wait()
        for _ in range(8):
            await asyncio.sleep(0)
        self.assertEqual(seen, [0])
        self.assertNotIn(rx.fileno(), loop._selector.get_map())
        release.set()
        result = await task
        self.assertEqual(seen, list(range(8)))
        self.assertEqual(result["reader_callbacks"], 8)

    async def test_cancellation_cleans_idle_and_busy_readers_and_owned_task(self):
        loop = asyncio.get_running_loop()
        for asynchronous in (False, True):
            for buffered in (False, True):
                with self.subTest(asynchronous=asynchronous, buffered=buffered):
                    rx, _ = self.sockets(8 if buffered else 0)
                    seen, ended = [], []
                    started = asyncio.Event()

                    def sync_handler(token, _now):
                        seen.append(token.sequence)
                        started.set()

                    async def async_handler(token, _now):
                        sync_handler(token, _now)
                        try:
                            await asyncio.Future()
                        finally:
                            ended.append(True)

                    existing = asyncio.all_tasks()
                    task = asyncio.create_task(self.drive(
                        rx, async_handler if asynchronous else sync_handler, 64, asynchronous))
                    if buffered:
                        await started.wait()
                    else:
                        await asyncio.sleep(0)
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                    before = list(seen)
                    await asyncio.sleep(0)
                    self.assertEqual(seen, before)
                    self.assertNotIn(rx.fileno(), loop._selector.get_map())
                    self.assertFalse(asyncio.all_tasks() - existing)
                    if asynchronous and buffered:
                        self.assertEqual(ended, [True])

    async def test_errors_in_admission_are_not_swallowed_as_socket_readiness(self):
        loop = asyncio.get_running_loop()
        for asynchronous in (False, True):
            for exception in (ValueError, BlockingIOError, InterruptedError):
                with self.subTest(asynchronous=asynchronous, exception=exception):
                    rx, _ = self.sockets(4)
                    calls = []

                    def sync_handler(*_):
                        calls.append(True)
                        raise exception("adapter failure")

                    async def async_handler(*args):
                        sync_handler(*args)

                    with self.assertRaisesRegex(exception, "adapter failure"):
                        await self.drive(rx, async_handler if asynchronous else sync_handler,
                                         4, asynchronous)
                    self.assertEqual(calls, [True])
                    self.assertNotIn(rx.fileno(), loop._selector.get_map())

    async def test_self_cancelled_async_handler_propagates(self):
        rx, _ = self.sockets(4)

        async def handler(*_):
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.drive(rx, handler, 4, True)
        self.assertNotIn(rx.fileno(), asyncio.get_running_loop()._selector.get_map())

    async def test_malformed_datagrams_fail_instead_of_disappearing(self):
        for data in (b"", b"bad", b"x" * 64):
            rx, tx = self.sockets()
            tx.send(data)
            with self.assertRaisesRegex(ValueError, "invalid external pacer"):
                await self.drive(rx, lambda *_: self.fail("invalid token dispatched"), 1)

    async def test_truncated_phase_does_not_invent_or_replay_tokens(self):
        rx, _ = self.sockets(3)
        seen = []
        result = await self.drive(rx, lambda token, _: seen.append(token.sequence), 8,
                                  until_ns=time.monotonic_ns() + 10_000_000)
        self.assertEqual(seen, [0, 1, 2])
        self.assertEqual(result["tokens_received"], 3)
        self.assertEqual(result["driver"], "readiness_one_token")

    async def test_idle_phase_and_zero_count_complete_and_release_descriptor(self):
        for count in (0, 1):
            rx, _ = self.sockets()
            result = await self.drive(rx, lambda *_: self.fail("invented token"), count,
                                      until_ns=time.monotonic_ns() + 5_000_000)
            self.assertEqual(result["tokens_received"], 0)
            self.assertNotIn(rx.fileno(), asyncio.get_running_loop()._selector.get_map())

    async def test_unfinished_async_admission_at_deadline_is_an_error_not_success(self):
        rx, _ = self.sockets(2)
        cancelled = []

        async def handler(*_):
            try:
                await asyncio.Future()
            finally:
                cancelled.append(True)

        with self.assertRaisesRegex(TimeoutError, "admission exceeded receive window"):
            await self.drive(rx, handler, 2, True, until_ns=time.monotonic_ns() + 10_000_000)
        self.assertEqual(cancelled, [True])

    async def test_one_socket_can_be_reused_for_a_new_phase(self):
        rx, tx = self.sockets(4)
        seen = []
        await self.drive(rx, lambda token, _: seen.append(token.sequence), 4)
        now = time.monotonic_ns()
        for seq in range(3):
            tx.send(pack_token(seq, now, now))
        await self.drive(rx, lambda token, _: seen.append(token.sequence), 3)
        self.assertEqual(seen, [0, 1, 2, 3, 0, 1, 2])

    async def test_unsupported_reader_has_no_background_fallback(self):
        loop = asyncio.get_running_loop()
        rx, _ = self.sockets(4)
        existing = asyncio.all_tasks()

        async def handler(*_):
            self.fail("unexpected publish")

        with patch.object(loop, "add_reader", side_effect=NotImplementedError):
            with self.assertRaises(NotImplementedError):
                await self.drive(rx, handler, 4, True)
        self.assertFalse(asyncio.all_tasks() - existing)

    async def test_blocking_socket_is_rejected(self):
        rx, _ = self.sockets()
        rx.setblocking(True)
        with self.assertRaisesRegex(ValueError, "nonblocking"):
            await self.drive(rx, lambda *_: None, 1)

    @unittest.skipUnless(hasattr(asyncio, "eager_task_factory"), "requires Python 3.12+")
    async def test_eager_factory_preserves_one_owned_consumer_and_cancellation(self):
        loop = asyncio.get_running_loop()
        factory = loop.get_task_factory()
        loop.set_task_factory(asyncio.eager_task_factory)
        try:
            rx, _ = self.sockets(16)
            seen = []

            async def handler(token, _now):
                seen.append(token.sequence)

            result = await self.drive(rx, handler, 16, True)
            self.assertEqual(seen, list(range(16)))
            self.assertEqual(result["tokens_received"], 16)
        finally:
            loop.set_task_factory(factory)

class RttExternalContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_ready_tokens_preserve_admission_accounting_for_both_shapes(self):
        from tests.test_rtt_cooperation import _Adapter, _state
        from mqtt_client_bench.roles import rtt_initiator as rtt
        from types import SimpleNamespace
        from mqtt_client_bench.temporal_trace import TemporalTraceSampler

        for sync in (False, True):
            for refuse in (False, True):
                with self.subTest(sync=sync, refuse=refuse):
                    rx, tx = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
                    rx.setblocking(False)
                    tx.setblocking(False)
                    state = _state()
                    state["temporal_trace"] = TemporalTraceSampler(8, stride=2)
                    now = time.monotonic_ns()
                    for seq in range(8):
                        tx.send(pack_token(seq, now, now))

                    class Adapter(_Adapter):
                        def publish_nowait(self, *args, **kwargs):
                            mid = super().publish_nowait(*args, **kwargs)
                            return None if refuse else mid

                    adapter = Adapter(lambda payload: rtt._on_message(state, SimpleNamespace(payload=payload)))
                    try:
                        await rtt._send_loop_async(
                            adapter, state, "req", 1, b"12345678", 32, 3942,
                            time.perf_counter() + 2, pacer_mode="external",
                            pacer_sock=rx, sync_on_loop=sync,
                            until_ns=now + 2_000_000_000, expected_tokens=8,
                        )
                        self.assertEqual(state["offered"], 8)
                        self.assertEqual(state["completed_in_window"], 0 if refuse else 8)
                        self.assertEqual(state["sent_in_window"], 0 if refuse else 8)
                        self.assertEqual(state["inflight"], {})
                        self.assertEqual(state["early_rtt"], {})
                        self.assertEqual(state["trace_pending"], {})
                        self.assertIsNone(state["publishing_seq"])
                        self.assertEqual(state["external_admission"]["tokens_received"], 8)
                    finally:
                        rx.close()
                        tx.close()

    async def test_cancelled_async_publish_retires_pending_trace_without_success(self):
        from tests.test_rtt_cooperation import _state
        from mqtt_client_bench.roles import rtt_initiator as rtt
        from mqtt_client_bench.temporal_trace import TemporalTraceSampler
        state = _state()
        state["temporal_trace"] = TemporalTraceSampler(4)
        entered = asyncio.Event()

        class Adapter:
            async def publish(self, *_args, **_kwargs):
                entered.set()
                await asyncio.Future()

        rx, tx = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        rx.setblocking(False)
        now = time.monotonic_ns()
        tx.send(pack_token(0, now, now))
        task = asyncio.create_task(rtt._send_loop_async(
            Adapter(), state, "req", 1, b"12345678", 32, 3942, time.perf_counter() + 2,
            pacer_mode="external", pacer_sock=rx, sync_on_loop=False,
            until_ns=now + 2_000_000_000, expected_tokens=1,
        ))
        try:
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(state["offered"], 1)
            self.assertEqual(state["sent_in_window"], 0)
            self.assertEqual(state["completed_in_window"], 0)
            self.assertEqual(state["inflight"], {})
            self.assertEqual(state["trace_pending"], {})
            self.assertIsNone(state["publishing_seq"])
        finally:
            rx.close()
            tx.close()

    async def test_schedule_latency_keeps_pre_admission_wait_visible(self):
        from mqtt_client_bench.roles import rtt_initiator as rtt
        state = {}
        trace = {
            "stride": 12,
            "scheduled_deadline_ns": [1_000_000, 2_000_000],
            "publish_call_ns": [3_000_000, 5_000_000],
            "receive_ns": [5_000_000, 8_000_000],
        }
        body = rtt._result_body(
            state, {}, 1, 0, [2_000_000, 3_000_000], {}, 2, 2, 3, 1,
            temporal_trace=trace,
        )
        self.assertEqual(body["scheduled_response_latency"]["p95_ms"], 6.0)
        self.assertEqual(body["scheduled_admission_delay"]["p95_ms"], 3.0)
        self.assertEqual(body["scheduled_response_latency"]["sampling"], "every_Nth_completed_request")
        self.assertEqual(body["offered"], 3)
        self.assertEqual(body["missed_due_to_backpressure"], 1)
        self.assertEqual(body["latencies_ns"], [2_000_000, 3_000_000])


if __name__ == "__main__":
    unittest.main()
