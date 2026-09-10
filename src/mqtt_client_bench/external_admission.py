"""Event-driven external-pacer admission for native asyncio clients.

The calendar lives in the pacer process. This consumer has no deadline loop:
one readiness callback consumes at most one datagram and returns to the loop.
In particular, already-buffered tokens cannot bypass other ready descriptors.

Synchronous nonblocking admissions run directly in that callback. Awaitable
admissions use one phase-owned consumer task and one pending token; the reader
is paused until that admission finishes. There is no per-token task, timeout,
user-space backlog, thread bridge, or speculative socket read.
"""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import Awaitable, Callable

from mqtt_client_bench.pacing import PaceToken, TOKEN_SIZE, unpack_token


async def consume_external_tokens(
    sock: socket.socket,
    on_token: Callable[[PaceToken, int], None] | Callable[[PaceToken, int], Awaitable[None]],
    *,
    until_ns: int,
    expected_tokens: int | None,
    asynchronous: bool = False,
) -> dict:
    """Consume a bounded phase on a selector loop, without owning ``sock``.

    ``until_ns`` includes the predeclared receive grace, never new deadlines.
    Missing tokens remain a stimulus failure in the existing recorder. Malformed
    input, callback failure and an admission suspended past the bound fail loud.
    Cancellation removes the reader before waiting for the owned task to stop.
    """
    if sock.gettimeout() != 0.0:
        raise ValueError("external pacer receiver must be nonblocking")
    if expected_tokens is not None and expected_tokens < 0:
        raise ValueError("expected_tokens must be nonnegative")
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    ready = asyncio.Event() if asynchronous else None
    fd = sock.fileno()
    reader_installed = False
    timer = None
    worker = None
    pending = None
    received = 0
    callbacks = 0

    def disarm():
        nonlocal reader_installed
        if reader_installed:
            loop.remove_reader(fd)
            reader_installed = False

    def finish(error=None):
        disarm()
        if not done.done():
            if error is None:
                done.set_result(None)
            else:
                done.set_exception(error)

    def arm():
        nonlocal reader_installed
        if not done.done() and not reader_installed:
            loop.add_reader(fd, readable)
            reader_installed = True

    def expired():
        nonlocal timer
        remaining = (until_ns - time.monotonic_ns()) / 1_000_000_000
        if remaining > 0:
            # asyncio timers may wake slightly early; do not shorten the phase.
            timer = loop.call_later(remaining, expired)
        elif pending is not None:
            finish(TimeoutError("external pacer admission exceeded receive window"))
        else:
            finish()

    def complete_token():
        if expected_tokens is not None and received >= expected_tokens:
            finish()

    def readable():
        nonlocal pending, received, callbacks
        if done.done():
            return
        callbacks += 1
        if time.monotonic_ns() >= until_ns:
            expired()
            return
        try:
            # Exactly ONE recv. Never drain to EAGAIN or probe before readiness.
            data = sock.recv(TOKEN_SIZE + 1)
        except (BlockingIOError, InterruptedError):
            # Readiness may be stale. Handler exceptions must not enter this case.
            return
        except OSError as exc:
            finish(exc)
            return
        try:
            receiver_ns = time.monotonic_ns()
            if len(data) != TOKEN_SIZE:
                raise ValueError("invalid external pacer datagram size")
            token = unpack_token(data)
            if token is None:
                raise ValueError("invalid external pacer datagram")
            received += 1
            if asynchronous:
                disarm()
                pending = (token, receiver_ns)
                ready.set()
            else:
                on_token(token, receiver_ns)
                complete_token()
        except BaseException as exc:
            finish(exc)

    async def consume():
        nonlocal pending
        try:
            while True:
                await ready.wait()
                ready.clear()
                token, receiver_ns = pending
                await on_token(token, receiver_ns)
                pending = None
                complete_token()
                if done.done():
                    return
                arm()
        except asyncio.CancelledError:
            if not done.done():
                finish(asyncio.CancelledError())
            raise
        except BaseException as exc:
            finish(exc)

    try:
        if expected_tokens == 0 or time.monotonic_ns() >= until_ns:
            return {"driver": "readiness_one_token", "tokens_received": 0, "reader_callbacks": 0}
        if asynchronous:
            # Before admission, also with eager_task_factory: first wait is empty.
            worker = loop.create_task(consume())
        arm()  # Unsupported selector APIs fail closed, never silently bridge threads.
        timer = loop.call_later(max(0, (until_ns - time.monotonic_ns()) / 1e9), expired)
        await done
        return {
            "driver": "readiness_one_token",
            "tokens_received": received,
            "reader_callbacks": callbacks,
        }
    finally:
        disarm()
        if not done.done():
            done.cancel()
        if timer is not None:
            timer.cancel()
        if worker is not None:
            if not worker.done():
                worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
