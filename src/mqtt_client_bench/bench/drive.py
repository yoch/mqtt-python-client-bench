"""The only benchmark code that runs inside the client's process.

Everything here is counting and pacing, written so that each message costs the
harness the same few operations whatever the library: an attribute increment
per publish and per completion, a token per publish on capacity points, one
clock read, an 8-byte prefix and one send-time store on fixed-rate points.
There is no per-message bookkeeping by message id, no reservoir and no lock;
latency is measured by the neutral peer, or, when the client is the receiver,
as one subtraction.

Fixed offers number their messages. The stamp is the actual publish time, so
latency is transit only; how late the publish itself was against the message's
due time is the schedule lag, computed from the stored send times after the
run and reported next to latency, so a client that falls behind its schedule
cannot hide it.

Shapes, chosen once per run from the adapter's capabilities:

- sync: the library runs its own network thread; the main thread publishes.
- nowait: the library admits a publish synchronously on the worker's loop
  (mqttium, gmqtt).
- awaited: ``await publish()`` is the library's only API (aiomqtt, aiomqtt3,
  amqtt, zmqtt); ``window`` coroutines keep that many publishes in flight, as
  awaiting one at a time would measure round trips, not capacity.

``harness_cost`` runs these same functions against a null client.
"""

from __future__ import annotations

import asyncio
import time
from array import array
from collections import deque
from queue import SimpleQueue

NS = 1_000_000_000
TICK_NS = 1_000_000
UNSENT = -1
# A late wake-up owes at most this many ticks; beyond that the offer is lost,
# not repaid as a burst, and the run reports that it did not hold its rate.
MAX_CATCHUP_TICKS = 4
AWAITED_FIXED_WORKERS = 256

monotonic_ns = time.monotonic_ns


class Run:
    """Counters shared by the drive loop, the callbacks and the snapshot thread."""

    __slots__ = (
        "stop",
        "wake",
        "sent",
        "done",
        "failed",
        "rejected",
        "received",
        "skipped",
        "latencies",
        "latency_overflow",
        "sends",
        "send_overflow",
    )

    def __init__(self, latency_capacity: int = 0, send_capacity: int = 0) -> None:
        self.stop = False
        # Thread-safe callable that unparks the drive loop after ``stop`` is set.
        self.wake = None
        self.sent = 0  # publishes the library accepted
        self.done = 0  # completions: QoS 0 handed to transport, QoS 1 PUBACK
        self.failed = 0  # completions carrying a failure reason code
        self.rejected = 0  # publishes the library refused synchronously
        self.received = 0  # PUBLISH delivered to on_message
        self.skipped = 0  # fixed-rate offer the client fell too far behind to send
        # Receiver-side one-way / round-trip ns, indexed by ``received``.
        # Allocated and touched up front so it is part of the baseline RSS and
        # does not grow inside the window the client's memory is read over.
        self.latencies = array("q", bytes(8 * latency_capacity))
        self.latency_overflow = 0
        # Publish time, indexed by the message's place in the fixed offer;
        # UNSENT marks offered messages never published.
        self.sends = array("q", [UNSENT]) * send_capacity
        self.send_overflow = 0

    def request_stop(self) -> None:
        self.stop = True
        if self.wake is not None:
            self.wake()

    def snapshot(self) -> dict:
        return {
            "sent": self.sent,
            "done": self.done,
            "failed": self.failed,
            "rejected": self.rejected,
            "received": self.received,
            "skipped": self.skipped,
            "at_ns": monotonic_ns(),
        }

    def window_latencies(self, start: dict, end: dict):
        lo = min(start["received"], len(self.latencies))
        hi = min(end["received"], len(self.latencies))
        return self.latencies[lo:hi]

    def window_lags(self, rate: int, t_start: int, t_measure: int, t_end: int):
        """``(lags, unsent)`` of the messages due inside the window.

        Computed after the run, so a publish pays one store, not the arithmetic.
        """
        lo = min(rate * (t_measure - t_start) // NS, len(self.sends))
        hi = min(rate * (t_end - t_start) // NS, len(self.sends))
        lags = [sent - due_ns(i, rate, t_start) for i, sent in enumerate(self.sends[lo:hi], lo) if sent != UNSENT]
        return lags, (hi - lo) - len(lags)


def payload_maker(size: int):
    """``make()`` -> the same unstamped payload, for capacity points."""
    body = b"A" * size

    def make() -> bytes:
        return body

    return make


def due_ns(index: int, rate: int, t_start: int) -> int:
    """When ``_owed`` first counts message ``index`` of a fixed offer."""
    return t_start + ((index + 1) * NS + rate - 1) // rate


def fixed_payload_maker(size: int, run: Run):
    """``make(index)`` for fixed offers: stamps the payload, keeps the send time."""
    if size < 8:
        raise ValueError("a fixed offer needs 8 payload bytes for the send stamp")
    sends = run.sends
    tail = b"A" * (size - 8)

    def make(index: int) -> bytes:
        now = monotonic_ns()
        try:
            sends[index] = now
        except IndexError:
            run.send_overflow += 1
        return now.to_bytes(8, "little") + tail

    return make


def _is_failure(reason_code) -> bool:
    if reason_code is None or reason_code == 0:
        return False
    try:
        return int(getattr(reason_code, "value", reason_code)) >= 128
    except (TypeError, ValueError):
        return False


def completion_callback(run: Run, on_complete=None):
    """``on_publish`` with paho VERSION2's signature."""
    if on_complete is None:

        def on_publish(client, userdata, mid, reason_code=None, properties=None):
            if reason_code and _is_failure(reason_code):
                run.failed += 1
            else:
                run.done += 1

        return on_publish

    def on_publish_notify(client, userdata, mid, reason_code=None, properties=None):
        if reason_code and _is_failure(reason_code):
            run.failed += 1
        else:
            run.done += 1
        on_complete()

    return on_publish_notify


def message_callback(run: Run, *, stamped: bool):
    """``on_message``: a count, and the one-way latency when stamped."""
    if not stamped:

        def on_message(client, userdata, msg):
            run.received += 1

        return on_message

    latencies = run.latencies
    from_bytes = int.from_bytes

    def on_message_stamped(client, userdata, msg):
        now = monotonic_ns()
        try:
            latencies[run.received] = now - from_bytes(msg.payload[:8], "little")
        except IndexError:
            run.latency_overflow += 1
        run.received += 1

    return on_message_stamped


def sleep_until_ns(deadline: int) -> None:
    delay = deadline - monotonic_ns()
    if delay > 0:
        time.sleep(delay / 1e9)


def _owed(rate: int, t_start: int, now: int, run: Run, max_due: int) -> int:
    due = rate * (now - t_start) // NS - run.sent - run.rejected - run.skipped
    if due > max_due:
        run.skipped += due - max_due
        due = max_due
    return due


# ------------------------------------------------------------------ sync


def pub_capacity_sync(adapter, run: Run, *, topic, qos, make, window, t_start) -> None:
    """Closed loop: at most ``window`` publishes awaiting completion."""
    tokens: SimpleQueue = SimpleQueue()
    put = tokens.put
    get = tokens.get
    for _ in range(window):
        put(None)
    adapter.on_publish = completion_callback(run, lambda: put(None))
    run.wake = lambda: put(None)
    publish = adapter.publish
    sleep_until_ns(t_start)
    while True:
        get()
        if run.stop:
            break
        if publish(topic, make(), qos).rc == 0:
            run.sent += 1
        else:
            run.rejected += 1
            put(None)
            time.sleep(0)


def pub_fixed_sync(adapter, run: Run, *, topic, qos, make, rate, t_start, t_end) -> None:
    """Open loop: ``rate`` publishes per second, in 1 ms ticks."""
    adapter.on_publish = completion_callback(run)
    publish = adapter.publish
    max_due = rate * TICK_NS * MAX_CATCHUP_TICKS // NS + 1
    next_tick = t_start
    sleep_until_ns(t_start)
    while True:
        now = monotonic_ns()
        if now >= t_end:
            break
        owed = _owed(rate, t_start, now, run, max_due)
        first = run.sent + run.rejected + run.skipped
        for index in range(first, first + owed):
            if publish(topic, make(index), qos).rc == 0:
                run.sent += 1
            else:
                run.rejected += 1
        next_tick += TICK_NS
        if next_tick < now:
            next_tick = now + TICK_NS
        sleep_until_ns(next_tick)


# ----------------------------------------------------------------- async


async def asleep_until_ns(deadline: int) -> None:
    delay = deadline - monotonic_ns()
    if delay > 0:
        await asyncio.sleep(delay / 1e9)


async def pub_capacity_nowait(adapter, run: Run, *, topic, qos, make, window, t_start) -> None:
    """Refill the window whenever a quarter of it has completed.

    Waking per completion would cost a future per message; waking per quarter
    window keeps the window between 3/4 and full and pays one per 16 messages.
    """
    loop = asyncio.get_running_loop()
    state = {"waiter": None}
    low_water = window - max(1, window // 4)

    def release() -> None:
        waiter = state["waiter"]
        state["waiter"] = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def on_complete() -> None:
        if state["waiter"] is not None and run.sent - run.done - run.failed <= low_water:
            release()

    adapter.on_publish = completion_callback(run, on_complete)
    run.wake = lambda: loop.call_soon_threadsafe(release)
    publish_nowait = adapter.publish_nowait
    await asleep_until_ns(t_start)
    while not run.stop:
        burst = 0
        while run.sent - run.done - run.failed < window:
            run.sent += 1
            if publish_nowait(topic, make(), qos) is None:
                run.sent -= 1
                run.rejected += 1
                break
            burst += 1
            if burst >= window:
                break
        if run.sent - run.done - run.failed >= window:
            waiter = loop.create_future()
            state["waiter"] = waiter
            await waiter
        else:
            # QoS 0 completes inline; yield so the transport can flush.
            await asyncio.sleep(0)


async def pub_capacity_awaited(adapter, run: Run, *, topic, qos, make, window, t_start) -> None:
    publish = adapter.publish

    async def worker() -> None:
        while not run.stop:
            run.sent += 1
            try:
                await publish(topic, make(), qos)
            except Exception:  # noqa: BLE001
                run.failed += 1
                continue
            run.done += 1

    await asleep_until_ns(t_start)
    await asyncio.gather(*(worker() for _ in range(window)))


async def pub_fixed_nowait(adapter, run: Run, *, topic, qos, make, rate, t_start, t_end) -> None:
    adapter.on_publish = completion_callback(run)
    publish_nowait = adapter.publish_nowait
    max_due = rate * TICK_NS * MAX_CATCHUP_TICKS // NS + 1
    next_tick = t_start
    await asleep_until_ns(t_start)
    while True:
        now = monotonic_ns()
        if now >= t_end:
            break
        owed = _owed(rate, t_start, now, run, max_due)
        first = run.sent + run.rejected + run.skipped
        for index in range(first, first + owed):
            run.sent += 1
            if publish_nowait(topic, make(index), qos) is None:
                run.sent -= 1
                run.rejected += 1
        next_tick += TICK_NS
        if next_tick < now:
            next_tick = now + TICK_NS
        await asleep_until_ns(next_tick)


async def pub_fixed_awaited(adapter, run: Run, *, topic, qos, make, rate, t_start, t_end) -> None:
    """A pacer hands out credits each tick; idle workers wait on one shared gate.

    Credits are ``[next, end)`` ranges of message indices, oldest first, so a
    worker publishes the message it took the credit for.
    """
    loop = asyncio.get_running_loop()
    publish = adapter.publish
    credits: deque = deque()
    gate = {"future": loop.create_future(), "done": False}

    async def worker() -> None:
        while True:
            while not credits:
                if gate["done"]:
                    return
                await gate["future"]
            span = credits[0]
            index = span[0]
            span[0] += 1
            if span[0] == span[1]:
                credits.popleft()
            run.sent += 1
            try:
                await publish(topic, make(index), qos)
            except Exception:  # noqa: BLE001
                run.failed += 1
                continue
            run.done += 1

    def open_gate() -> None:
        fut = gate["future"]
        gate["future"] = loop.create_future()
        if not fut.done():
            fut.set_result(None)

    async def pacer() -> None:
        max_due = rate * TICK_NS * MAX_CATCHUP_TICKS // NS + 1
        issued = 0
        next_tick = t_start
        await asleep_until_ns(t_start)
        while True:
            now = monotonic_ns()
            if now >= t_end:
                break
            total = rate * (now - t_start) // NS
            due = total - issued - run.skipped
            if due > max_due:
                run.skipped += due - max_due
                due = max_due
            if due > 0:
                issued += due
                if credits and credits[-1][1] == total - due:
                    credits[-1][1] = total
                else:
                    credits.append([total - due, total])
                open_gate()
            next_tick += TICK_NS
            if next_tick < now:
                next_tick = now + TICK_NS
            await asleep_until_ns(next_tick)
        # Credits still unspent at t_end were offered and never sent.
        run.skipped += sum(end - start for start, end in credits)
        credits.clear()
        gate["done"] = True
        open_gate()

    await asyncio.gather(pacer(), *(worker() for _ in range(AWAITED_FIXED_WORKERS)))


async def wait_until(predicate, deadline_ns: int, poll_s: float = 0.01) -> bool:
    while not predicate():
        if monotonic_ns() >= deadline_ns:
            return False
        await asyncio.sleep(poll_s)
    return True


def wait_until_sync(predicate, deadline_ns: int, poll_s: float = 0.01) -> bool:
    while not predicate():
        if monotonic_ns() >= deadline_ns:
            return False
        time.sleep(poll_s)
    return True
