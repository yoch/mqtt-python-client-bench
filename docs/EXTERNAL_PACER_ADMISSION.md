# Native external-pacer admission

## Contract

The pacer process owns the absolute calendar and emits one nonblocking Unix
datagram per scheduled offer. It never waits for the client or moves subsequent
deadlines. The SUT owns MQTT state and calls the library only on its native loop.
The orchestrator owns process lifecycle and phase coordination.

The native receiver is event-driven, not a task that polls a ready socket.
A persistent `add_reader` callback receives **at most one token per invocation**
and returns. Even an already-full socket must pass through the event-loop
selector between admissions. This bounds harness work per ready turn and lets
already-ready MQTT descriptors and deferred writes progress. It does not impose
a client-specific response priority or an unconditional sleep.

For synchronous nonblocking admission APIs (MQTTium and gmqtt), the callback
calls `publish_nowait` directly. There are zero per-token tasks or timeouts and
one reader registration plus one phase-end timer. For awaitable-only APIs, one
phase-owned task performs serial admission as before; the reader is paused while
that admission is pending. There is at most one pending token, no user-space
backlog, and no thread bridge. A suspended async publish past the receive bound
fails the run rather than silently passing an incomplete observation.

## What independence does and does not mean

The calendar and token emissions do not depend on SUT progress. Real API calls
still have to execute on the SUT's event-loop thread: Python cannot guarantee a
wall-clock admission while that thread is busy. No input architecture can both
retain all overdue offers and promise they execute at their original deadlines.
This design removes the **tight catch-up drain loop**, not physical scheduling
delay. It keeps already-emitted tokens in the bounded kernel socket and records
the delay instead of hiding it or sleeping until a more favorable instant.

Full-window offers are counted once as missed and never retried; refused
admissions produce no success sample. Missing/duplicate tokens and pacer
backpressure retain the existing stimulus checks. A reader callback must not
drain to EAGAIN. Unsupported selector loops fail closed instead of switching to
a concealed thread-based driver. Sync-facade and in-loop/capacity controls are
unchanged.

## Measurement and teardown

The primary `latency_summary` remains publish-call to reply. The original
temporal trace also supplies **scheduled deadline to reply** and **scheduled
deadline to publish** summaries, calculated off the timed path in the initiator
result. They explicitly include pre-admission delay and are based on every-Nth
completed requests, not the full RTT reservoir. They are supplemental, not a new
ranking gate. Refusals/timeouts/missed offers remain separate denominator fields;
a completed-only latency quantile is not an SLO for all offered work.

The receiver is removed synchronously before cancellation waits. The sole
phase timer is cancelled; any phase-owned task is cancelled and joined before
return. It never closes a caller-owned socket, so warmup and measurement can
reuse it. Callback exceptions (including BlockingIOError from an adapter) cannot
be mistaken for spurious socket readiness. No user code is executed after a
cancelled or failed phase is returned to the caller.

The new module is included in the structural harness fingerprint. Historical
and new measurements must not be pooled. Payload, pacing calendar, sample
reservation policy, time windows, estimator and quality thresholds are unchanged.
The historical RTT payload is still 40 bytes; changing the misleading catalogue
label or padding to 256 bytes is a separate experiment.

## Review and tests before the RPi experiment

Tests exercise real prefilled datagram sockets with an unsaturated window,
simultaneously ready reply descriptors, immediate and suspended awaitables,
forced saturation, exact-once offered/refused accounting, early replies,
malformed datagrams, idle/end-of-phase races, cancellation, eager task creation,
socket reuse and unsupported event loops. Instrumented selectors assert distinct
turns for successive admissions; spies assert zero per-token tasks and timers
on synchronous-admission clients. The old coroutine drain loop fails the new
unsaturated-backlog invariant.

No performance conclusion follows from these structural tests. The RPi campaign
must inspect exact source imports, all stimulus attempts, service RTT, scheduled
latency, CPU, context switches and misses; workflow success is not equivalence.
