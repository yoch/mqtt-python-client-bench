"""External fixed-rate pacing and temporal-stimulus telemetry.

Open-loop RTT used to sleep on the SUT event loop:

    deadline = start + n * interval
    await asyncio.sleep(deadline - now)
    publish()

At sub-millisecond intervals that sleep is coarse. A late wake then catch-up
sends, which changes batching and therefore RTT, while the *mean* offer rate
still looks right. Worse, SUT CPU shapes loop availability, which shapes the
pacer, which shapes the workload: two arms can share a rate and not share a
stimulus.

``ExternalRatePacer`` moves the calendar to a dedicated process. Tokens travel
one datagram each over a Unix DGRAM socket. The SUT loop publishes when a
token arrives; it does not compute the schedule.

In-loop mode is kept as the causal control and records the same telemetry
shape so the two arms can be compared.

Semantics
---------
* Absolute schedule: ``deadline(n) = start_ns + n * interval_ns``. A late
  wake does not shift later deadlines (never ``wake_actual + interval``).
* Sleep then spin: coarse sleep until ``spin_ns`` remains, then a short busy
  wait. Default ``spin_ns`` is 50 µs. Do not spin the whole interval.
* IPC backpressure: a send that would block is a drop. The pacer does not
  wait for the SUT. External pacing is valid only when the measure window
  saw the complete token sequence: no prefix loss, internal gap, suffix
  loss, or duplicate, ``tokens_emitted == tokens_scheduled -
  token_send_failures``, and ``tokens_received == tokens_emitted``. A lost
  last token fails even with no following sequence number. Exact reasons
  are persisted as ``pacer_stimulus_invalid:<kind>``. Lateness percentiles
  are reported, not gated.
* Absolute start: the initiator chooses ``absolute_start_ns =
  monotonic_ns() + startup_guard_ns`` after the receive socket is bound,
  then sends that deadline in START. The pacer uses
  ``deadline(n) = absolute_start_ns + n * interval_ns`` and must not emit
  before ``absolute_start_ns``. The guard is a few milliseconds; it is
  not tuned from ARM results.
* Catch-up event: token *n* is emitted at or after ``deadline(n+1)`` — at
  least one later scheduled deadline has already elapsed.
* Microburst emission: successive emission interval ``< 0.5 * target
  interval``. A burst is a run of such intervals of length >= 1 (two tokens
  closer than half the interval). These are diagnostic, not official gates.
"""

from __future__ import annotations

import errno
import json
import os
import socket
import struct
import subprocess
import sys
import time
from array import array
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Sequence

from mqtt_client_bench.metrics import percentile
from mqtt_client_bench.paths import PROJECT_ROOT
from mqtt_client_bench.temporal_trace import DEFAULT_TEMPORAL_TRACE_POINTS, trace_stride

PACER_MODES = ("in_loop", "external")
DEFAULT_PACER_MODE = "in_loop"
# Coarse-sleep / busy-wait handover. 50 µs is well above a typical
# ``time.sleep`` residual on Linux and well below a 200 µs / 5 kHz slot.
DEFAULT_PACER_SPIN_NS = 50_000
# Initiator-chosen lead before deadline(0). Milliseconds, explicit, not
# fitted to a campaign result. The receive loop must be armed before this
# instant; the pacer must not emit earlier.
DEFAULT_PACER_STARTUP_GUARD_NS = 5_000_000
DEFAULT_PACE_SAMPLE_LIMIT = DEFAULT_TEMPORAL_TRACE_POINTS
PACE_SAMPLE_COLUMNS = (
    "lateness_ns",
    "emission_interval_ns",
    "receiver_interval_ns",
    "emission_to_receiver_ns",
    "receiver_to_publish_ns",
)
TOKEN_MAGIC = b"PACE"
# magic(4) sequence(u32) scheduled_deadline_ns(u64) pacer_emission_ns(u64)
TOKEN_STRUCT = struct.Struct("<4sIQQ")
TOKEN_SIZE = TOKEN_STRUCT.size
MICROBURST_FRACTION = 0.5
CLOSED_LOOP_CADENCES = ("capacity", "burst", "microburst", "batch64")
PACER_STIMULUS_INVALID = "pacer_stimulus_invalid"
STIMULUS_PREFIX_LOSS = f"{PACER_STIMULUS_INVALID}:prefix_loss"
STIMULUS_SUFFIX_LOSS = f"{PACER_STIMULUS_INVALID}:suffix_loss"
STIMULUS_INTERNAL_GAP = f"{PACER_STIMULUS_INVALID}:internal_gap"
STIMULUS_DUPLICATE = f"{PACER_STIMULUS_INVALID}:duplicate"
STIMULUS_SEND_FAILURE = f"{PACER_STIMULUS_INVALID}:send_failure"
STIMULUS_EMITTED_VS_SCHEDULED = f"{PACER_STIMULUS_INVALID}:emitted_vs_scheduled"
STIMULUS_RECEIVED_VS_EMITTED = f"{PACER_STIMULUS_INVALID}:received_vs_emitted"
SNDBUF_BYTES = 1 << 20
RCVBUF_BYTES = 1 << 20


def resolve_pacer_mode(point: Optional[dict], target_rate: Optional[float]) -> str:
    """External pacing is only for an externally prescribed open-loop rate.

    Closed-loop capacity, bursts and completion-gated workloads keep the SUT
    in the offer loop on purpose. A point that asked for ``external`` in those
    regimes is silently kept on ``in_loop`` so a ranking path cannot change
    meaning by accident.
    """
    requested = str((point or {}).get("pacer_mode") or DEFAULT_PACER_MODE)
    if requested not in PACER_MODES:
        requested = DEFAULT_PACER_MODE
    if requested != "external":
        return DEFAULT_PACER_MODE
    if target_rate is None or float(target_rate) <= 0:
        return DEFAULT_PACER_MODE
    cadence = str((point or {}).get("cadence") or "capacity")
    if cadence in CLOSED_LOOP_CADENCES:
        return DEFAULT_PACER_MODE
    return "external"


def interval_ns_for_rate(target_rate: float) -> int:
    if target_rate <= 0:
        raise ValueError("target_rate must be positive")
    return max(1, int(round(1_000_000_000.0 / float(target_rate))))


def pack_token(sequence: int, scheduled_deadline_ns: int, pacer_emission_ns: int) -> bytes:
    return TOKEN_STRUCT.pack(
        TOKEN_MAGIC,
        sequence & 0xFFFFFFFF,
        scheduled_deadline_ns & 0xFFFFFFFFFFFFFFFF,
        pacer_emission_ns & 0xFFFFFFFFFFFFFFFF,
    )


def unpack_token(data: bytes) -> Optional["PaceToken"]:
    if not data or len(data) < TOKEN_SIZE:
        return None
    magic, sequence, deadline, emission = TOKEN_STRUCT.unpack_from(data, 0)
    if magic != TOKEN_MAGIC:
        return None
    return PaceToken(
        sequence=int(sequence),
        scheduled_deadline_ns=int(deadline),
        pacer_emission_ns=int(emission),
    )


@dataclass(frozen=True, slots=True)
class PaceToken:
    sequence: int
    scheduled_deadline_ns: int
    pacer_emission_ns: int

    def lateness_ns(self) -> int:
        return self.pacer_emission_ns - self.scheduled_deadline_ns


class SystemClock:
    """Host monotonic clock for the pacer process."""

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)

    def spin_until(self, deadline_ns: int) -> None:
        while self.monotonic_ns() < deadline_ns:
            pass


class FakeClock:
    """Deterministic clock: tests drive time; the pacer never sees wall time."""

    def __init__(self, now_ns: int = 0) -> None:
        self.now_ns = int(now_ns)
        self.sleeps_ns: List[int] = []

    def monotonic_ns(self) -> int:
        return self.now_ns

    def sleep(self, seconds: float) -> None:
        ns = max(0, int(seconds * 1_000_000_000.0))
        self.sleeps_ns.append(ns)
        self.now_ns += ns

    def spin_until(self, deadline_ns: int) -> None:
        if self.now_ns < deadline_ns:
            self.now_ns = int(deadline_ns)

    def advance(self, ns: int) -> None:
        self.now_ns += int(ns)


def _ns_percentiles(values: Sequence[int]) -> dict:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    floats = [float(v) for v in values]
    return {
        "p50": percentile(floats, 50),
        "p95": percentile(floats, 95),
        "p99": percentile(floats, 99),
        "max": float(max(values)),
    }


def tokens_expected_in_window(start_ns: int, until_ns: int, interval_ns: int) -> int:
    """Count ``n >= 0`` such that ``start_ns + n * interval_ns < until_ns``."""
    if interval_ns <= 0 or until_ns <= start_ns:
        return 0
    return (int(until_ns) - int(start_ns) - 1) // int(interval_ns) + 1


def choose_absolute_start_ns(
    now_ns: int, guard_ns: int = DEFAULT_PACER_STARTUP_GUARD_NS
) -> int:
    if guard_ns < 0:
        raise ValueError("startup_guard_ns must be >= 0")
    return int(now_ns) + int(guard_ns)


def absolute_start_ns_from_start_command(cmd: dict) -> int:
    raw = cmd.get("absolute_start_ns")
    if raw is None:
        raise ValueError("missing_absolute_start_ns")
    return int(raw)


class PaceTraceSampler:
    """Every-Nth signed deltas, hard-capped, preallocated.

    Same shape as ``TemporalTraceSampler``: integer modulo plus a store into a
    buffer allocated once. Percentiles come from these samples; O(1) counters
    stay outside. The SUT hot path must not grow arrays with every token.
    """

    __slots__ = (
        "max_points",
        "stride",
        "seen",
        "sampled_tokens",
        "last_was_sampled",
        "_cols",
        "_counts",
    )

    def __init__(
        self,
        max_points: int = DEFAULT_PACE_SAMPLE_LIMIT,
        stride: int = 1,
    ) -> None:
        if max_points < 0:
            raise ValueError("max_points must be >= 0")
        if stride < 1:
            raise ValueError("stride must be >= 1")
        self.max_points = int(max_points)
        self.stride = int(stride)
        self.seen = 0
        self.sampled_tokens = 0
        self.last_was_sampled = False
        self._cols = {
            name: array("q", [0] * self.max_points) if self.max_points else array("q")
            for name in PACE_SAMPLE_COLUMNS
        }
        self._counts = {name: 0 for name in PACE_SAMPLE_COLUMNS}

    def begin_token(self) -> bool:
        idx = self.seen
        self.seen += 1
        if self.max_points <= 0 or idx % self.stride != 0:
            self.last_was_sampled = False
            return False
        if self.sampled_tokens >= self.max_points:
            self.last_was_sampled = False
            return False
        self.sampled_tokens += 1
        self.last_was_sampled = True
        return True

    def store(self, column: str, value: int) -> None:
        if not self.last_was_sampled:
            return
        n = self._counts[column]
        if n >= self.max_points:
            return
        self._cols[column][n] = int(value)
        self._counts[column] = n + 1

    def values(self, column: str) -> array:
        return self._cols[column][: self._counts[column]]

    def sample_count(self) -> int:
        return self.sampled_tokens

    def memory_bytes(self) -> int:
        return sum(col.buffer_info()[1] * col.itemsize for col in self._cols.values())


class PaceRecorder:
    """SUT-minimal, bounded telemetry for one measure window.

    Per-token work on the initiator is O(1) counters plus an every-Nth store
    into a preallocated sampler. Unbounded timestamp arrays are forbidden:
    they would make the in_loop control a different event loop than the
    historical ``asyncio.sleep`` harness.

    Emission lateness for ``external`` mode is collected in the pacer
    process (``side="pacer"``) and merged into the summary. Catch-up /
    microburst counters are the diagnostic that p50-of-rate cannot show.
    """

    __slots__ = (
        "mode",
        "side",
        "target_rate",
        "target_interval_ns",
        "tokens_scheduled",
        "tokens_emitted",
        "tokens_received",
        "token_send_failures",
        "sequence_gaps",
        "last_sequence",
        "first_sequence",
        "tokens_expected_in_measure_window",
        "phase_start_ns",
        "first_scheduled_deadline_ns",
        "first_emission_ns",
        "first_receiver_ns",
        "catch_up_events",
        "microburst_emissions",
        "n_bursts",
        "max_burst_size",
        "_sampler",
        "_last_emission_ns",
        "_last_receiver_ns",
        "_burst_run",
        "invalid_tokens",
        "_pacer_side",
        "_sequence_flags",
    )

    def __init__(
        self,
        *,
        mode: str,
        target_rate: Optional[float],
        target_interval_ns: int,
        side: str = "sut",
        max_samples: int = DEFAULT_PACE_SAMPLE_LIMIT,
        expected_tokens: Optional[float] = None,
    ) -> None:
        self.mode = mode
        self.side = side
        self.target_rate = float(target_rate) if target_rate else None
        self.target_interval_ns = int(target_interval_ns)
        self.tokens_scheduled = 0
        self.tokens_emitted = 0
        self.tokens_received = 0
        self.token_send_failures = 0
        self.sequence_gaps = 0
        self.last_sequence: Optional[int] = None
        self.first_sequence: Optional[int] = None
        self.tokens_expected_in_measure_window: Optional[int] = None
        self.phase_start_ns: Optional[int] = None
        self.first_scheduled_deadline_ns: Optional[int] = None
        self.first_emission_ns: Optional[int] = None
        self.first_receiver_ns: Optional[int] = None
        self.catch_up_events = 0
        self.microburst_emissions = 0
        self.n_bursts = 0
        self.max_burst_size = 0
        stride = trace_stride(float(expected_tokens or 0.0), max_samples)
        self._sampler = PaceTraceSampler(max_points=max_samples, stride=stride)
        self._last_emission_ns: Optional[int] = None
        self._last_receiver_ns: Optional[int] = None
        self._burst_run = 0
        self.invalid_tokens = 0
        self._pacer_side = None
        self._sequence_flags: List[str] = []

    def memory_bytes(self) -> int:
        return self._sampler.memory_bytes()

    def sample_count(self) -> int:
        return self._sampler.sample_count()

    def set_window(self, start_ns: int, until_ns: int, interval_ns: int) -> None:
        self.phase_start_ns = int(start_ns)
        self.first_scheduled_deadline_ns = int(start_ns)
        self.tokens_expected_in_measure_window = tokens_expected_in_window(
            start_ns, until_ns, interval_ns
        )
        expected = self.tokens_expected_in_measure_window
        if expected:
            self._sampler.stride = trace_stride(float(expected), self._sampler.max_points)

    def _flag(self, reason: str) -> None:
        if reason not in self._sequence_flags:
            self._sequence_flags.append(reason)

    def note_gap(self, sequence: int) -> None:
        seq = int(sequence)
        if self.last_sequence is None:
            self.first_sequence = seq
            self.last_sequence = seq
            if seq != 0:
                self._flag(STIMULUS_PREFIX_LOSS)
            return
        expected = self.last_sequence + 1
        if seq == self.last_sequence or seq < self.last_sequence:
            self.sequence_gaps += 1
            self._flag(STIMULUS_DUPLICATE)
            return
        if seq != expected:
            self.sequence_gaps += seq - expected
            self._flag(STIMULUS_INTERNAL_GAP)
        self.last_sequence = seq

    def _note_emission_shape(self, scheduled_deadline_ns: int, emission_ns: int) -> Optional[int]:
        interval = None
        if self._last_emission_ns is not None:
            interval = int(emission_ns) - int(self._last_emission_ns)
            half = int(self.target_interval_ns * MICROBURST_FRACTION)
            if half > 0 and interval < half:
                self.microburst_emissions += 1
                self._burst_run += 1
                if self._burst_run == 1:
                    self.n_bursts += 1
                if self._burst_run + 1 > self.max_burst_size:
                    self.max_burst_size = self._burst_run + 1
            else:
                self._burst_run = 0
        self._last_emission_ns = int(emission_ns)
        if self.first_emission_ns is None:
            self.first_emission_ns = int(emission_ns)
        if self.target_interval_ns > 0 and emission_ns >= scheduled_deadline_ns + self.target_interval_ns:
            self.catch_up_events += 1
        return interval

    def record_emission(
        self,
        sequence: int,
        scheduled_deadline_ns: int,
        emission_ns: int,
        *,
        sent: bool,
    ) -> None:
        del sequence
        self.tokens_scheduled += 1
        if not sent:
            self.token_send_failures += 1
            return
        self.tokens_emitted += 1
        interval = self._note_emission_shape(scheduled_deadline_ns, emission_ns)
        if self._sampler.begin_token():
            self._sampler.store("lateness_ns", int(emission_ns) - int(scheduled_deadline_ns))
            if interval is not None:
                self._sampler.store("emission_interval_ns", interval)

    def record_receiver(
        self,
        token: PaceToken,
        receiver_ns: int,
        publish_call_ns: Optional[int] = None,
    ) -> None:
        if token is None:
            self.invalid_tokens += 1
            return
        self.tokens_received += 1
        self.note_gap(token.sequence)
        recv_ns = int(receiver_ns)
        if self.first_receiver_ns is None:
            self.first_receiver_ns = recv_ns
        recv_interval = None
        if self._last_receiver_ns is not None:
            recv_interval = recv_ns - int(self._last_receiver_ns)
        self._last_receiver_ns = recv_ns
        if self.mode == "in_loop":
            self.record_emission(
                token.sequence,
                token.scheduled_deadline_ns,
                token.pacer_emission_ns,
                sent=True,
            )
            if self._sampler.last_was_sampled:
                if recv_interval is not None:
                    self._sampler.store("receiver_interval_ns", recv_interval)
                self._sampler.store(
                    "emission_to_receiver_ns",
                    recv_ns - int(token.pacer_emission_ns),
                )
                if publish_call_ns is not None:
                    self._sampler.store(
                        "receiver_to_publish_ns",
                        int(publish_call_ns) - recv_ns,
                    )
            return
        if self.side != "sut":
            return
        if self._sampler.begin_token():
            if recv_interval is not None:
                self._sampler.store("receiver_interval_ns", recv_interval)
            self._sampler.store(
                "emission_to_receiver_ns",
                recv_ns - int(token.pacer_emission_ns),
            )
            if publish_call_ns is not None:
                self._sampler.store(
                    "receiver_to_publish_ns",
                    int(publish_call_ns) - recv_ns,
                )

    def note_receiver_to_publish(self, receiver_ns: int, publish_call_ns: int) -> None:
        if not self._sampler.last_was_sampled:
            return
        self._sampler.store(
            "receiver_to_publish_ns", int(publish_call_ns) - int(receiver_ns)
        )

    def merge_pacer_side(self, stats: Optional[dict]) -> None:
        """Copy the process-side calendar counters. Do not append raw samples.

        Emission lateness lives in the pacer process; the initiator records
        receiver delays. Overlaying the summary keeps one percentile set.
        """
        if not stats:
            return
        self._pacer_side = stats
        self.tokens_scheduled = int(stats.get("tokens_scheduled") or self.tokens_scheduled)
        self.tokens_emitted = int(stats.get("tokens_emitted") or self.tokens_emitted)
        self.token_send_failures = int(stats.get("token_send_failures") or 0)
        catch = stats.get("catch_up") or {}
        self.catch_up_events = int(catch.get("events") or stats.get("catch_up_events") or 0)
        burst = stats.get("microburst") or {}
        self.microburst_emissions = int(burst.get("emissions") or 0)
        self.n_bursts = int(burst.get("n_bursts") or 0)
        self.max_burst_size = int(burst.get("max_burst_size") or 0)
        if stats.get("phase_start_ns") is not None:
            self.phase_start_ns = int(stats["phase_start_ns"])
        if stats.get("first_scheduled_deadline_ns") is not None:
            self.first_scheduled_deadline_ns = int(stats["first_scheduled_deadline_ns"])
        if stats.get("first_emission_ns") is not None:
            self.first_emission_ns = int(stats["first_emission_ns"])
        if stats.get("tokens_expected_in_measure_window") is not None:
            self.tokens_expected_in_measure_window = int(
                stats["tokens_expected_in_measure_window"]
            )

    def actual_offered_rate(self, duration_s: float) -> Optional[float]:
        if duration_s <= 0:
            return None
        count = self.tokens_received or self.tokens_emitted
        return float(count) / float(duration_s)

    def completeness_reasons(self) -> List[str]:
        reasons: List[str] = []
        if self.token_send_failures > 0:
            reasons.append(STIMULUS_SEND_FAILURE)
        if self.tokens_emitted != self.tokens_scheduled - self.token_send_failures:
            reasons.append(STIMULUS_EMITTED_VS_SCHEDULED)
        reasons.extend(self._sequence_flags)
        expected = self.tokens_expected_in_measure_window
        if self.side == "pacer":
            return _unique(reasons)
        if expected is not None:
            if expected > 0 and self.first_sequence is None:
                self._flag(STIMULUS_PREFIX_LOSS)
                self._flag(STIMULUS_SUFFIX_LOSS)
            elif self.first_sequence is not None and self.first_sequence != 0:
                self._flag(STIMULUS_PREFIX_LOSS)
            if expected > 0 and (
                self.last_sequence is None or self.last_sequence < expected - 1
            ):
                self._flag(STIMULUS_SUFFIX_LOSS)
            if expected > 0 and self.last_sequence is not None and self.last_sequence > expected - 1:
                self._flag(STIMULUS_DUPLICATE)
            reasons = _unique(reasons + self._sequence_flags)
        if self.tokens_emitted and self.tokens_received != self.tokens_emitted:
            reasons.append(STIMULUS_RECEIVED_VS_EMITTED)
        return _unique(reasons)

    def stimulus_valid(self) -> bool:
        return not self.completeness_reasons()

    def summary(self, *, duration_s: Optional[float] = None) -> dict:
        side = self._pacer_side or {}
        offered = self.actual_offered_rate(duration_s) if duration_s else None
        lateness = side.get("pacer_lateness") or _ns_percentiles(
            self._sampler.values("lateness_ns")
        )
        emission = side.get("emission_intervals") or _ns_percentiles(
            self._sampler.values("emission_interval_ns")
        )
        invalid = self.completeness_reasons()
        return {
            "mode": self.mode,
            "target_rate": self.target_rate,
            "target_interval_ns": self.target_interval_ns,
            "tokens_scheduled": self.tokens_scheduled,
            "tokens_emitted": self.tokens_emitted,
            "tokens_received": self.tokens_received,
            "tokens_expected_in_measure_window": self.tokens_expected_in_measure_window,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "sequence_gaps": self.sequence_gaps,
            "token_send_failures": self.token_send_failures,
            "invalid_tokens": self.invalid_tokens,
            "phase_start_ns": self.phase_start_ns,
            "first_scheduled_deadline_ns": self.first_scheduled_deadline_ns,
            "first_emission_ns": self.first_emission_ns,
            "first_receiver_ns": self.first_receiver_ns,
            "pacer_lateness": lateness,
            "emission_intervals": emission,
            "receiver_intervals": _ns_percentiles(
                self._sampler.values("receiver_interval_ns")
            ),
            "emission_to_receiver_delay": _ns_percentiles(
                self._sampler.values("emission_to_receiver_ns")
            ),
            "receiver_to_publish_delay": _ns_percentiles(
                self._sampler.values("receiver_to_publish_ns")
            ),
            "actual_offered_rate": offered,
            "catch_up": {
                "definition": (
                    "token n emitted at or after deadline(n+1); at least one later "
                    "scheduled deadline had already elapsed"
                ),
                "events": self.catch_up_events,
            },
            "microburst": {
                "definition": (
                    f"successive emission interval < {MICROBURST_FRACTION} * "
                    "target_interval_ns"
                ),
                "emissions": self.microburst_emissions,
                "n_bursts": self.n_bursts,
                "max_burst_size": self.max_burst_size,
            },
            "stimulus_valid": not invalid,
            "stimulus_invalid_reasons": invalid,
        }


def _unique(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


class ExternalRatePacer:
    """Absolute-schedule token emitter. The calendar never lives on the SUT loop.

    ``send_fn(token) -> bool`` must not block on the SUT. False means the
    datagram could not be queued (drop); the engine still advances *n*.
    """

    def __init__(
        self,
        *,
        interval_ns: int,
        start_ns: int,
        spin_ns: int = DEFAULT_PACER_SPIN_NS,
        clock: Optional[object] = None,
        send_fn: Optional[Callable[[PaceToken], bool]] = None,
    ) -> None:
        if interval_ns <= 0:
            raise ValueError("interval_ns must be positive")
        if spin_ns < 0:
            raise ValueError("spin_ns must be >= 0")
        self.interval_ns = int(interval_ns)
        self.start_ns = int(start_ns)
        self.spin_ns = int(spin_ns)
        self.clock = clock or SystemClock()
        self.send_fn = send_fn or (lambda token: True)
        self.sequence = 0
        self.stop = False
        self.recorder = PaceRecorder(
            mode="external",
            target_rate=(1_000_000_000.0 / self.interval_ns),
            target_interval_ns=self.interval_ns,
            side="pacer",
        )

    def deadline(self, n: int) -> int:
        return self.start_ns + int(n) * self.interval_ns

    def wait_until(self, deadline_ns: int) -> None:
        now = self.clock.monotonic_ns()
        remaining = deadline_ns - now
        if remaining > self.spin_ns:
            self.clock.sleep((remaining - self.spin_ns) / 1_000_000_000.0)
        self.clock.spin_until(deadline_ns)

    def emit_one(self) -> PaceToken:
        n = self.sequence
        deadline_ns = self.deadline(n)
        self.wait_until(deadline_ns)
        emission_ns = self.clock.monotonic_ns()
        token = PaceToken(n, deadline_ns, emission_ns)
        sent = bool(self.send_fn(token))
        self.recorder.record_emission(n, deadline_ns, emission_ns, sent=sent)
        self.sequence += 1
        return token

    def emit_until(self, until_ns: int) -> PaceRecorder:
        self.recorder.set_window(self.start_ns, int(until_ns), self.interval_ns)
        while not self.stop and self.deadline(self.sequence) < until_ns:
            self.emit_one()
        return self.recorder


def datagram_send_fn(sock: socket.socket, dest: str) -> Callable[[PaceToken], bool]:
    """Non-blocking one-token datagram. EAGAIN is a counted drop, not a wait."""

    def _send(token: PaceToken) -> bool:
        payload = pack_token(
            token.sequence, token.scheduled_deadline_ns, token.pacer_emission_ns
        )
        try:
            sock.sendto(payload, dest)
            return True
        except BlockingIOError:
            return False
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK, errno.ENOBUFS):
                return False
            raise

    return _send


def bind_receiver_socket(path: str) -> socket.socket:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(path)
    sock.setblocking(False)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF_BYTES)
    return sock


def drain_datagrams(sock: socket.socket) -> int:
    drained = 0
    sock.setblocking(False)
    while True:
        try:
            sock.recv(256)
        except BlockingIOError:
            break
        drained += 1
    return drained


def pacer_affinity_preexec(cpuset: Optional[str]):
    if not cpuset or not hasattr(os, "sched_setaffinity"):
        return None
    cpus = {int(part) for part in cpuset.split(",") if part.strip() != ""}
    if not cpus:
        return None

    def _set_affinity() -> None:
        os.sched_setaffinity(0, cpus)

    return _set_affinity


class PacerClient:
    """Initiator-side handle: spawn the pacer process and feed START/STOP/QUIT."""

    def __init__(
        self,
        *,
        socket_path: str,
        stats_path: str,
        cpuset: Optional[str],
        spin_ns: int = DEFAULT_PACER_SPIN_NS,
    ) -> None:
        self.socket_path = socket_path
        self.stats_path = stats_path
        self.spin_ns = int(spin_ns)
        env = os.environ.copy()
        env.setdefault("PYTHONNOUSERSITE", "1")
        env["PYTHONUNBUFFERED"] = "1"
        src = str(PROJECT_ROOT / "src")
        env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        cmd = [
            sys.executable,
            "-m",
            "mqtt_client_bench.roles.rate_pacer",
            "--socket",
            socket_path,
            "--stats",
            stats_path,
            "--spin-ns",
            str(self.spin_ns),
        ]
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            preexec_fn=pacer_affinity_preexec(cpuset),
        )
        ready = self._readline()
        if not ready or not ready.get("ok"):
            self.close()
            raise RuntimeError(f"rate_pacer failed to start: {ready!r}")

    def _readline(self) -> Optional[dict]:
        assert self.proc.stdout is not None
        line = self.proc.stdout.readline()
        if not line:
            return None
        try:
            return json.loads(line.decode("utf-8"))
        except json.JSONDecodeError:
            return {"ok": False, "raw": line.decode("utf-8", "replace")}

    def start_phase(
        self, *, absolute_start_ns: int, interval_ns: int, duration_ns: int
    ) -> None:
        self._command(
            {
                "cmd": "start",
                "absolute_start_ns": int(absolute_start_ns),
                "interval_ns": int(interval_ns),
                "duration_ns": int(duration_ns),
                "spin_ns": self.spin_ns,
            }
        )

    def stop_phase(self) -> None:
        self._command({"cmd": "stop"})

    def _command(self, payload: dict) -> None:
        if self.proc.stdin is None:
            raise RuntimeError("rate_pacer stdin closed")
        self.proc.stdin.write((json.dumps(payload) + "\n").encode("ascii"))
        self.proc.stdin.flush()

    def read_stats(self) -> Optional[dict]:
        if not os.path.exists(self.stats_path):
            return None
        with open(self.stats_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def close(self) -> None:
        try:
            if self.proc.poll() is None and self.proc.stdin is not None:
                try:
                    self.proc.stdin.write(b'{"cmd":"quit"}\n')
                    self.proc.stdin.flush()
                except BrokenPipeError:
                    pass
            if self.proc.poll() is None:
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.terminate()
                    try:
                        self.proc.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        self.proc.kill()
        finally:
            for stream in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass


def stimulus_invalid_reasons(pacing: Optional[dict], *, mode: Optional[str] = None) -> List[str]:
    """Fail the run when the external stimulus itself is not a regular calendar.

    Token loss — including a missing last token with no following sequence
    number — means the SUT did not see the schedule the pacer computed.
    Lateness / burst percentiles stay diagnostic. Exact reasons are kept.
    """
    if not pacing:
        return []
    resolved = str(pacing.get("mode") or mode or DEFAULT_PACER_MODE)
    if resolved != "external":
        return []
    stored = pacing.get("stimulus_invalid_reasons")
    if stored:
        return [str(item) for item in stored]
    reasons: List[str] = []
    if int(pacing.get("token_send_failures") or 0) > 0:
        reasons.append(STIMULUS_SEND_FAILURE)
    scheduled = int(pacing.get("tokens_scheduled") or 0)
    emitted = int(pacing.get("tokens_emitted") or 0)
    failures = int(pacing.get("token_send_failures") or 0)
    if scheduled and emitted != scheduled - failures:
        reasons.append(STIMULUS_EMITTED_VS_SCHEDULED)
    received = int(pacing.get("tokens_received") or 0)
    if emitted and received != emitted:
        reasons.append(STIMULUS_RECEIVED_VS_EMITTED)
    expected = pacing.get("tokens_expected_in_measure_window")
    if expected is not None:
        expected_n = int(expected)
        if received != expected_n:
            if received == 0:
                reasons.append(STIMULUS_PREFIX_LOSS)
                reasons.append(STIMULUS_SUFFIX_LOSS)
            elif received < expected_n:
                reasons.append(STIMULUS_SUFFIX_LOSS)
    if int(pacing.get("sequence_gaps") or 0) > 0:
        reasons.append(STIMULUS_INTERNAL_GAP)
    if pacing.get("stimulus_valid") is False and not reasons:
        reasons.append(PACER_STIMULUS_INVALID)
    return _unique(reasons)


def pacer_stimulus_reasons(point: dict, worker_results: Iterable[dict]) -> List[str]:
    reasons: List[str] = []
    mode = str(point.get("pacer_mode") or DEFAULT_PACER_MODE)
    for result in worker_results:
        if result.get("role") != "rtt_initiator":
            continue
        reasons.extend(stimulus_invalid_reasons(result.get("pacing"), mode=mode))
    return reasons
