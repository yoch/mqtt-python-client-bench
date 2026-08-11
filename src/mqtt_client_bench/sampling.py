"""Bounded metric sampling and online integrity summaries."""

from __future__ import annotations

import random

DEFAULT_METRIC_SAMPLE_LIMIT = 50_000
DEFAULT_SEQUENCE_EXACT_LIMIT = 20_000
DEFAULT_PAYLOAD_BACKLOG_BYTES = 64 * 1024 * 1024
_MASK64 = (1 << 64) - 1


class ReservoirSampler:
    """Deterministic Algorithm-R reservoir with fixed retained memory."""

    __slots__ = ("capacity", "seen", "_random", "_values")

    def __init__(self, capacity: int = DEFAULT_METRIC_SAMPLE_LIMIT, *, seed: int = 1) -> None:
        if capacity < 0:
            raise ValueError("sample capacity must be non-negative")
        self.capacity = capacity
        self._random = random.Random(seed)
        self.seen = 0
        self._values: list[int] = []

    def add(self, value: int) -> None:
        self.seen += 1
        if len(self._values) < self.capacity:
            self._values.append(value)
            return
        if self.capacity == 0:
            return
        slot = self._random.randrange(self.seen)
        if slot < self.capacity:
            self._values[slot] = value

    def clear(self) -> None:
        self.seen = 0
        self._values.clear()

    def snapshot(self) -> list[int]:
        return list(self._values)

    def metadata(self) -> dict:
        return {
            "strategy": "reservoir_algorithm_r",
            "capacity": self.capacity,
            "observed": self.seen,
            "retained": len(self._values),
        }


def _mix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & _MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _MASK64
    return value ^ (value >> 31)


class SequenceTracker:
    """Online, order-independent sequence fingerprint with bounded exact detail."""

    __slots__ = (
        "count",
        "exact_limit",
        "first",
        "last",
        "out_of_order",
        "_exact",
        "_sum",
        "_xor",
    )

    def __init__(self, exact_limit: int = DEFAULT_SEQUENCE_EXACT_LIMIT) -> None:
        if exact_limit < 0:
            raise ValueError("sequence exact limit must be non-negative")
        self.exact_limit = exact_limit
        self.clear()

    def clear(self) -> None:
        self.count = 0
        self.first: int | None = None
        self.last: int | None = None
        self.out_of_order = 0
        self._sum = 0
        self._xor = 0
        self._exact: list[int] | None = []

    def add(self, sequence: int) -> None:
        if self.first is None:
            self.first = sequence
        elif self.last is not None and sequence < self.last:
            self.out_of_order += 1
        self.last = sequence
        self.count += 1
        mixed = _mix64(sequence)
        self._sum = (self._sum + mixed) & _MASK64
        self._xor ^= mixed
        if self._exact is not None:
            if len(self._exact) < self.exact_limit:
                self._exact.append(sequence)
            else:
                # Exact detail is all-or-nothing. Keeping a misleading prefix
                # has no integrity value, so release it as soon as the cap hits.
                self._exact = None

    def exact_values(self) -> list[int] | None:
        return None if self._exact is None else list(self._exact)

    def summary(self) -> dict:
        return {
            "count": self.count,
            "first": self.first,
            "last": self.last,
            "digest_sum64": f"{self._sum:016x}",
            "digest_xor64": f"{self._xor:016x}",
            "out_of_order": self.out_of_order,
            "exact_limit": self.exact_limit,
            "exact_retained": self._exact is not None,
        }


def integrity_from_summaries(expected: dict, received: dict) -> dict:
    """Compare online fingerprints without retaining every sequence value."""
    expected_count = int(expected.get("count") or 0)
    received_count = int(received.get("count") or 0)
    digest_match = (
        expected_count == received_count
        and expected.get("digest_sum64") == received.get("digest_sum64")
        and expected.get("digest_xor64") == received.get("digest_xor64")
    )
    return {
        "expected": expected_count,
        "received": received_count,
        "unique": expected_count if digest_match else None,
        "missing": 0 if digest_match else None,
        "duplicates": 0 if digest_match else None,
        "unexpected": 0 if digest_match else None,
        "out_of_order": int(received.get("out_of_order") or 0),
        "count_delta": received_count - expected_count,
        "digest_match": digest_match,
        "probabilistic": True,
    }


def bound_payload_backlog(
    outstanding: int,
    payload_bytes: int,
    limit_bytes: int | None = DEFAULT_PAYLOAD_BACKLOG_BYTES,
) -> dict:
    if outstanding <= 0:
        raise ValueError("outstanding must be positive")
    if payload_bytes < 0:
        raise ValueError("payload_bytes must be non-negative")
    if limit_bytes is not None and limit_bytes <= 0:
        raise ValueError("payload backlog limit must be positive or None")
    effective = outstanding
    if limit_bytes is not None and payload_bytes:
        effective = min(outstanding, max(1, limit_bytes // payload_bytes))
    return {
        "requested_outstanding": outstanding,
        "effective_outstanding": effective,
        "payload_bytes": payload_bytes,
        "limit_bytes": limit_bytes,
        "maximum_bytes": effective * payload_bytes,
    }


class _NullSequenceTracker:
    """Drop-in tracker for runs whose sequences nobody reconciles.

    A publisher_only point has no subscriber to compare against, so the exact
    values and the fingerprints are computed and then discarded. Keeping the
    same shape means the loop needs no branch on the hot path.
    """

    __slots__ = ()

    def add(self, sequence: int) -> None:
        return None

    def exact_values(self) -> list[int]:
        return []

    def summary(self) -> dict:
        return {"strategy": "not_tracked", "count": 0}


def sequence_tracker(limit: int, *, enabled: bool = True):
    """Return a real tracker, or a no-op when nothing will read it."""
    return SequenceTracker(limit) if enabled else _NullSequenceTracker()
