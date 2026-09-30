"""Latency histograms that merge exactly across runs and processes.

Same indexing as ``peer/mqtt_peer.c``: values below 32 ns are their own bucket,
above that each power of two is split into 16 equal buckets, so a bucket is at
most 6.25 % wide. A histogram is a sparse ``{index: count}`` map plus exact
count / min / max / sum, which is what both the C peer and the Python worker
emit. Merging is addition, so percentiles over three runs are percentiles of
all their samples rather than a median of medians.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

SUB_BUCKETS = 16
N_BUCKETS = 976


def bucket_of(value: int) -> int:
    if value < 32:
        return int(value)
    e = value.bit_length() - 1
    return (e - 3) * SUB_BUCKETS + ((value >> (e - 4)) & (SUB_BUCKETS - 1))


def bucket_bounds(index: int) -> tuple:
    """``[low, high)`` of a bucket, in the histogram's unit (ns)."""
    if index < 32:
        return index, index + 1
    e = index // SUB_BUCKETS + 3
    sub = index % SUB_BUCKETS
    width = 1 << (e - 4)
    low = (SUB_BUCKETS + sub) << (e - 4)
    return low, low + width


def empty() -> dict:
    return {"count": 0, "min_ns": 0, "max_ns": 0, "sum_ns": 0, "negative": 0, "buckets": []}


def from_values(values: Iterable[int]) -> dict:
    """Histogram of raw ns values (negative ones are counted, not binned)."""
    buckets: Dict[int, int] = {}
    count = total = negative = 0
    lo: Optional[int] = None
    hi = 0
    for v in values:
        if v < 0:
            negative += 1
            continue
        i = bucket_of(v)
        buckets[i] = buckets.get(i, 0) + 1
        count += 1
        total += v
        if lo is None or v < lo:
            lo = v
        if v > hi:
            hi = v
    return {
        "count": count,
        "min_ns": lo or 0,
        "max_ns": hi,
        "sum_ns": total,
        "negative": negative,
        "buckets": sorted([i, c] for i, c in buckets.items()),
    }


def merge(histograms: Iterable[dict]) -> dict:
    buckets: Dict[int, int] = {}
    count = total = negative = 0
    lo: Optional[int] = None
    hi = 0
    for h in histograms:
        if not h or not h.get("count"):
            negative += int((h or {}).get("negative", 0))
            continue
        for i, c in h["buckets"]:
            buckets[int(i)] = buckets.get(int(i), 0) + int(c)
        count += int(h["count"])
        total += int(h["sum_ns"])
        negative += int(h.get("negative", 0))
        lo = int(h["min_ns"]) if lo is None else min(lo, int(h["min_ns"]))
        hi = max(hi, int(h["max_ns"]))
    return {
        "count": count,
        "min_ns": lo or 0,
        "max_ns": hi,
        "sum_ns": total,
        "negative": negative,
        "buckets": sorted([i, c] for i, c in buckets.items()),
    }


def percentile(h: dict, q: float) -> Optional[float]:
    """Value at quantile ``q`` in [0, 1], as the midpoint of its bucket.

    The exact min and max clamp the ends, so p0 and p100 are exact.
    """
    count = int(h.get("count", 0))
    if count == 0:
        return None
    if q <= 0:
        return float(h["min_ns"])
    if q >= 1:
        return float(h["max_ns"])
    rank = q * count
    seen = 0
    for i, c in h["buckets"]:
        seen += c
        if seen >= rank:
            low, high = bucket_bounds(int(i))
            mid = (low + high - 1) / 2
            return float(min(max(mid, h["min_ns"]), h["max_ns"]))
    return float(h["max_ns"])


def summary(h: dict) -> dict:
    """p50/p90/p99/p99.9/max and mean, in microseconds."""
    count = int(h.get("count", 0))
    if count == 0:
        return {"count": 0}
    out: dict = {"count": count, "mean_us": h["sum_ns"] / count / 1e3}
    for name, q in (("p50", 0.5), ("p90", 0.9), ("p99", 0.99), ("p999", 0.999)):
        out[f"{name}_us"] = percentile(h, q) / 1e3
    out["min_us"] = h["min_ns"] / 1e3
    out["max_us"] = h["max_ns"] / 1e3
    if h.get("negative"):
        out["negative"] = int(h["negative"])
    return out


def quantiles(h: dict, qs: List[float]) -> List[Optional[float]]:
    return [percentile(h, q) for q in qs]
