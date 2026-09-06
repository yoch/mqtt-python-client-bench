"""Bounded temporal traces of application end-to-end RTT.

The payload header already carries ``sequence`` and ``send_ns`` (the
application publish-call timestamp, ``time.perf_counter_ns()`` immediately
before ``publish()``). That is **not** a socket ``send()``/``write()`` time.
The responder echoes the payload; the initiator computes

    application_e2e_latency = receive_ns - send_ns

``ReservoirSampler`` estimates a distribution and destroys order. The
bimodality on ARM (p50 ≈ 0.24 ms vs ≈ 0.40 ms) needs the time axis: blocks of
one mode, alternation, or a switch locked to a pacer catch-up burst.

``TemporalTraceSampler`` keeps every Nth completed RTT, with N derived from
the expected offer and a hard cap (default 4096). Every-Nth, not a reservoir:
it covers the whole measure window at a uniform resolution and is a couple of
integer ops on the sampled path. Preallocated ``array('Q')`` columns, no
per-message dict.

This is diagnostic. It is not an official ranking gate.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from array import array
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

DEFAULT_TEMPORAL_TRACE_POINTS = 4096
TRACE_METRIC = "application_e2e_latency"
TRACE_COLUMNS = (
    "sequence",
    "send_ns",
    "receive_ns",
    "latency_ns",
    "scheduled_deadline_ns",
    "pacer_emission_ns",
    "receiver_token_ns",
    "publish_call_ns",
)
OPTIONAL_ZERO = (
    "scheduled_deadline_ns",
    "pacer_emission_ns",
    "receiver_token_ns",
    "publish_call_ns",
)
# Ordering slack: Linux CLOCK_MONOTONIC vs perf_counter are the same domain,
# but equal timestamps and 1-tick inversions are not a stimulus failure.
CLOCK_ORDER_SLACK_NS = 1_000


def trace_stride(expected_messages: float, max_points: int = DEFAULT_TEMPORAL_TRACE_POINTS) -> int:
    if max_points <= 0:
        return 1
    if expected_messages <= 0:
        return 1
    return max(1, int(math.ceil(float(expected_messages) / float(max_points))))


class TemporalTraceSampler:
    """Every-Nth completed RTT, hard-capped, insertion-ordered.

    ``want(sequence)`` is the send-path reservation (cheap modulo). ``add`` is
    the completion-path write. Reservations that time out leave a hole rather
    than over-filling later — the bound is the point, not the mean density.
    """

    __slots__ = (
        "max_points",
        "stride",
        "metric",
        "seen",
        "reserved",
        "invalid",
        "_cols",
        "_count",
    )

    def __init__(
        self,
        max_points: int = DEFAULT_TEMPORAL_TRACE_POINTS,
        stride: int = 1,
        *,
        metric: str = TRACE_METRIC,
    ) -> None:
        if max_points < 0:
            raise ValueError("max_points must be >= 0")
        if stride < 1:
            raise ValueError("stride must be >= 1")
        self.max_points = int(max_points)
        self.stride = int(stride)
        self.metric = metric
        self.seen = 0
        self.reserved = 0
        self.invalid = 0
        self._cols = {name: array("Q") for name in TRACE_COLUMNS}
        for name in TRACE_COLUMNS:
            if self.max_points:
                self._cols[name].extend([0] * self.max_points)
        self._count = 0

    def want(self, sequence: int) -> bool:
        self.seen += 1
        if self.max_points <= 0:
            return False
        if self.reserved >= self.max_points:
            return False
        if int(sequence) % self.stride != 0:
            return False
        self.reserved += 1
        return True

    def add(
        self,
        *,
        sequence: int,
        send_ns: int,
        receive_ns: int,
        scheduled_deadline_ns: int = 0,
        pacer_emission_ns: int = 0,
        receiver_token_ns: int = 0,
        publish_call_ns: int = 0,
    ) -> bool:
        if self._count >= self.max_points:
            return False
        if send_ns is None or receive_ns is None:
            self.invalid += 1
            return False
        send_ns = int(send_ns)
        receive_ns = int(receive_ns)
        if send_ns <= 0 or receive_ns <= 0:
            self.invalid += 1
            return False
        if receive_ns < send_ns:
            self.invalid += 1
            return False
        latency_ns = receive_ns - send_ns
        if publish_call_ns <= 0:
            publish_call_ns = send_ns
        idx = self._count
        cols = self._cols
        cols["sequence"][idx] = int(sequence) & 0xFFFFFFFFFFFFFFFF
        cols["send_ns"][idx] = send_ns & 0xFFFFFFFFFFFFFFFF
        cols["receive_ns"][idx] = receive_ns & 0xFFFFFFFFFFFFFFFF
        cols["latency_ns"][idx] = latency_ns & 0xFFFFFFFFFFFFFFFF
        cols["scheduled_deadline_ns"][idx] = int(scheduled_deadline_ns) & 0xFFFFFFFFFFFFFFFF
        cols["pacer_emission_ns"][idx] = int(pacer_emission_ns) & 0xFFFFFFFFFFFFFFFF
        cols["receiver_token_ns"][idx] = int(receiver_token_ns) & 0xFFFFFFFFFFFFFFFF
        cols["publish_call_ns"][idx] = int(publish_call_ns) & 0xFFFFFFFFFFFFFFFF
        self._count += 1
        return True

    def __len__(self) -> int:
        return self._count

    def memory_bytes(self) -> int:
        return self.max_points * len(TRACE_COLUMNS) * 8

    def records(self) -> List[dict]:
        out = []
        for idx in range(self._count):
            row = {name: int(self._cols[name][idx]) for name in TRACE_COLUMNS}
            row["latency_ns"] = int(row["receive_ns"] - row["send_ns"])
            out.append(row)
        return out

    def to_columnar(self) -> dict:
        n = self._count
        payload = {
            "kind": "application_e2e_temporal_trace",
            "metric": self.metric,
            "stride": self.stride,
            "max_points": self.max_points,
            "count": n,
            "seen": self.seen,
            "reserved": self.reserved,
            "invalid": self.invalid,
            "memory_bytes": self.memory_bytes(),
            "columns": list(TRACE_COLUMNS),
        }
        for name in TRACE_COLUMNS:
            payload[name] = [int(self._cols[name][i]) for i in range(n)]
        return payload

    def write_jsonl(self, path: str) -> int:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        written = 0
        with open(path, "w", encoding="utf-8") as fh:
            for row in self.records():
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
                written += 1
        return written


def traces_from_columnar(payload: Optional[dict]) -> List[dict]:
    if not payload or not isinstance(payload, dict):
        return []
    count = int(payload.get("count") or 0)
    columns = payload.get("columns") or TRACE_COLUMNS
    rows = []
    for idx in range(count):
        row = {}
        ok = True
        for name in columns:
            values = payload.get(name) or []
            if idx >= len(values):
                ok = False
                break
            row[name] = int(values[idx])
        if not ok:
            continue
        if "latency_ns" not in row and "receive_ns" in row and "send_ns" in row:
            row["latency_ns"] = int(row["receive_ns"]) - int(row["send_ns"])
        rows.append(row)
    return rows


def load_jsonl(path: str | Path) -> List[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def round_trip_records(records: Sequence[dict]) -> List[dict]:
    sampler = TemporalTraceSampler(max_points=max(len(records), 1), stride=1)
    for row in records:
        sampler.add(
            sequence=int(row["sequence"]),
            send_ns=int(row["send_ns"]),
            receive_ns=int(row["receive_ns"]),
            scheduled_deadline_ns=int(row.get("scheduled_deadline_ns") or 0),
            pacer_emission_ns=int(row.get("pacer_emission_ns") or 0),
            receiver_token_ns=int(row.get("receiver_token_ns") or 0),
            publish_call_ns=int(row.get("publish_call_ns") or 0),
        )
    return sampler.records()


def clock_chain_ok(row: dict, *, slack_ns: int = CLOCK_ORDER_SLACK_NS) -> bool:
    """scheduled <= emission <= receiver <= publish, with slack.

    Missing optional timestamps (zeros) skip that link. Used for external
    tokens; in-loop records may only have send/receive.
    """
    chain = [
        int(row.get("scheduled_deadline_ns") or 0),
        int(row.get("pacer_emission_ns") or 0),
        int(row.get("receiver_token_ns") or 0),
        int(row.get("publish_call_ns") or row.get("send_ns") or 0),
    ]
    present = [value for value in chain if value > 0]
    for idx in range(1, len(present)):
        if present[idx] + slack_ns < present[idx - 1]:
            return False
    send_ns = int(row.get("send_ns") or 0)
    receive_ns = int(row.get("receive_ns") or 0)
    if send_ns > 0 and receive_ns > 0 and receive_ns + slack_ns < send_ns:
        return False
    return True


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if pct <= 0:
        return float(ordered[0])
    if pct >= 100:
        return float(ordered[-1])
    rank = int(math.ceil((pct / 100.0) * len(ordered))) - 1
    rank = min(max(rank, 0), len(ordered) - 1)
    return float(ordered[rank])


def _mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / float(len(values))


def lag1_autocorr(values: Sequence[float]) -> Optional[float]:
    if len(values) < 3:
        return None
    mean = sum(values) / float(len(values))
    var = sum((v - mean) ** 2 for v in values)
    if var <= 0:
        return None
    cov = sum((values[i] - mean) * (values[i + 1] - mean) for i in range(len(values) - 1))
    return cov / var


def _run_lengths(flags: Sequence[bool]) -> List[int]:
    if not flags:
        return []
    runs = [1]
    for idx in range(1, len(flags)):
        if flags[idx] == flags[idx - 1]:
            runs[-1] += 1
        else:
            runs.append(1)
    return runs


def analyze_trace(
    records: Sequence[dict],
    *,
    interval_ns: Optional[int] = None,
    windows: int = 10,
) -> dict:
    """Off-hot-path quantitative view of one measure-window trace.

    Does not gate a run. Used to test whether high-RTT samples sit on
    late/catch-up tokens or are independent of the stimulus.
    """
    latencies = [float(row["latency_ns"]) for row in records if row.get("latency_ns") is not None]
    n = len(latencies)
    if n == 0:
        return {"n": 0, "metric": TRACE_METRIC}
    median = _percentile(latencies, 50)
    high = [lat > float(median) for lat in latencies] if median is not None else [False] * n
    switches = sum(1 for i in range(1, n) if high[i] != high[i - 1])
    half = max(1, interval_ns or 0)
    late_mask = []
    catch_up_mask = []
    burst_mask = []
    prev_emission = None
    for row in records:
        emission = int(row.get("pacer_emission_ns") or 0)
        deadline = int(row.get("scheduled_deadline_ns") or 0)
        lateness = (emission - deadline) if emission and deadline else 0
        late_mask.append(lateness >= half if half > 1 else lateness > 0)
        catch_up_mask.append(bool(interval_ns) and emission > 0 and deadline > 0 and emission >= deadline + interval_ns)
        if prev_emission and emission and interval_ns:
            burst_mask.append((emission - prev_emission) < int(interval_ns * 0.5))
        else:
            burst_mask.append(False)
        prev_emission = emission or prev_emission

    def _cond(mask: Sequence[bool]) -> dict:
        on = [latencies[i] for i, flag in enumerate(mask) if flag]
        off = [latencies[i] for i, flag in enumerate(mask) if not flag]
        return {
            "n_on": len(on),
            "n_off": len(off),
            "p50_on_ns": _percentile(on, 50),
            "p50_off_ns": _percentile(off, 50),
            "mean_on_ns": _mean(on),
            "mean_off_ns": _mean(off),
        }

    window_stats = []
    if windows > 0 and n:
        size = max(1, math.ceil(n / windows))
        for start in range(0, n, size):
            chunk = latencies[start : start + size]
            window_stats.append(
                {
                    "start_index": start,
                    "n": len(chunk),
                    "p50_ns": _percentile(chunk, 50),
                    "p95_ns": _percentile(chunk, 95),
                }
            )

    lateness_values = []
    for row in records:
        emission = int(row.get("pacer_emission_ns") or 0)
        deadline = int(row.get("scheduled_deadline_ns") or 0)
        if emission and deadline:
            lateness_values.append(emission - deadline)
    lateness_p75 = _percentile([float(v) for v in lateness_values], 75) if lateness_values else None
    lateness_high = []
    if lateness_p75 is not None and lateness_values:
        for row in records:
            emission = int(row.get("pacer_emission_ns") or 0)
            deadline = int(row.get("scheduled_deadline_ns") or 0)
            if emission and deadline:
                lateness_high.append((emission - deadline) >= lateness_p75)
            else:
                lateness_high.append(False)
    else:
        lateness_high = [False] * n

    return {
        "n": n,
        "metric": TRACE_METRIC,
        "p50_ns": median,
        "p95_ns": _percentile(latencies, 95),
        "p99_ns": _percentile(latencies, 99),
        "min_ns": min(latencies),
        "max_ns": max(latencies),
        "lag1_autocorr": lag1_autocorr(latencies),
        "regime": {
            "split": "above_vs_at_or_below_median",
            "switches": switches,
            "alternation_rate": (switches / (n - 1)) if n > 1 else None,
            "run_lengths": _run_lengths(high),
            "max_run": max(_run_lengths(high) or [0]),
        },
        "windows": window_stats,
        "conditioned_on_catch_up": _cond(catch_up_mask),
        "conditioned_on_microburst": _cond(burst_mask),
        "conditioned_on_late_token": _cond(late_mask),
        "conditioned_on_lateness_p75": _cond(lateness_high),
        "association_note": (
            "A large p50_on vs p50_off gap is compatible with pacer catch-up "
            "shaping RTT; it is not by itself a causal proof. Compare the same "
            "cell under in_loop vs external pacing."
        ),
    }


def _esc(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def polyline_svg(
    xs: Sequence[float],
    ys: Sequence[float],
    *,
    x_title: str,
    y_title: str,
    aria: str,
    width: float = 760.0,
    height: float = 280.0,
) -> str:
    points = [(float(x), float(y)) for x, y in zip(xs, ys) if y is not None]
    if len(points) < 2:
        return ""
    min_x = min(p[0] for p in points)
    max_x = max(p[0] for p in points)
    min_y = min(p[1] for p in points)
    max_y = max(p[1] for p in points)
    if max_x <= min_x:
        max_x = min_x + 1.0
    if max_y <= min_y:
        max_y = min_y + 1.0
    pad_l, pad_t, pad_r, pad_b = 72.0, 28.0, 16.0, 48.0
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    def x_of(value: float) -> float:
        return pad_l + (value - min_x) / (max_x - min_x) * plot_w

    def y_of(value: float) -> float:
        return pad_t + (max_y - value) / (max_y - min_y) * plot_h

    coords = " ".join(f"{x_of(x):.1f},{y_of(y):.1f}" for x, y in points)
    mid_y = min_y + (max_y - min_y) / 2.0
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:.0f} {height:.0f}" '
        f'role="img" aria-label="{_esc(aria)}">',
        f'<rect x="0" y="0" width="{width:.0f}" height="{height:.0f}" fill="#fff"/>',
        f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + plot_h}" stroke="#ccc"/>',
        f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{pad_l + plot_w}" y2="{pad_t + plot_h}" stroke="#ccc"/>',
        f'<polyline fill="none" stroke="#1d4ed8" stroke-width="1.2" points="{coords}"/>',
        f'<text x="{pad_l - 8}" y="{pad_t + 4}" text-anchor="end" font-size="11">{_esc(f"{max_y:.3g}")}</text>',
        f'<text x="{pad_l - 8}" y="{y_of(mid_y) + 4}" text-anchor="end" font-size="11">{_esc(f"{mid_y:.3g}")}</text>',
        f'<text x="{pad_l - 8}" y="{pad_t + plot_h}" text-anchor="end" font-size="11">{_esc(f"{min_y:.3g}")}</text>',
        f'<text x="{pad_l}" y="{height - 8}" font-size="11">{_esc(x_title)}</text>',
        f'<text x="{pad_l - 8}" y="{pad_t - 10}" text-anchor="end" font-size="11">{_esc(y_title)}</text>',
        "</svg>",
    ]
    return "".join(parts)


def overlay_svg(
    series: Sequence[Dict[str, Any]],
    *,
    x_title: str,
    y_title: str,
    aria: str,
    width: float = 760.0,
    height: float = 280.0,
) -> str:
    """Several y-series against a shared x, each normalised to 0-1 for overlay."""
    colors = ("#1d4ed8", "#b45309", "#15803d")
    prepared = []
    for item in series:
        xs = list(item.get("x") or [])
        ys = list(item.get("y") or [])
        pts = [(float(x), float(y)) for x, y in zip(xs, ys) if y is not None]
        if len(pts) < 2:
            continue
        ymin, ymax = min(p[1] for p in pts), max(p[1] for p in pts)
        if ymax <= ymin:
            ymax = ymin + 1.0
        prepared.append(
            {
                "name": str(item.get("name") or ""),
                "pts": pts,
                "ymin": ymin,
                "ymax": ymax,
            }
        )
    if not prepared:
        return ""
    min_x = min(p[0] for item in prepared for p in item["pts"])
    max_x = max(p[0] for item in prepared for p in item["pts"])
    if max_x <= min_x:
        max_x = min_x + 1.0
    pad_l, pad_t, pad_r, pad_b = 72.0, 36.0, 16.0, 48.0
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    def x_of(value: float) -> float:
        return pad_l + (value - min_x) / (max_x - min_x) * plot_w

    def y_of(norm: float) -> float:
        return pad_t + (1.0 - norm) * plot_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:.0f} {height:.0f}" '
        f'role="img" aria-label="{_esc(aria)}">',
        f'<rect x="0" y="0" width="{width:.0f}" height="{height:.0f}" fill="#fff"/>',
    ]
    for idx, item in enumerate(prepared):
        span = item["ymax"] - item["ymin"]
        coords = []
        for x, y in item["pts"]:
            norm = (y - item["ymin"]) / span
            coords.append(f"{x_of(x):.1f},{y_of(norm):.1f}")
        color = colors[idx % len(colors)]
        parts.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="1.2" points="{" ".join(coords)}"/>'
        )
        parts.append(
            f'<text x="{pad_l + idx * 160}" y="16" font-size="11" fill="{color}">{_esc(item["name"])}</text>'
        )
    parts.append(f'<text x="{pad_l}" y="{height - 8}" font-size="11">{_esc(x_title)}</text>')
    parts.append(
        f'<text x="{pad_l - 8}" y="{pad_t - 10}" text-anchor="end" font-size="11">{_esc(y_title)}</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def extract_run_traces(doc: dict) -> List[dict]:
    """Pull temporal traces out of a compare or scenario document."""
    found = []
    blocks = list(doc.get("points") or [])
    if not blocks and doc.get("runs"):
        blocks = [{"point": doc.get("point") or {}, "runs": doc.get("runs")}]
    for block in blocks:
        point = block.get("point") or doc.get("point") or {}
        for run in block.get("runs") or []:
            workers = run.get("workers") or []
            initiator = next((w for w in workers if w.get("role") == "rtt_initiator"), None)
            if initiator is None:
                continue
            payload = initiator.get("temporal_trace") or run.get("temporal_trace")
            records = traces_from_columnar(payload) if isinstance(payload, dict) else []
            found.append(
                {
                    "run_id": run.get("run_id"),
                    "client": run.get("client"),
                    "ab_label": run.get("ab_label"),
                    "slot": run.get("slot"),
                    "pacer_mode": (run.get("point") or point).get("pacer_mode")
                    or (initiator.get("pacing") or {}).get("mode"),
                    "target_rate": (run.get("point") or point).get("target_rate"),
                    "pacing": initiator.get("pacing") or run.get("pacing"),
                    "records": records,
                    "analysis": analyze_trace(
                        records,
                        interval_ns=(initiator.get("pacing") or {}).get("target_interval_ns"),
                    ),
                }
            )
    return found


def render_trace_report(doc: dict) -> str:
    traces = extract_run_traces(doc)
    sections = [
        "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>",
        "<title>RTT temporal trace (diagnostic)</title>",
        "<style>body{font:14px/1.4 sans-serif;max-width:960px;margin:24px auto;padding:0 16px}",
        "h1,h2{font-weight:600} .note{color:#555} svg{width:100%;height:auto;border:1px solid #eee;margin:8px 0}",
        "pre{background:#f6f6f6;padding:8px;overflow:auto}</style></head><body>",
        "<h1>Application E2E RTT temporal trace</h1>",
        "<p class='note'>Diagnostic only. <code>application_e2e_latency = receive_ns - send_ns</code> ",
        "where <code>send_ns</code> is the application publish-call timestamp, not a socket write. ",
        "Not an official ranking gate.</p>",
    ]
    if not traces:
        sections.append("<p>No temporal traces in this document.</p></body></html>")
        return "\n".join(sections)
    for item in traces:
        records = item["records"]
        title = (
            f"{item.get('client')} slot={item.get('slot')} "
            f"pacer={item.get('pacer_mode')} n={len(records)}"
        )
        sections.append(f"<h2>{_esc(title)}</h2>")
        if records:
            xs = [float(row["sequence"]) for row in records]
            ys_ms = [float(row["latency_ns"]) / 1e6 for row in records]
            sections.append(
                polyline_svg(
                    xs,
                    ys_ms,
                    x_title="sequence",
                    y_title="application_e2e_latency (ms)",
                    aria=f"E2E latency vs sequence {title}",
                )
            )
            lateness = []
            intervals = []
            token_to_pub = []
            prev_em = None
            for row in records:
                em = int(row.get("pacer_emission_ns") or 0)
                dl = int(row.get("scheduled_deadline_ns") or 0)
                recv = int(row.get("receiver_token_ns") or 0)
                pub = int(row.get("publish_call_ns") or row.get("send_ns") or 0)
                lateness.append(((em - dl) / 1e3) if em and dl else None)
                if prev_em and em:
                    intervals.append((em - prev_em) / 1e3)
                else:
                    intervals.append(None)
                token_to_pub.append(((pub - recv) / 1e3) if recv and pub else None)
                prev_em = em or prev_em
            if any(v is not None for v in lateness):
                sections.append("<h3>Pacer lateness (µs)</h3>")
                sections.append(
                    polyline_svg(
                        xs,
                        [v if v is not None else 0.0 for v in lateness],
                        x_title="sequence",
                        y_title="pacer lateness (µs)",
                        aria=f"pacer lateness {title}",
                    )
                )
            if any(v is not None for v in intervals):
                sections.append("<h3>Emission interval (µs)</h3>")
                sections.append(
                    polyline_svg(
                        xs,
                        [v if v is not None else 0.0 for v in intervals],
                        x_title="sequence",
                        y_title="emission interval (µs)",
                        aria=f"emission interval {title}",
                    )
                )
            if any(v is not None for v in lateness) and any(v is not None for v in token_to_pub):
                sections.append("<h3>Stimulus vs RTT (normalised overlay)</h3>")
                sections.append(
                    overlay_svg(
                        [
                            {"name": "pacer lateness", "x": xs, "y": [v or 0.0 for v in lateness]},
                            {
                                "name": "token→publish (µs)",
                                "x": xs,
                                "y": [v or 0.0 for v in token_to_pub],
                            },
                            {"name": "application RTT (ms)", "x": xs, "y": ys_ms},
                        ],
                        x_title="sequence",
                        y_title="normalised",
                        aria=f"overlay {title}",
                    )
                )
        sections.append("<pre>" + _esc(json.dumps(item.get("analysis") or {}, indent=2)) + "</pre>")
    sections.append("</body></html>")
    return "\n".join(sections)


def write_trace_artifacts(doc: dict, output_dir: str | Path) -> dict:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    traces = extract_run_traces(doc)
    written = []
    for item in traces:
        stem = f"{item.get('client') or 'client'}-slot{item.get('slot')}-{item.get('pacer_mode') or 'pacer'}"
        jsonl = root / f"{stem}.temporal_trace.jsonl"
        with open(jsonl, "w", encoding="utf-8") as fh:
            for row in item["records"]:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
        analysis = root / f"{stem}.analysis.json"
        analysis.write_text(json.dumps(item.get("analysis") or {}, indent=2) + "\n", encoding="utf-8")
        written.append({"jsonl": str(jsonl), "analysis": str(analysis), "n": len(item["records"])})
    html_path = root / "temporal_trace.html"
    html_path.write_text(render_trace_report(doc), encoding="utf-8")
    return {"html": str(html_path), "traces": written, "n_runs": len(traces)}


def _load_docs(input_path: Path) -> List[dict]:
    docs = []
    if input_path.is_file():
        docs.append(json.loads(input_path.read_text(encoding="utf-8")))
        return docs
    for path in sorted(input_path.glob("*.json")):
        try:
            docs.append(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            continue
    return docs


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract and plot diagnostic RTT temporal traces (not a ranking gate)."
    )
    parser.add_argument("--input", required=True, help="Compare JSON file or directory")
    parser.add_argument("--output", required=True, help="Directory for jsonl / HTML")
    args = parser.parse_args(argv)
    input_path = Path(args.input)
    output_root = Path(args.output)
    output_root.mkdir(parents=True, exist_ok=True)
    docs = _load_docs(input_path)
    summaries = []
    for idx, doc in enumerate(docs):
        target = output_root / (doc.get("baseline_client") or f"doc{idx}")
        if doc.get("pacer_mode"):
            target = Path(str(target) + f"-{doc['pacer_mode']}")
        if doc.get("points") and (doc["points"][0].get("point") or {}).get("shared_load_fraction"):
            frac = (doc["points"][0]["point"] or {}).get("shared_load_fraction")
            target = Path(str(target) + f"-f{frac}")
        summaries.append(write_trace_artifacts(doc, target))
    (output_root / "index.json").write_text(json.dumps(summaries, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"docs": len(docs), "outputs": summaries}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
