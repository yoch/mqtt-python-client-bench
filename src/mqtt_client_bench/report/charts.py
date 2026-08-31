"""Inline SVG chart primitives.

Server-rendered on purpose. The page used to pull Chart.js and Google Fonts from
CDNs, so the report needed the network to display at all and leaked a request per
reader; everything here ships inside the HTML. The optional ``app.js`` adds
hover and sorting on top, but no chart depends on it — every mark also carries a
native ``<title>``, and every value on this site is reachable from a table.

Marks are painted with ``var(--c-<client>)`` rather than a hex literal so one
stylesheet swap re-themes the whole site for dark mode.

Two layout rules are load-bearing, because breaking them is what made the
previous charts unreadable:

* **Category labels are never rotated into a clip.** Long names (a scenario plus
  its protocol runs to forty characters) get horizontal bars with the label in a
  left gutter, which is the documented answer for long-named categories.
* **The drawn height includes the axis band.** Sizing a container to the plot
  alone put the tick labels outside it and gave every card its own tiny scrollbar.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .model import _esc, _fmt_num
from .theme import client_paint

# Geometry shared by every chart so they stack into one visual system.
_GRID_STEPS = 4
_BAR_RADIUS = 4
_MARK_GAP = 2  # surface gap between adjacent fills, never a border


def _nice_top(value: float) -> float:
    """Round an axis maximum up to a step that produces readable gridlines."""
    if value <= 0:
        return 0.0
    magnitude = 10 ** max(0, len(str(int(value))) - 2)
    return math.ceil(value / magnitude) * magnitude


def _tick_values(top: float, steps: int = _GRID_STEPS) -> List[float]:
    return [top * i / steps for i in range(steps + 1)]


def _fmt_axis(value: float, unit: str) -> str:
    if unit == "ms":
        return f"{value:.3g}"
    if unit in ("%", "ratio"):
        return f"{value:.2g}"
    return _fmt_num(value, digits=0)


def _text_width(text: str, size: float = 11.0) -> float:
    """Rough advance width, good enough to size a gutter without measuring."""
    return len(text) * size * 0.55


def _wrap(svg: str, *, min_width: float = 0.0, extra: str = "") -> str:
    """Put a chart in a box that scrolls rather than shrinks.

    A chart squeezed into a phone-width column keeps its layout and loses its
    type: 11px labels become 6px. The wrapper carries the chart's own drawn
    width as a floor, so a narrow viewport scrolls it at full size instead.
    """
    cls = f"chart-scroll {extra}".strip()
    style = f' style="min-width:{min_width:.0f}px"' if min_width else ""
    return f'<div class="{cls}"><div class="chart-inner"{style}>{svg}</div></div>'


def _legend(series: Sequence[Dict[str, Any]]) -> str:
    """A legend is present whenever two or more series share a chart.

    One series needs none: the panel heading already names it, and a legend of
    one is noise.
    """
    if len(series) < 2:
        return ""
    items = "".join(
        f'<span class="legend-item" data-series="{_esc(s.get("client", ""))}">'
        f'<span class="swatch" style="background:{client_paint(s.get("client", ""))}"></span>'
        f'{_esc(s.get("client", ""))}</span>'
        for s in series
    )
    return f'<p class="legend">{items}</p>'


def _mark(title: str) -> str:
    """Native tooltip plus the hook app.js upgrades into a real one."""
    safe = _esc(title)
    return f' data-tip="{safe}"><title>{safe}</title>'


# --------------------------------------------------------------------------
# Horizontal grouped bars — the ranking form
# --------------------------------------------------------------------------

def bar_extent(series: Sequence[Dict[str, Any]]) -> float:
    """Largest value or whisker end in ``series`` — the input to a shared scale."""
    numbers = [
        v
        for s in series
        for v in list(s.get("values") or []) + list(s.get("high") or [])
        if v is not None
    ]
    return max(numbers) if numbers else 0.0


def bar_group(
    categories: Sequence[str],
    series: Sequence[Dict[str, Any]],
    *,
    unit: str = "msg/s",
    aria: str = "Comparison by category and client",
    value_labels: bool = True,
    scale_max: Optional[float] = None,
) -> str:
    """Grouped horizontal bars with observed min/max whiskers.

    ``series`` items are ``{"client", "values", "low", "high"}``, each list
    aligned to ``categories``. A ``None`` value is a gap — never a zero — because
    "not measured" and "measured as nothing" are different claims.

    ``scale_max`` forces a shared domain across a set of small multiples, so a
    bar of a given length means the same rate in every chart on the page.
    """
    if not categories or not series:
        return ""
    top = _nice_top(scale_max if scale_max else bar_extent(series))
    if top <= 0:
        return ""

    n_series = len(series)
    bar_h = 15 if n_series > 1 else 20

    # Only the series that actually measured this row take a slot. Reserving a
    # slot for every client left tall blank bands wherever a library declined a
    # point, which reads as missing ink rather than as a refusal — and the
    # refusals are already named in the matrix.
    present: List[List[int]] = []
    for c_idx in range(len(categories)):
        rows = []
        for s_idx, serie in enumerate(series):
            values = serie.get("values") or []
            if c_idx < len(values) and values[c_idx] is not None:
                rows.append(s_idx)
        present.append(rows)

    row_heights = [max(1, len(rows)) * (bar_h + _MARK_GAP) + 14 for rows in present]
    row_tops: List[float] = []
    running = 0.0
    for h in row_heights:
        row_tops.append(running)
        running += h

    gutter = min(300.0, max(120.0, max(_text_width(c) for c in categories) + 14))
    pad_r = 92 if value_labels else 26
    pad_t, axis_band = 10, 34
    plot_w = 560.0
    plot_h = running
    width = gutter + plot_w + pad_r
    height = pad_t + plot_h + axis_band

    def x_of(value: float) -> float:
        return gutter + (value / top) * plot_w

    parts = [
        f'<svg class="chart-svg" viewBox="0 0 {width:.0f} {height:.0f}" role="img" '
        f'aria-label="{_esc(aria)}" preserveAspectRatio="xMinYMin meet">'
    ]
    # Gridlines and the value axis, drawn under the marks.
    for tick in _tick_values(top):
        x = x_of(tick)
        parts.append(
            f'<line class="grid" x1="{x:.1f}" y1="{pad_t}" x2="{x:.1f}" y2="{pad_t + plot_h:.1f}" />'
            f'<text class="axis" x="{x:.1f}" y="{pad_t + plot_h + 16:.1f}" text-anchor="middle">'
            f"{_esc(_fmt_axis(tick, unit))}</text>"
        )
    parts.append(
        f'<text class="axis-unit" x="{gutter:.1f}" y="{pad_t + plot_h + 30:.1f}">{_esc(unit)}</text>'
    )

    for c_idx, category in enumerate(categories):
        top_y = pad_t + row_tops[c_idx]
        row_h = row_heights[c_idx]
        parts.append(
            f'<text class="cat" x="{gutter - 10:.1f}" y="{top_y + row_h / 2 + 4:.1f}" '
            f'text-anchor="end">{_esc(category)}</text>'
        )
        if c_idx:
            parts.append(
                f'<line class="row-rule" x1="{gutter:.1f}" y1="{top_y:.1f}" '
                f'x2="{gutter + plot_w:.1f}" y2="{top_y:.1f}" />'
            )
        for slot, s_idx in enumerate(present[c_idx]):
            serie = series[s_idx]
            values = serie.get("values") or []
            value = values[c_idx]
            client = serie.get("client", "")
            y = top_y + 7 + slot * (bar_h + _MARK_GAP)
            w = max(x_of(float(value)) - gutter, 1.0)
            parts.append(
                f'<rect class="bar" x="{gutter:.1f}" y="{y:.1f}" width="{w:.1f}" '
                f'height="{bar_h}" rx="{_BAR_RADIUS}" fill="{client_paint(client)}" '
                f'data-series="{_esc(client)}"'
                + _mark(f"{client} · {category}: {_fmt_num(value)} {unit}")
                + "</rect>"
            )
            lows, highs = serie.get("low") or [], serie.get("high") or []
            low = lows[c_idx] if c_idx < len(lows) else None
            high = highs[c_idx] if c_idx < len(highs) else None
            label_x = gutter + w
            if high is not None:
                label_x = max(label_x, x_of(float(high)))
            if low is not None and high is not None and high > low:
                cy = y + bar_h / 2
                x_lo, x_hi = x_of(float(low)), x_of(float(high))
                # The low end of the range falls inside the bar. Drawn as a bare
                # line it read as a notch cut out of the fill, so the mark gets a
                # surface-coloured halo first: over the bar it reads as a ring,
                # over the plot background it disappears into it.
                geometry = (
                    ('x1="{lo:.1f}" y1="{cy:.1f}" x2="{hi:.1f}" y2="{cy:.1f}"'),
                    ('x1="{lo:.1f}" y1="{up:.1f}" x2="{lo:.1f}" y2="{dn:.1f}"'),
                    ('x1="{hi:.1f}" y1="{up:.1f}" x2="{hi:.1f}" y2="{dn:.1f}"'),
                )
                coords = {"lo": x_lo, "hi": x_hi, "cy": cy, "up": cy - 3.5, "dn": cy + 3.5}
                for css in ("whisker-halo", "whisker"):
                    for shape in geometry:
                        parts.append(f'<line class="{css}" {shape.format(**coords)} />')
            if value_labels:
                parts.append(
                    f'<text class="bar-value" x="{label_x + 7:.1f}" y="{y + bar_h - 3:.1f}">'
                    f"{_esc(_fmt_num(value))}</text>"
                )
    parts.append(
        f'<line class="axis-line" x1="{gutter:.1f}" y1="{pad_t:.1f}" '
        f'x2="{gutter:.1f}" y2="{pad_t + plot_h:.1f}" />'
    )
    parts.append("</svg>")
    return _wrap("".join(parts), min_width=width) + _legend(series)


# --------------------------------------------------------------------------
# Line sweep — the form an ordinal axis deserves
# --------------------------------------------------------------------------

def sweep_extent(series: Sequence[Dict[str, Any]]) -> float:
    """Largest plotted value in ``series`` — the input to a shared scale."""
    numbers = [v for s in series for v in (s.get("values") or []) if v is not None]
    return max(numbers) if numbers else 0.0


def line_sweep(
    x_labels: Sequence[str],
    series: Sequence[Dict[str, Any]],
    *,
    unit: str = "msg/s",
    x_title: str = "",
    aria: str = "Sweep by client",
    log_y: bool = False,
    scale_max: Optional[float] = None,
) -> str:
    """One line per client across an ordered axis, with a marker per point.

    A sweep is a curve. Collapsing ``pub_payload_sweep_qos0`` — seven payload
    sizes spanning zero to a megabyte — into a single bar threw away the only
    thing the scenario was built to show.
    """
    if not x_labels or not series:
        return ""
    numbers = [v for s in series for v in (s.get("values") or []) if v is not None]
    if not numbers:
        return ""
    positive = [v for v in numbers if v > 0]
    use_log = log_y and positive and max(positive) / min(positive) >= 50
    top = _nice_top(scale_max if scale_max else max(numbers))
    floor = min(positive) / 2 if use_log and positive else 0.0
    if top <= 0:
        return ""

    # The first and last category labels are centred on the plot edges, so the
    # padding has to hold half of each or they get clipped by the viewBox — which
    # is exactly what happened to the widest payload name.
    pad_l = max(66.0, _text_width(str(x_labels[0])) / 2 + 10)
    pad_r = max(26.0, _text_width(str(x_labels[-1])) / 2 + 10)
    # Room above the plot for the value-axis unit: at pad_t=14 it sat on top of
    # the highest gridline label.
    pad_t = 28
    axis_band = 46 if x_title else 34
    plot_w = max(360, 108 * (len(x_labels) - 1) + 60)
    plot_h = 210
    width = pad_l + plot_w + pad_r
    height = pad_t + plot_h + axis_band

    def y_of(value: float) -> float:
        if use_log:
            lo, hi = math.log10(max(floor, 1e-9)), math.log10(max(top, 1e-9))
            pos = (math.log10(max(float(value), floor)) - lo) / (hi - lo) if hi > lo else 0.0
            return pad_t + plot_h - pos * plot_h
        return pad_t + plot_h - (float(value) / top) * plot_h

    def x_of(idx: int) -> float:
        if len(x_labels) == 1:
            return pad_l + plot_w / 2
        return pad_l + idx * (plot_w / (len(x_labels) - 1))

    parts = [
        f'<svg class="chart-svg" viewBox="0 0 {width:.0f} {height:.0f}" role="img" '
        f'aria-label="{_esc(aria)}" preserveAspectRatio="xMinYMin meet">'
    ]
    ticks = (
        [floor * (top / floor) ** (i / _GRID_STEPS) for i in range(_GRID_STEPS + 1)]
        if use_log and floor > 0
        else _tick_values(top)
    )
    for tick in ticks:
        y = y_of(tick)
        parts.append(
            f'<line class="grid" x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w:.1f}" y2="{y:.1f}" />'
            f'<text class="axis" x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end">'
            f"{_esc(_fmt_axis(tick, unit))}</text>"
        )
    for idx, label in enumerate(x_labels):
        x = x_of(idx)
        parts.append(
            f'<text class="cat" x="{x:.1f}" y="{pad_t + plot_h + 17:.1f}" text-anchor="middle">'
            f"{_esc(label)}</text>"
        )
    if x_title:
        parts.append(
            f'<text class="axis-unit" x="{pad_l + plot_w / 2:.1f}" '
            f'y="{pad_t + plot_h + 38:.1f}" text-anchor="middle">{_esc(x_title)}</text>'
        )
    # Name the value axis: the panel title says what is measured, but the ticks
    # need their unit beside them or the reader has to go looking for it.
    parts.append(
        f'<text class="axis-unit" x="{pad_l - 8:.1f}" y="{pad_t - 11:.1f}" text-anchor="end">'
        f"{_esc(unit)}</text>"
    )

    for serie in series:
        client = serie.get("client", "")
        values = serie.get("values") or []
        paint = client_paint(client)
        # Break the line across gaps rather than bridging them: a straight
        # segment over a refused point would invent a measurement.
        run: List[Tuple[float, float]] = []
        runs: List[List[Tuple[float, float]]] = []
        for idx in range(len(x_labels)):
            value = values[idx] if idx < len(values) else None
            if value is None:
                if len(run) > 1:
                    runs.append(run)
                run = []
                continue
            run.append((x_of(idx), y_of(float(value))))
        if len(run) > 1:
            runs.append(run)
        for segment in runs:
            d = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y) in enumerate(segment))
            parts.append(
                f'<path class="sweep-line" d="{d}" fill="none" stroke="{paint}" '
                f'data-series="{_esc(client)}" />'
            )
        for idx in range(len(x_labels)):
            value = values[idx] if idx < len(values) else None
            if value is None:
                continue
            cx, cy = x_of(idx), y_of(float(value))
            parts.append(
                f'<circle class="sweep-dot" cx="{cx:.1f}" cy="{cy:.1f}" r="5" '
                f'fill="{paint}" data-series="{_esc(client)}"'
                + _mark(f"{client} · {x_labels[idx]}: {_fmt_num(value)} {unit}")
                + "</circle>"
            )
    parts.append(
        f'<line class="axis-line" x1="{pad_l}" y1="{pad_t + plot_h:.1f}" '
        f'x2="{pad_l + plot_w:.1f}" y2="{pad_t + plot_h:.1f}" />'
    )
    parts.append("</svg>")
    return _wrap("".join(parts), min_width=width) + _legend(series)


# --------------------------------------------------------------------------
# Latency ranges — p50 to p99 as a span, not three disconnected numbers
# --------------------------------------------------------------------------

def range_extent(rows: Sequence[Dict[str, Any]]) -> float:
    """Largest percentile in ``rows`` — the input to a shared scale."""
    values = [
        float(r.get("p99") or r.get("p95") or r["p50"])
        for r in rows
        if r.get("p50") is not None
    ]
    return max(values) if values else 0.0


def range_bars(
    rows: Sequence[Dict[str, Any]],
    *,
    unit: str = "ms",
    aria: str = "Latency percentiles",
    scale_max: Optional[float] = None,
) -> str:
    """One row per subject: a p50-to-p99 span with the median marked.

    Three percentile columns in a table hide the shape they describe. Drawn as a
    span, a client whose p99 sits far from its p50 is visibly the unstable one.
    ``rows`` are ``{"label", "client", "p50", "p95", "p99"}``.

    ``scale_max`` forces a shared domain across a set of small multiples. Left to
    itself each chart scaled to its own worst value, so a 2 ms span and a 7 ms
    span drew the same width — the one comparison the eye makes automatically
    between stacked charts, and it was wrong every time.
    """
    rows = [r for r in rows if r.get("p50") is not None]
    if not rows:
        return ""
    top = _nice_top(scale_max if scale_max else range_extent(rows))
    if top <= 0:
        return ""
    row_h = 30
    # The gutter holds the row label; the p50 value is written just inside the
    # plot, so the plot itself starts far enough right for both to fit.
    gutter = min(280.0, max(110.0, max(_text_width(str(r.get("label", ""))) for r in rows) + 14))
    pad_t, pad_r, axis_band = 10, 84, 34
    plot_w, plot_h = 520.0, row_h * len(rows)
    width, height = gutter + plot_w + pad_r, pad_t + plot_h + axis_band

    def x_of(value: float) -> float:
        return gutter + (value / top) * plot_w

    parts = [
        f'<svg class="chart-svg" viewBox="0 0 {width:.0f} {height:.0f}" role="img" '
        f'aria-label="{_esc(aria)}" preserveAspectRatio="xMinYMin meet">'
    ]
    for tick in _tick_values(top):
        x = x_of(tick)
        parts.append(
            f'<line class="grid" x1="{x:.1f}" y1="{pad_t}" x2="{x:.1f}" y2="{pad_t + plot_h:.1f}" />'
            f'<text class="axis" x="{x:.1f}" y="{pad_t + plot_h + 16:.1f}" text-anchor="middle">'
            f"{_esc(_fmt_axis(tick, unit))}</text>"
        )
    parts.append(
        f'<text class="axis-unit" x="{gutter:.1f}" y="{pad_t + plot_h + 30:.1f}">{_esc(unit)}</text>'
    )
    for idx, row in enumerate(rows):
        client = row.get("client", "")
        paint = client_paint(client)
        cy = pad_t + idx * row_h + row_h / 2
        p50 = float(row["p50"])
        p95 = float(row["p95"]) if row.get("p95") is not None else p50
        p99 = float(row["p99"]) if row.get("p99") is not None else p95
        parts.append(
            f'<text class="cat" x="{gutter - 10:.1f}" y="{cy + 4:.1f}" text-anchor="end">'
            f'{_esc(str(row.get("label", client)))}</text>'
        )
        tip = f"{client} · p50 {p50:.3g} · p95 {p95:.3g} · p99 {p99:.3g} {unit}"
        parts.append(
            f'<line class="span-line" x1="{x_of(p50):.1f}" y1="{cy:.1f}" '
            f'x2="{x_of(p99):.1f}" y2="{cy:.1f}" stroke="{paint}" data-series="{_esc(client)}"'
            + _mark(tip)
            + "</line>"
        )
        parts.append(
            f'<circle class="span-p95" cx="{x_of(p95):.1f}" cy="{cy:.1f}" r="3.5" fill="{paint}"'
            + _mark(f"{client} · p95 {p95:.3g} {unit}")
            + "</circle>"
        )
        parts.append(
            f'<circle class="span-p50" cx="{x_of(p50):.1f}" cy="{cy:.1f}" r="5" fill="{paint}"'
            + _mark(f"{client} · p50 {p50:.3g} {unit}")
            + "</circle>"
        )
        # Both ends are labelled: percentile spans on this corpus are often a
        # small fraction of the axis, and without the numbers the marks read as
        # two dots rather than as a median and its tail.
        parts.append(
            f'<text class="span-label" x="{x_of(p50) - 8:.1f}" y="{cy + 3.5:.1f}" '
            f'text-anchor="end">{_esc(f"{p50:.3g}")}</text>'
        )
        parts.append(
            f'<text class="bar-value" x="{x_of(p99) + 8:.1f}" y="{cy + 4:.1f}">'
            f"p99 {_esc(f'{p99:.3g}')}</text>"
        )
    parts.append(
        f'<line class="axis-line" x1="{gutter:.1f}" y1="{pad_t:.1f}" '
        f'x2="{gutter:.1f}" y2="{pad_t + plot_h:.1f}" />'
    )
    parts.append("</svg>")
    return _wrap("".join(parts), min_width=width)


# --------------------------------------------------------------------------
# Scatter — throughput against what it costs
# --------------------------------------------------------------------------

def scatter(
    points: Sequence[Dict[str, Any]],
    *,
    x_title: str,
    y_title: str,
    aria: str = "Throughput against cost",
) -> str:
    """Points labelled directly rather than coloured by identity.

    Nine clients exceed any categorical palette's all-pairs budget, so identity
    is carried by the label beside each dot. Colour still follows the client, but
    nothing depends on telling two hues apart.
    """
    points = [p for p in points if p.get("x") is not None and p.get("y") is not None]
    if not points:
        return ""
    top_x = _nice_top(max(float(p["x"]) for p in points))
    top_y = _nice_top(max(float(p["y"]) for p in points))
    if top_x <= 0 or top_y <= 0:
        return ""
    pad_l, pad_t, pad_r = 70, 28, 120
    axis_band = 46
    plot_w, plot_h = 520.0, 300.0
    width, height = pad_l + plot_w + pad_r, pad_t + plot_h + axis_band

    def x_of(v: float) -> float:
        return pad_l + (v / top_x) * plot_w

    def y_of(v: float) -> float:
        return pad_t + plot_h - (v / top_y) * plot_h

    parts = [
        f'<svg class="chart-svg" viewBox="0 0 {width:.0f} {height:.0f}" role="img" '
        f'aria-label="{_esc(aria)}" preserveAspectRatio="xMinYMin meet">'
    ]
    for tick in _tick_values(top_y):
        y = y_of(tick)
        parts.append(
            f'<line class="grid" x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w:.1f}" y2="{y:.1f}" />'
            f'<text class="axis" x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end">'
            f"{_esc(_fmt_num(tick, digits=0))}</text>"
        )
    for tick in _tick_values(top_x):
        x = x_of(tick)
        parts.append(
            f'<text class="axis" x="{x:.1f}" y="{pad_t + plot_h + 16:.1f}" text-anchor="middle">'
            f"{_esc(_fmt_num(tick, digits=0))}</text>"
        )
    parts.append(
        f'<text class="axis-unit" x="{pad_l + plot_w / 2:.1f}" y="{pad_t + plot_h + 38:.1f}" '
        f'text-anchor="middle">{_esc(x_title)}</text>'
    )
    parts.append(
        f'<text class="axis-unit" x="{pad_l - 8:.1f}" y="{pad_t - 11:.1f}" text-anchor="end">'
        f"{_esc(y_title)}</text>"
    )
    for point in points:
        client = str(point.get("client", ""))
        cx, cy = x_of(float(point["x"])), y_of(float(point["y"]))
        parts.append(
            f'<circle class="dot" cx="{cx:.1f}" cy="{cy:.1f}" r="6" '
            f'fill="{client_paint(client)}" data-series="{_esc(client)}"'
            + _mark(f"{client} · {_fmt_num(point['x'])} {x_title} · {_fmt_num(point['y'])} {y_title}")
            + "</circle>"
        )
        parts.append(
            f'<text class="dot-label" x="{cx + 10:.1f}" y="{cy + 4:.1f}">{_esc(client)}</text>'
        )
    parts.append(
        f'<line class="axis-line" x1="{pad_l}" y1="{pad_t + plot_h:.1f}" '
        f'x2="{pad_l + plot_w:.1f}" y2="{pad_t + plot_h:.1f}" />'
    )
    parts.append("</svg>")
    return _wrap("".join(parts), min_width=width)


# --------------------------------------------------------------------------
# Heatmap — coverage as magnitude, one hue
# --------------------------------------------------------------------------

def heatmap(
    rows: Sequence[str],
    cols: Sequence[str],
    values: Dict[Tuple[str, str], Optional[float]],
    *,
    aria: str = "Coverage",
    cell_tip=None,
) -> str:
    """Client x scenario on a single-hue ramp; ``None`` renders as absence."""
    if not rows or not cols:
        return ""
    cell_w, cell_h = 30, 26
    # Column labels are rotated -60°, so each one reaches up and to the right of
    # its cell. The head band holds the vertical reach and the right padding
    # holds the horizontal one; without the second, the last column's name ran
    # off the viewBox and was clipped.
    angle = math.radians(60)
    longest = max(_text_width(c) for c in cols)
    gutter = min(260.0, max(110.0, max(_text_width(r) for r in rows) + 14))
    head = min(230.0, max(90.0, longest * math.sin(angle) + 18))
    pad_r = min(150.0, max(18.0, longest * math.cos(angle) + 12))
    width = gutter + cell_w * len(cols) + pad_r
    height = head + cell_h * len(rows) + 12
    parts = [
        f'<svg class="chart-svg heat" viewBox="0 0 {width:.0f} {height:.0f}" role="img" '
        f'aria-label="{_esc(aria)}" preserveAspectRatio="xMinYMin meet">'
    ]
    for c_idx, col in enumerate(cols):
        x = gutter + c_idx * cell_w + cell_w / 2
        parts.append(
            f'<text class="heat-col" x="{x:.1f}" y="{head - 8:.1f}" '
            f'transform="rotate(-60 {x:.1f} {head - 8:.1f})">{_esc(col)}</text>'
        )
    for r_idx, row in enumerate(rows):
        y = head + r_idx * cell_h
        parts.append(
            f'<text class="cat" x="{gutter - 10:.1f}" y="{y + cell_h / 2 + 4:.1f}" '
            f'text-anchor="end">{_esc(row)}</text>'
        )
        for c_idx, col in enumerate(cols):
            value = values.get((row, col))
            x = gutter + c_idx * cell_w
            if value is None:
                parts.append(
                    f'<rect class="heat-cell heat-none" x="{x + 1:.1f}" y="{y + 1:.1f}" '
                    f'width="{cell_w - 2}" height="{cell_h - 2}" rx="3"'
                    + _mark(f"{row} · {col}: not run")
                    + "</rect>"
                )
                continue
            step = min(6, max(0, int(round(float(value) * 6))))
            tip = cell_tip(row, col, value) if cell_tip else f"{row} · {col}: {value:.0%}"
            parts.append(
                f'<rect class="heat-cell heat-{step}" x="{x + 1:.1f}" y="{y + 1:.1f}" '
                f'width="{cell_w - 2}" height="{cell_h - 2}" rx="3"'
                + _mark(tip)
                + "</rect>"
            )
    parts.append("</svg>")
    legend = (
        '<p class="legend heat-legend"><span>0% valid</span>'
        + "".join(f'<span class="heat-key heat-{i}"></span>' for i in range(7))
        + "<span>100%</span></p>"
    )
    return _wrap("".join(parts), min_width=width) + legend


# --------------------------------------------------------------------------
# A/B effect — ratio with its interval, against the line of no effect
# --------------------------------------------------------------------------

def effect_dots(rows: Sequence[Dict[str, Any]], *, aria: str = "A/B effect per point") -> str:
    """Median ratio per point with its confidence interval and a 1.0 reference.

    An interval that straddles 1.0 is an inconclusive result, and drawing it
    against that line says so at a glance where a table of numbers does not.

    Rows whose verdict produced no ratio keep their place. Dropping them made a
    six-point comparison render as four points without saying so, which is the
    kind of silent omission this report exists to avoid.
    """
    rows = [r for r in rows if r.get("label")]
    plotted = [r for r in rows if r.get("ratio") is not None]
    if not plotted:
        return ""
    span = [float(r["ratio"]) for r in plotted]
    span += [float(r["lo"]) for r in plotted if r.get("lo") is not None]
    span += [float(r["hi"]) for r in plotted if r.get("hi") is not None]
    lo_v, hi_v = min(span + [1.0]), max(span + [1.0])
    pad = max(0.08, (hi_v - lo_v) * 0.12)
    # Round the domain outwards to a tenth so the ticks read as numbers a person
    # would choose rather than as artefacts of the padding.
    lo_v = math.floor((lo_v - pad) * 10) / 10
    hi_v = math.ceil((hi_v + pad) * 10) / 10
    if hi_v <= lo_v:
        hi_v = lo_v + 0.1

    row_h = 30
    gutter = min(280.0, max(120.0, max(_text_width(str(r.get("label", ""))) for r in rows) + 14))
    pad_t, pad_r, axis_band = 10, 170, 46
    plot_w, plot_h = 460.0, row_h * len(rows)
    width, height = gutter + plot_w + pad_r, pad_t + plot_h + axis_band

    def x_of(v: float) -> float:
        return gutter + ((v - lo_v) / (hi_v - lo_v)) * plot_w

    parts = [
        f'<svg class="chart-svg" viewBox="0 0 {width:.0f} {height:.0f}" role="img" '
        f'aria-label="{_esc(aria)}" preserveAspectRatio="xMinYMin meet">'
    ]
    for i in range(_GRID_STEPS + 1):
        v = lo_v + (hi_v - lo_v) * i / _GRID_STEPS
        x = x_of(v)
        parts.append(
            f'<line class="grid" x1="{x:.1f}" y1="{pad_t}" x2="{x:.1f}" y2="{pad_t + plot_h:.1f}" />'
            f'<text class="axis" x="{x:.1f}" y="{pad_t + plot_h + 16:.1f}" text-anchor="middle">'
            f"{v:.2f}</text>"
        )
    ref = x_of(1.0)
    parts.append(
        f'<line class="ref-line" x1="{ref:.1f}" y1="{pad_t}" x2="{ref:.1f}" y2="{pad_t + plot_h:.1f}" />'
        f'<text class="axis-unit" x="{ref:.1f}" y="{pad_t + plot_h + 33:.1f}" text-anchor="middle">'
        "no effect</text>"
    )
    for idx, row in enumerate(rows):
        cy = pad_t + idx * row_h + row_h / 2
        verdict = str(row.get("verdict", "")) or "none"
        parts.append(
            f'<text class="cat" x="{gutter - 10:.1f}" y="{cy + 4:.1f}" text-anchor="end">'
            f'{_esc(str(row.get("label", "")))}</text>'
        )
        if row.get("ratio") is None:
            parts.append(
                f'<text class="span-label" x="{gutter + 10:.1f}" y="{cy + 4:.1f}" '
                f'text-anchor="start">{_esc(verdict)} — no ratio computed</text>'
            )
            continue
        ratio = float(row["ratio"])
        if row.get("lo") is not None and row.get("hi") is not None:
            parts.append(
                f'<line class="whisker" x1="{x_of(float(row["lo"])):.1f}" y1="{cy:.1f}" '
                f'x2="{x_of(float(row["hi"])):.1f}" y2="{cy:.1f}" />'
            )
        parts.append(
            f'<circle class="effect-dot effect-{_esc(verdict)}" cx="{x_of(ratio):.1f}" '
            f'cy="{cy:.1f}" r="6"'
            + _mark(f'{row.get("label", "")}: ratio {ratio:.3f} ({verdict})')
            + "</circle>"
        )
        parts.append(
            f'<text class="bar-value" x="{gutter + plot_w + 10:.1f}" y="{cy + 4:.1f}">'
            f"{ratio:.3f} {_esc(verdict)}</text>"
        )
    parts.append("</svg>")
    return _wrap("".join(parts), min_width=width)


# --------------------------------------------------------------------------
# Small forms
# --------------------------------------------------------------------------

def sparkline(values: Sequence[Optional[float]], *, label: str = "", unit: str = "") -> str:
    """A shape, not a reading: the run's own time series at a glance."""
    points = [(i, v) for i, v in enumerate(values) if v is not None]
    if len(points) < 2:
        return ""
    numbers = [v for _, v in points]
    lo, hi = min(numbers), max(numbers)
    span = (hi - lo) or 1.0
    w, h = 200.0, 34.0
    step = w / max(1, len(values) - 1)
    d = " ".join(
        f"{'M' if i == 0 else 'L'}{idx * step:.1f},{h - ((v - lo) / span) * h:.1f}"
        for i, (idx, v) in enumerate(points)
    )
    tip = f"{label}: {_fmt_num(lo)}–{_fmt_num(hi)} {unit}".strip()
    return (
        f'<svg class="sparkline" viewBox="0 0 {w:.0f} {h:.0f}" role="img" '
        f'aria-label="{_esc(tip)}" preserveAspectRatio="none">'
        f'<path d="{d}" fill="none"/><title>{_esc(tip)}</title></svg>'
    )


def meter(value: Optional[float], total: Optional[float], *, label: str = "", unit: str = "msg/s") -> str:
    """Delivered against offered on one track — a ratio against its limit."""
    if value is None or not total:
        return ""
    frac = max(0.0, min(1.0, float(value) / float(total)))
    tip = f"{label}: {_fmt_num(value)} of {_fmt_num(total)} {unit} ({frac:.0%})"
    return (
        f'<span class="meter" data-tip="{_esc(tip)}" title="{_esc(tip)}">'
        f'<span class="meter-track"><span class="meter-fill" style="width:{frac * 100:.1f}%"></span></span>'
        f'<span class="meter-value">{frac:.0%}</span></span>'
    )
