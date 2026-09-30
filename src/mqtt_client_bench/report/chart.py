"""Inline SVG: latency by percentile, one line per client, log scale."""

from __future__ import annotations

import math
from html import escape
from typing import Dict, List, Tuple

from mqtt_client_bench.bench import histogram
from mqtt_client_bench.report.views import METRICS

QUANTILES: List[Tuple[str, float]] = [
    ("p50", 0.5),
    ("p75", 0.75),
    ("p90", 0.9),
    ("p99", 0.99),
    ("p99.9", 0.999),
    ("max", 1.0),
]

W, H = 640, 300
LEFT, RIGHT, TOP, BOTTOM = 64, 110, 16, 32


def _decade(us: float) -> str:
    for unit, scale in (("s", 1e6), ("ms", 1e3)):
        if us >= scale:
            return f"{us / scale:g} {unit}"
    return f"{us:g} µs"


def _series(hist: dict) -> List[float]:
    return [max(histogram.percentile(hist, q) or 1.0, 1.0) / 1e3 for _, q in QUANTILES]


def latency_chart(hists: Dict[str, dict], colors: Dict[str, str]) -> str:
    """``hists`` maps client -> merged histogram of its valid runs."""
    if not hists:
        return ""
    series = {client: _series(h) for client, h in hists.items()}
    lo = min(min(v) for v in series.values())
    hi = max(max(v) for v in series.values())
    lo_e = math.floor(math.log10(lo))
    hi_e = math.ceil(math.log10(hi)) if hi > lo else lo_e + 1
    plot_w, plot_h = W - LEFT - RIGHT, H - TOP - BOTTOM

    def x(i: int) -> float:
        return LEFT + plot_w * i / (len(QUANTILES) - 1)

    def y(us: float) -> float:
        return TOP + plot_h * (1 - (math.log10(us) - lo_e) / (hi_e - lo_e))

    parts = [f'<svg class="chart" viewBox="0 0 {W} {H}" role="img" aria-label="Latency by percentile">']
    for k in range(lo_e, hi_e + 1):
        yy = y(10.0**k)
        parts.append(f'<line class="grid" x1="{LEFT}" x2="{W - RIGHT}" y1="{yy:.1f}" y2="{yy:.1f}"/>')
        parts.append(f'<text class="axis" x="{LEFT - 6}" y="{yy + 4:.1f}" text-anchor="end">{escape(_decade(10.0**k))}</text>')
    for i, (label, _) in enumerate(QUANTILES):
        parts.append(f'<text class="axis" x="{x(i):.1f}" y="{H - 10}" text-anchor="middle">{label}</text>')
    ends = []
    for client, values in series.items():
        color = colors.get(client, "currentColor")
        pts = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(values))
        tip = ", ".join(f"{q} {METRICS['p50'].fmt(v)}" for (q, _), v in zip(QUANTILES, values))
        parts.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{pts}"><title>{escape(client)}: {escape(tip)}</title></polyline>'
        )
        ends.append((y(values[-1]), client, color))
    last_y = -1e9
    for yy, client, color in sorted(ends):
        yy = max(yy, last_y + 13)
        last_y = yy
        parts.append(f'<text class="legend" x="{W - RIGHT + 8}" y="{yy + 4:.1f}" fill="{color}">{escape(client)}</text>')
    parts.append("</svg>")
    return "".join(parts)
