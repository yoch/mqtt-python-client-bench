"""Build a static HTML report site from committed benchmark JSON results."""

from __future__ import annotations

import html
import json
import math
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ASSETS_DIR = Path(__file__).resolve().parent / "report_assets"


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-").lower()
    return cleaned or "result"


def _fmt_num(value: Any, *, digits: int = 1) -> str:
    if value is None:
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if digits == 0:
        return f"{number:,.0f}"
    return f"{number:,.{digits}f}"


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


_CLIENT_ORDER = (
    "awscrt",
    "gmqtt",
    "paho",
    "amqtt",
    "aiomqtt",
    "zmqtt",
    "aiomqtt3",
    "mqttium",
    "mqttium-compat",
)

# Rate-capped / functional / niche scenarios: primary msg/s either echoes an
# injected ceiling or is only meaningful for one client (paho-native callbacks).
# Keep them in the matrix (last); omit them from the overview throughput chart.
_CHART_EXCLUDED_SCENARIOS = frozenset(
    {
        "session_resume_qos1",
        "reconnect_ordering",
        "duplex_gateway",
        "e2e_integrity",
        "sub_callback_matching",
        "remaining_length_boundaries",
        "broker_ceiling_ingress",
        "client_ceiling_ingress",
        # Fraction-of-own-capacity latency is an intra-client question, not a
        # cross-client ranking. The public latency comparison is
        # puback_latency_fixed_rate (equal absolute offered rates).
        "puback_latency_qos1",
        "application_rtt_qos1",
    }
)
_CHART_EXCLUDED_ORDER = (
    "session_resume_qos1",
    "reconnect_ordering",
    "duplex_gateway",
    "e2e_integrity",
    "sub_callback_matching",
    "remaining_length_boundaries",
    "broker_ceiling_ingress",
    "client_ceiling_ingress",
    "puback_latency_qos1",
    "application_rtt_qos1",
)

# Latency scenarios whose primary question is intra-client (near own ceiling).
# Shown in the matrix with an explicit note; never crowned as a cross-client win.
_FRACTION_LATENCY_SCENARIOS = frozenset(
    {
        "puback_latency_qos1",
        "application_rtt_qos1",
    }
)

# Failures that reflect how the SUT behaved under the offered load (or its
# protocol/API limits). These must stay visible in the report — excluding them
# from the throughput chart is fine; burying them is not.
_CLIENT_LOAD_REASON_PREFIXES = (
    "open_loop_rate_out_of_tolerance",
    "open_loop_backpressure_misses",
    "protocol_failed",
    "timed_out_mids",
    "rtt_timeouts",
    "warmup_drain_timeout",
    "no_delivery_despite_load",
    "integrity_mismatch",
    "worker_error:",
)
_CLIENT_CAPABILITY_PREFIX = "not_implemented:"
_ENVIRONMENT_REASON_PREFIXES = (
    "container_cpu_high:",
    "broker_telemetry_missing",
    "loadgen_emitted_nothing",
    "loadgen_below_half_nominal",
    "barrier_failed",
    "sys_publish_dropped",
    "delivery_below_half_offer",
)

# Values within this relative tolerance of the row/series maximum are treated
# as tied. Medians carry float noise (e.g. a 1000 msg/s rate cap surfaces as
# 999.9944...), so a strict `==` comparison against the max silently picks a
# single "winner" among values that are displayed identically.
_TIE_RELATIVE_TOLERANCE = 1e-3


def _is_tied_with_best(value: float, best: float) -> bool:
    if value == best:
        return True
    scale = max(abs(best), 1e-9)
    return abs(value - best) / scale <= _TIE_RELATIVE_TOLERANCE


def _reason_kind(reason: str) -> str:
    if reason.startswith(_CLIENT_CAPABILITY_PREFIX):
        return "capability"
    if any(reason.startswith(p) for p in _CLIENT_LOAD_REASON_PREFIXES):
        return "load"
    if any(reason.startswith(p) for p in _ENVIRONMENT_REASON_PREFIXES):
        return "environment"
    return "other"


def _short_reason(reason: str) -> str:
    if reason.startswith(_CLIENT_CAPABILITY_PREFIX):
        return reason[len(_CLIENT_CAPABILITY_PREFIX) :]
    if reason.startswith("container_cpu_high:"):
        return "broker_cpu"
    if reason.startswith("sys_publish_dropped"):
        return "broker_drops"
    if reason.startswith("delivery_below_half_offer"):
        return "delivery_lt_half_offer"
    if reason.startswith("worker_error:"):
        return "worker_error"
    if reason.startswith("barrier_failed"):
        return "barrier"
    return reason


def _order_matrix_scenarios(scenarios: Sequence[str]) -> List[str]:
    """Throughput scenarios first; rate-capped / niche rows last (stable order)."""
    primary = [s for s in scenarios if _scenario_base(s) not in _CHART_EXCLUDED_SCENARIOS]
    trailing = [s for s in scenarios if _scenario_base(s) in _CHART_EXCLUDED_SCENARIOS]
    rank = {name: i for i, name in enumerate(_CHART_EXCLUDED_ORDER)}
    trailing.sort(key=lambda s: (rank.get(_scenario_base(s), 99), s))
    return primary + trailing


def _matrix_row_id(scenario: str, protocol: Optional[str] = None) -> str:
    proto = protocol or "MQTTv311"
    return f"{scenario} · {proto}"


def _scenario_base(row_id: str) -> str:
    if " · " in row_id:
        return row_id.rsplit(" · ", 1)[0]
    return row_id


def _protocol_from_row_id(row_id: str) -> str:
    if " · " in row_id:
        return row_id.rsplit(" · ", 1)[1]
    return "MQTTv311"

# One stable colour per known client, shared by the matrix swatches, the
# results table, and the overview chart so the same client always reads the
# same colour anywhere on the site.
_CLIENT_COLORS = {
    "paho": "#0f6e56",
    "mqttium": "#1a9b74",
    "mqttium-compat": "#5a8f7a",
    "gmqtt": "#245b7a",
    "aiomqtt": "#9a5b12",
    "amqtt": "#6b4f7a",
    "awscrt": "#8b3a3a",
    "zmqtt": "#3f6b4d",
    "aiomqtt3": "#4a5a78",
}
_FALLBACK_PALETTE = ["#5c6b64", "#7a6a4f", "#4f6b7a", "#7a4f5c", "#6b5c7a", "#7a7a4f"]

# Peer-group order for display. io_model itself is read from client_identity in
# the result JSON (see ClientMeta), never from a table in this file.
_IO_MODEL_ORDER = ("sync", "asyncio_bridged", "crt_event_loop")
_STABILITY_ORDER = ("stable", "experimental")


@dataclass
class ClientMeta:
    """Per-client facts, taken from the results rather than hard-coded here."""

    name: str
    io_model: str = "unknown"
    stability: str = "unknown"
    version: Optional[str] = None
    private_api: Dict[str, str] = field(default_factory=dict)

    @property
    def peer_group(self) -> tuple:
        return (self.stability, self.io_model)


def _registry_meta(name: str) -> tuple:
    """Fall back to the adapter registry for results predating client_identity.

    Still not a table in this file: the registry is the same source the harness
    stamps into new results, so the two can never disagree.
    """
    try:
        from mqtt_client_bench.adapters.registry import get_adapter_class

        caps = get_adapter_class(name).capabilities()
        return caps.io_model, caps.stability
    except Exception:  # noqa: BLE001 - unknown client, or adapter deps missing
        return None, None


def _client_has_native_async_path(client: Optional[str]) -> bool:
    if not client:
        return False
    try:
        from mqtt_client_bench.adapters.registry import has_async_adapter

        return bool(has_async_adapter(client))
    except Exception:  # noqa: BLE001
        return client in {
            "gmqtt",
            "aiomqtt",
            "amqtt",
            "zmqtt",
            "aiomqtt3",
            "mqttium",
        }


def _run_rankable(run: Dict[str, Any], *, client: Optional[str] = None) -> bool:
    """True when a run may enter a cross-client median or latency aggregate.

    Native and facade publish paths measure different harness tax; a
    sync_facade result for a client that has a native path is diagnostic even
    when an older JSON omitted non_comparable.
    """
    if run.get("status") != "valid" or run.get("non_comparable"):
        return False
    if run.get("publish_path") == "sync_facade" and _client_has_native_async_path(client):
        return False
    return True


def _client_meta(docs: Sequence["ResultDoc"]) -> Dict[str, ClientMeta]:
    """Collect client metadata from the documents' client_identity blocks."""
    meta: Dict[str, ClientMeta] = {}
    for doc in docs:
        if doc.kind != "scenario" or not doc.client:
            continue
        entry = meta.setdefault(doc.client, ClientMeta(name=doc.client))
        if doc.io_model:
            entry.io_model = doc.io_model
        if doc.stability:
            entry.stability = doc.stability
        if doc.client_version:
            entry.version = doc.client_version
        if doc.private_api:
            entry.private_api.update(doc.private_api)
    for name, entry in meta.items():
        if entry.io_model != "unknown" and entry.stability != "unknown":
            continue
        io_model, stability = _registry_meta(name)
        if entry.io_model == "unknown" and io_model:
            entry.io_model = io_model
        if entry.stability == "unknown" and stability:
            entry.stability = stability
    return meta


def _sort_clients(clients: Sequence[str], meta: Optional[Dict[str, ClientMeta]] = None) -> List[str]:
    """Order columns by peer group first, then by the display order, then name."""
    rank = {name: i for i, name in enumerate(_CLIENT_ORDER)}

    def key(client: str):
        info = (meta or {}).get(client)
        stability = info.stability if info else "unknown"
        io_model = info.io_model if info else "unknown"
        return (
            _STABILITY_ORDER.index(stability) if stability in _STABILITY_ORDER else len(_STABILITY_ORDER),
            _IO_MODEL_ORDER.index(io_model) if io_model in _IO_MODEL_ORDER else len(_IO_MODEL_ORDER),
            rank.get(client, len(_CLIENT_ORDER)),
            client,
        )

    return sorted(clients, key=key)


def _client_colors(clients: Sequence[str]) -> Dict[str, str]:
    colors: Dict[str, str] = {}
    fallback_idx = 0
    for client in clients:
        if client in _CLIENT_COLORS:
            colors[client] = _CLIENT_COLORS[client]
        else:
            colors[client] = _FALLBACK_PALETTE[fallback_idx % len(_FALLBACK_PALETTE)]
            fallback_idx += 1
    return colors


def _client_swatch(
    name: str,
    colors: Dict[str, str],
    meta: Optional[Dict[str, ClientMeta]] = None,
) -> str:
    color = colors.get(name, "#5c6b64")
    info = (meta or {}).get(name)
    badges = ""
    if info and info.io_model != "unknown":
        badges += f' <span class="io-badge" title="I/O model peer group">{_esc(info.io_model)}</span>'
    if info and info.stability == "experimental":
        badges += ' <span class="io-badge badge-exp" title="Experimental — ranked separately">exp</span>'
    return f'<span class="swatch" style="background:{_esc(color)}"></span>{_esc(name)}{badges}'


def _svg_grouped_bars(
    categories: Sequence[str],
    series: Sequence[Dict[str, Any]],
    *,
    height: int = 340,
    bar_slot: int = 13,
    group_gap: int = 18,
    label_room: int = 150,
) -> str:
    """Render a grouped bar chart as inline SVG with min/max whiskers.

    Deliberately server-rendered: the page used to pull Chart.js and Google
    Fonts from CDNs, so the report needed the network to display at all and
    leaked a request per reader. Everything here ships in the HTML.
    """
    if not categories or not series:
        return ""
    n_series = len(series)
    group_w = n_series * bar_slot + group_gap
    plot_w = max(320, group_w * len(categories))
    pad_l, pad_r, pad_t = 64, 16, 16
    plot_h = height - label_room
    width = pad_l + plot_w + pad_r

    values = [
        v
        for s in series
        for v in list(s.get("values") or []) + list(s.get("high") or [])
        if v is not None
    ]
    top = max(values) if values else 0.0
    if top <= 0:
        return ""
    # Round the axis up to a friendly step so gridlines read cleanly.
    magnitude = 10 ** max(0, len(str(int(top))) - 2)
    top = math.ceil(top / magnitude) * magnitude

    def y_of(value: float) -> float:
        return pad_t + plot_h - (value / top) * plot_h

    parts = [
        f'<svg class="chart-svg" viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="Median throughput by scenario and client" '
        f'preserveAspectRatio="xMinYMin meet">'
    ]
    for i in range(5):
        value = top * i / 4
        y = y_of(value)
        parts.append(
            f'<line class="grid" x1="{pad_l}" y1="{y:.1f}" x2="{pad_l + plot_w}" y2="{y:.1f}" />'
            f'<text class="axis" x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end">{_fmt_num(value, digits=0)}</text>'
        )
    for c_idx, category in enumerate(categories):
        gx = pad_l + c_idx * group_w + group_gap / 2
        for s_idx, serie in enumerate(series):
            values_list = serie.get("values") or []
            value = values_list[c_idx] if c_idx < len(values_list) else None
            if value is None:
                continue
            x = gx + s_idx * bar_slot
            y = y_of(float(value))
            h = pad_t + plot_h - y
            color = serie.get("color", "#5c6b64")
            title = f"{serie.get('client', '')} · {category}: {_fmt_num(value)} msg/s"
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_slot - 3}" height="{max(h, 0.5):.1f}" '
                f'fill="{_esc(color)}"><title>{_esc(title)}</title></rect>'
            )
            lows = serie.get("low") or []
            highs = serie.get("high") or []
            low = lows[c_idx] if c_idx < len(lows) else None
            high = highs[c_idx] if c_idx < len(highs) else None
            if low is not None and high is not None and high > low:
                cx = x + (bar_slot - 3) / 2
                y_low, y_high = y_of(float(low)), y_of(float(high))
                parts.append(
                    f'<line class="whisker" x1="{cx:.1f}" y1="{y_low:.1f}" x2="{cx:.1f}" y2="{y_high:.1f}" />'
                    f'<line class="whisker" x1="{cx - 3:.1f}" y1="{y_high:.1f}" x2="{cx + 3:.1f}" y2="{y_high:.1f}" />'
                    f'<line class="whisker" x1="{cx - 3:.1f}" y1="{y_low:.1f}" x2="{cx + 3:.1f}" y2="{y_low:.1f}" />'
                )
        tx = gx + (n_series * bar_slot) / 2
        ty = pad_t + plot_h + 12
        parts.append(
            f'<text class="cat" x="{tx:.1f}" y="{ty:.1f}" transform="rotate(-40 {tx:.1f} {ty:.1f})" '
            f'text-anchor="end">{_esc(category)}</text>'
        )
    parts.append(
        f'<line class="axis-line" x1="{pad_l}" y1="{pad_t + plot_h}" '
        f'x2="{pad_l + plot_w}" y2="{pad_t + plot_h}" />'
    )
    parts.append("</svg>")
    legend = " ".join(
        f'<span class="legend-item"><span class="swatch" style="background:{_esc(s.get("color", "#5c6b64"))}"></span>'
        f'{_esc(s.get("client", ""))}</span>'
        for s in series
    )
    return f'<div class="chart-scroll">{"".join(parts)}</div><p class="legend">{legend}</p>'


def _svg_single_bars(labels: Sequence[str], values: Sequence[Optional[float]]) -> str:
    """Single-series bar chart (detail pages), inline SVG."""
    series = [{"client": "median msg/s", "color": "#0f6e56", "values": list(values)}]
    return _svg_grouped_bars(labels, series, height=300, bar_slot=26, group_gap=14, label_room=130)


def _overview_charts_html(
    scenarios: Sequence[str],
    series: Sequence[Dict[str, Any]],
    meta: Optional[Dict[str, "ClientMeta"]] = None,
) -> str:
    """One chart per peer group instead of one chart for everything.

    All nine clients across every scenario row came to 91 bars in a 1970 px
    viewBox: unreadable at any realistic width, and it invited precisely the
    cross-group comparison the matrix refuses to make. Splitting on the peer
    group fixes both at once — each chart holds only clients that are actually
    substitutes. A scenario with no data in a group is dropped from that group's
    chart rather than rendered as a gap.
    """
    meta = meta or {}
    grouped: Dict[tuple, List[Dict[str, Any]]] = {}
    for serie in series:
        info = meta.get(serie.get("client", ""))
        group = info.peer_group if info else ("unknown", "unknown")
        grouped.setdefault(group, []).append(serie)

    sections: List[str] = []
    for group, members in grouped.items():
        keep = [
            i
            for i, _ in enumerate(scenarios)
            if any((m.get("values") or [None] * len(scenarios))[i] is not None for m in members)
        ]
        if not keep:
            continue
        labels = [scenarios[i] for i in keep]

        def _pick(serie: Dict[str, Any], key: str) -> List[Any]:
            source = serie.get(key) or []
            return [source[i] if i < len(source) else None for i in keep]

        trimmed = [
            {
                "client": m.get("client"),
                "color": m.get("color"),
                "values": _pick(m, "values"),
                "low": _pick(m, "low"),
                "high": _pick(m, "high"),
            }
            for m in members
        ]
        svg = _svg_grouped_bars(labels, trimmed, bar_slot=18, group_gap=22)
        if not svg:
            continue
        sections.append(
            f"""
      <section class="panel">
        <div class="panel-head">
          <h2>Throughput — {_esc(group[1])} <span class="group-sub">{_esc(group[0])}</span></h2>
          <p class="hint">Only clients that are substitutes for one another share a chart: this one holds the
          <strong>{_esc(group[0])}</strong> clients whose I/O model is <code>{_esc(group[1])}</code>. Bars are grouped
          by scenario · MQTT protocol and are comparable only within the same protocol; whiskers show the observed
          run-to-run min/max. Comparing a bar here with a bar in another chart is not meaningful.</p>
        </div>
        <div class="chart-wrap chart-wrap-wide">
          {svg}
        </div>
      </section>"""
        )
    return "\n".join(sections)


def _performance_matrix_html(
    scenarios: Sequence[str],
    clients: Sequence[str],
    by_key: Dict[tuple, Optional[float]],
    colors: Dict[str, str],
    meta: Optional[Dict[str, ClientMeta]] = None,
    cells_by: Optional[Dict[tuple, PointRow]] = None,
) -> str:
    """Compact scenario × client table for immediate reading on the index page.

    The "best" value is computed **per peer group**, not across the whole row.
    Highlighting the global maximum contradicted the page's own warning that a
    bridged client and a sync client are not comparable, and it silently crowned
    the native CRT client over every pure-Python one.
    """
    if not scenarios or not clients:
        return ""
    meta = meta or {}
    cells_by = cells_by or {}
    ordered = _sort_clients(clients, meta)
    groups: List[tuple] = []
    for client in ordered:
        info = meta.get(client)
        group = info.peer_group if info else ("unknown", "unknown")
        if not groups or groups[-1][0] != group:
            groups.append((group, [client]))
        else:
            groups[-1][1].append(client)

    group_head = "".join(
        f'<th scope="col" class="group-head group-start" colspan="{len(members)}">'
        f'{_esc(group[1])}<span class="group-sub">{_esc(group[0])}</span></th>'
        for group, members in groups
    )
    # The group boundary has to be visible on every row, not just in the header:
    # reading a row, the eye has no way to tell where one peer group ends, which
    # is what made an unhighlighted 19,536 next to a highlighted 14,358 look
    # arbitrary rather than "different groups".
    group_first = {members[0] for _group, members in groups}
    head = "".join(
        f'<th scope="col" class="num{" group-start" if c in group_first else ""}">'
        f"{_client_swatch(c, colors, meta)}</th>"
        for c in ordered
    )

    body_rows: List[str] = []
    for scenario in scenarios:
        tds: List[str] = []
        for group, members in groups:
            values = [by_key.get((scenario, c)) for c in members]
            numeric = [v for v in values if v is not None]
            best = max(numeric) if numeric else None
            tied = sum(1 for v in numeric if best is not None and _is_tied_with_best(v, best))
            # A tie across every populated cell isn't a "winner" — it means the
            # scenario is rate-capped, not that one client outperformed the rest.
            all_tied = bool(numeric) and tied == len(numeric)
            for client, value in zip(members, values):
                edge = " group-start" if client == members[0] else ""
                if value is None:
                    row = cells_by.get((scenario, client))
                    kind = (row.empty_reason if row else None) or "missing"
                    glyph, title = EMPTY_GLYPHS.get(kind, EMPTY_GLYPHS["missing"])
                    detail = (row.reason_detail if row else "") or ""
                    tip = f"{title}{': ' + detail if detail else ''}"
                    tds.append(
                        f'<td class="num empty empty-{kind}{edge}" title="{_esc(tip)}">{glyph}</td>'
                    )
                    continue
                row = cells_by.get((scenario, client))
                classes = ["num"]
                if edge:
                    classes.append("group-start")
                solo = len(numeric) == 1
                if solo:
                    # Not a winner and not a loser: there is nobody in this peer
                    # group to rank it against. Saying so beats leaving the
                    # reader to wonder why a large number is not highlighted.
                    classes.append("solo")
                elif not all_tied and best is not None and _is_tied_with_best(value, best):
                    classes.append("best")
                tip_bits = []
                if row:
                    if row.bottleneck:
                        tip_bits.append(f"bottleneck: {row.bottleneck}")
                    if row.relative_spread_pct is not None:
                        tip_bits.append(f"spread: ±{row.relative_spread_pct / 2:.1f}%")
                    if row.valid_runs:
                        tip_bits.append(f"n={row.valid_runs}/{row.total_runs}")
                    if row.broker_cpu_max_pct is not None:
                        tip_bits.append(f"broker CPU {row.broker_cpu_max_pct:.0f}%")
                    if row.bottleneck and row.bottleneck != "sut_limited":
                        classes.append("suspect")
                if solo:
                    tip_bits.insert(0, f"alone in the {group[1]}/{group[0]} group — not ranked")
                tip = " · ".join(tip_bits)
                tds.append(
                    f'<td class="{" ".join(classes)}" title="{_esc(tip)}">{_esc(_fmt_num(value))}</td>'
                )
        body_rows.append(
            f'<tr><th scope="row" class="scenario">{_esc(scenario)}</th>{"".join(tds)}</tr>'
        )

    legend = " ".join(
        [
            '<span class="legend-item"><span class="legend-glyph legend-best"></span>'
            "best in its peer group</span>",
            '<span class="legend-item"><span class="legend-glyph legend-solo"></span>'
            "alone in its group — not ranked</span>",
        ]
        + [
            f'<span class="legend-item"><span class="legend-glyph empty-{kind}">{glyph}</span>{_esc(title)}</span>'
            for kind, (glyph, title) in EMPTY_GLYPHS.items()
        ]
    )
    return f"""
      <section class="panel">
        <div class="panel-head">
          <h2>Performance matrix</h2>
          <p class="hint">Median msg/s per scenario × MQTT protocol × client, comparable runs only. Rows are never mixed across protocols, and the best value is highlighted <strong>within each peer group</strong> — the vertical rules mark those groups, formed by I/O model and stability. So the highest number in a row is often <em>not</em> highlighted: it belongs to another group. A client alone in its group is shown in outline and never crowned, because there is nothing to rank it against. A dotted underline marks a number the harness did not attribute to the client itself; hover any cell for its bottleneck, run count and spread.</p>
          <p class="hint"><strong>Latency rows paced at a fraction of each client's own capacity</strong> (<code>puback_latency_qos1</code>, <code>application_rtt_qos1</code>) answer an intra-client question and are not a cross-client ranking — a faster client is offered a higher absolute rate. For equal-offer latency compare <code>puback_latency_fixed_rate</code>.</p>
          <p class="legend">{legend}</p>
        </div>
        <div class="table-wrap table-wrap-sticky-col">
          <table class="matrix">
            <thead>
              <tr>
                <th scope="col" class="scenario-head" rowspan="2">Scenario</th>
                {group_head}
              </tr>
              <tr>
                {head}
              </tr>
            </thead>
            <tbody>
              {"".join(body_rows)}
            </tbody>
          </table>
        </div>
      </section>
"""


def _client_signals_html(docs: Sequence[ResultDoc], colors: Dict[str, str]) -> str:
    """Single dedicated table for SUT-attributable failures and capability gaps."""
    rows: List[tuple] = []
    for doc in docs:
        if doc.kind != "scenario":
            continue
        client = doc.client or "?"
        scenario = doc.scenario or doc.title
        if doc.load_reasons:
            detail = ", ".join(f"{name}×{count}" for name, count in sorted(doc.load_reasons.items()))
            rows.append(("load", client, scenario, detail, doc.inconclusive_runs, doc.total_runs, doc.slug))
        if doc.capability_reasons:
            detail = ", ".join(sorted(doc.capability_reasons))
            rows.append(("capability", client, scenario, detail, doc.inconclusive_runs, doc.total_runs, doc.slug))
    if not rows:
        return ""

    kind_rank = {"load": 0, "capability": 1}
    client_rank = {name: i for i, name in enumerate(_CLIENT_ORDER)}
    rows.sort(
        key=lambda r: (
            kind_rank.get(r[0], 9),
            client_rank.get(r[1], len(_CLIENT_ORDER)),
            r[1],
            r[2],
        )
    )

    body = []
    for kind, client, scenario, detail, failed, total, slug in rows:
        swatch = _client_swatch(client, colors) if client in colors else _esc(client)
        kind_label = "under load" if kind == "load" else "capability"
        body.append(
            f"<tr>"
            f"<td><span class=\"badge badge-{'partial' if kind == 'load' else 'inconclusive'}\">{_esc(kind_label)}</span></td>"
            f"<td>{swatch}</td>"
            f"<td class=\"mono\">{_esc(scenario)}</td>"
            f"<td class=\"mono\">{_esc(detail)}</td>"
            f"<td class=\"num\">{_esc(failed)}/{_esc(total)}</td>"
            f"<td><a href=\"runs/{_esc(slug)}.html\">detail</a></td>"
            f"</tr>"
        )
    return f"""
      <section class="panel panel-signal">
        <div class="panel-head">
          <h2>Client issues</h2>
          <p class="hint">SUT-attributable problems kept out of the throughput median on purpose: missed open-loop targets / protocol failures under load, and points refused for missing adapter capabilities. Broker drops / CPU saturation invalidate runs (see Environment warnings above) and must not poison medians.</p>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Kind</th>
                <th>Client</th>
                <th>Scenario</th>
                <th>Signal</th>
                <th class="num">Failed runs</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {"".join(body)}
            </tbody>
          </table>
        </div>
      </section>
"""


def _environment_warnings_html(docs: Sequence[ResultDoc]) -> str:
    """Large, explicit banner when broker/loadgen invalidated ranking runs."""
    rows: List[tuple] = []
    drop_docs = 0
    cpu_docs = 0
    for doc in docs:
        if doc.kind != "scenario":
            continue
        env = doc.environment_reasons or {}
        if not env:
            continue
        if "broker_drops" in env:
            drop_docs += 1
        if "broker_cpu" in env:
            cpu_docs += 1
        detail = ", ".join(f"{name}×{count}" for name, count in sorted(env.items()))
        rows.append(
            (
                doc.client or "?",
                doc.scenario or doc.title,
                detail,
                doc.inconclusive_runs,
                doc.total_runs,
                doc.slug,
            )
        )
    if not rows:
        return ""

    body = []
    for client, scenario, detail, failed, total, slug in sorted(rows, key=lambda r: (r[0], r[1])):
        body.append(
            f"<tr>"
            f"<td class=\"mono\">{_esc(client)}</td>"
            f"<td class=\"mono\">{_esc(scenario)}</td>"
            f"<td class=\"mono\">{_esc(detail)}</td>"
            f"<td class=\"num\">{_esc(failed)}/{_esc(total)}</td>"
            f"<td><a href=\"runs/{_esc(slug)}.html\">detail</a></td>"
            f"</tr>"
        )
    summary_bits = []
    if drop_docs:
        summary_bits.append(f"{drop_docs} result file(s) with broker $SYS publish drops")
    if cpu_docs:
        summary_bits.append(f"{cpu_docs} with Mosquitto CPU ≥85%")
    summary = "; ".join(summary_bits) if summary_bits else f"{len(rows)} environment-limited result file(s)"

    return f"""
      <section class="panel panel-warning" role="alert">
        <div class="panel-head">
          <h2>WARNING — Environment invalidations — do not trust affected medians</h2>
          <p class="hint warning-lead">
            { _esc(summary) }. Runs with material Mosquitto <code>$SYS</code> publish drops
            or broker CPU saturation are <strong>inconclusive</strong> (fail-closed):
            their throughput must not enter ranking medians. Re-run those scenarios on an idle host.
          </p>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Client</th>
                <th>Scenario</th>
                <th>Signal</th>
                <th class="num">Failed runs</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {"".join(body)}
            </tbody>
          </table>
        </div>
      </section>
"""


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


# Axes that a scenario can sweep. A label built from a fixed subset collapsed
# distinct points into identical rows (pub_qos1_inflight showed three identical
# lines for inflight 1/20/100), which silently hid the very thing being swept.
_LABEL_AXES = (
    ("payload", "payload"),
    ("qos_publish", "qos"),
    ("qos_subscribe", "sub_qos"),
    ("protocol", "proto"),
    ("topology", "topo"),
    ("inflight", "inflight"),
    ("load_fraction", "load"),
    ("subscription", "sub"),
    ("topic_topology", "topics"),
    ("callback_filters", "filters"),
    ("subscription_count", "subs"),
    ("subscribers", "n_sub"),
    ("loadgen_clients", "gen"),
    ("cadence", "cadence"),
    ("properties_profile", "props"),
    ("connect_mode", "connect"),
    ("fleet_size", "fleet"),
    ("network", "net"),
    ("tls", "tls"),
)


def _point_label(point: Dict[str, Any], varying: Optional[Sequence[str]] = None) -> str:
    """Label a measurement point.

    ``varying`` restricts the label to the axes that actually differ inside the
    scenario, keeping labels short while guaranteeing they stay distinct.
    """
    parts = []
    for key, alias in _LABEL_AXES:
        if varying is not None and key not in varying:
            continue
        value = point.get(key)
        if value is None:
            continue
        if key == "tls" and not value:
            continue
        if key == "network" and value == "localhost":
            continue
        parts.append(f"{alias}={value}")
    return ", ".join(parts) if parts else "default"


def _varying_axes(points: Sequence[Dict[str, Any]]) -> List[str]:
    """Axes whose value is not constant across a scenario's points."""
    if len(points) <= 1:
        return [k for k, _ in _LABEL_AXES if points and points[0].get(k) is not None][:5]
    varying = []
    for key, _ in _LABEL_AXES:
        seen = {repr(p.get(key)) for p in points}
        if len(seen) > 1:
            varying.append(key)
    if not varying:
        # Genuinely identical points (repeat runs): fall back to a stable subset.
        varying = ["payload", "qos_publish", "protocol", "topology"]
    return varying


def _dominant(runs: Sequence[Dict[str, Any]], key: str) -> Optional[str]:
    """Most common non-empty value of ``key`` across a point's runs."""
    counts: Dict[str, int] = {}
    for run in runs:
        value = run.get(key)
        if value:
            counts[str(value)] = counts.get(str(value), 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _max_of(runs: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    values = [float(r[key]) for r in runs if r.get(key) is not None]
    return max(values) if values else None


def _ratio_of(runs: Sequence[Dict[str, Any]]) -> Optional[float]:
    values = [
        float((r.get("broker_reconciliation") or {}).get("ratio"))
        for r in runs
        if (r.get("broker_reconciliation") or {}).get("ratio") is not None
    ]
    return (sum(values) / len(values)) if values else None


def _cost_of(runs: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    values = [
        float((r.get("cost_per_message") or {}).get(key))
        for r in runs
        if (r.get("cost_per_message") or {}).get(key) is not None
    ]
    return (sum(values) / len(values)) if values else None


# What an empty cell means. Collapsing four different outcomes into one em-dash
# made "the client cannot do this" indistinguishable from "we never ran it".
EMPTY_GLYPHS = {
    "refused": ("⊘", "refused — client lacks the capability"),
    "failed": ("✕", "failed under load"),
    "environment": ("!", "invalidated by the environment (broker or host)"),
    "missing": ("–", "not run"),
}


def _empty_cell_reason(
    runs: Sequence[Dict[str, Any]], median: Optional[float]
) -> tuple:
    """Classify why a point has no comparable number."""
    if median is not None:
        return None, ""
    if not runs:
        return "missing", ""
    reasons = [str(r) for run in runs for r in (run.get("reasons") or [])]
    if not reasons:
        return "missing", ""
    kinds = {_reason_kind(r) for r in reasons}
    shorts = sorted({_short_reason(r) for r in reasons})
    detail = ", ".join(shorts[:3])
    if "capability" in kinds:
        return "refused", detail
    if "environment" in kinds:
        return "environment", detail
    if "load" in kinds:
        return "failed", detail
    return "missing", detail


def _collect_latency(runs: Sequence[Dict[str, Any]], *, client: Optional[str] = None) -> Dict[str, Optional[float]]:
    p50: List[float] = []
    p95: List[float] = []
    p99: List[float] = []
    p99_gated = False
    for run in runs:
        if not _run_rankable(run, client=client):
            continue
        for worker in run.get("workers") or []:
            summary = worker.get("latency_summary") or {}
            if summary.get("p50_ms") is not None:
                p50.append(float(summary["p50_ms"]))
            if summary.get("p95_ms") is not None:
                p95.append(float(summary["p95_ms"]))
            if summary.get("p99_ms") is not None and summary.get("p99_published"):
                p99.append(float(summary["p99_ms"]))
                p99_gated = True
    def median(values: List[float]) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        return ordered[len(ordered) // 2]

    return {
        "p50_ms": median(p50),
        "p95_ms": median(p95),
        "p99_ms": median(p99),
        "p99_gated": p99_gated and bool(p99),
    }


def _collect_integrity(runs: Sequence[Dict[str, Any]], *, client: Optional[str] = None) -> Optional[Dict[str, Any]]:
    totals = {
        "expected": 0,
        "received": 0,
        "unique": 0,
        "missing": 0,
        "duplicates": 0,
        "out_of_order": 0,
        "unexpected": 0,
    }
    worst_missing = 0
    found = False
    for run in runs:
        # Only valid, comparable runs enter integrity medians — the same rule
        # as throughput. Including inconclusive runs used to paper over
        # mismatches that validate_run now refuses.
        if not _run_rankable(run, client=client):
            continue
        for worker in run.get("workers") or []:
            integ = worker.get("integrity")
            if not integ:
                continue
            found = True
            for key in totals:
                totals[key] += int(integ.get(key) or 0)
            worst_missing = max(worst_missing, int(integ.get("missing") or 0))
    if not found:
        return None
    totals["worst_missing"] = worst_missing
    return totals


def _run_status_counts(runs: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    valid = sum(1 for r in runs if r.get("status") == "valid")
    return {"valid": valid, "total": len(runs), "inconclusive": len(runs) - valid}


@dataclass
class PointRow:
    label: str
    median_msgs_per_s: Optional[float]
    status: str
    valid_runs: int
    total_runs: int
    non_comparable: bool
    latency: Dict[str, Optional[float]] = field(default_factory=dict)
    integrity: Optional[Dict[str, Any]] = None
    spread_low: Optional[float] = None
    spread_high: Optional[float] = None
    protocol: Optional[str] = None
    # Attribution and confidence signals. All of these were already produced by
    # the harness and dropped on the floor by the report; without them a reader
    # cannot tell a client-limited number from a broker-limited one, nor a
    # repeatable median from a noisy one.
    bottleneck: Optional[str] = None
    mad: Optional[float] = None
    relative_spread_pct: Optional[float] = None
    broker_cpu_max_pct: Optional[float] = None
    broker_reconcile_ratio: Optional[float] = None
    delivery_offer_ratio: Optional[float] = None
    effective_offer: Optional[float] = None
    cost_us_per_message: Optional[float] = None
    rss_peak_kb: Optional[float] = None
    # Why the cell is empty, when it is: refused | failed | environment | missing
    empty_reason: Optional[str] = None
    reason_detail: str = ""


@dataclass
class ResultDoc:
    source_name: str
    slug: str
    kind: str
    title: str
    client: Optional[str]
    scenario: Optional[str]
    profile: Optional[str]
    non_comparable: bool
    status: str
    median_msgs_per_s: Optional[float]
    points: List[PointRow]
    environment: Dict[str, Any]
    broker: Dict[str, Any]
    verdict: Optional[Dict[str, Any]]
    raw_meta: Dict[str, Any]
    spread_low: Optional[float] = None
    spread_high: Optional[float] = None
    # Aggregated inconclusive-run signals for the index page. Keys are reason
    # strings; values are run counts. Split by attribution so load/capability
    # failures stay visible even when excluded from the throughput chart.
    load_reasons: Dict[str, int] = field(default_factory=dict)
    capability_reasons: Dict[str, int] = field(default_factory=dict)
    environment_reasons: Dict[str, int] = field(default_factory=dict)
    inconclusive_runs: int = 0
    total_runs: int = 0
    # Taken from client_identity in the JSON rather than a hard-coded table, so a
    # newly added client is grouped and labelled correctly with no report edit.
    io_model: Optional[str] = None
    stability: Optional[str] = None
    client_version: Optional[str] = None
    private_api: Dict[str, str] = field(default_factory=dict)
    interleaved_with: List[str] = field(default_factory=list)


def classify_payload(data: Dict[str, Any], source_name: str) -> ResultDoc:
    slug = _slug(Path(source_name).stem)
    if "scenarios" in data and "suite" in data:
        nested = data.get("scenarios") or []
        scenario_names = [s.get("scenario", "?") for s in nested]
        clients = sorted({s.get("client") for s in nested if s.get("client")})
        scenario_entries = []
        for s in nested:
            medians = []
            for block in s.get("results") or []:
                median = (block.get("summary") or {}).get("median")
                if median is not None:
                    medians.append(float(median))
            scenario_entries.append(
                {
                    "scenario": s.get("scenario"),
                    "client": s.get("client"),
                    "profile": s.get("profile"),
                    "median_msgs_per_s": (sorted(medians)[len(medians) // 2] if medians else None),
                    "source_hint": f"{s.get('client')}-{s.get('scenario')}",
                }
            )
        return ResultDoc(
            source_name=source_name,
            slug=slug,
            kind="suite",
            title=f"Suite {data.get('suite', '?')}",
            client=", ".join(clients) if clients else None,
            scenario=", ".join(scenario_names[:6]) + ("…" if len(scenario_names) > 6 else ""),
            profile=None,
            non_comparable=False,
            status="suite",
            median_msgs_per_s=None,
            points=[],
            environment={},
            broker={},
            verdict=None,
            raw_meta={
                "suite": data.get("suite"),
                "estimate": data.get("estimate"),
                "scenario_count": len(scenario_names),
                "scenario_names": scenario_names,
                "scenario_entries": scenario_entries,
            },
        )

    if data.get("verdict") is not None and data.get("order") is not None:
        verdict = data.get("verdict") or {}
        return ResultDoc(
            source_name=source_name,
            slug=slug,
            kind="compare",
            title=f"Compare {data.get('baseline_client', '?')} vs {data.get('candidate_client', '?')}",
            client=f"{data.get('baseline_client')} / {data.get('candidate_client')}",
            scenario=data.get("scenario"),
            profile=data.get("profile"),
            non_comparable=False,
            status=str(verdict.get("verdict", "inconclusive")),
            median_msgs_per_s=None,
            points=[],
            environment=data.get("environment") or {},
            broker=data.get("broker") or {},
            verdict=verdict if isinstance(verdict, dict) else {"verdict": verdict},
            raw_meta={
                "order": data.get("order"),
                "cooldown_s": data.get("cooldown_s"),
                "baseline_identity": data.get("baseline_identity"),
                "candidate_identity": data.get("candidate_identity"),
                "loadgen": data.get("loadgen"),
                "points": data.get("points"),
            },
        )

    if "capacity_msgs_per_s" in data and "fractions" in data:
        return ResultDoc(
            source_name=source_name,
            slug=slug,
            kind="calibrate",
            title=f"Calibrate {data.get('client', '?')}",
            client=data.get("client"),
            scenario="calibration",
            profile=data.get("profile"),
            non_comparable=False,
            status="calibrate",
            median_msgs_per_s=data.get("capacity_msgs_per_s"),
            points=[],
            environment=data.get("environment") or {},
            broker=data.get("broker") or {},
            verdict=None,
            raw_meta={
                "fractions": data.get("fractions"),
                "rtt_capacity_msgs_per_s": data.get("rtt_capacity_msgs_per_s"),
                "rtt_fractions": data.get("rtt_fractions"),
                "protocol_capacities": data.get("protocol_capacities"),
            },
        )

    points: List[PointRow] = []
    # (median, min, max) per comparable point; used both for the scenario's
    # headline value and for the observed run-to-run range behind it.
    median_min_max: List[tuple] = []
    any_non_comparable = False
    overall_valid = 0
    overall_total = 0
    load_reasons: Dict[str, int] = {}
    capability_reasons: Dict[str, int] = {}
    environment_reasons: Dict[str, int] = {}
    inconclusive_runs = 0
    identity = data.get("client_identity") or {}
    client_name = data.get("client") or identity.get("client")
    blocks = data.get("results") or []
    varying = _varying_axes([b.get("point") or {} for b in blocks])
    for block in blocks:
        point = block.get("point") or {}
        runs = block.get("runs") or []
        summary = block.get("summary") or {}
        counts = _run_status_counts(runs)
        overall_valid += counts["valid"]
        overall_total += counts["total"]
        for run in runs:
            if run.get("status") == "valid":
                continue
            inconclusive_runs += 1
            for reason in run.get("reasons") or []:
                kind = _reason_kind(str(reason))
                short = _short_reason(str(reason))
                if kind == "load":
                    load_reasons[short] = load_reasons.get(short, 0) + 1
                elif kind == "capability":
                    capability_reasons[short] = capability_reasons.get(short, 0) + 1
                elif kind == "environment":
                    environment_reasons[short] = environment_reasons.get(short, 0) + 1
        stale_facade = any(
            r.get("publish_path") == "sync_facade" for r in runs
        ) and _client_has_native_async_path(client_name)
        non_comparable = (
            any(bool(r.get("non_comparable")) for r in runs)
            or bool(point.get("non_comparable"))
            or stale_facade
        )
        any_non_comparable = any_non_comparable or non_comparable
        # Prefer summary computed from valid runs only; drop stale facade rates.
        rankable_rates = [
            float(r["primary_msgs_per_s"])
            for r in runs
            if _run_rankable(r, client=client_name) and r.get("primary_msgs_per_s") is not None
        ]
        if rankable_rates:
            ordered_rates = sorted(rankable_rates)
            median_rate = ordered_rates[len(ordered_rates) // 2]
            point_min = ordered_rates[0]
            point_max = ordered_rates[-1]
        else:
            median_rate = None if non_comparable else summary.get("median")
            point_min = summary.get("min")
            point_max = summary.get("max")
        if median_rate is not None and not non_comparable:
            median_min_max.append(
                (
                    float(median_rate),
                    float(point_min) if point_min is not None else float(median_rate),
                    float(point_max) if point_max is not None else float(median_rate),
                )
            )
        status = "valid" if counts["valid"] == counts["total"] and counts["total"] else (
            "partial" if counts["valid"] else "inconclusive"
        )
        mad = summary.get("mad")
        relative_spread = None
        if median_rate and point_min is not None and point_max is not None and float(median_rate) > 0:
            relative_spread = 100.0 * (float(point_max) - float(point_min)) / float(median_rate)
        empty_reason, reason_detail = _empty_cell_reason(runs, median_rate)
        if stale_facade and empty_reason == "missing":
            empty_reason, reason_detail = "failed", "sync_facade publish path (not comparable)"
        points.append(
            PointRow(
                label=_point_label(point, varying),
                median_msgs_per_s=median_rate if not non_comparable else None,
                status=status,
                valid_runs=counts["valid"],
                total_runs=counts["total"],
                non_comparable=non_comparable,
                latency=_collect_latency(runs, client=client_name),
                integrity=_collect_integrity(runs, client=client_name),
                spread_low=float(point_min) if point_min is not None and not non_comparable else None,
                spread_high=float(point_max) if point_max is not None and not non_comparable else None,
                protocol=str(point.get("protocol") or "MQTTv311"),
                bottleneck=_dominant(runs, "bottleneck"),
                mad=float(mad) if mad is not None else None,
                relative_spread_pct=relative_spread,
                broker_cpu_max_pct=_max_of(runs, "broker_cpu_max_pct"),
                broker_reconcile_ratio=_ratio_of(runs),
                delivery_offer_ratio=_max_of(runs, "delivery_offer_ratio"),
                effective_offer=_max_of(runs, "effective_offer_msgs_per_s"),
                cost_us_per_message=_cost_of(runs, "cpu_us_per_message"),
                rss_peak_kb=_cost_of(runs, "rss_peak_kb"),
                empty_reason=empty_reason,
                reason_detail=reason_detail,
            )
        )

    if overall_total == 0:
        status = "empty"
    elif overall_valid == overall_total:
        status = "valid"
    elif overall_valid == 0:
        status = "inconclusive"
    else:
        status = "partial"

    primary_median = None
    spread_low = None
    spread_high = None
    if median_min_max:
        ordered_mmm = sorted(median_min_max, key=lambda t: t[0])
        primary_median, spread_low, spread_high = ordered_mmm[len(ordered_mmm) // 2]

    return ResultDoc(
        source_name=source_name,
        slug=slug,
        kind="scenario",
        title=data.get("scenario") or source_name,
        client=data.get("client"),
        scenario=data.get("scenario"),
        profile=data.get("profile"),
        non_comparable=any_non_comparable,
        status=status,
        median_msgs_per_s=primary_median,
        points=points,
        environment=data.get("environment") or {},
        broker=data.get("broker") or {},
        verdict=None,
        raw_meta={
            "runs": data.get("runs"),
            "seed": data.get("seed"),
            "client_identity": data.get("client_identity"),
        },
        spread_low=spread_low,
        spread_high=spread_high,
        load_reasons=load_reasons,
        capability_reasons=capability_reasons,
        environment_reasons=environment_reasons,
        inconclusive_runs=inconclusive_runs,
        total_runs=overall_total,
        io_model=identity.get("io_model"),
        stability=identity.get("stability"),
        client_version=identity.get("client_version"),
        private_api=dict(identity.get("private_api") or {}),
        interleaved_with=list(data.get("interleaved_with") or []),
    )


def load_results(input_dir: Path) -> List[ResultDoc]:
    docs: List[ResultDoc] = []
    # Skip ephemeral local artefacts (gitignored ``_*.json`` / smoke probes).
    paths = sorted(
        path
        for path in input_dir.glob("*.json")
        if not path.name.startswith("_") and not path.name.endswith("-smoke.json")
    )
    for path in paths:
        data = _load_json(path)
        if data is None:
            continue
        docs.append(classify_payload(data, path.name))
    # Ensure unique slugs.
    seen: Dict[str, int] = {}
    for doc in docs:
        base = doc.slug
        count = seen.get(base, 0)
        seen[base] = count + 1
        if count:
            doc.slug = f"{base}-{count + 1}"
    return docs


def _page_shell(title: str, body: str, *, relative_root: str = ".") -> str:
    css_href = f"{relative_root}/assets/style.css".replace("/./", "/")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{_esc(title)}</title>
  <!-- No CDN: charts are server-rendered SVG and fonts are system stacks, so the
       site works offline and issues no third-party request per reader. -->
  <link rel="stylesheet" href="{_esc(css_href)}" />
</head>
<body>
  <div class="page">
    <header class="site-header">
      <a class="brand" href="{_esc(f'{relative_root}/index.html')}">MQTT Python client bench</a>
      <p class="tagline">Comparative publish/subscribe results against Mosquitto</p>
      <nav class="site-nav">
        <a href="{_esc(f'{relative_root}/index.html')}">Results</a>
        <a href="{_esc(f'{relative_root}/methodology.html')}">Methodology</a>
      </nav>
    </header>
    {body}
    <footer class="site-footer">
      <p>Generated locally from committed <code>results/*.json</code>. Raw JSON stays in the repository, not on this site.</p>
    </footer>
  </div>
</body>
</html>
"""


def _doc_point_count(doc: "ResultDoc") -> int:
    """Points a document actually measured.

    Compare docs keep ``points`` empty because their per-point payload has a
    different shape from a scenario's, so counting ``doc.points`` reported 0 for
    every A/B run.
    """
    if doc.kind == "compare":
        return len(doc.raw_meta.get("points") or [])
    return len(doc.points)


def _status_badge(status: str, non_comparable: bool = False) -> str:
    label = status
    if non_comparable:
        label = f"{status} · non-comparable"
    return f'<span class="badge badge-{_esc(status)}">{_esc(label)}</span>'


def render_index(docs: Sequence[ResultDoc], generated_at: str) -> str:
    if not docs:
        body = f"""
    <main>
      <section class="hero">
        <h1>Benchmark reports</h1>
        <p>No result files yet. Run scenarios locally with <code>--output results/&lt;name&gt;.json</code>, commit the JSON, and push to refresh this site.</p>
        <p class="meta">Generated { _esc(generated_at) }</p>
      </section>
    </main>
"""
        return _page_shell("Benchmark reports", body)

    # Grouped bar chart + matrix: one x-tick / row per scenario·protocol, one
    # series / column per client. Never merge medians across MQTT protocols.
    scenario_docs = [
        doc
        for doc in docs
        if doc.kind == "scenario" and doc.median_msgs_per_s is not None and not doc.non_comparable
    ]

    def _protocol_aggregates(doc: ResultDoc) -> Dict[str, tuple]:
        """protocol -> (median, spread_low, spread_high) from comparable points."""
        buckets: Dict[str, List[PointRow]] = {}
        for point in doc.points:
            if point.non_comparable or point.median_msgs_per_s is None:
                continue
            proto = point.protocol or "MQTTv311"
            buckets.setdefault(proto, []).append(point)
        out: Dict[str, tuple] = {}
        for proto, pts in buckets.items():
            ordered = sorted(pts, key=lambda p: float(p.median_msgs_per_s or 0.0))
            mid = ordered[len(ordered) // 2]
            lows = [p.spread_low if p.spread_low is not None else p.median_msgs_per_s for p in pts]
            highs = [p.spread_high if p.spread_high is not None else p.median_msgs_per_s for p in pts]
            out[proto] = (
                float(mid.median_msgs_per_s or 0.0),
                float(min(v for v in lows if v is not None)),
                float(max(v for v in highs if v is not None)),
            )
        if not out and doc.median_msgs_per_s is not None:
            out["MQTTv311"] = (
                float(doc.median_msgs_per_s),
                float(doc.spread_low if doc.spread_low is not None else doc.median_msgs_per_s),
                float(doc.spread_high if doc.spread_high is not None else doc.median_msgs_per_s),
            )
        return out

    row_ids: List[str] = []
    by_key: Dict[tuple, Optional[float]] = {}
    by_key_low: Dict[tuple, Optional[float]] = {}
    by_key_high: Dict[tuple, Optional[float]] = {}
    # Representative point per cell, so the matrix can show attribution and
    # confidence (bottleneck, spread, run count) instead of a bare number.
    cells_by: Dict[tuple, PointRow] = {}
    for doc in scenario_docs:
        scenario = doc.scenario or doc.title
        client = doc.client or "?"
        for proto, (median_v, low_v, high_v) in _protocol_aggregates(doc).items():
            row_id = _matrix_row_id(scenario, proto)
            if row_id not in row_ids:
                row_ids.append(row_id)
            by_key[(row_id, client)] = median_v
            by_key_low[(row_id, client)] = low_v
            by_key_high[(row_id, client)] = high_v
        for point in doc.points:
            row_id = _matrix_row_id(scenario, point.protocol or "MQTTv311")
            existing = cells_by.get((row_id, client))
            # Prefer a point that produced a number; otherwise keep the first
            # refusal/failure so the empty cell can still explain itself.
            if existing is None or (
                existing.median_msgs_per_s is None and point.median_msgs_per_s is not None
            ):
                cells_by[(row_id, client)] = point
    # Clients whose every point was refused produce no scenario aggregate, so
    # their columns would otherwise vanish from the matrix entirely.
    for doc in docs:
        if doc.kind != "scenario" or not doc.client:
            continue
        scenario = doc.scenario or doc.title
        for point in doc.points:
            row_id = _matrix_row_id(scenario, point.protocol or "MQTTv311")
            cells_by.setdefault((row_id, doc.client), point)

    # Single-library scenario clients only. ABBA compare docs store a composite
    # "a / b" label in `client` and must not inflate the Clients stat.
    all_clients: List[str] = []
    for doc in docs:
        if doc.kind != "scenario" or not doc.client:
            continue
        if doc.client not in all_clients:
            all_clients.append(doc.client)
    scenario_clients: List[str] = []
    for doc in scenario_docs:
        name = doc.client or "?"
        if name not in scenario_clients:
            scenario_clients.append(name)
    meta = _client_meta(docs)
    colors = _client_colors(_sort_clients(all_clients, meta))
    # Include clients/scenarios that only produced capability or load signals so
    # the issues table and matrix columns stay aligned with the full campaign.
    for doc in docs:
        if doc.kind != "scenario":
            continue
        if doc.client and doc.client not in scenario_clients:
            scenario_clients.append(doc.client)
        if not (doc.capability_reasons or doc.load_reasons):
            continue
        scenario = doc.scenario or doc.title
        protos = list(_protocol_aggregates(doc)) if doc.points else ["MQTTv311"]
        if not protos:
            protos = ["MQTTv311"]
        for proto in protos:
            row_id = _matrix_row_id(scenario, proto)
            if row_id not in row_ids:
                row_ids.append(row_id)
    chart_scenarios = [s for s in row_ids if _scenario_base(s) not in _CHART_EXCLUDED_SCENARIOS]
    matrix_scenarios = _order_matrix_scenarios(row_ids)
    overview_series = [
        {
            "client": client,
            "color": colors.get(client, "#5c6b64"),
            "values": [by_key.get((scenario, client)) for scenario in chart_scenarios],
            "low": [by_key_low.get((scenario, client)) for scenario in chart_scenarios],
            "high": [by_key_high.get((scenario, client)) for scenario in chart_scenarios],
        }
        for client in _sort_clients(scenario_clients, meta)
    ]
    overview_payload = {"scenarios": chart_scenarios, "series": overview_series}
    overview_charts_html = _overview_charts_html(chart_scenarios, overview_series, meta)
    matrix_html = _performance_matrix_html(
        matrix_scenarios, scenario_clients, by_key, colors, meta=meta, cells_by=cells_by
    )
    env_warnings_html = _environment_warnings_html(docs)
    signals_html = _client_signals_html(docs, colors)

    non_comparable_n = sum(1 for doc in docs if doc.non_comparable)
    stats_html = f"""
      <div class="stats">
        <article>
          <p class="stat-label">Clients</p>
          <p class="stat-value">{_esc(len(all_clients))}</p>
        </article>
        <article>
          <p class="stat-label">Scenario rows</p>
          <p class="stat-value">{_esc(len(row_ids))}</p>
        </article>
        <article>
          <p class="stat-label">Result files</p>
          <p class="stat-value">{_esc(len(docs))}<span> {_esc(non_comparable_n)} non-comparable</span></p>
        </article>
      </div>
"""

    client_rank = {name: i for i, name in enumerate(_CLIENT_ORDER)}
    rows = []
    for doc in sorted(
        docs,
        key=lambda d: (
            client_rank.get(d.client or "", len(_CLIENT_ORDER)),
            d.client or "~",
            d.kind,
            d.scenario or d.title,
        ),
    ):
        client_cell = (
            _client_swatch(doc.client, colors, meta) if doc.client in colors else _esc(doc.client or "—")
        )
        rows.append(
            f"""<tr>
  <td><a href="runs/{_esc(doc.slug)}.html">{_esc(doc.title)}</a></td>
  <td>{_esc(doc.kind)}</td>
  <td>{client_cell}</td>
  <td>{_esc(doc.profile or "—")}</td>
  <td>{_status_badge(doc.status, doc.non_comparable)}</td>
  <td class="num">{_esc(_fmt_num(doc.median_msgs_per_s))}</td>
  <td class="mono muted">{_esc(doc.source_name)}</td>
</tr>"""
        )

    body = f"""
    <main>
      <section class="hero">
        <h1>Benchmark reports</h1>
        <p>Readable summaries of local MQTT client runs. Higher throughput is better; latency and integrity appear on each detail page.</p>
        <p class="meta">generated { _esc(generated_at) }</p>
        {stats_html}
      </section>

      {env_warnings_html}

      {overview_charts_html}

      {matrix_html}

      {signals_html}

      <section class="panel">
        <div class="panel-head">
          <h2>All results</h2>
          <p class="hint">Every committed result file, including diagnostics and smoke runs excluded from the charts above.</p>
        </div>
        <div class="table-wrap table-wrap-scroll">
          <table>
            <thead>
              <tr>
                <th>Result</th>
                <th>Kind</th>
                <th>Client</th>
                <th>Profile</th>
                <th>Status</th>
                <th class="num">Median msg/s</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows)}
            </tbody>
          </table>
        </div>
      </section>
    </main>
"""
    return _page_shell("Benchmark reports", body)


def render_detail(doc: ResultDoc, generated_at: str, related: Optional[Dict[str, str]] = None) -> str:
    related = related or {}
    env_bits = []
    for key in ("hostname", "platform", "python", "cpu_count"):
        if doc.environment.get(key) is not None:
            env_bits.append(f"<li><strong>{_esc(key)}</strong> {_esc(doc.environment[key])}</li>")
    versions = (doc.environment.get("client_versions") or {}) if isinstance(doc.environment, dict) else {}
    if versions:
        env_bits.append(
            "<li><strong>client_versions</strong> "
            + _esc(", ".join(f"{k}={v}" for k, v in sorted(versions.items()) if v))
            + "</li>"
        )

    broker_bits = []
    for key, value in (doc.broker or {}).items():
        broker_bits.append(f"<li><strong>{_esc(key)}</strong> {_esc(value)}</li>")

    point_rows = []
    for idx, point in enumerate(doc.points):
        lat = point.latency
        integ = point.integrity or {}
        median_cell = _esc(_fmt_num(point.median_msgs_per_s))
        if point.median_msgs_per_s is None and point.empty_reason:
            glyph, title = EMPTY_GLYPHS.get(point.empty_reason, EMPTY_GLYPHS["missing"])
            tip = f"{title}{': ' + point.reason_detail if point.reason_detail else ''}"
            median_cell = f'<span class="empty-{point.empty_reason}" title="{_esc(tip)}">{glyph}</span>'
        # Dispersion is what says whether a median is repeatable; it was computed
        # for every point and never shown.
        spread_cell = "—"
        if point.relative_spread_pct is not None:
            spread_cell = f"±{point.relative_spread_pct / 2:.1f}%"
            if point.mad is not None:
                spread_cell += f' <span class="muted">(mad {_fmt_num(point.mad)})</span>'
        cost_cell = "—"
        if point.cost_us_per_message is not None:
            cost_cell = f"{point.cost_us_per_message:.1f} µs"
            if point.rss_peak_kb:
                cost_cell += f' <span class="muted">/ {point.rss_peak_kb / 1024:.0f} MiB</span>'
        checks = []
        if point.broker_cpu_max_pct is not None:
            checks.append(f"broker CPU {point.broker_cpu_max_pct:.0f}%")
        if point.broker_reconcile_ratio is not None:
            checks.append(f"broker×{point.broker_reconcile_ratio:.2f}")
        if point.delivery_offer_ratio is not None:
            checks.append(f"offer×{point.delivery_offer_ratio:.2f}")
        point_rows.append(
            f"""<tr>
  <td>{_esc(point.label)}</td>
  <td>{_status_badge(point.status, point.non_comparable)}</td>
  <td class="num">{median_cell}</td>
  <td class="num">{spread_cell}</td>
  <td>{_esc(point.bottleneck or '—')}</td>
  <td class="num">{_esc(_fmt_num(lat.get('p50_ms'), digits=2))}</td>
  <td class="num">{_esc(_fmt_num(lat.get('p95_ms'), digits=2))}</td>
  <td class="num">{_esc(_fmt_num(lat.get('p99_ms'), digits=2))}{' *' if lat.get('p99_gated') else ''}</td>
  <td class="num">{_esc(integ.get('missing', '—'))} / {_esc(integ.get('duplicates', '—'))} (worst {_esc(integ.get('worst_missing', '—'))})</td>
  <td class="num">{cost_cell}</td>
  <td class="muted">{_esc(' · '.join(checks)) or '—'}</td>
  <td>{_esc(point.valid_runs)}/{_esc(point.total_runs)}</td>
</tr>"""
        )

    chart_block = ""
    if doc.points and any(p.median_msgs_per_s is not None for p in doc.points):
        series = [
            {
                "client": "median msg/s",
                "color": "#0f6e56",
                "values": [p.median_msgs_per_s for p in doc.points],
                "low": [p.spread_low for p in doc.points],
                "high": [p.spread_high for p in doc.points],
            }
        ]
        chart_svg = _svg_grouped_bars(
            [p.label for p in doc.points], series, height=320, bar_slot=28, group_gap=16, label_room=150
        )
        chart_block = f"""
      <section class="panel">
        <h2>Per-point throughput</h2>
        <p class="hint">Whiskers show the observed run-to-run min/max at each point. Dual-protocol scenarios list MQTTv311 and MQTTv5 points separately (labels include <code>proto=</code>).</p>
        <div class="chart-wrap">
          {chart_svg}
        </div>
      </section>
"""

    compare_block = ""
    if doc.kind == "compare" and doc.verdict:
        identity_bits = []
        for label, key in (("baseline", "baseline_identity"), ("candidate", "candidate_identity")):
            ident = doc.raw_meta.get(key) or {}
            identity_bits.append(
                f"<li><strong>{label}</strong> {_esc(ident.get('client'))} "
                f"v{_esc(ident.get('client_version'))} "
                f"({_esc(ident.get('stability'))}/{_esc(ident.get('implementation_language'))})</li>"
            )
        point_cal = []
        for point in doc.raw_meta.get("points") or []:
            cals = point.get("calibrations") or {}
            if cals:
                point_cal.append(
                    f"<li>point {_esc(point.get('point_index'))}: {_esc(json.dumps(cals))}</li>"
                )
        loadgen = doc.raw_meta.get("loadgen") or {}
        # A compare over a multi-point scenario carries no top-level ratio or CI:
        # the aggregate verdict is the string "multi_point" and every statistic
        # lives on the individual points. Rendering only the single-point shape
        # showed an empty verdict panel for every campaign comparison, since
        # `pub_qos_sweep_telemetry` always expands to MQTTv311/v5 x QoS 0/1/2.
        per_point_rows = []
        for point in doc.raw_meta.get("points") or []:
            pv = point.get("verdict") or {}
            if not pv:
                continue
            spec = point.get("point") or {}
            label = f"{spec.get('protocol', '?')} qos={spec.get('qos_publish', '?')}"
            per_point_rows.append(
                "<tr>"
                f"<td>{_esc(label)}</td>"
                f"<td>{_status_badge(str(pv.get('verdict', 'inconclusive')))}</td>"
                f"<td>{_esc(_fmt_num(pv.get('median_ratio'), digits=3))}</td>"
                f"<td>{_esc(_fmt_num(pv.get('absolute_effect_pct'), digits=2))}</td>"
                f"<td>{_esc(_fmt_num(pv.get('ci_low'), digits=3))} … "
                f"{_esc(_fmt_num(pv.get('ci_high'), digits=3))}</td>"
                f"<td>{_esc(pv.get('n_blocks'))}</td>"
                "</tr>"
            )
        per_point_table = ""
        if per_point_rows:
            per_point_table = f"""
        <h3>Per-point verdicts</h3>
        <p class="hint">Ratio is candidate over baseline; the interval is the
        bootstrap CI of the relative effect, so it excludes zero exactly when the
        verdict is not inconclusive.</p>
        <div class="table-wrap">
        <table>
          <thead><tr><th>point</th><th>verdict</th><th>median ratio</th>
          <th>effect %</th><th>effect CI</th><th>blocks</th></tr></thead>
          <tbody>{''.join(per_point_rows)}</tbody>
        </table>
        </div>"""
        aggregate_bits = ""
        if doc.verdict.get("median_ratio") is not None:
            aggregate_bits = f"""
          <li><strong>median ratio</strong> {_esc(_fmt_num(doc.verdict.get('median_ratio'), digits=3))}</li>
          <li><strong>effect %</strong> {_esc(_fmt_num(doc.verdict.get('absolute_effect_pct'), digits=2))}</li>
          <li><strong>CI</strong> {_esc(_fmt_num(doc.verdict.get('ci_low'), digits=3))} … {_esc(_fmt_num(doc.verdict.get('ci_high'), digits=3))}</li>"""
        compare_block = f"""
      <section class="panel">
        <h2>A/B verdict</h2>
        <p>{_status_badge(str(doc.verdict.get('verdict', 'inconclusive')))}</p>
        <ul class="kv">
          <li><strong>profile</strong> {_esc(doc.profile)}</li>
          <li><strong>cooldown_s</strong> {_esc(doc.raw_meta.get('cooldown_s'))}</li>
          <li><strong>order</strong> {_esc(doc.raw_meta.get('order'))}</li>{aggregate_bits}
          <li><strong>loadgen</strong> {_esc(loadgen.get('image'))} digest={_esc(loadgen.get('image_digest'))}</li>
          {''.join(identity_bits)}
        </ul>
        {per_point_table}
        {"<h3>Per-client calibrations</h3><ul>" + ''.join(point_cal) + "</ul>" if point_cal else ""}
      </section>
"""

    calibrate_block = ""
    if doc.kind == "calibrate":
        fractions = doc.raw_meta.get("fractions")
        rtt_fractions = doc.raw_meta.get("rtt_fractions")
        rtt_capacity = doc.raw_meta.get("rtt_capacity_msgs_per_s")
        protocol_capacities = doc.raw_meta.get("protocol_capacities")
        proto_block = ""
        if protocol_capacities:
            proto_block = f"""
        <h3>Per-protocol capacities</h3>
        <pre class="code-block">{_esc(json.dumps(protocol_capacities, indent=2))}</pre>
"""
        calibrate_block = f"""
      <section class="panel">
        <h2>Calibration</h2>
        <p>Publish capacity (primary): <strong>{_esc(_fmt_num(doc.median_msgs_per_s))}</strong> msg/s</p>
        <p>RTT capacity (primary): <strong>{_esc(_fmt_num(rtt_capacity))}</strong> pairs/s</p>
        {proto_block}
        <h3>Publish fractions</h3>
        <pre class="code-block">{_esc(json.dumps(fractions, indent=2))}</pre>
        <h3>RTT fractions</h3>
        <pre class="code-block">{_esc(json.dumps(rtt_fractions, indent=2))}</pre>
      </section>
"""

    suite_block = ""
    if doc.kind == "suite":
        scenario_links = []
        for entry in doc.raw_meta.get("scenario_entries") or []:
            name = entry.get("scenario") or "?"
            client = entry.get("client") or ""
            href = related.get(f"{client}:{name}") or related.get(name)
            label = f"{name}" + (f" ({client})" if client else "")
            median = entry.get("median_msgs_per_s")
            median_txt = f" — {_fmt_num(median)} msg/s" if median is not None else ""
            if href:
                scenario_links.append(
                    f'<li><a href="{_esc(href)}">{_esc(label)}</a>{_esc(median_txt)}</li>'
                )
            else:
                scenario_links.append(f"<li>{_esc(label)}{_esc(median_txt)}</li>")
        if not scenario_links:
            for name in doc.raw_meta.get("scenario_names") or []:
                href = related.get(name)
                if href:
                    scenario_links.append(f'<li><a href="{_esc(href)}">{_esc(name)}</a></li>')
                else:
                    scenario_links.append(f"<li>{_esc(name)}</li>")
        suite_block = f"""
      <section class="panel">
        <h2>Suite overview</h2>
        <ul class="kv">
          <li><strong>suite</strong> {_esc(doc.raw_meta.get('suite'))}</li>
          <li><strong>scenarios</strong> {_esc(doc.raw_meta.get('scenario_count'))}</li>
        </ul>
        <h3>Scenario results</h3>
        <ul>{''.join(scenario_links)}</ul>
        <pre class="code-block">{_esc(json.dumps(doc.raw_meta.get('estimate'), indent=2))}</pre>
      </section>
"""

    points_table = ""
    if point_rows:
        points_table = f"""
      <section class="panel">
        <h2>Measurement points</h2>
        <div class="table-wrap table-wrap-scroll">
          <table>
            <thead>
              <tr>
                <th>Point</th>
                <th>Status</th>
                <th>Median msg/s</th>
                <th>Spread</th>
                <th>Bottleneck</th>
                <th>p50 ms</th>
                <th>p95 ms</th>
                <th>p99 ms</th>
                <th>Missing / dup</th>
                <th>CPU / RSS per msg</th>
                <th>Checks</th>
                <th>Valid runs</th>
              </tr>
            </thead>
            <tbody>
              {''.join(point_rows)}
            </tbody>
          </table>
        </div>
        <p class="hint"><strong>Spread</strong> is half the observed run-to-run range, as a percentage of the median — a wide spread means the number is not repeatable, whatever its value. <strong>Bottleneck</strong> is the harness's attribution: only <code>sut_limited</code> says something about the client. <strong>Checks</strong> shows peak broker CPU, the broker-confirmed fraction of reported publishes, and delivered/offered where applicable. * marks a gated p99 (incomplete sample coverage).</p>
      </section>
"""

    body = f"""
    <main>
      <p class="crumb"><a href="../index.html">← All results</a></p>
      <section class="hero">
        <h1>{_esc(doc.title)}</h1>
        <p>
          {_status_badge(doc.status, doc.non_comparable)}
          · client <strong>{_esc(doc.client or '—')}</strong>
          · profile <strong>{_esc(doc.profile or '—')}</strong>
        </p>
        <p class="meta">Source <code>{_esc(doc.source_name)}</code> · generated { _esc(generated_at) }</p>
      </section>

      <section class="stats">
        <article>
          <p class="stat-label">Median throughput</p>
          <p class="stat-value">{_esc(_fmt_num(doc.median_msgs_per_s))} <span>msg/s</span></p>
        </article>
        <article>
          <p class="stat-label">Kind</p>
          <p class="stat-value">{_esc(doc.kind)}</p>
        </article>
        <article>
          <p class="stat-label">Points</p>
          <p class="stat-value">{_esc(_doc_point_count(doc))}</p>
        </article>
      </section>

      {chart_block}
      {points_table}
      {compare_block}
      {calibrate_block}
      {suite_block}

      <section class="panel two-col">
        <div>
          <h2>Environment</h2>
          <ul class="kv">{''.join(env_bits) or '<li>—</li>'}</ul>
        </div>
        <div>
          <h2>Broker</h2>
          <ul class="kv">{''.join(broker_bits) or '<li>—</li>'}</ul>
        </div>
      </section>
    </main>
"""
    return _page_shell(doc.title, body, relative_root="..")


def render_methodology(docs: Sequence[ResultDoc], generated_at: str) -> str:
    """Explain how to read the numbers, next to the numbers themselves.

    These rules used to live only in the README and SCENARIOS.md, so a reader
    arriving at the published site had no way to know what a cell meant.
    """
    meta = _client_meta(docs)
    client_rows = "".join(
        f"<tr><td class='mono'>{_esc(name)}</td><td>{_esc(info.version or '—')}</td>"
        f"<td>{_esc(info.io_model)}</td><td>{_esc(info.stability)}</td>"
        f"<td>{_esc(', '.join(sorted(info.private_api)) or '—')}</td></tr>"
        for name, info in sorted(meta.items(), key=lambda kv: _sort_clients([kv[0]], meta))
    )
    legend = "".join(
        f"<li><span class='legend-glyph empty-{kind}'>{glyph}</span> {_esc(title)}</li>"
        for kind, (glyph, title) in EMPTY_GLYPHS.items()
    )
    body = f"""
    <main>
      <section class="hero">
        <h1>Methodology</h1>
        <p>What these numbers mean, and what they deliberately do not mean.</p>
        <p class="meta">generated {_esc(generated_at)}</p>
      </section>

      <section class="panel">
        <h2>Three measurement protocols, never mixed</h2>
        <ul class="prose">
          <li><strong>Capacity</strong> — closed loop with a bounded outstanding window. The primary metric is <code>completed_success</code> inside the measure window.</li>
          <li><strong>Latency</strong> — open loop. Fractions of <em>that client's own</em> capacity (<code>puback_latency_qos1</code>, <code>application_rtt_qos1</code>) answer how the client behaves near its ceiling; they are not a cross-client comparison. Equal absolute offered rates (<code>puback_latency_fixed_rate</code>) are the public cross-client latency ranking.</li>
          <li><strong>Integrity</strong> — bounded rate with a sequence header; counts missing, duplicate and out-of-order messages. Not a throughput race.</li>
        </ul>
      </section>

      <section class="panel">
        <h2>What "completed" means</h2>
        <p class="hint">The publish completion boundary is part of the contract; an adapter that cannot honour one declares the capability as missing instead of approximating it. QoS0 in particular is not identical across libraries: Paho fires after the socket send completes (<code>qos0_boundary: socket</code>), while MQTTium admits to its write pump (<code>qos0_boundary: queue</code>) — both are declared in <code>client_identity</code>.</p>
        <table class="table">
          <thead><tr><th>QoS</th><th><code>on_publish</code> fires when</th></tr></thead>
          <tbody>
            <tr><td>0</td><td>the packet has been handed to the transport</td></tr>
            <tr><td>1</td><td>PUBACK is received</td></tr>
            <tr><td>2</td><td>PUBCOMP is received (not PUBREC)</td></tr>
          </tbody>
        </table>
        <p class="hint">For single-publisher scenarios the completions are reconciled against the broker's own <code>$SYS</code> received-publish counter. A run the broker cannot confirm is marked inconclusive rather than published.</p>
      </section>

      <section class="panel">
        <h2>Peer groups</h2>
        <p class="hint">A ranking is only meaningful inside a peer group. The matrix highlights the best value per group, never across groups.</p>
        <ul class="prose">
          <li><code>sync</code> — the library exposes a blocking/callback API and is driven directly.</li>
          <li><code>asyncio_bridged</code> — an asyncio library driven through a private event-loop thread. That bridge has a cost, it is assumed and documented, and it is paid equally by every bridged client.</li>
          <li><code>crt_event_loop</code> — a native (non-Python) engine; not comparable with pure-Python clients.</li>
        </ul>
        <p class="hint">Stable and experimental clients are also ranked separately, and MQTT 3.1.1 and MQTT 5 rows are never merged.</p>
      </section>

      <section class="panel">
        <h2>Why a cell can be empty</h2>
        <ul class="prose legend-list">{legend}</ul>
      </section>

      <section class="panel">
        <h2>Attribution</h2>
        <p class="hint">Every run carries a bottleneck attribution. Only <code>sut_limited</code> runs say something about the client.</p>
        <ul class="prose">
          <li><code>sut_limited</code> — the client was the constraint.</li>
          <li><code>broker_limited</code> — Mosquitto was at or near saturation; the number is partly the broker's.</li>
          <li><code>broker_unconfirmed</code> — the broker did not confirm the reported completions.</li>
          <li><code>loadgen_limited</code> / <code>offer_limited</code> — the injected load, not the client, set the ceiling.</li>
        </ul>
      </section>

      <section class="panel">
        <h2>Clients in this report</h2>
        <p class="hint">Read from each result's <code>client_identity</code>. "Internals used" lists library-private attributes the adapter depends on, because reaching into internals changes what is being measured.</p>
        <div class="table-wrap">
          <table class="table">
            <thead><tr><th>Client</th><th>Version</th><th>I/O model</th><th>Stability</th><th>Internals used</th></tr></thead>
            <tbody>{client_rows}</tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <h2>Known limits</h2>
        <ul class="prose">
          <li>All runs are local, against a Dockerised Mosquitto on loopback. Nothing here predicts WAN behaviour.</li>
          <li>Netem profiles (<code>lan</code>/<code>wan</code>/<code>edge</code>) and smoke runs are diagnostic and marked non-comparable.</li>
          <li>Application RTT drives both sides with the same library, which amplifies stack cost on purpose; it is not a neutral peer RTT.</li>
        </ul>
      </section>
    </main>
"""
    return _page_shell("Methodology — MQTT Python client bench", body)


def build_site(input_dir: Path | str, output_dir: Path | str) -> Dict[str, Any]:
    """Generate the static site under output_dir from JSON files in input_dir."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    docs = load_results(input_path)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if output_path.exists():
        shutil.rmtree(output_path)
    runs_dir = output_path / "runs"
    assets_out = output_path / "assets"
    runs_dir.mkdir(parents=True, exist_ok=True)
    assets_out.mkdir(parents=True, exist_ok=True)

    # app.js only existed to drive Chart.js; charts are server-rendered SVG now.
    shutil.copy2(ASSETS_DIR / "style.css", assets_out / "style.css")

    (output_path / "index.html").write_text(render_index(docs, generated_at), encoding="utf-8")
    (output_path / "methodology.html").write_text(
        render_methodology(docs, generated_at), encoding="utf-8"
    )
    related: Dict[str, str] = {}
    for doc in docs:
        if doc.kind == "scenario" and doc.scenario:
            related[doc.scenario] = f"{doc.slug}.html"
            if doc.client:
                related[f"{doc.client}:{doc.scenario}"] = f"{doc.slug}.html"
    for doc in docs:
        (runs_dir / f"{doc.slug}.html").write_text(
            render_detail(doc, generated_at, related=related),
            encoding="utf-8",
        )

    # No raw JSON copied into site/.
    return {
        "input": str(input_path),
        "output": str(output_path),
        "results": len(docs),
        "generated_at": generated_at,
    }
