"""One result file, in full.

Two things were wrong here beyond styling. The points were rendered in file
order, so a QoS sweep read ``qos=1, qos=0, qos=1, qos=2, qos=0, qos=2`` and the
sweep it describes was invisible; and the environment block printed Python
``repr()`` of nested dictionaries, certificate paths and all. Both are fixed
below, and the per-run time series the harness has always sampled finally appear.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

from .. import charts
from ..catalog import axis_sort_key, facts_for, primary_ordinal_axis
from ..components import (
    kv_list,
    num,
    panel,
    scenario_href,
    stat_tile,
    status_badge,
    table,
)
from ..model import EMPTY_GLYPHS, PointRow, ResultDoc, _esc, _fmt_num
from ..shell import crumb, hero, page_shell, stats_row


def render(doc: ResultDoc, generated_at: str, related: Optional[Dict[str, str]] = None) -> str:
    related = related or {}
    points = _sorted_points(doc)
    facts = facts_for(doc.scenario or "")

    scenario_link = ""
    if doc.kind == "scenario" and doc.scenario:
        scenario_link = (
            f' · <a href="{scenario_href(doc.scenario, root="..")}">compare all clients</a>'
        )

    tiles = _stat_tiles(doc, points)

    body = f"""
    <main>
      {crumb("../index.html", "All results")}
{hero(
    _esc(doc.title),
    f'{status_badge(doc.status, doc.non_comparable)} · client <strong>{_esc(doc.client or "—")}</strong>'
    f' · profile <strong>{_esc(doc.profile or "—")}</strong>{scenario_link}',
    f'Source <code>{_esc(doc.source_name)}</code> · generated {_esc(generated_at)}',
)}
      {stats_row(tiles)}
{_sweep_panel(points, facts, doc)}
{_latency_panel(points, doc.client or '')}
{_telemetry_panel(points)}
{_compare_panel(doc)}
{_calibrate_panel(doc)}
{_suite_panel(doc, related)}
{_points_panel(points, doc)}
{_context_panel(doc)}
    </main>
"""
    return page_shell(doc.title, body, root="..", active="")


# --------------------------------------------------------------------------

def _stat_tiles(doc: ResultDoc, points: Sequence[PointRow]) -> List[str]:
    """Tiles that suit the document in hand.

    An A/B comparison has no ``points`` list of its own, so the generic tiles
    reported "0 valid runs" for a campaign that ran ninety-six — a zero that
    means "not applicable" is worse than no tile at all.
    """
    tiles = [
        stat_tile("Kind", _esc(doc.kind)),
        stat_tile("Points", str(_point_count(doc))),
    ]
    if doc.kind == "compare":
        blocks = sum(
            (pt.get("verdict") or {}).get("n_blocks") or 0
            for pt in doc.raw_meta.get("points") or []
        )
        tiles.insert(0, stat_tile("Verdict", _esc(str((doc.verdict or {}).get("verdict", "—")))))
        tiles.append(stat_tile("A/B blocks", str(blocks) if blocks else "—"))
        return tiles
    if doc.kind == "calibrate":
        tiles.insert(0, stat_tile("Publish capacity", num(doc.median_msgs_per_s), "msg/s"))
        tiles.append(
            stat_tile(
                "RTT capacity",
                num(doc.raw_meta.get("rtt_capacity_msgs_per_s")),
                "pairs/s",
            )
        )
        return tiles
    tiles.insert(0, stat_tile("Median throughput", num(doc.median_msgs_per_s), "msg/s"))
    total = sum(p.total_runs for p in points)
    tiles.append(
        stat_tile(
            "Valid runs",
            str(sum(p.valid_runs for p in points)),
            f"of {total}" if total else "",
        )
    )
    return tiles


def _point_count(doc: ResultDoc) -> int:
    if doc.kind == "compare":
        return len(doc.raw_meta.get("points") or [])
    return len(doc.points)


def _sorted_points(doc: ResultDoc) -> List[PointRow]:
    """Order points along the axis the scenario actually sweeps.

    File order is the order the matrix runner happened to interleave them in.
    Rendering that verbatim turned every sweep into noise.
    """
    if not doc.points:
        return []
    axes = []
    for point in doc.points:
        for part in str(point.label or "").split(", "):
            if "=" in part:
                key = part.split("=", 1)[0]
                if key not in axes:
                    axes.append(key)
    axis = primary_ordinal_axis(axes) or (axes[0] if axes else "")

    def value_on(point: PointRow) -> Any:
        for part in str(point.label or "").split(", "):
            if "=" in part:
                key, value = part.split("=", 1)
                if key == axis:
                    return value
        return point.label

    return sorted(
        doc.points,
        key=lambda p: (p.protocol or "", axis_sort_key(axis, value_on(p)), str(p.label)),
    )


def _sweep_panel(points: Sequence[PointRow], facts, doc: ResultDoc) -> str:
    if not points or not any(p.median_msgs_per_s is not None for p in points):
        return ""
    protocols: List[str] = []
    for point in points:
        proto = point.protocol or "MQTTv311"
        if proto not in protocols:
            protocols.append(proto)
    blocks: List[str] = []
    for proto in protocols:
        scoped = [p for p in points if (p.protocol or "MQTTv311") == proto]
        labels = [_strip_proto(p.label) for p in scoped]
        series = [
            {
                "client": doc.client or "median",
                "values": [p.median_msgs_per_s for p in scoped],
                "low": [p.spread_low for p in scoped],
                "high": [p.spread_high for p in scoped],
            }
        ]
        if facts.axis_kind == "ordinal":
            svg = charts.line_sweep(
                labels, series, unit=facts.unit, x_title=facts.axis_label,
                aria=f"{facts.metric} across {facts.axis_label}", log_y=True,
            )
        else:
            svg = charts.bar_group(labels, series, unit=facts.unit, aria=f"{facts.metric} by point")
        if not svg:
            continue
        heading = (
            f'<h3 class="group-title">{_esc(proto)}</h3>' if len(protocols) > 1 else ""
        )
        blocks.append(f'<div class="group-block">{heading}<div class="chart-wrap">{svg}</div></div>')
    if not blocks:
        return ""
    hint = (
        "Points are ordered along the scenario's own axis. Whiskers show the observed run-to-run "
        "min/max."
    )
    if len(protocols) > 1:
        hint += (
            " Each MQTT protocol is drawn separately — a median across both would be a number "
            "about neither."
        )
    return panel(f"Per-point {_esc(facts.metric)}", "".join(blocks), hint=hint)


def _strip_proto(label: str) -> str:
    parts = [p for p in str(label or "").split(", ") if not p.startswith("proto=")]
    return ", ".join(parts) or str(label or "default")


def _latency_panel(points: Sequence[PointRow], client: str) -> str:
    rows = []
    for point in points:
        latency = point.latency or {}
        if latency.get("p50_ms") is None:
            continue
        rows.append(
            {
                "client": client,
                "label": point.label,
                "p50": latency.get("p50_ms"),
                "p95": latency.get("p95_ms"),
                "p99": latency.get("p99_ms"),
            }
        )
    if not rows:
        return ""
    svg = charts.range_bars(rows, unit="ms", aria="Latency percentiles per point")
    if not svg:
        return ""
    return panel(
        "Latency per point",
        f'<div class="chart-wrap">{svg}</div>',
        hint="The span runs from p50 to p99 with the median marked. A span that widens as the "
             "offered load rises is the queue building up behind the client.",
    )


def _telemetry_panel(points: Sequence[PointRow]) -> str:
    """The measure window's own shape, sampled at about 1 Hz.

    A pegged broker and a broker that ramped look identical in a median. These
    series were recorded on every run since the beginning and never displayed.
    Columns appear only when this topology produced them: a publisher-only run
    has no injector, and a column of em-dashes is not information.
    """
    has_cpu = any(p.broker_cpu_series for p in points)
    has_rss = any(p.worker_rss_series for p in points)
    has_offer = any(p.loadgen_rate_series for p in points)
    has_meter = any(p.effective_offer and p.median_msgs_per_s for p in points)
    if not (has_cpu or has_rss or has_offer):
        return ""

    headers = ["Point"]
    if has_cpu:
        headers.append("Broker CPU")
    if has_rss:
        headers.append("Worker RSS")
    if has_offer:
        headers.append("Loadgen rate")
    if has_meter:
        headers.append("Delivered / offered")

    rows = []
    for point in points:
        row = [_esc(point.label)]
        if has_cpu:
            spark = charts.sparkline(point.broker_cpu_series, label="broker CPU", unit="%")
            row.append(
                spark + '<span class="spark-value">' + num(point.broker_cpu_max_pct, digits=0)
                + "% peak</span>"
                if spark
                else "—"
            )
        if has_rss:
            spark = charts.sparkline(point.worker_rss_series, label="worker RSS", unit="MiB")
            peak = max(point.worker_rss_series or [0])
            row.append(
                spark + '<span class="spark-value">' + num(peak, digits=0) + " MiB</span>"
                if spark
                else "—"
            )
        if has_offer:
            spark = charts.sparkline(point.loadgen_rate_series, label="loadgen", unit="msg/s")
            row.append(
                spark + '<span class="spark-value">' + num(point.effective_offer, digits=0)
                + " offered</span>"
                if spark
                else "—"
            )
        if has_meter:
            row.append(
                charts.meter(point.median_msgs_per_s, point.effective_offer, label="delivered of offered")
                or "—"
            )
        rows.append(row)
    return panel(
        "Inside the measure window",
        table(headers, rows, css="results-table spark-table", sortable=False),
        hint="Sampled at roughly 1 Hz across the measure window. These are shapes, not readings: "
             "a broker that climbed into saturation mid-run is a different measurement from one "
             "that sat flat, and a median cannot tell them apart.",
    )


def _points_panel(points: Sequence[PointRow], doc: ResultDoc) -> str:
    """The full table, with columns that only appear when they hold something.

    A publisher-only scenario has no integrity counters and no injector; a column
    of em-dashes crowds out the ones that do carry a number.
    """
    if not points:
        return ""
    has_latency = any((p.latency or {}).get("p50_ms") is not None for p in points)
    has_integrity = any(p.integrity for p in points)
    has_cost = any(p.cost_us_per_message is not None for p in points)
    has_checks = any(
        p.broker_cpu_max_pct is not None
        or p.broker_reconcile_ratio is not None
        or p.delivery_offer_ratio is not None
        for p in points
    )

    headers = ["Point", "Status", "Median msg/s", "Spread", "Bottleneck"]
    numeric = [2, 3]
    if has_latency:
        headers += ["p50 ms", "p95 ms", "p99 ms"]
        numeric += [len(headers) - 3, len(headers) - 2, len(headers) - 1]
    if has_integrity:
        headers.append("Missing / dup")
        numeric.append(len(headers) - 1)
    if has_cost:
        headers.append("CPU / RSS per msg")
        numeric.append(len(headers) - 1)
    if has_checks:
        headers.append("Checks")
    headers.append("Valid runs")
    numeric.append(len(headers) - 1)

    rows = []
    for point in points:
        latency = point.latency or {}
        integrity = point.integrity or {}
        median_cell = _esc(_fmt_num(point.median_msgs_per_s))
        if point.median_msgs_per_s is None and point.observed_msgs_per_s is not None:
            tip = point.reason_detail or point.empty_reason or "not publishable"
            median_cell = (
                '<span class="observed" title="Observed but not publishable: '
                + _esc(tip)
                + '">'
                + _esc(_fmt_num(point.observed_msgs_per_s))
                + "</span>"
            )
        elif point.median_msgs_per_s is None and point.empty_reason:
            glyph, title = EMPTY_GLYPHS.get(point.empty_reason, EMPTY_GLYPHS["missing"])
            tip = f"{title}{': ' + point.reason_detail if point.reason_detail else ''}"
            median_cell = f'<span class="empty-{point.empty_reason}" title="{_esc(tip)}">{glyph}</span>'
        spread_cell = "—"
        if point.relative_spread_pct is not None:
            spread_cell = f"±{point.relative_spread_pct / 2:.1f}%"
            if point.mad is not None:
                spread_cell += f' <span class="muted">(mad {_fmt_num(point.mad)})</span>'
        mark = " †" if point.latency_boundary else ""

        row = [
            _esc(point.label),
            status_badge(point.status, point.non_comparable),
            f'<span class="num">{median_cell}</span>',
            f'<span class="num">{spread_cell}</span>',
            f'<code class="mono">{_esc(point.bottleneck or "—")}</code>',
        ]
        if has_latency:
            for key in ("p50_ms", "p95_ms", "p99_ms"):
                gate = " *" if key == "p99_ms" and latency.get("p99_gated") else ""
                row.append(
                    '<span class="num">'
                    + _esc(_fmt_num(latency.get(key), digits=2))
                    + gate
                    + mark
                    + "</span>"
                )
        if has_integrity:
            row.append(
                '<span class="num">'
                + f'{_esc(integrity.get("missing", "—"))} / {_esc(integrity.get("duplicates", "—"))}'
                + f' <span class="muted">(worst {_esc(integrity.get("worst_missing", "—"))})</span>'
                + "</span>"
            )
        if has_cost:
            cost_cell = "—"
            if point.cost_us_per_message is not None:
                cost_cell = f"{point.cost_us_per_message:.1f} µs"
                if point.rss_peak_kb:
                    cost_cell += f' <span class="muted">/ {point.rss_peak_kb / 1024:.0f} MiB</span>'
            row.append(f'<span class="num">{cost_cell}</span>')
        if has_checks:
            checks = []
            if point.broker_cpu_max_pct is not None:
                checks.append(f"broker CPU {point.broker_cpu_max_pct:.0f}%")
            if point.broker_reconcile_ratio is not None:
                checks.append(f"broker×{point.broker_reconcile_ratio:.2f}")
            if point.delivery_offer_ratio is not None:
                checks.append(f"offer×{point.delivery_offer_ratio:.2f}")
            row.append(f'<span class="muted">{_esc(" · ".join(checks)) or "—"}</span>')
        row.append(f'<span class="num">{point.valid_runs}/{point.total_runs}</span>')
        rows.append(row)

    boundaries = sorted({p.latency_boundary for p in points if p.latency_boundary})
    notes = [
        "<strong>Spread</strong> is half the observed run-to-run range as a percentage of the "
        "median — a wide spread means the number is not repeatable, whatever its value. "
        "<strong>Bottleneck</strong> is the harness's attribution: only <code>sut_limited</code> "
        "says something about the client."
    ]
    if has_checks:
        notes[0] += (
            " <strong>Checks</strong> shows peak broker CPU, the broker-confirmed fraction of "
            "reported publishes, and delivered/offered where applicable."
        )
    if has_latency:
        notes[0] += " <code>*</code> marks a gated p99 (incomplete sample coverage)."
    if boundaries:
        notes.append(
            "† QoS0 completion latency for this client is measured at the "
            f"<code>{_esc(', '.join(boundaries))}</code> boundary declared in "
            "<code>client_identity.qos0_boundary</code> (admission to the client's write path), "
            "not at the socket write. Throughput stays broker-reconciled and comparable; these "
            "latency percentiles must not be compared with socket-boundary clients such as Paho."
        )
    return panel(
        "Measurement points",
        table(headers, rows, css="results-table points-table", numeric=tuple(numeric))
        + "".join(f'<p class="hint">{n}</p>' for n in notes),
    )


def _compare_panel(doc: ResultDoc) -> str:
    if doc.kind != "compare" or not doc.verdict:
        return ""
    identity_bits = []
    for label, key in (("baseline", "baseline_identity"), ("candidate", "candidate_identity")):
        ident = doc.raw_meta.get(key) or {}
        identity_bits.append(
            (
                label,
                f'{_esc(ident.get("client"))} v{_esc(ident.get("client_version"))} '
                f'({_esc(ident.get("stability"))}/{_esc(ident.get("implementation_language"))})',
            )
        )
    loadgen = doc.raw_meta.get("loadgen") or {}
    effect_rows = []
    per_point_rows = []
    for point in doc.raw_meta.get("points") or []:
        verdict = point.get("verdict") or {}
        if not verdict:
            continue
        spec = point.get("point") or {}
        label = f"{spec.get('protocol', '?')} qos={spec.get('qos_publish', '?')}"
        ratio = verdict.get("median_ratio")
        # ci_low/ci_high are the relative effect, so they sit one unit below the
        # ratio; plotting them raw against a ratio axis would misplace every
        # interval by exactly 1.0.
        lo = verdict.get("ci_low")
        hi = verdict.get("ci_high")
        effect_rows.append(
            {
                "label": label,
                "ratio": ratio,
                "lo": (lo + 1.0) if isinstance(lo, (int, float)) else None,
                "hi": (hi + 1.0) if isinstance(hi, (int, float)) else None,
                "verdict": str(verdict.get("verdict", "")),
            }
        )
        per_point_rows.append(
            "<tr>"
            f"<td>{_esc(label)}</td>"
            f"<td>{status_badge(str(verdict.get('verdict', 'inconclusive')))}</td>"
            f'<td class="num">{_esc(_fmt_num(verdict.get("median_ratio"), digits=3))}</td>'
            f'<td class="num">{_esc(_fmt_num(verdict.get("absolute_effect_pct"), digits=2))}</td>'
            f'<td class="num">{_esc(_fmt_num(verdict.get("ci_low"), digits=3))} … '
            f'{_esc(_fmt_num(verdict.get("ci_high"), digits=3))}</td>'
            f'<td class="num">{_esc(verdict.get("n_blocks"))}</td>'
            "</tr>"
        )
    chart = charts.effect_dots(effect_rows, aria="A/B effect per point")
    per_point_table = ""
    if per_point_rows:
        per_point_table = f"""
        <h3 class="sub">Per-point verdicts</h3>
        <p class="hint">Ratio is candidate over baseline; the interval is the
        bootstrap CI of the relative effect, so it excludes zero exactly when the
        verdict is not inconclusive.</p>
        <div class="table-wrap">
        <table class="results-table">
          <thead><tr><th>point</th><th>verdict</th><th class="num">median ratio</th>
          <th class="num">effect %</th><th class="num">effect CI</th><th class="num">blocks</th></tr></thead>
          <tbody>{''.join(per_point_rows)}</tbody>
        </table>
        </div>"""
    pairs = [
        ("verdict", status_badge(str(doc.verdict.get("verdict", "inconclusive")))),
        ("profile", _esc(doc.profile)),
        ("cooldown_s", _esc(doc.raw_meta.get("cooldown_s"))),
        ("order", f'<code class="mono">{_esc("".join(doc.raw_meta.get("order") or []))}</code>'),
    ]
    if doc.verdict.get("median_ratio") is not None:
        pairs += [
            ("median ratio", _esc(_fmt_num(doc.verdict.get("median_ratio"), digits=3))),
            ("effect %", _esc(_fmt_num(doc.verdict.get("absolute_effect_pct"), digits=2))),
            (
                "CI",
                f'{_esc(_fmt_num(doc.verdict.get("ci_low"), digits=3))} … '
                f'{_esc(_fmt_num(doc.verdict.get("ci_high"), digits=3))}',
            ),
        ]
    image = str(loadgen.get("image") or "")
    digest = str(loadgen.get("image_digest") or "")
    if image and digest and digest in image:
        loadgen_text = f'<code class="mono">{_esc(image)}</code>'
    elif image or digest:
        loadgen_text = f'<code class="mono">{_esc(image or digest)}</code>'
    else:
        loadgen_text = ""
    if loadgen_text:
        pairs.append(("loadgen", loadgen_text))
    pairs += identity_bits
    chart_html = f'<div class="chart-wrap">{chart}</div>' if chart else ""
    return panel(
        "A/B verdict",
        kv_list(pairs, css="kv kv-wide") + chart_html + per_point_table,
        hint="Blocks alternate A-B-B-A so slow drift cancels rather than becoming the result. "
             "An interval that straddles the no-effect line is an inconclusive point, however "
             "large its ratio looks.",
    )


def _calibrate_panel(doc: ResultDoc) -> str:
    if doc.kind != "calibrate":
        return ""
    blocks = [
        f'<p>Publish capacity: <strong>{_esc(_fmt_num(doc.median_msgs_per_s))}</strong> msg/s</p>',
        f'<p>RTT capacity: <strong>{_esc(_fmt_num(doc.raw_meta.get("rtt_capacity_msgs_per_s")))}</strong> pairs/s</p>',
    ]
    for title, key in (
        ("Per-protocol capacities", "protocol_capacities"),
        ("Publish fractions", "fractions"),
        ("RTT fractions", "rtt_fractions"),
    ):
        value = doc.raw_meta.get(key)
        if value:
            blocks.append(
                f'<h3 class="sub">{title}</h3>'
                f'<pre class="code-block">{_esc(json.dumps(value, indent=2))}</pre>'
            )
    return panel(
        "Calibration",
        "".join(blocks),
        hint="Open-loop scenarios are offered a fraction of these capacities. A load_fraction "
             "without a matching entry here is refused rather than defaulted.",
    )


def _suite_panel(doc: ResultDoc, related: Dict[str, str]) -> str:
    if doc.kind != "suite":
        return ""
    links = []
    for entry in doc.raw_meta.get("scenario_entries") or []:
        name = entry.get("scenario") or "?"
        client = entry.get("client") or ""
        href = related.get(f"{client}:{name}") or related.get(name)
        label = name + (f" ({client})" if client else "")
        median = entry.get("median_msgs_per_s")
        suffix = f" — {_fmt_num(median)} msg/s" if median is not None else ""
        links.append(
            f'<li><a href="{_esc(href)}">{_esc(label)}</a>{_esc(suffix)}</li>'
            if href
            else f"<li>{_esc(label)}{_esc(suffix)}</li>"
        )
    if not links:
        for name in doc.raw_meta.get("scenario_names") or []:
            href = related.get(name)
            links.append(
                f'<li><a href="{_esc(href)}">{_esc(name)}</a></li>' if href else f"<li>{_esc(name)}</li>"
            )
    return panel(
        "Suite overview",
        kv_list(
            [
                ("suite", _esc(doc.raw_meta.get("suite"))),
                ("scenarios", _esc(doc.raw_meta.get("scenario_count"))),
            ]
        )
        + f'<h3 class="sub">Scenario results</h3><ul class="bullets">{"".join(links)}</ul>'
        + f'<pre class="code-block">{_esc(json.dumps(doc.raw_meta.get("estimate"), indent=2))}</pre>',
    )


def _context_panel(doc: ResultDoc) -> str:
    """Host and broker facts, rendered as fields rather than as Python repr.

    The broker block used to print ``str(dict)`` for nested values, so a reader
    got a page of quoted certificate paths where a fingerprint would have done.
    """
    env_pairs = []
    for key in ("hostname", "platform", "machine", "python", "cpu_model", "cpu_count"):
        value = (doc.environment or {}).get(key)
        if value is not None:
            env_pairs.append((key, _esc(value)))
    if doc.clock_unpinned:
        env_pairs.append(
            (
                "clock",
                "unpinned — this host exposes no cpufreq governor and its profile declares it. "
                "Run-to-run variance is wider than on a pinned clock; these numbers rank clients "
                "against each other on this machine and are not published.",
            )
        )
    versions = (doc.environment or {}).get("client_versions") or {}
    if isinstance(versions, dict) and versions:
        env_pairs.append(
            (
                "client versions",
                ", ".join(
                    f'<code class="mono">{_esc(k)}={_esc(v)}</code>'
                    for k, v in sorted(versions.items())
                    if v
                ),
            )
        )

    broker_pairs = []
    for key, value in (doc.broker or {}).items():
        if key == "certs" and isinstance(value, dict):
            fingerprint = value.get("fingerprint")
            broker_pairs.append(
                ("certs", f'<code class="mono">{_esc(fingerprint or "generated locally")}</code>')
            )
            continue
        if isinstance(value, (dict, list)):
            broker_pairs.append((key, f'<code class="mono">{_esc(json.dumps(value))}</code>'))
            continue
        if key in ("image", "image_digest", "config_hash", "container_name"):
            broker_pairs.append((key, f'<code class="mono">{_esc(value)}</code>'))
            continue
        broker_pairs.append((key, _esc(value)))

    env_html = kv_list(env_pairs, css="kv kv-wide")
    broker_html = kv_list(broker_pairs, css="kv kv-wide")
    if not env_html and not broker_html:
        return ""
    return f"""
      <section class="panel two-col">
        <div>
          <h2>Environment</h2>
          {env_html}
        </div>
        <div>
          <h2>Broker</h2>
          {broker_html}
        </div>
      </section>"""
