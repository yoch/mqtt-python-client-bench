"""One page per scenario: every client, along the scenario's own axis.

This is the view the site did not have. The index shows one bar per scenario and
a run page shows one client, so the question a reader actually arrives with —
*on this workload, how do these libraries compare, and how does that change
across the sweep?* — had no page at all. A scenario is a curve; seven payload
sizes or four offered loads collapsed to a single median threw away the shape
that was the point of sweeping.
"""

from __future__ import annotations

from typing import Dict, List, Sequence

from .. import charts
from ..aggregate import (
    cost_points,
    docs_for_scenario,
    peer_groups,
    protocols_in,
    sweep,
)
from ..catalog import facts_for
from ..components import (
    client_swatch,
    direction_badge,
    note,
    num,
    panel,
    group_title,
    run_href,
    stat_tile,
    status_badge,
    table,
)
from ..model import ClientMeta, ResultDoc, _client_meta, _esc, _slug, _sort_clients

try:
    from mqtt_client_bench.scenarios import SCENARIOS
except Exception:  # pragma: no cover - the report must still build
    SCENARIOS = []
from ..shell import crumb, hero, page_shell, stats_row

# What an absent number means. "not run" is only true for the last of these; a
# refused point was attempted and declined, which is a different claim.
_EMPTY_LABELS = {
    "refused": "refused",
    "failed": "failed",
    "environment": "invalidated",
    "missing": "not run",
}


def render_all(docs: Sequence[ResultDoc], generated_at: str) -> Dict[str, str]:
    """slug -> HTML for every scenario present in the corpus."""
    meta = _client_meta(docs)
    names = sorted({d.scenario or d.title for d in docs if d.kind == "scenario" and d.scenario})
    pages = {_slug(name): render(name, docs, meta, generated_at) for name in names}
    pages["index"] = render_listing(names, docs, meta, generated_at)
    return pages


def render_listing(
    names: Sequence[str],
    docs: Sequence[ResultDoc],
    meta: Dict[str, ClientMeta],
    generated_at: str,
) -> str:
    """The whole catalogue, not only the part that has been run.

    Listing just the thirteen scenarios with committed results made the other
    twenty-seven invisible: a reader browsing Scenarios had no way to learn that
    the full suite exists, or that the one the docs call the public cross-client
    latency ranking has never been executed.
    """
    measured = set(names)
    rows: List[List[str]] = []
    for name, suite in _catalogue(measured):
        facts = facts_for(name)
        if name in measured:
            scoped = docs_for_scenario(docs, name)
            valid = sum(p.valid_runs for d in scoped for p in d.points)
            total = sum(p.total_runs for d in scoped for p in d.points)
            link = f'<a href="{_slug(name)}.html">{_esc(name)}</a>'
            clients = f'<span class="num">{len(scoped)}</span>'
            runs = f'<span class="num">{valid} / {total}</span>'
            if valid == 0 and total:
                runs = f'<span class="num observed" title="Every run was invalidated">0 / {total}</span>'
        else:
            link = f'<span class="unmeasured">{_esc(name)}</span>'
            clients = '<span class="num">—</span>'
            runs = '<span class="badge badge-empty">not run</span>'
        rows.append(
            [
                link,
                f'<span class="badge badge-suite">{_esc(suite)}</span>',
                _esc(facts.question or "—"),
                f'{_esc(facts.metric)} <span class="unit">{_esc(facts.unit)}</span>',
                direction_badge(facts.direction),
                clients,
                runs,
            ]
        )
    missing = sum(1 for name, _ in _catalogue(measured) if name not in measured)
    lead = ""
    if missing:
        lead = note(
            f"<strong>{len(measured)} of {len(measured) + missing}</strong> catalogued scenarios "
            "have committed results. The rest are executable but have never been run; they are "
            "listed here rather than hidden, so the shape of the gap is visible. See "
            '<a href="../corpus.html">the corpus page</a> for the detail.',
            kind="info",
        )
    body = f"""
    <main>
      {crumb("../index.html", "Overview")}
{hero("Scenarios", "What each workload asks, what it measures, and which way is better.",
      f"generated {_esc(generated_at)}")}
      {panel("Catalogue",
             lead + table(["Scenario", "Suite", "Question", "Primary metric", "Direction",
                           "Clients", "Valid runs"],
                          rows, css="results-table", numeric=(5, 6)),
             hint="A greyed name is catalogued and executable but has no committed result. "
                  "<code>0 / n</code> means every run was attempted and then invalidated — that "
                  "is measurement debt, not a slow client.")}
    </main>
"""
    return page_shell("Scenarios", body, root="..", active="scenarios")


def _catalogue(measured):
    """(name, suite) for every scenario, catalogue order, measured ones first."""
    entries = [(s.name, s.suite) for s in SCENARIOS]
    known = {name for name, _ in entries}
    # A result whose scenario has since left the catalogue still has a page.
    entries += [(name, "retired") for name in sorted(measured - known)]
    return sorted(entries, key=lambda kv: (kv[0] not in measured, kv[1], kv[0]))


def render(
    scenario: str,
    docs: Sequence[ResultDoc],
    meta: Dict[str, ClientMeta],
    generated_at: str,
) -> str:
    scoped = docs_for_scenario(docs, scenario)
    facts = facts_for(scenario)
    clients = _sort_clients(sorted({d.client for d in scoped if d.client}), meta)
    groups = peer_groups(clients, meta)
    protocols = protocols_in(scoped)

    valid = sum(p.valid_runs for d in scoped for p in d.points)
    total = sum(p.total_runs for d in scoped for p in d.points)
    tiles = [
        stat_tile("Primary metric", _esc(facts.metric), _esc(facts.unit)),
        stat_tile("Clients measured", str(len(clients))),
        stat_tile("Valid runs", f"{valid}", f"of {total}" if total else ""),
        stat_tile("Protocols", " · ".join(_esc(p) for p in protocols) or "—"),
    ]

    caveats = "".join(note(_esc(c), kind="warn") for c in facts.caveats)
    if facts.intra_client_only:
        caveats += note(
            "Points on this scenario are offered as a fraction of <em>each client's own</em> "
            "capacity, so a comparison across clients is comparing different loads. Read one "
            "client at a time.",
            kind="warn",
        )
    if not facts.ranked:
        caveats += note(
            "The primary rate here is imposed by the harness rather than found by the client, "
            "so no winner is crowned on this page.",
            kind="info",
        )

    body = f"""
    <main>
      {crumb("index.html", "All scenarios")}
{hero(
    _esc(scenario),
    _esc(facts.question or "See the scenario definition for what this measures."),
    f"generated {_esc(generated_at)}",
    extra=f'<p class="hero-meta">{direction_badge(facts.direction)} '
          f'<span class="metric-name">{_esc(facts.metric)}</span> '
          f'<span class="unit">{_esc(facts.unit)}</span></p>',
)}
      {stats_row(tiles)}
      {f'<section class="panel panel-caution"><div class="panel-head"><h2>How to read this</h2></div>{caveats}</section>' if caveats else ''}
{_sweep_panels(scoped, groups, facts, protocols, meta)}
{_latency_panel(scoped, groups, facts, meta)}
{_cost_panel(scoped, clients)}
{_integrity_panel(scoped, clients, meta)}
{_points_table(scoped, clients, meta, facts)}
    </main>
"""
    return page_shell(f"{scenario} — scenario", body, root="..", active="scenarios")


def _sweep_panels(docs, groups, facts, protocols, meta) -> str:
    """The scenario's own axis, faceted by peer group and by protocol.

    Every facet shares one value scale: faceting is there to keep incomparable
    clients out of each other's frame, not to rescale each frame so that
    different rates draw the same length.
    """
    multi_protocol = len(protocols) > 1
    prepared = []
    for proto in protocols or [None]:
        labels, by_client = sweep(docs, protocol=proto if multi_protocol else None)
        if not labels:
            continue
        for io_model, members in groups:
            series = []
            for client in members:
                points = by_client.get(client) or {}
                values = [
                    (points[l].median_msgs_per_s if l in points and not points[l].non_comparable else None)
                    for l in labels
                ]
                if not any(v is not None for v in values):
                    continue
                series.append(
                    {
                        "client": client,
                        "values": values,
                        "low": [points[l].spread_low if l in points else None for l in labels],
                        "high": [points[l].spread_high if l in points else None for l in labels],
                    }
                )
            if series:
                prepared.append((proto, io_model, labels, series))
    if not prepared:
        return ""

    ordinal = facts.axis_kind == "ordinal"
    extent = charts.sweep_extent if ordinal else charts.bar_extent
    scale_max = max((extent(series) for _p, _m, _l, series in prepared), default=0.0)

    blocks: List[str] = []
    for proto, io_model, labels, series in prepared:
        if ordinal:
            svg = charts.line_sweep(
                labels, series, unit=facts.unit, x_title=facts.axis_label,
                aria=f"{facts.metric} across {facts.axis_label} for {io_model} clients",
                log_y=True, scale_max=scale_max,
            )
        else:
            svg = charts.bar_group(
                labels, series, unit=facts.unit,
                aria=f"{facts.metric} by {facts.axis_label} for {io_model} clients",
                scale_max=scale_max,
            )
        if not svg:
            continue
        heading = group_title(io_model, [s.get("client") for s in series], meta)
        if multi_protocol and proto:
            heading = heading.replace("</h3>", f' <span class="group-sub">{_esc(proto)}</span></h3>')
        blocks.append(
            f'<div class="group-block">{heading}<div class="chart-wrap">{svg}</div></div>'
        )
    if not blocks:
        return ""

    axis_name = facts.axis_label or "the scenario axis"
    hint = (
        f"Every client that measured this scenario, along <strong>{_esc(axis_name)}</strong>. "
        "One chart per peer group, because only clients that are substitutes for one another are "
        "ranked against each other; <strong>all of them share one value scale</strong>, so a "
        "curve that sits higher really is faster. A break in a line is a point that client did "
        "not produce — never a zero. The value axis starts at zero, so a nearly flat line means "
        "the metric really did barely move."
    )
    if len(protocols) > 1:
        hint += " MQTT 3.1.1 and MQTT 5 are drawn separately and are never merged."
    return panel(f"{_esc(facts.metric)} across {_esc(axis_name)}", "".join(blocks), hint=hint)


def _latency_panel(docs, groups, facts, meta) -> str:
    collected = [(io_model, _latency_rows(docs, members)) for io_model, members in groups]
    scale_max = max((charts.range_extent(rows) for _m, rows in collected), default=0.0)
    sections: List[str] = []
    for io_model, rows in collected:
        if not rows:
            continue
        svg = charts.range_bars(
            rows,
            unit="ms",
            aria=f"Latency percentiles for {io_model} clients",
            scale_max=scale_max,
        )
        if not svg:
            continue
        sections.append(
            panel(
                f"Latency percentiles — {_esc(io_model)}",
                f'<div class="chart-wrap">{svg}</div>',
                hint="Span from p50 to p99 with the median marked; every point that reported "
                     "percentiles is listed, so the tail can be read against the load that "
                     "produced it. Every group on this page shares one millisecond scale.",
            )
        )
    return "\n".join(sections)


def _latency_rows(docs, members):
    """Every point that reported percentiles, for the clients in one group."""
    rows = []
    for client in members:
        for doc in docs:
            if (doc.client or "?") != client:
                continue
            for point in doc.points:
                latency = point.latency or {}
                if latency.get("p50_ms") is None:
                    continue
                rows.append(
                    {
                        "client": client,
                        "label": f"{client} · {point.label}",
                        "p50": latency.get("p50_ms"),
                        "p95": latency.get("p95_ms"),
                        "p99": latency.get("p99_ms"),
                    }
                )
    return rows


def _cost_panel(docs, clients) -> str:
    """What a message costs, which no chart on this site used to show."""
    points = cost_points(docs, clients)
    if len(points) < 2:
        return ""
    svg = charts.scatter(
        points,
        x_title="median msg/s",
        y_title="µs CPU per message",
        aria="Throughput against CPU cost per message",
    )
    return panel(
        "Throughput against what it costs",
        f'<div class="chart-wrap">{svg}</div>',
        hint="Median rate on x, CPU microseconds per message on y — down and to the right is "
             "cheaper and faster. Dots are labelled rather than distinguished by colour alone. "
             "This mixes peer groups on purpose: it is an efficiency picture, not a ranking.",
    )


def _integrity_panel(docs, clients, meta) -> str:
    rows = []
    for client in clients:
        for doc in docs:
            if (doc.client or "?") != client:
                continue
            for point in doc.points:
                integrity = point.integrity
                if not integrity:
                    continue
                bad = (
                    (integrity.get("missing") or 0)
                    + (integrity.get("duplicates") or 0)
                    + (integrity.get("out_of_order") or 0)
                )
                rows.append(
                    [
                        client_swatch(client, meta),
                        _esc(point.label),
                        f'<span class="num">{num(integrity.get("expected"), digits=0)}</span>',
                        f'<span class="num">{num(integrity.get("received"), digits=0)}</span>',
                        f'<span class="num {"bad" if integrity.get("missing") else "ok"}">'
                        f'{num(integrity.get("missing"), digits=0)}</span>',
                        f'<span class="num {"bad" if integrity.get("duplicates") else "ok"}">'
                        f'{num(integrity.get("duplicates"), digits=0)}</span>',
                        f'<span class="num {"bad" if integrity.get("out_of_order") else "ok"}">'
                        f'{num(integrity.get("out_of_order"), digits=0)}</span>',
                        '<span class="badge badge-valid">clean</span>'
                        if not bad
                        else '<span class="badge badge-inconclusive">lost or reordered</span>',
                    ]
                )
    if not rows:
        return ""
    return panel(
        "Sequence integrity",
        table(
            ["Client", "Point", "Expected", "Received", "Missing", "Duplicates", "Out of order", "Verdict"],
            rows,
            css="results-table",
            numeric=(2, 3, 4, 5, 6),
        ),
        hint="Zero is the only good answer in the three counter columns. Throughput on an "
             "integrity scenario is capped by design and says nothing about capacity.",
    )


def _points_table(docs, clients, meta, facts) -> str:
    """Every point, with columns that are only present when they hold something.

    Rendering p50, p99 and cost columns on a scenario that measures none of them
    filled a third of the table with em-dashes, which is how the invalidation
    reasons ended up squeezed out of view.
    """
    entries = []
    for doc in sorted(docs, key=lambda d: (d.client or "")):
        for point in doc.points:
            entries.append((doc, point))
    if not entries:
        return ""

    show_latency = any((p.latency or {}).get("p50_ms") is not None for _, p in entries)
    show_cost = any(p.cost_us_per_message is not None for _, p in entries)
    show_signal = any(p.reason_detail for _, p in entries)

    headers = ["Client", "Point", "Status", f"Median {facts.unit}"]
    numeric = [3]
    if show_signal:
        headers.append("Signal")
    headers.append("Spread")
    numeric.append(len(headers) - 1)
    headers.append("Bottleneck")
    if show_latency:
        headers += ["p50 ms", "p99 ms"]
        numeric += [len(headers) - 2, len(headers) - 1]
    if show_cost:
        headers.append("µs/msg")
        numeric.append(len(headers) - 1)
    headers.append("Valid runs")
    numeric.append(len(headers) - 1)
    headers.append("")

    rows = []
    for doc, point in entries:
        latency = point.latency or {}
        if point.median_msgs_per_s is not None:
            median_cell = '<span class="num">' + num(point.median_msgs_per_s) + "</span>"
        elif point.observed_msgs_per_s is not None:
            # Shown, but never as a result: the value is real and the reason it
            # cannot be ranked travels with it.
            tip = point.reason_detail or point.empty_reason or "not publishable"
            median_cell = (
                '<span class="num observed" title="Observed but not publishable: '
                + _esc(tip)
                + '">'
                + num(point.observed_msgs_per_s)
                + "</span>"
            )
        else:
            median_cell = (
                '<span class="empty-value">' + _EMPTY_LABELS.get(point.empty_reason or "", "not run")
                + "</span>"
            )

        row = [
            client_swatch(doc.client or "?", meta),
            _esc(point.label),
            status_badge(point.status, point.non_comparable),
            median_cell,
        ]
        if show_signal:
            row.append(
                '<code class="mono signal">' + _esc(point.reason_detail) + "</code>"
                if point.reason_detail
                else "—"
            )
        spread = (
            "±%.1f%%" % (point.relative_spread_pct / 2)
            if point.relative_spread_pct is not None
            else "—"
        )
        row.append('<span class="num">' + spread + "</span>")
        row.append('<code class="mono">' + _esc(point.bottleneck or "—") + "</code>")
        if show_latency:
            row.append('<span class="num">' + num(latency.get("p50_ms"), digits=2) + "</span>")
            row.append('<span class="num">' + num(latency.get("p99_ms"), digits=2) + "</span>")
        if show_cost:
            row.append(
                '<span class="num">' + num(point.cost_us_per_message, digits=1) + "</span>"
            )
        row.append('<span class="num">' + f"{point.valid_runs}/{point.total_runs}" + "</span>")
        row.append('<a href="' + run_href(doc.slug, root="..") + '">detail</a>')
        rows.append(row)

    hint = (
        "Only <code>sut_limited</code> says something about the client; every other bottleneck "
        "names something else as the constraint."
    )
    if any(p.median_msgs_per_s is None and p.observed_msgs_per_s is not None for _, p in entries):
        hint += (
            " A <span class=\"observed\">greyed value</span> is a rate the harness observed but "
            "refused to publish — hover it for the reason. It is measurement debt, not a result."
        )
    return panel(
        "Every measured point",
        table(headers, rows, css="results-table points-table", numeric=tuple(numeric)),
        hint=hint,
    )
