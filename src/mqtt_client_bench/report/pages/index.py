"""The landing page: what the campaign found, and what it refuses to claim."""

from __future__ import annotations

from typing import Dict, List, Sequence

from .. import charts
from ..aggregate import Overview, peer_groups, trim_empty_rows
from ..components import (
    client_swatch,
    group_title,
    num,
    panel,
    run_href,
    stat_tile,
    status_badge,
    table,
)
from ..model import (
    ClientMeta,
    ResultDoc,
    _CLIENT_ORDER,
    _client_meta,
    _doc_point_count,
    _esc,
    _order_matrix_scenarios,
)
from ..panels import client_signals_html, performance_matrix_html
from ..shell import hero, page_shell, stats_row

_LATENCY_SOURCE = "puback_latency_qos1"


def render(docs: Sequence[ResultDoc], generated_at: str) -> str:
    if not docs:
        body = f"""
    <main>
      <section class="hero">
        <h1>Benchmark reports</h1>
        <p>No result files yet. Run scenarios locally with <code>--output results/&lt;name&gt;.json</code>,
        commit the JSON, and push to refresh this site.</p>
        <p class="meta">Generated {_esc(generated_at)}</p>
      </section>
    </main>
"""
        return page_shell("Benchmark reports", body, active="")

    meta = _client_meta(docs)
    overview = Overview(docs, meta)
    groups = peer_groups(overview.clients, meta)

    valid_runs = sum(p.valid_runs for d in docs for p in d.points)
    total_runs = sum(p.total_runs for d in docs for p in d.points)
    non_comparable_n = sum(1 for doc in docs if doc.non_comparable)
    scenarios = sorted({d.scenario or d.title for d in docs if d.kind == "scenario"})

    tiles = [
        stat_tile("Clients", str(len(overview.all_clients))),
        stat_tile("Scenarios", str(len(scenarios)), f"{len(overview.row_ids)} protocol rows"),
        stat_tile(
            "Valid runs",
            num(valid_runs, digits=0),
            f"of {num(total_runs, digits=0)}" if total_runs else "",
        ),
        stat_tile(
            "Result files",
            str(len(docs)),
            f"{non_comparable_n} non-comparable" if non_comparable_n else "",
        ),
    ]

    body = f"""
    <main>
{hero(
    "Benchmark reports",
    "Nine Python MQTT client libraries, measured end to end against a local Mosquitto. "
    "Every ranking below is confined to clients that are genuine substitutes for one another; "
    "comparing across two charts is not meaningful.",
    f"generated {_esc(generated_at)}",
)}
      {stats_row(tiles)}
      {_corpus_banner(docs)}
{_throughput_panels(overview, groups, meta)}
{_latency_panels(docs, groups, meta)}
{_matrix_panel(overview, meta)}
{_signals_panel(docs)}
{_all_results_panel(docs, meta)}
    </main>
"""
    return page_shell("Benchmark reports", body, active="")


def _corpus_banner(docs: Sequence[ResultDoc]) -> str:
    """One line where a full-width red panel used to sit.

    The invalidations matter, but putting the longest table on the site above
    every result made the page read as a failure report. The count still leads;
    the detail moved to the page that is about the corpus."""
    affected = [d for d in docs if d.environment_reasons]
    if not affected:
        return ""
    runs = sum(sum(d.environment_reasons.values()) for d in affected)
    return (
        '<p class="banner banner-warn" role="alert">'
        f"<strong>{len(affected)} result files</strong> carry environment invalidations "
        f"({runs} affected runs): the broker or the host, not the client, set those numbers. "
        'Their medians are excluded from every ranking here — '
        '<a href="corpus.html">see what was invalidated and why</a>.</p>'
    )


def _throughput_panels(overview: Overview, groups, meta: Dict[str, ClientMeta]) -> str:
    """One ranking chart per peer group, on one shared scale.

    All nine clients across every scenario row came to ninety-odd bars in a
    single frame: unreadable, and it invited a cross-group comparison the matrix
    is careful about. Faceting fixed the density; the charts then each scaled to
    their own maximum, so a 24k bar and a 38k bar drew the same width. The
    domain is now computed across every group before anything is drawn, which
    makes bar length mean one thing on the whole page.
    """
    rows = overview.chart_rows
    prepared = []
    for io_model, members in groups:
        kept_rows, series = trim_empty_rows(rows, overview.series_for(rows, members))
        if kept_rows:
            prepared.append((io_model, kept_rows, series))
    scale_max = max((charts.bar_extent(s) for _m, _r, s in prepared), default=0.0)

    blocks: List[str] = []
    for io_model, kept_rows, series in prepared:
        svg = charts.bar_group(
            kept_rows,
            series,
            unit="msg/s",
            aria=f"Median throughput for {io_model} clients",
            scale_max=scale_max,
        )
        if not svg:
            continue
        blocks.append(
            f'<div class="group-block">'
            f"{group_title(io_model, [s.get('client') for s in series], meta)}"
            f'<div class="chart-wrap">{svg}</div></div>'
        )
    if not blocks:
        return ""
    return panel(
        "Throughput rankings",
        "".join(blocks),
        hint=(
            "One chart per peer group: only clients that are substitutes for one another are "
            "ranked against each other. Rows are scenario · MQTT protocol and are comparable only "
            "within the same protocol; whiskers show the observed run-to-run min/max. "
            "<strong>All charts share one msg/s scale</strong>, so a bar twice as long is twice "
            "the rate wherever it appears — but a bigger number across two charts is not a better "
            "library: a bridged asyncio client, a native CRT engine and a blocking client are not "
            "doing the same work, and only the highlight inside a chart is a ranking."
        ),
    )


def _latency_panels(docs: Sequence[ResultDoc], groups, meta: Dict[str, ClientMeta]) -> str:
    """Percentiles drawn as a span — the first time this site has shown them.

    p50, p95 and p99 sat in three table columns where the shape they describe was
    invisible. Drawn as a span from median to tail, a client whose p99 runs away
    from its p50 is visibly the unstable one.
    """
    source = [d for d in docs if (d.scenario or d.title) == _LATENCY_SOURCE]
    if not source:
        return ""
    blocks: List[str] = []
    gated = False
    boundaries = set()
    # One domain for every group, computed before any chart is drawn: stacked
    # small multiples that each scaled to their own worst value made a 2 ms span
    # and a 7 ms span the same width.
    per_group = [(io_model, _latency_rows_for(source, members)) for io_model, members in groups]
    scale_max = max((charts.range_extent(rows) for _m, rows in per_group), default=0.0)
    for io_model, rows in per_group:
        if not rows:
            continue
        svg = charts.range_bars(
            rows, unit="ms", aria=f"PUBACK latency for {io_model} clients", scale_max=scale_max
        )
        if not svg:
            continue
        gated = gated or any(r.get("p99") is None for r in rows)
        boundaries.update(r["boundary"] for r in rows if r.get("boundary"))
        blocks.append(
            f'<div class="group-block">'
            f"{group_title(io_model, [r['client'] for r in rows], meta)}"
            f'<div class="chart-wrap">{svg}</div></div>'
        )
    if not blocks:
        return ""
    footnotes = []
    if gated:
        footnotes.append(
            "A p99 measured from an incomplete sample is withheld rather than published."
        )
    if boundaries:
        footnotes.append(
            "† QoS0 percentiles measured at a non-socket completion boundary "
            f"({', '.join(_esc(b) for b in sorted(boundaries))}) are not comparable with "
            "socket-boundary ones."
        )
    return panel(
        "PUBACK latency",
        "".join(blocks) + "".join(f'<p class="hint">{f}</p>' for f in footnotes),
        hint=(
            "Each span runs from p50 to p99 with the median marked, taken at the highest offered "
            "load that still produced a valid run. <strong>Read this within one client, not across "
            "them:</strong> each library is offered a fraction of <em>its own</em> capacity, so "
            "they are not under the same load. A published 2.95× latency gap collapsed to 1.24× "
            "once the offered rate was matched. The cross-client comparison is "
            "<code>puback_latency_fixed_rate</code>, which has not been run yet. "
            "All three charts share one millisecond scale, so a span twice as long really is "
            "twice the latency."
        ),
        extra_class="panel-caution",
    )


def _latency_rows_for(docs: Sequence[ResultDoc], clients: Sequence[str]) -> List[Dict]:
    rows: List[Dict] = []
    for client in clients:
        best = None
        for doc in docs:
            if (doc.client or "?") != client:
                continue
            for point in doc.points:
                latency = point.latency or {}
                if latency.get("p50_ms") is None or point.status != "valid":
                    continue
                # One point speaks for the client: the highest offered fraction
                # that still produced a valid measurement, which is where the
                # tail actually shows itself.
                if best is None or _fraction_of(point) > _fraction_of(best):
                    best = point
        if best is None:
            continue
        latency = best.latency or {}
        rows.append(
            {
                "client": client,
                "label": f"{client} · {best.label}",
                "p50": latency.get("p50_ms"),
                "p95": latency.get("p95_ms"),
                "p99": latency.get("p99_ms"),
                "boundary": best.latency_boundary,
            }
        )
    return rows


def _fraction_of(point) -> float:
    for part in str(point.label or "").split(", "):
        if part.startswith("load="):
            try:
                return float(part.split("=", 1)[1])
            except ValueError:
                return 0.0
    return 0.0


def _matrix_panel(overview: Overview, meta: Dict[str, ClientMeta]) -> str:
    rows = _order_matrix_scenarios(overview.row_ids)
    html = performance_matrix_html(
        rows, overview.clients, overview.median, meta=meta, cells_by=overview.cells
    )
    return html


def _signals_panel(docs: Sequence[ResultDoc]) -> str:
    return client_signals_html(docs)


def _all_results_panel(docs: Sequence[ResultDoc], meta: Dict[str, ClientMeta]) -> str:
    order = {name: i for i, name in enumerate(_CLIENT_ORDER)}
    ordered = sorted(
        docs,
        key=lambda d: (
            order.get(d.client or "", len(order)),
            d.client or "",
            d.kind,
            d.scenario or d.title,
        ),
    )
    rows = []
    for doc in ordered:
        rows.append(
            [
                f'<a href="{run_href(doc.slug)}">{_esc(doc.title)}</a>',
                f'<span class="badge badge-{_esc(doc.kind)}">{_esc(doc.kind)}</span>',
                client_swatch(doc.client, meta, show_exp=False)
                if doc.client and doc.kind == "scenario"
                else _esc(doc.client or "—"),
                _esc(doc.profile or "—"),
                status_badge(doc.status, doc.non_comparable),
                f'<span class="num">{num(doc.median_msgs_per_s)}</span>',
                str(_doc_point_count(doc)),
                f'<code class="mono">{_esc(doc.source_name)}</code>',
            ]
        )
    return panel(
        "All results",
        table(
            ["Result", "Kind", "Client", "Profile", "Status", "Median msg/s", "Points", "Source"],
            rows,
            css="results-table",
            numeric=(5, 6),
        ),
        hint="Every committed result file, including the ones no ranking may use.",
    )
