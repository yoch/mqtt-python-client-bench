"""The state of the measurement corpus: what is covered, what is not, what broke.

The environment banner used to be the first thing on the site — a full-width red
table above every result, which made the report read as a failure log. The
information is worth keeping and worth being blunt about, so it moved here, next
to the other honest accounting: which cells were never measured, which scenarios
in the catalogue have never been run at all, and which hosts were skipped.
"""

from __future__ import annotations

from typing import Dict, Optional, Sequence, Tuple

from .. import charts
from ..aggregate import coverage
from ..catalog import facts_for
from ..components import num, panel, scenario_href, stat_tile, table
from ..model import ResultDoc, _client_meta, _esc, _sort_clients
from ..panels import environment_warnings_html
from ..shell import crumb, hero, page_shell, stats_row

try:
    from mqtt_client_bench.scenarios import SCENARIOS
except Exception:  # pragma: no cover - the report must still build
    SCENARIOS = []


def render(
    docs: Sequence[ResultDoc],
    generated_at: str,
    *,
    skipped_by_host: Optional[Dict[str, int]] = None,
) -> str:
    meta = _client_meta(docs)
    clients, scenarios, values, counts = coverage(docs)
    clients = _sort_clients(clients, meta)

    valid = sum(v for v, _ in counts.values())
    total = sum(t for _, t in counts.values())
    measured = {s for s in scenarios}
    catalogued = [(s.name, s.suite) for s in SCENARIOS]
    never_run = [(name, suite) for name, suite in catalogued if name not in measured]

    tiles = [
        stat_tile("Valid runs", num(valid, digits=0), f"of {num(total, digits=0)}" if total else ""),
        stat_tile(
            "Coverage",
            f"{(valid / total * 100):.0f}%" if total else "—",
            "of attempted runs",
        ),
        stat_tile("Scenarios measured", f"{len(measured)}", f"of {len(catalogued)} catalogued"),
        stat_tile("Clients", str(len(clients))),
    ]

    body = f"""
    <main>
      {crumb("index.html", "Overview")}
{hero(
    "Corpus",
    "What has been measured, how much of it survived the gates, and what is still missing. "
    "A run that the broker or the host constrained is discarded rather than published, so the "
    "gap between attempted and valid runs is the honest cost of measuring on one machine.",
    f"generated {_esc(generated_at)}",
)}
      {stats_row(tiles)}
{_coverage_panel(clients, scenarios, values, counts)}
{_environment_panel(docs)}
{_never_run_panel(never_run)}
{_skipped_panel(skipped_by_host)}
    </main>
"""
    return page_shell("Corpus", body, active="corpus")


def _coverage_panel(clients, scenarios, values, counts) -> str:
    if not clients or not scenarios:
        return ""

    def tip(row: str, col: str, value: float) -> str:
        valid, total = counts.get((row, col), (0, 0))
        return f"{row} · {col}: {valid} of {total} runs valid ({value:.0%})"

    svg = charts.heatmap(
        clients,
        scenarios,
        values,
        aria="Valid-run coverage by client and scenario",
        cell_tip=tip,
    )
    if not svg:
        return ""
    dead_scenarios = sorted(
        {
            s
            for s in scenarios
            if all((values.get((c, s)) or 0) == 0 for c in clients if (c, s) in values)
        }
    )
    footer = ""
    if dead_scenarios:
        items = "".join(
            f'<li><a href="{scenario_href(s)}"><code class="mono">{_esc(s)}</code></a> — '
            f"{_esc(_dead_reason(s))}</li>"
            for s in dead_scenarios
        )
        footer = (
            '<div class="callout"><h3 class="sub">Scenarios with no valid run at all</h3>'
            f"<ul class=\"bullets\">{items}</ul>"
            "<p>These were attempted and are shown on their scenario pages with the observed "
            "numbers greyed out and the invalidation named. They are measurement debt, not "
            "library results.</p></div>"
        )
    return panel(
        "Coverage",
        f'<div class="chart-wrap">{svg}</div>{footer}',
        hint="Share of attempted runs that passed every gate, per client and scenario. "
             "A pale cell is not a slow client — it is a cell whose numbers the harness "
             "refused to publish.",
    )


def _dead_reason(scenario: str) -> str:
    facts = facts_for(scenario)
    if facts.caveats:
        return facts.caveats[0]
    return "every run was invalidated before it could enter a ranking."


def _environment_panel(docs: Sequence[ResultDoc]) -> str:
    return environment_warnings_html(docs)


def _never_run_panel(never_run: Sequence[Tuple[str, str]]) -> str:
    if not never_run:
        return ""
    rows = []
    for name, suite in sorted(never_run, key=lambda kv: (kv[1], kv[0])):
        facts = facts_for(name)
        rows.append(
            [
                f'<code class="mono">{_esc(name)}</code>',
                f'<span class="badge badge-suite">{_esc(suite)}</span>',
                _esc(facts.question or "—"),
                _esc(facts.metric),
            ]
        )
    highlight = ""
    if any(name in ("puback_latency_fixed_rate", "application_rtt_fixed_rate") for name, _ in never_run):
        highlight = (
            '<p class="note note-warn"><code>puback_latency_fixed_rate</code> and '
            "<code>application_rtt_fixed_rate</code> are the public cross-client latency "
            "rankings — every client offered the same absolute rate. Until they have been "
            "run, the latency figures on this site are intra-client readings only.</p>"
        )
    return panel(
        "Catalogued but never run",
        highlight
        + table(
            ["Scenario", "Suite", "Question it would answer", "Primary metric"],
            rows,
            css="results-table",
        ),
        hint="Scenarios that exist in the catalogue and are executable, but for which no result "
             "has been committed. Listed rather than hidden, so the shape of the gap is visible.",
        extra_class="panel-caution",
    )


def _skipped_panel(skipped_by_host: Optional[Dict[str, int]]) -> str:
    if not skipped_by_host:
        return ""
    rows = [
        [f'<code class="mono">{_esc(host)}</code>', f'<span class="num">{count}</span>']
        for host, count in sorted(skipped_by_host.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    return panel(
        "Skipped: measured on another host",
        table(["Host", "Result files"], rows, css="results-table", numeric=(1,)),
        hint="One host per published site. Results from a machine other than the reference host "
             "are counted here rather than silently dropped, because a number is only meaningful "
             "against the ceilings of the machine that produced it.",
    )
