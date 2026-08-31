"""Panels whose shape is part of the report's contract.

The performance matrix, the client-issue list and the environment banner all
encode a rule the project cares about more than it cares about looking good: a
number that cannot be trusted must say so where it is shown, not in a footnote.
Their markup is pinned by tests, so changes here are deliberate.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from .components import client_swatch
from .model import (
    ClientMeta,
    EMPTY_GLYPHS,
    PointRow,
    ResultDoc,
    _CLIENT_ORDER,
    _esc,
    _fmt_num,
    _is_tied_with_best,
    _sort_clients,
)

def performance_matrix_html(
    scenarios: Sequence[str],
    clients: Sequence[str],
    by_key: Dict[tuple, Optional[float]],
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
        group = info.peer_group if info else "unknown"
        if not groups or groups[-1][0] != group:
            groups.append((group, [client]))
        else:
            groups[-1][1].append(client)

    group_head = "".join(
        f'<th scope="col" class="group-head group-start" colspan="{len(members)}">'
        f"{_esc(group)}</th>"
        for group, members in groups
    )
    # The group boundary has to be visible on every row, not just in the header:
    # reading a row, the eye has no way to tell where one peer group ends, which
    # is what made an unhighlighted 19,536 next to a highlighted 14,358 look
    # arbitrary rather than "different groups".
    group_first = {members[0] for _group, members in groups}
    # Column headers carry the client and nothing else. The I/O-model badge is
    # already the group header spanning these columns, and the pre-release badge
    # belongs to the client, not to a cell in a table of rates — both were the
    # longest text in the header, which is what made the columns wide and
    # unequal. Stability is on the client pages and the methodology table.
    head = "".join(
        f'<th scope="col" class="num{" group-start" if c in group_first else ""}">'
        f"{client_swatch(c, meta, show_io=False, show_exp=False)}</th>"
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
                    tip_bits.insert(0, f"alone in the {group} group — not ranked")
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
          <p class="hint">Median msg/s per scenario × MQTT protocol × client, comparable runs only. Rows are never mixed across protocols, and the best value is highlighted <strong>within each peer group</strong> — the vertical rules mark those groups, formed by I/O model. So the highest number in a row is often <em>not</em> highlighted: it belongs to another group. Stable and pre-release libraries share a group and compete directly. A client alone in its group is shown in outline and never crowned, because there is nothing to rank it against. A dotted underline marks a number the harness did not attribute to the client itself; hover any cell for its bottleneck, run count and spread.</p>
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


def client_signals_html(docs: Sequence[ResultDoc]) -> str:
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
        swatch = client_swatch(client)
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


def environment_warnings_html(docs: Sequence[ResultDoc]) -> str:
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
