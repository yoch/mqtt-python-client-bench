"""One page per client library: identity, peers, refusals, and every result.

The site could tell you that gmqtt scored 33k on a scenario, but not what gmqtt
*is* — which completion boundary it honours, which private attributes the adapter
has to reach into, or which points it declines outright and why. Those facts are
already in every result file under ``client_identity``; they had nowhere to land.
"""

from __future__ import annotations

from collections import Counter
from typing import Dict, List, Optional, Sequence

from ..aggregate import peer_groups, protocol_aggregates
from ..catalog import facts_for
from ..components import (
    client_swatch,
    kv_list,
    num,
    panel,
    run_href,
    scenario_href,
    stat_tile,
    status_badge,
    table,
)
from ..model import ClientMeta, ResultDoc, _client_meta, _esc, _slug, _sort_clients
from ..shell import crumb, hero, page_shell, stats_row


def render_all(docs: Sequence[ResultDoc], generated_at: str) -> Dict[str, str]:
    meta = _client_meta(docs)
    names = _sort_clients(
        sorted({d.client for d in docs if d.kind == "scenario" and d.client}), meta
    )
    pages = {_slug(name): render(name, docs, meta, generated_at) for name in names}
    pages["index"] = render_listing(names, docs, meta, generated_at)
    return pages


def render_listing(
    names: Sequence[str], docs: Sequence[ResultDoc], meta: Dict[str, ClientMeta], generated_at: str
) -> str:
    groups = peer_groups(names, meta)
    sections: List[str] = []
    for io_model, members in groups:
        rows = []
        for client in members:
            info = meta.get(client)
            scoped = [d for d in docs if d.client == client and d.kind == "scenario"]
            valid = sum(p.valid_runs for d in scoped for p in d.points)
            total = sum(p.total_runs for d in scoped for p in d.points)
            refusals = Counter()
            for doc in scoped:
                refusals.update(doc.capability_reasons)
            rows.append(
                [
                    f'<a href="{_slug(client)}.html">{client_swatch(client, meta, link=False)}</a>',
                    _esc((info.version if info else None) or "—"),
                    f'<span class="num">{len(scoped)}</span>',
                    f'<span class="num">{valid} / {total}</span>',
                    ", ".join(f'<code class="mono">{_esc(r)}</code>' for r in sorted(refusals)) or "—",
                ]
            )
        sections.append(
            panel(
                f"{_esc(io_model)}",
                table(
                    ["Client", "Version", "Scenarios", "Valid runs", "Declined capabilities"],
                    rows,
                    css="results-table",
                    numeric=(2, 3),
                ),
                hint="Clients in one panel are substitutes for one another; a ranking across "
                     "panels is not meaningful.",
            )
        )
    body = f"""
    <main>
      {crumb("../index.html", "Overview")}
{hero("Clients", "The libraries under test, grouped by the peer group they may be ranked inside.",
      f"generated {_esc(generated_at)}")}
{"".join(sections)}
    </main>
"""
    return page_shell("Clients", body, root="..", active="clients")


def render(
    client: str, docs: Sequence[ResultDoc], meta: Dict[str, ClientMeta], generated_at: str
) -> str:
    scoped = [d for d in docs if d.client == client and d.kind == "scenario"]
    info = meta.get(client)
    identity = _identity_of(scoped)
    peers = [
        name
        for name, other in meta.items()
        if info and other.peer_group == info.peer_group and name != client
    ]

    valid = sum(p.valid_runs for d in scoped for p in d.points)
    total = sum(p.total_runs for d in scoped for p in d.points)
    refusals: Counter = Counter()
    load: Counter = Counter()
    env: Counter = Counter()
    for doc in scoped:
        refusals.update(doc.capability_reasons)
        load.update(doc.load_reasons)
        env.update(doc.environment_reasons)

    tiles = [
        stat_tile("Version", _esc((info.version if info else None) or "—")),
        stat_tile("I/O model", _esc(info.io_model if info else "unknown")),
        stat_tile("Stability", _esc(info.stability if info else "unknown")),
        stat_tile("Valid runs", str(valid), f"of {total}" if total else ""),
    ]

    body = f"""
    <main>
      {crumb("index.html", "All clients")}
{hero(
    client_swatch(client, meta, link=False),
    _lead_for(client, identity, info, peers),
    f"generated {_esc(generated_at)}",
)}
      {stats_row(tiles)}
{_identity_panel(identity, info, peers, meta)}
{_capability_panel(refusals)}
{_results_panel(scoped, meta)}
{_signal_panel(load, env)}
    </main>
"""
    return page_shell(f"{client} — client", body, root="..", active="clients")


def _identity_of(docs: Sequence[ResultDoc]) -> Dict[str, str]:
    """Pull the client_identity block out of the newest result that has one."""
    for doc in docs:
        raw = doc.raw_meta.get("client_identity") if doc.raw_meta else None
        if raw:
            return dict(raw)
    return {}


def _module_label(path: Optional[str]) -> str:
    """Show where the library was imported from, not where the machine keeps it.

    ``client_module`` is an absolute path on the machine that ran the campaign.
    The interesting part is which installed package answered the import; the
    operator's home directory is noise on a published page.
    """
    if not path:
        return "—"
    text = str(path)
    for marker in ("site-packages/", "dist-packages/", "/src/"):
        if marker in text:
            return text.split(marker, 1)[1]
    return text.rsplit("/", 2)[-2] + "/" + text.rsplit("/", 1)[-1] if "/" in text else text


def _lead_for(client: str, identity: Dict[str, str], info, peers: Sequence[str]) -> str:
    """One sentence saying what this library is and who it may be ranked against."""
    version = identity.get("client_version") or (info.version if info else None)
    io_model = identity.get("io_model") or (info.io_model if info else "unknown")
    stability = identity.get("stability") or (info.stability if info else "unknown")
    shape = {
        "sync": "a blocking/callback API driven directly by the role worker",
        "asyncio_bridged": "an asyncio library driven on the role worker's own event loop",
        "crt_event_loop": "a native, non-Python engine",
    }.get(io_model, "a client library under test")
    lead = f"{_esc(client)}"
    if version:
        lead += f" {_esc(version)} — "
    else:
        lead += " — "
    lead += f"{shape}, tracked as <strong>{_esc(stability)}</strong>."
    if peers:
        lead += " Ranked against " + ", ".join(f"<strong>{_esc(p)}</strong>" for p in sorted(peers)) + "."
    else:
        lead += " Alone in its peer group, so it is shown but never crowned."
    return lead


def _identity_panel(identity, info, peers, meta) -> str:
    boundary = identity.get("qos0_boundary")
    pairs = [
        ("Adapter", _esc(identity.get("adapter") or "—")),
        ("Imported from", f'<code class="mono">{_esc(_module_label(identity.get("client_module")))}</code>'),
        ("Version", _esc(identity.get("client_version") or (info.version if info else "") or "—")),
        ("Implementation language", _esc(identity.get("implementation_language") or "—")),
        ("Completion mechanism", f'<code class="mono">{_esc(identity.get("completion_mechanism") or "—")}</code>'),
        (
            "QoS0 completion boundary",
            f'<code class="mono">{_esc(boundary or "not declared")}</code>'
            + (
                " — percentiles at this boundary are not comparable with socket-boundary ones"
                if boundary and boundary != "socket"
                else ""
            ),
        ),
        ("Synthetic message ids", "yes" if identity.get("synthetic_mids") else "no"),
        (
            "Comparable with",
            ", ".join(client_swatch(p, meta) for p in sorted(peers)) or "nothing — alone in its peer group",
        ),
    ]
    private = identity.get("private_api") or (info.private_api if info else {}) or {}
    private_html = ""
    if private:
        private_html = (
            '<h3 class="sub">Library internals this adapter depends on</h3>'
            + table(
                ["Private symbol", "Why the adapter needs it"],
                [
                    [f'<code class="mono">{_esc(k)}</code>', _esc(v)]
                    for k, v in sorted(private.items())
                ],
                css="results-table",
                sortable=False,
            )
        )
    return panel(
        "Identity",
        kv_list(pairs, css="kv kv-wide") + private_html,
        hint="Read from the result files rather than from a table in the report, so it stays "
             "true to what actually ran. Reaching into a library's private API changes what is "
             "being measured, which is why it is declared here.",
    )


def _capability_panel(refusals: Counter) -> str:
    if not refusals:
        return ""
    rows = [
        [f'<code class="mono">{_esc(name)}</code>', f'<span class="num">{count}</span>']
        for name, count in sorted(refusals.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    return panel(
        "Declined capabilities",
        table(["Feature", "Points refused"], rows, css="results-table", numeric=(1,)),
        hint="A capability the library cannot honour is declared missing so the point comes back "
             "<code>inconclusive</code>. Approximating it to make a point run would put a number "
             "on the site that means something different from its neighbours.",
        extra_class="panel-caution",
    )


def _results_panel(docs: Sequence[ResultDoc], meta) -> str:
    rows = []
    for doc in sorted(docs, key=lambda d: (d.scenario or d.title)):
        scenario = doc.scenario or doc.title
        facts = facts_for(scenario)
        protos = protocol_aggregates(doc)
        rows.append(
            [
                f'<a href="{scenario_href(scenario, root="..")}">{_esc(scenario)}</a>',
                _esc(facts.metric),
                status_badge(doc.status, doc.non_comparable),
                f'<span class="num">{num(doc.median_msgs_per_s)}</span>',
                " · ".join(
                    f'{_esc(p)} <span class="num">{num(v[0])}</span>' for p, v in sorted(protos.items())
                ) or "—",
                f'<a href="{run_href(doc.slug, root="..")}">detail</a>',
            ]
        )
    return panel(
        "Results",
        table(
            ["Scenario", "Measures", "Status", "Median msg/s", "Per protocol", ""],
            rows,
            css="results-table",
            numeric=(3,),
        ),
    )


def _signal_panel(load: Counter, env: Counter) -> str:
    if not load and not env:
        return ""
    rows = [
        ['<span class="badge badge-partial">under load</span>',
         f'<code class="mono">{_esc(k)}</code>', f'<span class="num">{v}</span>']
        for k, v in sorted(load.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    rows += [
        ['<span class="badge badge-inconclusive">environment</span>',
         f'<code class="mono">{_esc(k)}</code>', f'<span class="num">{v}</span>']
        for k, v in sorted(env.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    return panel(
        "Why runs were discarded",
        table(["Kind", "Signal", "Runs"], rows, css="results-table", numeric=(2,)),
        hint="An <strong>under load</strong> signal is about this client; an "
             "<strong>environment</strong> signal is about the broker or the host and says "
             "nothing about the library.",
    )
