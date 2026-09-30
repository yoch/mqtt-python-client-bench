"""The site's pages: results by question, one page per point and per client,
coverage, and a methodology generated from the checks themselves."""

from __future__ import annotations

from typing import Dict, List, Optional

from mqtt_client_bench.bench.checks import (
    CHECK_DOCS,
    FLAG_DOCS,
    INVALID,
    NOT_SUSTAINED,
    STATUS_DOCS,
    VALID,
    tolerance,
)
from mqtt_client_bench.report.chart import latency_chart
from mqtt_client_bench.report.data import Campaign
from mqtt_client_bench.report.html import (
    STATE_TEXT,
    cell_html,
    client_link,
    e,
    flag_tags,
    fmt,
    metric_head,
    page,
    point_link,
    run_detail,
    table,
)
from mqtt_client_bench.report.views import (
    METRICS,
    QUESTIONS,
    Column,
    payload_label,
    point_columns,
    rate_label,
    sort_key,
)

COLORS = {
    "paho": "#1f77b4",
    "gmqtt": "#d62728",
    "aiomqtt": "#2ca02c",
    "aiomqtt3": "#98df8a",
    "amqtt": "#9467bd",
    "awscrt": "#ff7f0e",
    "zmqtt": "#8c564b",
    "mqttium": "#e377c2",
}
FALLBACK_COLORS = ("#7f7f7f", "#bcbd22", "#17becf", "#aec7e8")


def client_colors(clients: List[str]) -> Dict[str, str]:
    extra = iter(FALLBACK_COLORS * (1 + len(clients) // len(FALLBACK_COLORS)))
    return {c: COLORS.get(c) or next(extra) for c in clients}


def _points_by_name(campaign: Campaign) -> Dict[str, dict]:
    return {p["name"]: p for p in campaign.points}


def _ranked(campaign: Campaign, point: str, metric: str) -> List[str]:
    """Clients by the cell's value, best first; clients without one after, by state."""
    order = {VALID: 0, NOT_SUSTAINED: 1, INVALID: 2, "missing": 3, "unsupported": 4}

    def key(client: str):
        cell = campaign.cell(point, client)
        value = cell.value(metric) if cell.state == VALID else None
        rank = sort_key(metric, value)
        if cell.bounded(metric) == "offer":
            # Everyone who took the whole offer ties; their order is the noise.
            rank = (0, float("-inf"))
        return (order[cell.state], rank, client)

    return sorted(campaign.clients, key=key)


def _best(campaign: Campaign, point: str, metric: str) -> Optional[float]:
    values = [campaign.cell(point, c).value(metric) for c in campaign.clients if campaign.cell(point, c).state == VALID]
    values = [v for v in values if v is not None]
    return max(values) if values else None


def _ceiling(session: dict) -> str:
    rate = (session.get("ceiling") or {}).get("msgs_per_s")
    return f"{rate:,.0f} msgs/s" if rate else "not measured"


def _span(values: List[str]) -> str:
    distinct = list(dict.fromkeys(values))
    return distinct[0] if len(distinct) == 1 else " / ".join(distinct)


def _session_facts(campaign: Campaign) -> str:
    sessions = campaign.sessions or [{}]
    host = (sessions[-1].get("host") or {})
    image = str((sessions[-1].get("broker") or {}).get("image") or "?").split("@")[0]
    floors = [max(v.values()) for v in ((s.get("harness_cost") or {}).get("ns_per_msg") for s in sessions) if v]
    profile = campaign.manifest.get("profile") or {}
    runs = f"{campaign.manifest.get('runs_per_point', '?')} per point, {profile.get('measure_s', '?')} s window, clients interleaved"
    if len(sessions) > 1:
        runs += f"; {len(sessions)} sessions (see methodology)"
    facts = [
        ("Machine", f"{host.get('cpu_model', '?')}, {host.get('physical_cores', '?')} cores, governor {host.get('governor', '?')}"),
        ("Broker", f"{image}, one core"),
        ("Broker C→C ceiling", _span([_ceiling(s) for s in sessions])),
        ("Runs", runs),
        ("Harness floor", f"≤ {max(floors):.0f} ns per message" if floors else "not measured"),
        ("Date", campaign.manifest.get("started_at", "?")),
    ]
    items = "".join(f"<div><dt>{e(k)}</dt><dd>{e(v)}</dd></div>" for k, v in facts)
    return f'<dl class="facts">{items}</dl>'


def _question_table(campaign: Campaign, columns: List[Column]) -> str:
    first = columns[0]
    points = _points_by_name(campaign)
    head = ["client"]
    for c in columns:
        unit = METRICS[c.metric].label
        if unit in c.label:
            head.append(point_link(points[c.point], 0, c.label) + " " + metric_head(c.metric, ""))
        else:
            head.append(point_link(points[c.point], 0, c.label) + "<br>" + metric_head(c.metric))
    best = {(c.point, c.metric): _best(campaign, c.point, c.metric) for c in columns}
    rows = []
    for client in _ranked(campaign, first.point, first.metric):
        cells = "".join(cell_html(campaign.cell(c.point, client), c.metric, best=best[(c.point, c.metric)]) for c in columns)
        rows.append(f"<tr><th>{client_link(campaign, client, 0)}</th>{cells}</tr>")
    return table(head, rows, "results")


def index_page(campaign: Campaign, skipped: List[str]) -> str:
    points = _points_by_name(campaign)
    sections, toc, used = [], [], set()
    for q in QUESTIONS:
        columns = [c for c in q.columns if c.point in points]
        if not columns:
            continue
        used.update(c.point for c in columns)
        toc.append(f'<li><a href="#{q.slug}">{e(q.title)}</a></li>')
        sections.append(
            f'<section id="{q.slug}"><h2>{e(q.title)}</h2><p class="lede">{e(q.lede)}</p>'
            f"{_question_table(campaign, columns)}</section>"
        )
    others = [p for p in campaign.points if p["name"] not in used]
    if others:
        items = "".join(f"<li>{point_link(p, 0)} — {e(p['question'])}</li>" for p in others)
        sections.append(f'<section id="other"><h2>Other points</h2><ul>{items}</ul></section>')
    skipped_note = (
        f'<p class="note">Not published: {e(", ".join(skipped))} (development or older campaigns).</p>' if skipped else ""
    )
    body = f"""<h1>Python MQTT clients, measured by what can be checked</h1>
<p class="lede">{len(campaign.clients)} client libraries against one Mosquitto on one machine. Every rate is a count the broker's
<code>$SYS</code> counters and a neutral C peer confirm; every cost is read from <code>/proc</code> from outside the client.
Tables sort best first; click a value to unfold the raw counts and checks of each run. Only valid runs enter a value.</p>
{_session_facts(campaign)}
<nav class="toc"><ol>{"".join(toc)}</ol></nav>
{"".join(sections)}
{skipped_note}"""
    return page("Results", body, depth=0, campaign=campaign, current="index.html")


def point_page(campaign: Campaign, point: dict) -> str:
    metrics = point_columns(point)
    best = {m: _best(campaign, point["name"], m) for m in metrics}
    rows = []
    for client in _ranked(campaign, point["name"], metrics[0]):
        cell = campaign.cell(point["name"], client)
        cells = "".join(cell_html(cell, m, best=best[m]) for m in metrics)
        runs = f"{len(cell.valid)}/{len(cell.runs)}" if cell.runs else "—"
        rows.append(f"<tr><th>{client_link(campaign, client, 1)}</th><td class=\"runs\">{runs}</td>{cells}</tr>")
    head = ["client", '<span title="valid runs / runs">valid</span>'] + [metric_head(m) for m in metrics]
    chart = ""
    if point["rate"] and point["kind"] != "idle":
        hists = {}
        for client in campaign.clients:
            h = campaign.cell(point["name"], client).latency()
            if h:
                hists[client] = h
        if hists:
            chart = "<h2>Latency by percentile</h2>" + latency_chart(hists, client_colors(campaign.clients))
    spec = (
        f"{point['kind']} · QoS {point['qos']} · {payload_label(point['payload'])} · {rate_label(point)} · {point['protocol']}"
    )
    body = f"""<p class="crumb"><a href="../index.html">Results</a> / {e(point["name"])}</p>
<h1>{e(point["name"])}</h1>
<p class="lede">{e(point["question"])}</p>
<p class="spec">{e(spec)} · <a href="../methodology.html#points">how it is wired</a></p>
{table(head, rows, "results")}
{chart}"""
    return page(point["name"], body, depth=1, campaign=campaign)


def client_page(campaign: Campaign, client: str) -> str:
    doc = campaign.docs.get(client) or {}
    identity = doc.get("identity") or {}
    shapes = sorted({(r.get("worker") or {}).get("shape") for r in doc.get("runs", [])} - {None})
    pythons = sorted({(r.get("worker") or {}).get("python") for r in doc.get("runs", [])} - {None})
    facts = [
        ("version", doc.get("version") or campaign.manifest.get("clients", {}).get(client)),
        ("I/O model", identity.get("io_model")),
        ("implementation", identity.get("implementation_language")),
        ("stability", identity.get("stability")),
        ("completion", identity.get("completion_mechanism")),
        ("drive shape", ", ".join(shapes)),
        ("Python", ", ".join(pythons)),
    ]
    rows = "".join(f"<tr><th>{e(k)}</th><td>{e(v)}</td></tr>" for k, v in facts if v)
    private = identity.get("private_api") or {}
    private_html = ""
    if private:
        items = "".join(f"<li><code>{e(k)}</code>: {e(v)}</li>" for k, v in private.items())
        private_html = f"<h2>Private API the adapter depends on</h2><ul>{items}</ul>"
    refused = doc.get("unsupported") or []
    refused_html = ""
    if refused:
        items = "".join(f"<li>{e(u['point'])}: {e(', '.join(u['reasons']))}</li>" for u in refused)
        refused_html = f"<h2>Refused</h2><p>Declared unsupported by the adapter, never approximated.</p><ul>{items}</ul>"
    point_rows = []
    for point in campaign.points:
        cell = campaign.cell(point["name"], client)
        headline = point_columns(point)[0]
        state = cell.state
        value = fmt(headline, cell.value(headline)) if state == VALID else STATE_TEXT[state]
        cpu = fmt("cpu_us_per_msg", cell.value("cpu_us_per_msg")) if state == VALID and point["kind"] != "idle" else ""
        rss = fmt("rss_peak_kb", cell.value("rss_peak_kb")) if state == VALID else ""
        point_rows.append(
            f"<tr><th>{point_link(point, 1)}</th><td>{e(METRICS[headline].label)}</td><td>{e(value)}</td>"
            f"<td>{e(cpu)}</td><td>{e(rss)}</td><td>{flag_tags(cell.flags())}</td></tr>"
        )
    body = f"""<p class="crumb"><a href="../index.html">Results</a> / {e(client)}</p>
<h1>{client_link(campaign, client, 1)}</h1>
<table class="kv">{rows}</table>
{private_html}
{refused_html}
<h2>Every point</h2>
{table(["point", "headline", "value", "CPU / msg", "peak RSS", "flags"], point_rows, "results")}"""
    return page(client, body, depth=1, campaign=campaign)


def coverage_page(campaign: Campaign) -> str:
    head = ["point"] + [e(c) for c in campaign.clients]
    rows = []
    for point in campaign.points:
        cells = []
        for client in campaign.clients:
            cell = campaign.cell(point["name"], client)
            state = cell.state
            if state == "unsupported":
                cells.append(f'<td class="cov unsupported" title="{e(", ".join(cell.unsupported or []))}">—</td>')
                continue
            text = f"{len(cell.valid)}/{len(cell.runs)}" if cell.runs else "0"
            extra = f" +{len(cell.retried)} retried" if cell.retried else ""
            cells.append(f'<td class="cov {e(state)}">{text}{extra}</td>')
        rows.append(f"<tr><th>{point_link(point, 0)}</th>{''.join(cells)}</tr>")
    problems = []
    for point in campaign.points:
        for client in campaign.clients:
            cell = campaign.cell(point["name"], client)
            for r in cell.runs + cell.retried:
                if r["status"] != VALID:
                    problems.append(f"<details><summary>{e(point['name'])} · {e(client)} · {e(r['status'])}</summary>{run_detail(r)}</details>")
    listing = "".join(problems) or "<p>Every run is valid.</p>"
    body = f"""<h1>Coverage</h1>
<p class="lede">What was run, what counted, and what did not. A cell reads valid runs / runs; “retried” counts invalid
attempts the campaign ran again; “—” is a point the client's adapter declares unsupported (hover for why).</p>
<p>{campaign.done_runs()} of {campaign.expected_runs()} planned runs are recorded.</p>
{table(head, rows, "coverage")}
<h2>Runs that did not count</h2>
{listing}"""
    return page("Coverage", body, depth=0, campaign=campaign, current="coverage.html")


def _dl(items) -> str:
    return "<dl>" + "".join(f"<dt><code>{e(k)}</code></dt><dd>{e(v)}</dd>" for k, v in items) + "</dl>"


def methodology_page(campaign: Campaign, data_files: List[str]) -> str:
    slack = (tolerance(1_000_000) - tolerance(0)) / 1_000_000
    points = "".join(
        f"<tr><th>{point_link(p, 0)}</th><td>{e(p['kind'])}</td><td>{p['qos']}</td><td>{e(payload_label(p['payload']))}</td>"
        f"<td>{e(rate_label(p))}</td><td>{e(p['protocol'])}</td><td>{e(p['question'])}</td></tr>"
        for p in campaign.points
    )
    sessions = []
    for i, s in enumerate(campaign.sessions):
        host, broker, harness = s.get("host") or {}, s.get("broker") or {}, s.get("harness") or {}
        cost = s.get("harness_cost") or {}
        per_shape = ", ".join(f"{k} {v:.0f} ns" for k, v in (cost.get("ns_per_msg") or {}).items())
        facts = [
            ("host", f"{host.get('hostname')} · {host.get('cpu_model')} · kernel {host.get('kernel')} · governor {host.get('governor')}"),
            ("cores", ", ".join(f"{k} {v}" for k, v in (s.get("cpusets") or {}).items())),
            ("broker", f"{broker.get('image')} · config {str(broker.get('config_hash'))[:12]}"),
            ("harness", f"{harness.get('commit')}{' (dirty)' if harness.get('dirty') else ''} · peer {harness.get('peer_digest')}"),
            (
                "harness floor",
                f"{per_shape} · worker RSS floor {cost.get('baseline_rss_kb', '?')} KiB · budget {cost.get('budget_ns', '?')} ns"
                if per_shape
                else f"not measured{': ' + str(cost['error']) if cost.get('error') else ''}",
            ),
            ("broker C→C ceiling", _ceiling(s)),
            ("receive offer", f"{s['sub_offer']:,} msgs/s" if s.get("sub_offer") else "not recorded"),
        ]
        sessions.append(f"<h3>Session {i + 1}</h3>" + _dl(facts))
    profile = campaign.manifest.get("profile") or {}
    downloads = "".join(f'<li><a href="data/{e(f)}">{e(f)}</a></li>' for f in data_files)
    body = f"""<h1>Methodology</h1>
<p class="lede">Generated from the harness's own check definitions and this campaign's manifest, so it cannot drift from
what was enforced.</p>

<h2>Three parties, no shared code</h2>
<p>The client under test runs alone in its own process and uv environment. Across the broker from it is a neutral C peer
(sink, source or echo). Mosquitto's <code>$SYS</code> counters are read fresh before and after each run. Each party has its own
physical core, and the orchestrator reads CPU, memory and context switches from <code>/proc</code> from outside the client.
Worker and peer follow one absolute <code>CLOCK_MONOTONIC</code> schedule: warm-up {profile.get("warmup_s")} s, a measured
window of {profile.get("measure_s")} s, then {profile.get("drain_s")} s of drain so totals reconcile. Latency is the time from a
stamp in the first 8 payload bytes to arrival, binned the same way in C and Python (16 buckets per power of two, at most
6.25 % wide); a cell's percentiles come from the merged histogram of its valid runs.</p>

<h2>Statuses</h2>
{_dl(STATUS_DOCS.items())}

<h2>Checks</h2>
<p>Counts from two parties must agree within {tolerance(0)} + {slack:.2%} of the count.</p>
{_dl(CHECK_DOCS.items())}

<h2>Flags</h2>
<p>A flag qualifies a valid run without invalidating it.</p>
{_dl(FLAG_DOCS.items())}

<h2>Metrics</h2>
{_dl((m.label, m.help + (" Higher" if m.better == "higher" else " Lower") + " is better.") for m in METRICS.values())}

<h2 id="points">Points</h2>
<p>Fixed-rate points offer every client the identical absolute load, so their costs and latencies compare across all
libraries. A client that cannot hold the offer is reported as not sustained rather than timed on its backlog.</p>
{table(["point", "kind", "QoS", "payload", "load", "protocol", "question"], [points], "points")}

<h2>Sessions</h2>
{"".join(sessions) or "<p>No session recorded.</p>"}

<h2>Raw data</h2>
<p>Every run of this campaign, with its counts, checks and histograms, as the harness wrote it.</p>
<ul>{downloads}</ul>"""
    return page("Methodology", body, depth=0, campaign=campaign, current="methodology.html")


def empty_page(skipped: List[str]) -> str:
    note = f"<p>Campaigns present but not publishable: {e(', '.join(skipped))}.</p>" if skipped else ""
    body = f"""<h1>No comparable campaign yet</h1>
<p class="lede">The report publishes the newest campaign run with the standard profile on a qualified host.</p>{note}"""
    return page("Results", body, depth=0, campaign=None, current="index.html")
