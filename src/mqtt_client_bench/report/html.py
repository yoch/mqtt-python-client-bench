"""HTML building blocks: page shell, badges, and the expandable result cell."""

from __future__ import annotations

import hashlib
from html import escape
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from mqtt_client_bench.bench.checks import CHECK_DOCS, FLAG_DOCS, INVALID, NOT_SUSTAINED, VALID
from mqtt_client_bench.report.data import Campaign, Cell
from mqtt_client_bench.report.views import METRICS

STYLE = Path(__file__).with_name("style.css")
# Versioned so a browser or the Pages CDN never pairs new markup with an old sheet.
STYLE_HREF = "style.css?v=" + hashlib.sha256(STYLE.read_bytes()).hexdigest()[:10]

NAV = (("index.html", "Results"), ("coverage.html", "Coverage"), ("methodology.html", "Methodology"))

STATE_TEXT = {
    NOT_SUSTAINED: "not sustained",
    INVALID: "invalid",
    "unsupported": "unsupported",
    "missing": "not run",
}


def e(value: object) -> str:
    return escape(str(value), quote=True)


def page(title: str, body: str, *, depth: int, campaign: Optional[Campaign], current: str = "") -> str:
    up = "../" * depth
    nav = "".join(
        f'<a href="{up}{href}"{" aria-current=page" if href == current else ""}>{e(text)}</a>'
        for href, text in (NAV if campaign is not None else NAV[:1])
    )
    banner = ""
    if campaign is not None and not campaign.comparable:
        banner += (
            '<p class="banner">Development campaign ('
            + e((campaign.manifest.get("profile") or {}).get("name", "?"))
            + " profile): short windows on an unqualified host. Not comparable, not for publication.</p>"
        )
    if campaign is not None and len(campaign.hosts()) > 1:
        banner += '<p class="banner">This campaign spans several machines: ' + e(", ".join(campaign.hosts())) + ".</p>"
    if campaign is not None and campaign.done_runs() < campaign.expected_runs():
        banner += (
            f'<p class="banner">Incomplete campaign: {campaign.done_runs()} of {campaign.expected_runs()} runs.</p>'
        )
    foot = f"Campaign {e(campaign.name)}" if campaign else "No campaign"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(title)} · MQTT Python client bench</title>
<link rel="stylesheet" href="{up}{STYLE_HREF}">
</head>
<body>
<header><a class="brand" href="{up}index.html">MQTT Python client bench</a><nav>{nav}</nav></header>
<main>
{banner}{body}
</main>
<footer>{foot} · every figure is a count confirmed by the broker or the C peer, or a resource read from /proc.</footer>
</body>
</html>
"""


def client_link(campaign: Campaign, client: str, depth: int) -> str:
    identity = (campaign.docs.get(client) or {}).get("identity") or {}
    version = (campaign.docs.get(client) or {}).get("version") or campaign.manifest.get("clients", {}).get(client) or ""
    badges = [f'<span class="badge io">{e(identity["io_model"])}</span>'] if identity.get("io_model") else []
    if identity.get("implementation_language") == "native":
        badges.append('<span class="badge native">native</span>')
    if identity.get("stability") and identity["stability"] != "stable":
        badges.append(f'<span class="badge exp">{e(identity["stability"])}</span>')
    return (
        f'<a class="client" href="{"../" * depth}client/{e(client)}.html">{e(client)}</a>'
        f' <span class="version">{e(version)}</span> ' + " ".join(badges)
    )


def point_link(point: dict, depth: int, text: Optional[str] = None) -> str:
    return f'<a href="{"../" * depth}point/{e(point["name"])}.html">{e(text or point["name"])}</a>'


def fmt(metric: str, value: Optional[float]) -> str:
    return "—" if value is None else METRICS[metric].fmt(value)


def flag_tags(flags: Iterable[str]) -> str:
    return "".join(f'<span class="flag" title="{e(FLAG_DOCS.get(f, f))}">{e(f.replace("_", " "))}</span>' for f in flags)


def _counts(record: dict) -> List[tuple]:
    """The raw counts behind a run, each attributed to the party that produced it."""
    kind = record["point"]["kind"]
    m = record.get("metrics") or {}
    w = (record.get("worker") or {}).get("final") or {}
    p = record.get("peer") or {}
    s = ((record.get("broker") or {}).get("sys")) or {}
    rows: List[tuple] = []
    if kind == "pub":
        rows = [
            ("client sent", w.get("sent")),
            ("client completed", w.get("done")),
            ("broker received ($SYS)", s.get("received")),
            ("broker sent ($SYS)", s.get("sent")),
            ("C sink received", p.get("received_total")),
            ("in window: sent / delivered", f"{m.get('client_sent', '—')} / {m.get('delivered', '—')}"),
        ]
    elif kind == "sub":
        rows = [
            ("C source sent", p.get("sent_total")),
            ("broker received ($SYS)", s.get("received")),
            ("broker sent ($SYS)", s.get("sent")),
            ("broker dropped ($SYS)", s.get("dropped")),
            ("client received", w.get("received")),
            ("in window: offered / received", f"{m.get('offered', '—')} / {m.get('delivered', '—')}"),
        ]
    elif kind == "rtt":
        rows = [
            ("client requests", w.get("sent")),
            ("C echo received", p.get("received_total")),
            ("C echo replied", p.get("echoed_total")),
            ("broker received ($SYS)", s.get("received")),
            ("broker sent ($SYS)", s.get("sent")),
            ("client replies", w.get("received")),
        ]
    else:
        rows = [("connect", f"{m.get('connect_ms', 0):.1f} ms")]
    if m.get("cpu_user_us_per_msg") is not None and m.get("cpu_sys_us_per_msg") is not None:
        rows.append(("CPU user / sys per msg", f"{fmt('cpu_user_us_per_msg', m['cpu_user_us_per_msg'])} / {fmt('cpu_sys_us_per_msg', m['cpu_sys_us_per_msg'])}"))
    if m.get("cpu_cores") is not None:
        rows.append(("CPU (share of a core)", fmt("cpu_cores", m["cpu_cores"])))
    if m.get("threads") is not None:
        rows.append(("threads", m["threads"]))
    return [(label, "—" if v is None else (f"{v:,}" if isinstance(v, int) else v)) for label, v in rows]


def run_detail(record: dict) -> str:
    status = record["status"]
    counts = "".join(f"<tr><th>{e(k)}</th><td>{e(v)}</td></tr>" for k, v in _counts(record))
    failed = [c for c in record.get("checks", []) if not c["passed"]]
    passed = [c for c in record.get("checks", []) if c["passed"]]
    checks = "".join(
        f'<li class="fail" title="{e(CHECK_DOCS.get(c["name"], ""))}"><b>{e(c["name"])}</b>: {e(c["detail"])}</li>'
        for c in failed
    )
    checks += "".join(
        f'<li class="pass" title="{e(CHECK_DOCS.get(c["name"], ""))}">{e(c["name"])}: {e(c["detail"])}</li>'
        for c in passed
    )
    error = f'<p class="error">{e(record["error"])}</p>' if record.get("error") else ""
    return (
        f'<div class="run"><p>run {int(record.get("run_index", 0)) + 1}'
        f'{" · attempt " + str(int(record["attempt"]) + 1) if record.get("attempt") else ""}'
        f' · <span class="st {e(status)}">{e(status.replace("_", " "))}</span> {flag_tags(record.get("flags", []))}</p>'
        f"{error}<table>{counts}</table><ul>{checks}</ul></div>"
    )


def cell_html(cell: Cell, metric: str, *, best: Optional[float] = None) -> str:
    """One table cell: the value (or why there is none), unfolding to its runs."""
    state = cell.state
    if state == "unsupported":
        return f'<td class="na" title="{e(", ".join(cell.unsupported or []))}">unsupported</td>'
    if state == "missing":
        return '<td class="na">not run</td>'
    if state == VALID:
        value = cell.value(metric)
        shown = fmt(metric, value)
        if cell.bounded(metric):
            shown = "≥ " + shown
        bar = ""
        if METRICS[metric].bar and value is not None and best:
            bar = f'<span class="bar" style="width:{max(2.0, 100.0 * value / best):.1f}%"></span>'
        others = len(cell.runs) - len(cell.valid)
        note = f' <span class="note">{len(cell.valid)}/{len(cell.runs)} valid</span>' if others else ""
        summary = f'<span class="v">{e(shown)}</span>{note}{flag_tags(cell.flags())}{bar}'
        klass = "val"
    else:
        summary = f'<span class="st {e(state)}">{e(STATE_TEXT[state])}</span>'
        klass = "na"
    runs = "".join(run_detail(r) for r in cell.runs)
    return f'<td class="{klass}"><details><summary>{summary}</summary><div class="raw">{runs}</div></details></td>'


def table(head: Sequence[str], rows: Iterable[str], klass: str = "") -> str:
    th = "".join(f"<th>{h}</th>" for h in head)
    return f'<div class="scroll"><table class="{klass}"><thead><tr>{th}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def metric_head(metric: str, label: Optional[str] = None) -> str:
    m = METRICS[metric]
    arrow = "↑" if m.better == "higher" else "↓"
    text = m.label if label is None else label
    inner = f"{e(text)} {arrow}" if text else arrow
    return f'<span title="{e(m.help)} {"Higher" if m.better == "higher" else "Lower"} is better.">{inner}</span>'
