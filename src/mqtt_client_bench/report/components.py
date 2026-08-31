"""Small HTML pieces shared by every page.

Kept apart from the page modules so a badge or a swatch looks the same on the
index, on a scenario page and on a run page without three copies drifting.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Sequence

from .model import ClientMeta, _esc, _fmt_num, _slug
from .theme import client_paint


def client_swatch(
    name: str,
    meta: Optional[Dict[str, ClientMeta]] = None,
    *,
    link: bool = True,
    show_io: bool = True,
    show_exp: bool = True,
) -> str:
    """A client's colour chip, its name, and the peer-group badges.

    ``show_io=False`` drops the I/O-model badge for callers that have already
    named the group — repeating it read as "crt_event_loop awscrt crt_event_loop".
    ``show_exp=False`` drops the pre-release badge, for callers that place it
    themselves; without it the matrix header rendered the badge twice.
    """
    info = (meta or {}).get(name)
    badges = ""
    if show_io and info and info.io_model != "unknown":
        badges += f' <span class="io-badge" title="I/O model peer group">{_esc(info.io_model)}</span>'
    if show_exp and info and info.stability == "experimental":
        badges += (
            ' <span class="io-badge badge-exp" title="Pre-release — ranked with the released'
            ' clients, sorted after them">exp</span>'
        )
    chip = f'<span class="swatch" style="background:{client_paint(name)}"></span>'
    label = f'<a class="client-link" href="{client_href(name)}">{_esc(name)}</a>' if link else _esc(name)
    return f"{chip}{label}{badges}"


def status_badge(status: str, non_comparable: bool = False) -> str:
    label = status
    if non_comparable:
        label = f"{status} · non-comparable"
    return f'<span class="badge badge-{_esc(status)}">{_esc(label)}</span>'


def direction_badge(direction: str) -> str:
    """Say which way is better, once, where the number is introduced."""
    if direction == "higher":
        return '<span class="dir dir-higher" title="Higher is better">higher is better</span>'
    if direction == "lower":
        return '<span class="dir dir-lower" title="Lower is better">lower is better</span>'
    return (
        '<span class="dir dir-none" title="The rate is imposed by the harness">'
        "rate-capped — not a ranking</span>"
    )


def scenario_href(scenario: str, *, root: str = ".") -> str:
    return f"{root}/scenarios/{_slug(scenario)}.html"


def client_href(client: str, *, root: str = ".") -> str:
    return f"{root}/clients/{_slug(client)}.html"


def run_href(slug: str, *, root: str = ".") -> str:
    return f"{root}/runs/{slug}.html"


def group_title(io_model: str, clients: Sequence[Optional[str]], meta=None) -> str:
    """Heading for one peer-group chart, naming the client when there is one.

    A chart with a single series carries no legend — the heading is supposed to
    name it. It named the I/O model instead, so a reader looking at the lone
    ``crt_event_loop`` chart had no way to learn it was awscrt. With one member
    the client is named here; with several the legend does that job.
    """
    named = [c for c in clients if c]
    label = f'<span class="group-model">{_esc(io_model)}</span>'
    if len(set(named)) == 1:
        label += f' <span class="group-client">{client_swatch(named[0], meta, show_io=False)}</span>'
    return f'<h3 class="group-title">{label}</h3>'


def panel(title: str, body: str, *, hint: str = "", extra_class: str = "", anchor: str = "") -> str:
    """One card: a heading, an optional explanation, then the content."""
    if not body:
        return ""
    cls = f"panel {extra_class}".strip()
    ident = f' id="{_esc(anchor)}"' if anchor else ""
    hint_html = f'<p class="hint">{hint}</p>' if hint else ""
    return f"""
      <section class="{cls}"{ident}>
        <div class="panel-head">
          <h2>{title}</h2>
          {hint_html}
        </div>
        {body}
      </section>"""


def stat_tile(label: str, value: str, note: str = "") -> str:
    note_html = f' <span>{note}</span>' if note else ""
    return (
        "<article>\n"
        f'          <p class="stat-label">{label}</p>\n'
        f'          <p class="stat-value">{value}{note_html}</p>\n'
        "        </article>"
    )


def table(
    headers: Sequence[str],
    rows: Iterable[Sequence[str]],
    *,
    css: str = "",
    numeric: Sequence[int] = (),
    sortable: bool = True,
) -> str:
    """A plain table. ``numeric`` columns get right alignment and sort hints."""
    body_rows = list(rows)
    if not body_rows:
        return ""
    numeric_set = set(numeric)
    cells = []
    for i, header in enumerate(headers):
        attrs = ""
        if i in numeric_set:
            attrs += ' class="num"'
        if sortable:
            attrs += ' data-sort="num"' if i in numeric_set else ' data-sort="text"'
        cells.append("<th{0}>{1}</th>".format(attrs, header))
    out = ['<div class="table-wrap"><table class="{0}">'.format(css.strip())]
    out.append("<thead><tr>{0}</tr></thead><tbody>".format("".join(cells)))
    for row in body_rows:
        out.append("<tr>{0}</tr>".format("".join("<td>{0}</td>".format(c) for c in row)))
    out.append("</tbody></table></div>")
    return "".join(out)


def kv_list(pairs: Iterable[tuple], *, css: str = "kv") -> str:
    items = [
        f"<li><strong>{_esc(str(key))}</strong> <span>{value}</span></li>"
        for key, value in pairs
        if value not in (None, "")
    ]
    if not items:
        return ""
    return f'<ul class="{css}">{"".join(items)}</ul>'


def note(text: str, *, kind: str = "info") -> str:
    """A caveat rendered as a caveat, not as body prose."""
    return f'<p class="note note-{_esc(kind)}">{text}</p>'


def num(value: Any, *, digits: int = 1) -> str:
    return _fmt_num(value, digits=digits)
