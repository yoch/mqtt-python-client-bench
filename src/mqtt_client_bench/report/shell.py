"""The page shell: head, navigation, footer, and the theme boot script.

Everything the site renders passes through :func:`page_shell`, so the nav, the
stylesheet link and the theme handling exist in exactly one place.
"""

from __future__ import annotations

from typing import Sequence

from .model import _esc

SITE_TITLE = "MQTT Python client bench"
SITE_TAGLINE = "Comparative publish/subscribe results against Mosquitto"

_NAV = (
    ("", "index.html", "Overview"),
    ("scenarios", "scenarios/index.html", "Scenarios"),
    ("clients", "clients/index.html", "Clients"),
    ("corpus", "corpus.html", "Corpus"),
    ("methodology", "methodology.html", "Methodology"),
)

# Applied before first paint so a dark-mode reader never sees a light flash.
# Kept inline and tiny for that reason; everything else lives in app.js.
_THEME_BOOT = (
    "<script>(function(){try{var t=localStorage.getItem('mcb-theme');"
    "if(t==='dark'||t==='light'){document.documentElement.setAttribute('data-theme',t);}}"
    "catch(e){}})();</script>"
)

_THEME_TOGGLE = (
    '<button type="button" class="theme-toggle" data-theme-toggle '
    'aria-label="Switch between light and dark" title="Switch between light and dark">'
    '<span class="theme-icon" aria-hidden="true"></span>'
    "</button>"
)


def nav_html(active: str, root: str) -> str:
    links = []
    for key, href, label in _NAV:
        current = ' aria-current="page"' if key == active else ""
        links.append(f'<a href="{root}/{href}"{current}>{_esc(label)}</a>')
    return f'<nav class="site-nav">{"".join(links)}</nav>'


def page_shell(
    title: str,
    body: str,
    *,
    root: str = ".",
    active: str = "",
    head_extra: str = "",
) -> str:
    """Wrap a page body in the site chrome.

    ``root`` is the relative path back to the site root, so a page nested under
    ``scenarios/`` links to the same stylesheet as the index without either of
    them knowing where the other lives.
    """
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{_esc(title)}</title>
<link rel="stylesheet" href="{root}/assets/style.css" />
<link rel="stylesheet" href="{root}/assets/series.css" />
{head_extra}
{_THEME_BOOT}
</head>
<body>
<div class="page">
  <header class="site-header">
    <div class="site-header-main">
      <div>
        <a class="brand" href="{root}/index.html">{_esc(SITE_TITLE)}</a>
        <p class="tagline">{_esc(SITE_TAGLINE)}</p>
      </div>
      {_THEME_TOGGLE}
    </div>
    {nav_html(active, root)}
  </header>
{body}
  <footer class="site-footer">
    <p>Generated locally from committed <code>results/*.json</code>. Raw JSON stays in the
    repository, not on this site. Charts are server-rendered SVG; the page needs no network
    and no third-party script to display.</p>
  </footer>
</div>
<script src="{root}/assets/app.js" defer></script>
</body>
</html>
"""


def hero(title: str, lead: str, meta: str = "", extra: str = "") -> str:
    meta_html = f'<p class="meta">{meta}</p>' if meta else ""
    return f"""
      <section class="hero">
        <h1>{title}</h1>
        <p>{lead}</p>
        {meta_html}
        {extra}
      </section>"""


def stats_row(tiles: Sequence[str]) -> str:
    if not tiles:
        return ""
    return '<section class="stats">\n        ' + "\n        ".join(tiles) + "\n      </section>"


def crumb(href: str, label: str) -> str:
    return f'<p class="crumb"><a href="{href}">← {_esc(label)}</a></p>'
