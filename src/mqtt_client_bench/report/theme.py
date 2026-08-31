"""Colour tokens for the report site.

Two rules shape this file.

**Colour follows the client, never its rank.** A reader who learns that gmqtt is
blue must find gmqtt blue on every page, so the mapping below is fixed and total;
nothing here is assigned from sort order or from how many series a chart happens
to hold.

**Charts emit CSS variables, not hex.** Inline SVG inherits custom properties
from its ancestors, so a mark painted ``fill="var(--c-gmqtt)"`` re-themes itself
when the page switches to dark — one stylesheet swap instead of a second render.
The hex pairs live here only to *generate* that stylesheet block.

The palette was validated with the data-viz six-checks validator against both
surfaces. Peer grouping caps a chart at three series (the two three-client
groups are ``stable/asyncio_bridged`` and ``experimental/asyncio_bridged``), and
both triples clear the all-pairs CVD and normal-vision floors in light and dark.
Three light-mode steps sit below 3:1 against the paper surface, which obliges
the relief rule: every chart on this site ships direct labels and a table of the
same numbers, so colour never carries a value alone.
"""

from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

# The palette below was validated against the chart surfaces declared as
# --surface-1 in style.css: #fffdf8 in light, #1e1c18 in dark. Re-run
# scripts/validate_palette.js against those exact values after changing either,
# or the pass no longer means anything.

# client -> (light step, dark step), drawn from the eight validated categorical
# slots. Nine clients do not fit in eight slots, and a generated ninth hue lands
# indistinguishably close to an existing one under CVD — every candidate tried
# failed either against its chart-mate or against its nearest neighbour in the
# swatch lists. So one slot is reused outright: mqttium-compat takes zmqtt's
# violet. They are in different peer groups (sync vs asyncio_bridged) and so
# never share a chart frame; the only places both appear are tables and legends,
# where the client's name sits beside its chip. An exact repeat reads as "two
# rows, same colour"; a near-miss would read as "are these the same?", which is
# the worse failure.
#
# The sets that actually share a frame are validated together:
#   sync             paho, mqttium-compat            (violet vs green: ΔE 27.9 / 25.6)
#   asyncio_bridged  gmqtt, amqtt, aiomqtt, zmqtt, aiomqtt3, mqttium  (adjacent, both modes)
#   crt_event_loop   awscrt                          (single series)
CLIENT_COLORS: Dict[str, Tuple[str, str]] = {
    "gmqtt": ("#2a78d6", "#3987e5"),
    "aiomqtt": ("#eb6834", "#d95926"),
    "amqtt": ("#1baf7a", "#199e70"),
    "aiomqtt3": ("#eda100", "#c98500"),
    "mqttium": ("#e87ba4", "#d55181"),
    "zmqtt": ("#4a3aa7", "#9085e9"),
    "paho": ("#008300", "#0ca30c"),
    "awscrt": ("#e34948", "#e66767"),
    "mqttium-compat": ("#4a3aa7", "#9085e9"),  # deliberate repeat of zmqtt; see above
}

# A client absent from the table above still gets a stable colour, because the
# site must keep rendering when a tenth adapter lands before this file is
# updated. These are deliberately muted: an unknown client should not out-shout
# a known one.
FALLBACK_COLORS: List[Tuple[str, str]] = [
    ("#5c6b64", "#8fa199"),
    ("#7a6a4f", "#b09a74"),
    ("#4f6b7a", "#7fa0b2"),
]

# The sequential ramp for magnitude (the corpus coverage heatmap) is a fixed
# one-hue scale that does not depend on which clients are published, so it lives
# in style.css as --seq-0 … --seq-6 rather than being generated here. Charts ask
# for it by step class; nothing in Python needs to know its hexes.


def css_var(client: str) -> str:
    """CSS custom property holding this client's colour."""
    return f"--c-{client.replace('.', '-').replace('_', '-')}"


def client_paint(client: str) -> str:
    """Paint value for a mark belonging to ``client``."""
    return f"var({css_var(client)}, var(--series-fallback))"


def assign_colors(clients: Sequence[str]) -> Dict[str, Tuple[str, str]]:
    """Resolve every client to a (light, dark) pair, fallbacks included."""
    resolved: Dict[str, Tuple[str, str]] = {}
    spare = 0
    for client in clients:
        if client in CLIENT_COLORS:
            resolved[client] = CLIENT_COLORS[client]
        else:
            resolved[client] = FALLBACK_COLORS[spare % len(FALLBACK_COLORS)]
            spare += 1
    return resolved


def client_color_css(clients: Sequence[str]) -> str:
    """The rules binding every client to its light and dark step.

    Written out as its own stylesheet at build time rather than inlined per
    page: the set of clients comes from the results actually being published, so
    it cannot drift from a list maintained by hand, and every page — index,
    scenario, client, run — picks up the same bindings from one file.
    """
    resolved = assign_colors(sorted(set(clients)))
    if not resolved:
        resolved = dict(CLIENT_COLORS)
    light = "\n".join(f"  {css_var(c)}: {pair[0]};" for c, pair in sorted(resolved.items()))
    dark = "\n".join(f"  {css_var(c)}: {pair[1]};" for c, pair in sorted(resolved.items()))
    return (
        "/* Series colours, generated from the published corpus. Declared three\n"
        " * times so an explicit theme choice wins in both directions while the\n"
        " * OS setting still drives an untouched page. */\n"
        f":root {{\n{light}\n}}\n\n"
        "@media (prefers-color-scheme: dark) {\n"
        f'  :root:not([data-theme="light"]) {{\n{dark}\n  }}\n}}\n\n'
        f':root[data-theme="dark"] {{\n{dark}\n}}\n'
    )
