"""How to read the numbers, next to the numbers themselves.

These rules used to live only in the README and SCENARIOS.md, so a reader
arriving at the published site had no way to know what a cell meant. The
scenario table at the bottom is generated from the same catalogue the scenario
pages use, so the direction of "better" cannot drift between the two.
"""

from __future__ import annotations

from typing import List, Sequence

from ..catalog import FACTS
from ..components import client_swatch, direction_badge, panel, scenario_href, table
from ..model import _STABILITY_OVERRIDES, EMPTY_GLYPHS, ResultDoc, _client_meta, _esc, _sort_clients
from ..shell import hero, page_shell

_PROTOCOLS = """
        <ul class="prose">
          <li><strong>Capacity</strong> — closed loop, same configuration for every client.
          The primary metric is <code>completed_success</code> inside the measure window.
          Different ceilings are the measurement.</li>
          <li><strong>Matched-load latency</strong> — the same absolute <code>target_rate</code>
          for every client, either written on the point or resolved once from
          <code>shared_load_fraction</code> × <code>C_common = min(capacities)</code>
          of <em>that matrix or compare only</em>. A third, slower client
          (Paho) must never size an asyncio pairwise grid.
          Official matched-load cells fail closed above
          <code>MATCHED_LOAD_BACKPRESSURE_MAX</code> (0.2&nbsp;% missed slots);
          relative-load characterization still uses 2&nbsp;%.
          If official capacity is null, only calibrate runs voided
          <em>exclusively</em> by <code>broker_headroom_low</code> may
          contribute an observed lower bound; timeouts and mixed failures
          stay unresolved. A client that cannot hold the shared point is
          <code>offer_limited</code>; the rate is never lowered.
          <code>puback_latency_fixed_rate</code> and
          <code>application_rtt_fixed_rate</code>.</li>
          <li><strong>Relative-load characterization</strong> — fractions of <em>that client's
          own</em> capacity (<code>puback_latency_qos1</code>,
          <code>application_rtt_qos1</code>). Marked
          <strong>NOT CROSS-CLIENT COMPARABLE</strong>. <code>run compare</code> /
          <code>run matrix</code> refuse them so a per-client
          <code>load_fraction</code> cannot silently become an A/B ranking.</li>
          <li><strong>Integrity</strong> — bounded rate with a sequence header; counts missing,
          duplicate and out-of-order messages. Not a throughput race.</li>
        </ul>"""

_COMPLETED = """
        <table class="results-table">
          <thead><tr><th>QoS</th><th><code>on_publish</code> fires when</th></tr></thead>
          <tbody>
            <tr><td>0</td><td>the packet has been handed to the transport</td></tr>
            <tr><td>1</td><td>PUBACK is received</td></tr>
            <tr><td>2</td><td>PUBCOMP is received (not PUBREC)</td></tr>
          </tbody>
        </table>
        <p class="hint">For single-publisher scenarios the completions are reconciled against the
        broker's own <code>$SYS</code> received-publish counter. A run the broker cannot confirm is
        marked inconclusive rather than published.</p>"""

_COMPLETED_HINT = (
    "The publish completion boundary is part of the contract; an adapter that cannot honour one "
    "declares the capability as missing instead of approximating it. QoS0 in particular is not "
    "identical across libraries: Paho fires after the socket send completes "
    "(<code>qos0_boundary: socket</code>), while MQTTium admits to its write pump "
    "(<code>qos0_boundary: queue</code>) — both are declared in <code>client_identity</code>."
)

_PEERS = """
        <ul class="prose">
          <li><code>sync</code> — the library exposes a blocking/callback API and is driven
          directly.</li>
          <li><code>asyncio_bridged</code> — an asyncio library driven through a private event-loop
          thread. That bridge has a cost, it is assumed and documented, and it is paid equally by
          every bridged client.</li>
          <li><code>crt_event_loop</code> — a native (non-Python) engine; not comparable with
          pure-Python clients.</li>
        </ul>
        <p class="hint">MQTT 3.1.1 and MQTT 5 rows are never merged. Stability does
        <em>not</em> split a group: a pre-release library competes directly with released ones of
        the same I/O model, sorts after them, and carries an <code>exp</code> badge. Splitting on
        it put libraries doing identical work in separate charts, and a client graduating to
        stable would silently have changed who it was compared against.</p>"""

_ATTRIBUTION = """
        <ul class="prose">
          <li><code>sut_limited</code> — the client was the constraint.</li>
          <li><code>broker_limited</code> — Mosquitto was at or near saturation; the number is
          partly the broker's.</li>
          <li><code>broker_unconfirmed</code> — the broker did not confirm the reported
          completions.</li>
          <li><code>loadgen_limited</code> / <code>offer_limited</code> — the injected load, not the
          client, set the ceiling.</li>
        </ul>"""

_HOW_TO_READ = """
        <ol class="prose">
          <li>Check <code>status</code> and <code>reasons</code> first — a number on an inconclusive
          run is not a result.</li>
          <li>Check <code>bottleneck</code>. It is a heuristic, not a truth, but it tells you
          whether the client was the thing being measured.</li>
          <li>On ingress scenarios, compare the delivered rate against
          <code>effective_offer_msgs_per_s</code>, never against a raw parsed QoS0 rate.</li>
          <li><code>duplex_gateway</code> and <code>e2e_integrity</code> are rate-capped on purpose;
          their throughput is not a capacity.</li>
          <li>Latency is comparable only at the same fraction and the same client calibration — or,
          better, at the same absolute rate.</li>
          <li>Ceiling questions belong to the ceiling-probe runbook, not to a ranking page.</li>
          <li>Rank only inside a peer group: same I/O model, same MQTT protocol, and never a stable
          client against an experimental one.</li>
        </ol>"""

_LIMITS = """
        <ul class="prose">
          <li>All runs are local, against a Dockerised Mosquitto on loopback. Nothing here predicts
          WAN behaviour.</li>
          <li>Netem profiles (<code>lan</code>/<code>wan</code>/<code>edge</code>) and smoke runs are
          diagnostic and marked non-comparable.</li>
          <li>Application RTT drives both sides with the same library, which amplifies stack cost on
          purpose; it is not a neutral peer RTT. mqttium and gmqtt take the
          worker's native asyncio loop; Paho stays on its sync facade.
          Bridged historical RTT is not evidence of a native ranking.</li>
          <li>The ARM three-way grid that sized <code>C_common</code> with Paho
          is <code>superseded</code> / contextual only. Official replacement:
          two pairwise campaigns in <code>scripts/run_pairwise_rtt_campaign.sh</code>.</li>
          <li>The 64 KiB and 1 MiB payload points are broker bound: the ranking inverts there and
          should not be read as a comparison between libraries.</li>
        </ul>"""

_CLIENTS_HINT = (
    "Read from each result's <code>client_identity</code>. &quot;Internals used&quot; lists "
    "library-private attributes the adapter depends on, because reaching into internals changes "
    "what is being measured."
)


def _override_note() -> str:
    """Name any client the report presents differently from its own results.

    Without this the site would show a library as stable while every committed
    result file says otherwise, and a reader comparing the two would have no way
    to tell which was wrong.
    """
    if not _STABILITY_OVERRIDES:
        return ""
    items = ", ".join(
        f"<code>{_esc(name)}</code> is shown as <strong>{_esc(value)}</strong>"
        for name, value in sorted(_STABILITY_OVERRIDES.items())
    )
    return (
        '<p class="hint">The committed results still declare these clients otherwise: '
        + items
        + ". The report anticipates a release that has not yet reached the adapter's "
        "declaration; it changes ordering and the badge, never a measurement.</p>"
    )


def render(docs: Sequence[ResultDoc], generated_at: str) -> str:
    meta = _client_meta(docs)
    measured = {d.scenario or d.title for d in docs if d.kind == "scenario"}

    client_rows: List[List[str]] = []
    for name in _sort_clients(sorted(meta), meta):
        info = meta[name]
        client_rows.append(
            [
                client_swatch(name, meta),
                _esc(info.version or "—"),
                '<code class="mono">' + _esc(info.io_model) + "</code>",
                _esc(info.stability),
                ", ".join(
                    '<code class="mono">' + _esc(k) + "</code>" for k in sorted(info.private_api)
                )
                or "—",
            ]
        )

    legend = "".join(
        "<li><span class='legend-glyph empty-" + kind + "'>" + glyph + "</span> " + _esc(title) + "</li>"
        for kind, (glyph, title) in EMPTY_GLYPHS.items()
    )

    scenario_rows: List[List[str]] = []
    for name, facts in sorted(FACTS.items()):
        if name in measured:
            link = (
                '<a href="' + scenario_href(name) + '"><code class="mono">'
                + _esc(name) + "</code></a>"
            )
        else:
            link = '<code class="mono muted">' + _esc(name) + "</code>"
        scenario_rows.append(
            [
                link,
                _esc(facts.metric),
                '<span class="unit">' + _esc(facts.unit) + "</span>",
                direction_badge(facts.direction),
                _esc(facts.question or "—"),
            ]
        )

    sections = [
        hero(
            "Methodology",
            "What these numbers mean, and what they deliberately do not mean.",
            "generated " + _esc(generated_at),
        ),
        panel("Three measurement protocols, never mixed", _PROTOCOLS),
        panel("What &quot;completed&quot; means", _COMPLETED, hint=_COMPLETED_HINT),
        panel(
            "Peer groups",
            _PEERS,
            hint="A ranking is only meaningful inside a peer group. The matrix highlights the best "
            "value per group, never across groups.",
        ),
        panel("Why a cell can be empty", '<ul class="prose legend-list">' + legend + "</ul>"),
        panel(
            "Attribution",
            _ATTRIBUTION,
            hint="Every run carries a bottleneck attribution. Only <code>sut_limited</code> runs "
            "say something about the client.",
        ),
        panel("How to read a result", _HOW_TO_READ),
        panel(
            "Clients in this report",
            table(
                ["Client", "Version", "I/O model", "Stability", "Internals used"],
                client_rows,
                css="results-table",
            )
            + _override_note(),
            hint=_CLIENTS_HINT,
        ),
        panel(
            "Scenario catalogue",
            table(
                ["Scenario", "Primary metric", "Unit", "Direction", "Question"],
                scenario_rows,
                css="results-table",
            ),
            hint="Greyed names are catalogued but have no committed results. The direction column "
            "is the one the scenario pages and this table share, so they cannot disagree.",
        ),
        panel("Known limits", _LIMITS),
    ]
    body = "\n    <main>\n" + "\n".join(sections) + "\n    </main>\n"
    return page_shell("Methodology — MQTT Python client bench", body, active="methodology")
