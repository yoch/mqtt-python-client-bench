"""What each scenario measures, and which way is better.

``scenarios.py`` says how a point is *wired*; SCENARIOS.md says how its number
should be *read*. Only the first was reachable from the report, so every
scenario was rendered as "throughput, higher is better" — which is wrong for the
latency scenarios, wrong for the integrity ones, and actively misleading for the
rate-capped ones whose primary rate is just the offer echoed back.

This table carries the second half. It is written by hand against SCENARIOS.md
rather than derived, because the direction of "better" is an editorial fact
about the experiment, not a property of the dataclass.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence

try:  # The report must still build if the catalogue moves under it.
    from mqtt_client_bench.workloads import PAYLOAD_SPECS
except Exception:  # pragma: no cover - defensive
    PAYLOAD_SPECS = {}


# How a metric improves. ``none`` marks a scenario whose primary rate is pinned
# by the harness (a fixed cadence, an injected offer), so "more" says nothing
# about the client and no winner may be crowned.
HIGHER, LOWER, NONE = "higher", "lower", "none"

# An ordinal axis is a quantity and gets a line; a nominal one is a set of
# distinct conditions and gets grouped bars. Reading a payload sweep as bars
# hides the shape of the curve, which is the entire point of sweeping it.
ORDINAL, NOMINAL = "ordinal", "nominal"


@dataclass(frozen=True)
class ScenarioFacts:
    """Everything the site needs to render one scenario honestly."""

    metric: str
    unit: str = "msg/s"
    direction: str = HIGHER
    axis_kind: str = NOMINAL
    axis_label: str = ""
    question: str = ""
    #: False when the primary rate must not produce a cross-client winner.
    ranked: bool = True
    #: True when points may only be compared inside one client (open-loop
    #: fractions of that client's own capacity).
    intra_client_only: bool = False
    caveats: Sequence[str] = field(default_factory=tuple)


_UNKNOWN = ScenarioFacts(
    metric="primary rate",
    question="Not described in the report catalogue; read the scenario definition.",
)


FACTS: Dict[str, ScenarioFacts] = {
    "pub_payload_sweep_qos0": ScenarioFacts(
        metric="publish completions",
        direction=HIGHER,
        axis_kind=ORDINAL,
        axis_label="payload size",
        question="How does QoS0 publish capacity fall as the payload grows?",
        caveats=(
            "The 64 KiB and 1 MiB points are broker bound: the ranking inverts there, and a "
            "median survives mainly for clients too slow to saturate Mosquitto. Do not read "
            "those two sizes as a comparison between libraries.",
        ),
    ),
    "pub_qos_sweep_telemetry": ScenarioFacts(
        metric="publish completions",
        direction=HIGHER,
        axis_kind=NOMINAL,
        axis_label="QoS",
        question="What does each delivery guarantee cost in publish capacity?",
        caveats=(
            "Completion means QoS0 handed to the transport, QoS1 PUBACK, QoS2 PUBCOMP. "
            "gmqtt completes QoS2 at PUBREC and so declines the point rather than "
            "reporting a number it cannot honour.",
        ),
    ),
    "pub_qos1_inflight": ScenarioFacts(
        metric="publish completions",
        direction=HIGHER,
        axis_kind=ORDINAL,
        axis_label="in-flight window",
        question="How much does the in-flight window govern QoS1 throughput?",
        caveats=(
            "The only scenario that sweeps the in-flight window; every other capacity point "
            "pins inflight to outstanding so libraries exposing the knob are not throttled "
            "below the ones that ignore it. Requires max_inflight, so most clients decline.",
        ),
    ),
    "remaining_length_boundaries": ScenarioFacts(
        metric="publish completions",
        direction=HIGHER,
        axis_kind=ORDINAL,
        axis_label="MQTT Remaining Length",
        question="Does the 1-to-2 byte Remaining Length transition cost anything?",
        ranked=False,
        caveats=(
            "A diagnostic on packet encoding, not a library ranking: the points straddle an "
            "encoding boundary rather than a workload a user would choose.",
        ),
    ),
    "sub_exact_telemetry": ScenarioFacts(
        metric="messages delivered to the callback",
        direction=HIGHER,
        axis_kind=NOMINAL,
        axis_label="MQTT protocol",
        question="How fast can the client drain an exact-topic firehose?",
        caveats=(
            "Compare the delivered rate against the loadgen's effective offer, never against "
            "the raw parsed rate. The offer is a deliberate over-offer whose job is to make "
            "the client the bottleneck, so a saturated broker here does not invalidate the "
            "delivery count.",
        ),
    ),
    "sub_hierarchy_telemetry": ScenarioFacts(
        metric="messages delivered to the callback",
        direction=HIGHER,
        axis_kind=NOMINAL,
        axis_label="subscription shape",
        question="What does wildcard matching cost against a 4k-topic fleet?",
        caveats=(
            "With one subscriber Mosquitto forwards roughly 76k msgs/s on the reference host. "
            "A Python client landing on that rate is reporting the broker's ceiling wearing "
            "the client's name, and is attributed broker_limited.",
        ),
    ),
    "sub_callback_matching": ScenarioFacts(
        metric="messages delivered to the callback",
        direction=HIGHER,
        axis_kind=ORDINAL,
        axis_label="registered callback filters",
        question="What does local topic-filter matching cost as filters multiply?",
        ranked=False,
        caveats=(
            "Needs native message_callback_add, so only paho, mqttium and mqttium-compat "
            "run it — rank within io_model, never across the sync/asyncio split.",
        ),
    ),
    "duplex_gateway": ScenarioFacts(
        metric="publisher throughput",
        direction=NONE,
        axis_kind=NOMINAL,
        axis_label="cadence",
        question="Does a concurrent command stream disturb the publish path?",
        ranked=False,
        caveats=(
            "Deliberately rate-capped: the number is the cadence the harness imposed, not a "
            "capacity. Read it for stability under duplex traffic, never as a ranking.",
        ),
    ),
    "burst_recovery": ScenarioFacts(
        metric="delivered rate in the window",
        direction=HIGHER,
        axis_kind=NOMINAL,
        axis_label="single point",
        question="How does the client absorb an ingress burst followed by silence?",
        caveats=(
            "Keeps the I=1 burst offer on purpose rather than the 200k ranking target: the "
            "shape of the recovery is the measurement, not the peak rate.",
        ),
    ),
    "e2e_integrity": ScenarioFacts(
        metric="missing / duplicate / out-of-order messages",
        unit="messages",
        direction=LOWER,
        axis_kind=NOMINAL,
        axis_label="QoS pair",
        question="Does the client lose, duplicate or reorder messages end to end?",
        ranked=False,
        caveats=(
            "Throughput here is capped by design. The substance is the sequence counters, "
            "where zero is the only good answer — this is not a throughput race.",
        ),
    ),
    "puback_latency_qos1": ScenarioFacts(
        metric="PUBACK latency",
        unit="ms",
        direction=LOWER,
        axis_kind=ORDINAL,
        axis_label="fraction of this client's own capacity",
        question="How does PUBACK latency degrade as the client approaches its own ceiling?",
        ranked=False,
        intra_client_only=True,
        caveats=(
            "Each client is offered a fraction of ITS OWN capacity, so the clients are not "
            "being held to the same load. Reading these percentiles across clients penalises "
            "the ones with headroom: a published 2.95x latency gap collapsed to 1.24x once "
            "the offered rate was matched. puback_latency_fixed_rate is the cross-client "
            "PUBACK comparison (equal absolute offered rates).",
        ),
    ),
    "puback_latency_fixed_rate": ScenarioFacts(
        metric="PUBACK latency",
        unit="ms",
        direction=LOWER,
        axis_kind=ORDINAL,
        axis_label="offered rate",
        question="At the same absolute offered rate, whose PUBACK comes back soonest?",
        caveats=(
            "The public cross-client latency ranking: every client is offered the same "
            "absolute rate, so the comparison is fair. A client that cannot sustain a rate "
            "comes back offer_limited rather than slow.",
        ),
    ),
    "rtt_capacity_qos1": ScenarioFacts(
        metric="completed request/response pairs",
        direction=HIGHER,
        axis_kind=NOMINAL,
        axis_label="MQTT protocol",
        question="How many application round trips per second can the client close?",
        caveats=(
            "The closed-loop baseline that application_rtt_qos1 is calibrated against. "
            "awscrt cannot set TCP_NODELAY and declines every RTT point.",
        ),
    ),
    "application_rtt_qos1": ScenarioFacts(
        metric="application round-trip latency",
        unit="ms",
        direction=LOWER,
        axis_kind=ORDINAL,
        axis_label="fraction of this client's own RTT capacity",
        question="How does round-trip latency degrade as the client nears its RTT ceiling?",
        ranked=False,
        intra_client_only=True,
        caveats=(
            "Fractions of each client's own RTT capacity — intra-client only, exactly as for "
            "puback_latency_qos1. The 0.50 / 0.75 offered rates already differ by the "
            "rtt_capacity ratio, so a throughput or p50 gap here is not proof of matched-load "
            "latency. NOT CROSS-CLIENT COMPARABLE. application_rtt_fixed_rate resolves "
            "shared_load_fraction once from C_common = min(client RTT capacities). Needs "
            "end-to-end TCP_NODELAY; without it the pair time plateaus around 84 ms on Nagle "
            "rather than on the library.",
        ),
    ),
    "application_rtt_fixed_rate": ScenarioFacts(
        metric="application round-trip latency",
        unit="ms",
        direction=LOWER,
        axis_kind=ORDINAL,
        axis_label="shared fraction of C_common",
        question="At the same absolute pair rate (a shared fraction of min capacity), whose RTT comes back soonest?",
        caveats=(
            "Matched-load: C_common = min(client RTT capacities), then 25/50/75/90 % of that "
            "one ceiling, the same target_rate for every client. Homogeneous product loop "
            "(initiator and responder are the same library). A client that cannot hold a "
            "shared point is offer_limited; the offered rate is never lowered to rescue it. "
            "Distinct from rtt_capacity_qos1 (ceiling) and application_rtt_qos1 "
            "(NOT CROSS-CLIENT COMPARABLE).",
        ),
    ),
    # --- suite full: catalogued, not yet measured -------------------------
    "pub_segment_threshold_16k": ScenarioFacts(
        metric="publish completions", axis_label="single point",
        question="Publish capacity at the 16 KiB segmentation threshold."),
    "pub_segment_block_64k": ScenarioFacts(
        metric="publish completions", axis_label="single point",
        question="Publish capacity at a 64 KiB block."),
    "pub_segment_blob_1m": ScenarioFacts(
        metric="publish completions", axis_label="single point",
        question="Publish capacity at a 1 MiB blob."),
    "sub_delivery_latency": ScenarioFacts(
        metric="delivery latency", unit="ms", direction=LOWER, axis_kind=ORDINAL,
        axis_label="fraction of capacity", intra_client_only=True, ranked=False,
        question="How long does a message take to reach the subscriber callback?"),
    "pubcomp_latency_qos2": ScenarioFacts(
        metric="PUBCOMP latency", unit="ms", direction=LOWER, axis_kind=ORDINAL,
        axis_label="fraction of capacity", intra_client_only=True, ranked=False,
        question="What does the four-packet QoS2 handshake cost in latency?"),
    "cost_per_message": ScenarioFacts(
        metric="CPU per message", unit="µs/msg", direction=LOWER, axis_label="single point",
        question="How much CPU does one message cost this client?"),
    "payload_stress": ScenarioFacts(
        metric="publish completions", axis_kind=ORDINAL, axis_label="payload",
        question="Does the client survive oversized and non-bytes payloads?"),
    "topic_stress": ScenarioFacts(
        metric="messages delivered", axis_label="topic shape",
        question="What do deep, long, unicode and numerous topics cost?"),
    "sub_multi_subscribe": ScenarioFacts(
        metric="messages delivered", axis_kind=ORDINAL, axis_label="subscriptions",
        question="Does holding many exact subscriptions slow delivery?"),
    "fanin_scaling": ScenarioFacts(
        metric="messages delivered", axis_kind=ORDINAL, axis_label="publishers",
        question="Does the ceiling move when the same load arrives from more publishers?"),
    "fanout_scaling": ScenarioFacts(
        metric="publish completions", axis_kind=ORDINAL, axis_label="subscribers",
        question="What does each additional subscriber cost the publisher?",
        caveats=(
            "The one-subscriber fan-out ceiling says nothing here: the broker reads once and "
            "writes once per subscriber, so the constraint changes shape with every extra one.",
        )),
    "periodic_and_microburst": ScenarioFacts(
        metric="messages delivered", direction=NONE, ranked=False, axis_label="cadence",
        question="How does the client handle shaped rather than saturating traffic?"),
    "mqttv5_properties": ScenarioFacts(
        metric="publish completions", axis_label="protocol and properties",
        question="What do MQTT 5 properties cost on the publish path?"),
    "mqttv5_rich": ScenarioFacts(
        metric="publish completions", axis_label="properties profile",
        question="Which MQTT 5 property features are implemented at all?"),
    "mqttv5_flow_control": ScenarioFacts(
        metric="publish completions", direction=NONE, ranked=False, axis_kind=ORDINAL,
        axis_label="Receive Maximum",
        question="Does the client actually honour the broker's Receive Maximum?",
        caveats=(
            "Higher is not better here: Receive Maximum 10 SHOULD score below 100. Two equal "
            "numbers mean the client ignored the limit, which is a failure, not a tie.",
        )),
    "qos_asymmetric": ScenarioFacts(
        metric="throughput", direction=NONE, ranked=False, axis_label="publish/subscribe QoS",
        question="What happens when publish and subscribe QoS disagree?"),
    "queue_rejection": ScenarioFacts(
        metric="accepted vs rejected submissions", unit="messages", direction=NONE,
        ranked=False, axis_label="single point",
        question="Does the client reject cleanly once its queue is full?",
        caveats=("Judged against an expected accept/reject split, not against a rate.",)),
    "retained_bootstrap": ScenarioFacts(
        metric="messages delivered", axis_kind=ORDINAL, axis_label="retained messages",
        ranked=False,
        question="How fast does a fresh subscriber drain a large retained set?",
        caveats=("Always non-comparable: the retained dump is a broker behaviour.",)),
    "session_resume_qos1": ScenarioFacts(
        metric="messages missing across the outage", unit="messages", direction=LOWER,
        ranked=False, axis_label="single point",
        question="Does a persistent session actually replay what it queued?",
        caveats=(
            "Missing roughly equal to outage x rate means the session was not resumed. It may "
            "be the bench adapter rebuilding its client rather than the library.",
        )),
    "reconnect_ordering": ScenarioFacts(
        metric="out-of-order / duplicate / missing", unit="messages", direction=LOWER,
        ranked=False, axis_kind=ORDINAL, axis_label="outage length",
        question="What does a reconnect do to ordering?"),
    "network_matrix": ScenarioFacts(
        metric="publish throughput", axis_label="network profile", ranked=False,
        question="How does the client behave under added latency and loss?",
        caveats=("Any profile other than localhost is diagnostic and non-comparable.",)),
    "tls_steady_state": ScenarioFacts(
        metric="publish completions", axis_label="single point",
        question="What does TLS cost once the handshake is behind us?",
        caveats=("Steady state only — this is not a handshake benchmark.",)),
    "connect_latency_and_churn": ScenarioFacts(
        metric="connect latency", unit="ms", direction=LOWER, axis_label="connect mode",
        question="How long does a connection take, and does churn survive?"),
    "client_fleet_idle": ScenarioFacts(
        metric="resident memory per client", unit="KiB", direction=LOWER, axis_kind=ORDINAL,
        axis_label="fleet size",
        question="What does an idle connection cost in memory and CPU?"),
    "broker_ceiling_ingress": ScenarioFacts(
        metric="reference subscriber receive rate", axis_kind=ORDINAL, axis_label="offered rate",
        ranked=False,
        question="Where is the broker's own ceiling on this host?",
        caveats=("Always non-comparable: there is no Python client in this topology.",)),
    "client_ceiling_ingress": ScenarioFacts(
        metric="messages delivered", axis_kind=ORDINAL, axis_label="offered rate", ranked=False,
        question="Where is the client's ceiling against a rising offer?",
        caveats=("Always non-comparable: a diagnostic probe, not a ranking point.",)),
}


def facts_for(scenario: str) -> ScenarioFacts:
    """Editorial facts for ``scenario``; a safe placeholder when unlisted."""
    return FACTS.get(scenario, _UNKNOWN)


def intra_client_only(scenario: str) -> bool:
    return facts_for(scenario).intra_client_only


_RL_RE = re.compile(r"^rl_(\d+)$")


def payload_order(name: str) -> float:
    """Sort key placing payload names on the byte-size axis they represent.

    ``sorted()`` on the names alone puts ``blob1m`` before ``empty0``, which
    turns a monotone sweep into noise. Remaining-Length names carry their own
    number and are ordered by it.
    """
    spec = PAYLOAD_SPECS.get(name)
    if spec is not None:
        return float(spec.get("size", 0))
    match = _RL_RE.match(str(name))
    if match:
        return float(match.group(1))
    return float("inf")


#: Axis keys whose values are quantities, ordered smallest first. Anything not
#: listed is treated as a set of conditions and rendered as grouped bars.
ORDINAL_AXES = {
    "payload": payload_order,
    "inflight": lambda v: float(v),
    "load_fraction": lambda v: float(v),
    "shared_load_fraction": lambda v: float(v),
    "target_rate": lambda v: float(v),
    "callback_filters": lambda v: float(v),
    "subscription_count": lambda v: float(v),
    "subscribers": lambda v: float(v),
    "loadgen_clients": lambda v: float(v),
    "fleet_size": lambda v: float(v),
    "retained_count": lambda v: float(v),
    "outage_s": lambda v: float(v),
    "receive_maximum": lambda v: float(v),
    "connect_count": lambda v: float(v),
}


def axis_sort_key(axis: str, value):
    """Order one point along ``axis``; falls back to string order."""
    fn = ORDINAL_AXES.get(axis)
    if fn is None:
        return (1, str(value))
    try:
        return (0, fn(value))
    except (TypeError, ValueError):
        return (1, str(value))


def primary_ordinal_axis(axes: Sequence[str]) -> Optional[str]:
    """The ordinal axis to put on x, if this scenario has one."""
    for axis in axes:
        if axis in ORDINAL_AXES:
            return axis
    return None
