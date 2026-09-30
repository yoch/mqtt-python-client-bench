"""What a campaign runs: a few points, each answering one question.

A point is one workload shape. ``rate=0`` is a capacity point (as fast as the
client goes, bounded by an in-flight ``window``); any other rate is a fixed
offer that every client receives identically, so CPU, memory and latency at
that point compare across every library regardless of how it is built.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Dict, List, Optional, Tuple

KINDS = ("pub", "sub", "rtt", "duplex", "idle")
PROPERTY_SETS = ("none", "realistic")

# Fixed offers sit well below the slowest client's capacity on the reference
# host (amqtt and aiomqtt publish 4.5-9k QoS 1 msgs/s, aiomqtt round-trips
# ~3k/s), so every client absorbs them and the point measures cost, not
# saturation. A saturated client's latency is its queue length.
FIXED_RATE = 2_000
RTT_RATE = 1_000


@dataclass(frozen=True)
class Point:
    name: str
    kind: str  # pub | sub | rtt | duplex | idle
    question: str
    qos: int = 0
    payload: int = 256
    rate: int = 0  # msgs/s (pairs/s for rtt, each way for duplex); 0 = capacity
    window: int = 64  # capacity points: publishes in flight
    protocol: str = "MQTTv311"
    suite: str = "core"
    # Extended knobs. as_dict() leaves them out at their defaults, so the
    # record of a point that does not use them is unchanged.
    topics: int = 1  # > 1: round-robin over <topic>/<i/10>/<i%10>, received through <topic>/#
    filters: int = 0  # > 0: message_callback_add on <topic>/<d>/+ for d < filters
    tls: bool = False
    properties: str = "none"  # PUBLISH property set, see PROPERTY_SETS
    topic_alias: bool = False  # publish the alias alone after the first message
    receive_maximum: int = 0  # > 0: CONNECT Receive Maximum set by the client
    remaining_lengths: Tuple[int, ...] = ()  # publish packets of these sizes in turn

    @property
    def fixed_rate(self) -> bool:
        return self.rate > 0 or self.kind == "idle"

    @property
    def latency(self) -> bool:
        return self.fixed_rate and self.kind in ("pub", "sub", "rtt", "duplex")

    def as_dict(self) -> dict:
        out = asdict(self)
        for key, default in _EXTENDED_DEFAULTS.items():
            if out[key] == default:
                del out[key]
        if "remaining_lengths" in out:
            out["remaining_lengths"] = list(out["remaining_lengths"])
        return out


_EXTENDED_DEFAULTS = {
    "topics": 1,
    "filters": 0,
    "tls": False,
    "properties": "none",
    "topic_alias": False,
    "receive_maximum": 0,
    "remaining_lengths": (),
}


CORE: List[Point] = [
    Point("pub_qos0_max", "pub", "How many QoS 0 messages can the client publish per second?", qos=0),
    Point("pub_qos1_max", "pub", "How many QoS 1 messages (PUBACK received) per second, 64 in flight?", qos=1),
    Point(
        "pub_qos1_fixed",
        "pub",
        "At 2,000 QoS 1 msgs/s, what does publishing cost and how long until delivery?",
        qos=1,
        rate=FIXED_RATE,
    ),
    Point("sub_qos0_max", "sub", "How many QoS 0 messages can the client receive per second?", qos=0),
    Point(
        "sub_qos1_fixed",
        "sub",
        "At 2,000 QoS 1 msgs/s, what does receiving cost and how late do messages arrive?",
        qos=1,
        rate=FIXED_RATE,
    ),
    Point(
        "rtt_qos1_fixed",
        "rtt",
        "At 1,000 requests/s against a neutral echo, what is the application round trip?",
        qos=1,
        rate=RTT_RATE,
    ),
    Point(
        "pub_16k_fixed",
        "pub",
        "At 1,000 QoS 1 msgs/s of 16 KiB, what do large payloads cost?",
        qos=1,
        payload=16_384,
        rate=1_000,
    ),
    Point("idle_connect", "idle", "How long does connecting take, and what does an idle connection cost?"),
]

# The fixed-rate points again over MQTT 5: the only core rows aiomqtt3 (v5
# only) can appear in, and the v3/v5 cost difference for everyone else.
V5_MIRROR: List[Point] = [
    replace(p, name=p.name + "_v5", protocol="MQTTv5", suite="v5")
    for p in CORE
    if p.name in ("pub_qos1_fixed", "sub_qos1_fixed", "rtt_qos1_fixed")
]

LOW_RATE = 500
HIGH_RATE = 5_000
FANOUT_TOPICS = 1_000
FILTERS = 100
# Packet sizes on each side of a remaining-length varint step: 1->2, 2->3 and
# 3->4 bytes. The last one is a 2 MiB packet, far below max_packet_size.
RL_BOUNDARIES = (127, 128, 16_383, 16_384, 2_097_151, 2_097_152)


def _ext(name: str, kind: str, question: str, **kw) -> Point:
    return Point(name, kind, question, suite="extended", **kw)


EXTENDED: List[Point] = [
    # Capacity beyond the core.
    _ext("sub_qos1_max", "sub", "How many QoS 1 messages can the client receive per second?", qos=1),
    _ext("pub_qos1_max_v5", "pub", "QoS 1 publish capacity over MQTT 5.", qos=1, protocol="MQTTv5"),
    _ext("sub_qos1_max_v5", "sub", "QoS 1 receive capacity over MQTT 5.", qos=1, protocol="MQTTv5"),
    _ext("pub_qos1_max_16k", "pub", "QoS 1 publish capacity with 16 KiB payloads.", qos=1, payload=16_384),
    # Load curve: the same absolute offers for every client, below and above
    # the core's 2,000/s.
    _ext("pub_qos1_fixed_500", "pub", "At 500 QoS 1 msgs/s, what does publishing cost?", qos=1, rate=LOW_RATE),
    _ext("pub_qos1_fixed_5k", "pub", "At 5,000 QoS 1 msgs/s, what does publishing cost?", qos=1, rate=HIGH_RATE),
    _ext("sub_qos1_fixed_500", "sub", "At 500 QoS 1 msgs/s, what does receiving cost?", qos=1, rate=LOW_RATE),
    _ext("sub_qos1_fixed_5k", "sub", "At 5,000 QoS 1 msgs/s, what does receiving cost?", qos=1, rate=HIGH_RATE),
    _ext("rtt_qos0_fixed", "rtt", "Round trip at 1,000 req/s with QoS 0.", qos=0, rate=RTT_RATE),
    # QoS 2: four packets per message instead of two.
    _ext("pub_qos2_max", "pub", "How many QoS 2 messages (PUBCOMP received) per second, 64 in flight?", qos=2),
    _ext("pub_qos2_fixed", "pub", "At 2,000 QoS 2 msgs/s, what does publishing cost?", qos=2, rate=FIXED_RATE),
    _ext("sub_qos2_fixed", "sub", "At 2,000 QoS 2 msgs/s, what does receiving cost?", qos=2, rate=FIXED_RATE),
    # Client-side dispatch.
    _ext(
        "pub_fanout_fixed",
        "pub",
        f"At 2,000 QoS 1 msgs/s spread over {FANOUT_TOPICS:,} topics, what does publishing cost?",
        qos=1,
        rate=FIXED_RATE,
        topics=FANOUT_TOPICS,
    ),
    _ext(
        "sub_fanin_fixed",
        "sub",
        f"At 2,000 QoS 1 msgs/s from {FANOUT_TOPICS:,} topics through one wildcard, what does receiving cost?",
        qos=1,
        rate=FIXED_RATE,
        topics=FANOUT_TOPICS,
    ),
    _ext(
        "sub_filters_fixed",
        "sub",
        f"The same, dispatched to {FILTERS} per-filter callbacks (message_callback_add)?",
        qos=1,
        rate=FIXED_RATE,
        topics=FANOUT_TOPICS,
        filters=FILTERS,
    ),
    # One client publishing and receiving at once.
    _ext(
        "duplex_qos1_fixed",
        "duplex",
        "Publishing and receiving 1,000 QoS 1 msgs/s each at once, what does it cost?",
        qos=1,
        rate=RTT_RATE,
    ),
    # TLS.
    _ext("pub_qos1_fixed_tls", "pub", "At 2,000 QoS 1 msgs/s over TLS, what does publishing cost?", qos=1, rate=FIXED_RATE, tls=True),
    _ext("sub_qos1_fixed_tls", "sub", "At 2,000 QoS 1 msgs/s over TLS, what does receiving cost?", qos=1, rate=FIXED_RATE, tls=True),
    _ext("idle_connect_tls", "idle", "How long does a TLS connect take, and what does an idle TLS connection cost?", tls=True),
    # MQTT 5 features.
    _ext(
        "pub_qos1_fixed_v5_props",
        "pub",
        "At 2,000 QoS 1 msgs/s with four PUBLISH properties, what does publishing cost?",
        qos=1,
        rate=FIXED_RATE,
        protocol="MQTTv5",
        properties="realistic",
    ),
    _ext(
        "sub_qos1_fixed_v5_props",
        "sub",
        "At 2,000 QoS 1 msgs/s with four PUBLISH properties, what does receiving cost?",
        qos=1,
        rate=FIXED_RATE,
        protocol="MQTTv5",
        properties="realistic",
    ),
    _ext(
        "pub_qos1_fixed_v5_alias",
        "pub",
        "At 2,000 QoS 1 msgs/s on a 200-byte topic sent as a topic alias, what does publishing cost?",
        qos=1,
        rate=FIXED_RATE,
        protocol="MQTTv5",
        topic_alias=True,
    ),
    _ext(
        "sub_qos1_max_v5_rm16",
        "sub",
        "QoS 1 receive capacity when the client allows only 16 unacknowledged deliveries (Receive Maximum).",
        qos=1,
        protocol="MQTTv5",
        receive_maximum=16,
    ),
    # Payload sizes.
    _ext("pub_64k_fixed", "pub", "At 500 QoS 1 msgs/s of 64 KiB, what does publishing cost?", qos=1, payload=65_536, rate=500),
    _ext("pub_1m_fixed", "pub", "At 50 QoS 1 msgs/s of 1 MiB, what does publishing cost?", qos=1, payload=1 << 20, rate=50),
    _ext(
        "pub_rl_boundaries",
        "pub",
        "Are packets on each side of a remaining-length step (127/128 B, 16 KiB, 2 MiB) all delivered intact?",
        qos=1,
        payload=0,
        rate=60,
        remaining_lengths=RL_BOUNDARIES,
    ),
]

SUITES: Dict[str, List[Point]] = {
    "core": CORE,
    "v5": V5_MIRROR,
    "extended": EXTENDED,
}

ALL_POINTS: Dict[str, Point] = {p.name: p for suite in SUITES.values() for p in suite}


def resolve(names: Optional[List[str]] = None, suites: Optional[List[str]] = None) -> List[Point]:
    if names:
        unknown = [n for n in names if n not in ALL_POINTS]
        if unknown:
            raise KeyError(f"unknown point(s): {', '.join(unknown)}")
        return [ALL_POINTS[n] for n in names]
    out: List[Point] = []
    for suite in suites or ["core"]:
        out.extend(SUITES[suite])
    return out


@dataclass(frozen=True)
class Profile:
    name: str
    warmup_s: float
    measure_s: float
    drain_s: float
    runs: int
    comparable: bool


PROFILES: Dict[str, Profile] = {
    "standard": Profile("standard", warmup_s=2.0, measure_s=8.0, drain_s=2.0, runs=3, comparable=True),
    "smoke": Profile("smoke", warmup_s=0.5, measure_s=2.0, drain_s=1.0, runs=1, comparable=False),
}

# Fixed cost of one run beyond its schedule: process start, connect, and one
# fresh $SYS reading before and after (sys_interval is 1 s). Measured ~1.8 s.
RUN_OVERHEAD_S = 2.0
# A standard campaign over every suite and client must fit in this, with the
# margin left for the session setup and the retried invalid runs.
FULL_CAMPAIGN_BUDGET_S = 3 * 3600
FULL_CAMPAIGN_MARGIN = 0.95
