"""What a campaign runs: a few points, each answering one question.

A point is one workload shape. ``rate=0`` is a capacity point (as fast as the
client goes, bounded by an in-flight ``window``); any other rate is a fixed
offer that every client receives identically, so CPU, memory and latency at
that point compare across every library regardless of how it is built.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Dict, List, Optional

KINDS = ("pub", "sub", "rtt", "idle")

# Fixed offers sit well below the slowest client's capacity on the reference
# host (amqtt and aiomqtt publish 4.5-9k QoS 1 msgs/s, aiomqtt round-trips
# ~3k/s), so every client absorbs them and the point measures cost, not
# saturation. A saturated client's latency is its queue length.
FIXED_RATE = 2_000
RTT_RATE = 1_000


@dataclass(frozen=True)
class Point:
    name: str
    kind: str  # pub | sub | rtt | idle
    question: str
    qos: int = 0
    payload: int = 256
    rate: int = 0  # msgs/s (pairs/s for rtt); 0 = capacity
    window: int = 64  # capacity points: publishes in flight
    protocol: str = "MQTTv311"
    suite: str = "core"

    @property
    def fixed_rate(self) -> bool:
        return self.rate > 0 or self.kind == "idle"

    @property
    def latency(self) -> bool:
        return self.fixed_rate and self.kind in ("pub", "sub", "rtt")

    def as_dict(self) -> dict:
        return asdict(self)


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

EXTENDED: List[Point] = [
    Point("pub_qos0_fixed", "pub", "At 2,000 QoS 0 msgs/s, what does publishing cost?", qos=0, rate=FIXED_RATE, suite="extended"),
    Point("sub_qos0_fixed", "sub", "At 2,000 QoS 0 msgs/s, what does receiving cost?", qos=0, rate=FIXED_RATE, suite="extended"),
    Point("sub_qos1_max", "sub", "How many QoS 1 messages can the client receive per second?", qos=1, suite="extended"),
    Point("pub_qos0_max_1k", "pub", "QoS 0 capacity with 1 KiB payloads.", qos=0, payload=1024, suite="extended"),
    Point("pub_qos1_max_16k", "pub", "QoS 1 capacity with 16 KiB payloads.", qos=1, payload=16_384, suite="extended"),
    Point("pub_qos1_max_v5", "pub", "QoS 1 capacity over MQTT 5.", qos=1, protocol="MQTTv5", suite="extended"),
    Point("sub_qos0_max_v5", "sub", "QoS 0 receive capacity over MQTT 5.", qos=0, protocol="MQTTv5", suite="extended"),
    Point("rtt_qos0_fixed", "rtt", "Round trip at 1,000 req/s with QoS 0.", qos=0, rate=1_000, suite="extended"),
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
