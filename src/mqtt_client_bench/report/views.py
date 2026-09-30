"""What the report shows: metrics, the home page's questions, point columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple


def _thousands(v: float) -> str:
    return f"{v:,.0f}"


def _micros(v: float) -> str:
    if v >= 10_000:
        return f"{v / 1e3:,.1f} ms"
    if v >= 100:
        return f"{v:,.0f} µs"
    return f"{v:.1f} µs"


def _mib(v: float) -> str:
    return f"{v / 1024:.1f} MiB"


def _percent(v: float) -> str:
    return f"{v * 100:.1f} %"


def _one(v: float) -> str:
    return f"{v:.1f}"


def _integer(v: float) -> str:
    return f"{v:.0f}"


@dataclass(frozen=True)
class Metric:
    label: str
    better: str  # "higher" | "lower"
    fmt: Callable[[float], str]
    help: str
    bar: bool = False


METRICS: Dict[str, Metric] = {
    "msgs_per_s": Metric("msgs/s", "higher", _thousands, "Messages confirmed delivered per second of the window.", bar=True),
    "cpu_us_per_msg": Metric("CPU / msg", "lower", _micros, "Client CPU (user + sys, all threads) per message."),
    "cpu_user_us_per_msg": Metric("user / msg", "lower", _micros, "Client user-mode CPU per message."),
    "cpu_sys_us_per_msg": Metric("sys / msg", "lower", _micros, "Client kernel-mode CPU per message."),
    "cpu_cores": Metric("CPU", "lower", _percent, "Share of one core the client used over the window."),
    "rss_peak_kb": Metric("peak RSS", "lower", _mib, "Peak resident memory of the client during the window."),
    "threads": Metric("threads", "lower", _integer, "Client threads at the end of the window."),
    "ctx_switches_per_1k_msgs": Metric("ctx sw / 1k", "lower", _thousands, "Context switches per thousand messages."),
    "connect_ms": Metric("connect", "lower", lambda v: f"{v:.1f} ms", "Connect call to CONNACK."),
    "undelivered_at_stop": Metric("in flight at stop", "lower", _thousands, "Messages the broker sent that the client had not read when the run stopped."),
    "p50": Metric("p50", "lower", _micros, "Median latency over every valid sample."),
    "p90": Metric("p90", "lower", _micros, "90th percentile latency."),
    "p99": Metric("p99", "lower", _micros, "99th percentile latency."),
    "p999": Metric("p99.9", "lower", _micros, "99.9th percentile latency."),
    "max": Metric("max", "lower", _micros, "Largest latency observed."),
    "lag_p99": Metric(
        "lag p99",
        "lower",
        _micros,
        "99th percentile of how late the client published against the fixed schedule. Up to 1 ms of it is the "
        "harness's pacing tick, identical for every client.",
    ),
    "lag_max": Metric("lag max", "lower", _micros, "Latest publish against the fixed schedule."),
    "rx_p50": Metric("receive p50", "lower", _micros, "Duplex: median latency of the messages the client received."),
    "rx_p99": Metric("receive p99", "lower", _micros, "Duplex: 99th percentile latency of the messages the client received."),
}


@dataclass(frozen=True)
class Column:
    point: str
    metric: str
    label: str


@dataclass(frozen=True)
class Question:
    slug: str
    title: str
    lede: str
    columns: Tuple[Column, ...]


QUESTIONS: List[Question] = [
    Question(
        "publish-capacity",
        "How fast can each client publish?",
        "Messages per second the C subscriber received, 256-byte payloads. QoS 1 keeps 64 publishes in flight and counts a message once its PUBACK is back. “≥” marks a rate at which the broker saturated first: the client's own capacity is at least that.",
        (
            Column("pub_qos0_max", "msgs_per_s", "QoS 0"),
            Column("pub_qos1_max", "msgs_per_s", "QoS 1"),
            Column("pub_qos1_max", "cpu_us_per_msg", "QoS 1 CPU / msg"),
        ),
    ),
    Question(
        "receive-capacity",
        "How fast can each client receive?",
        "Messages per second read by the client from a C publisher offering 90 % of the broker's own C→C ceiling. “≥” marks a client that took the whole offer: its capacity is higher still, and such clients tie.",
        (
            Column("sub_qos0_max", "msgs_per_s", "QoS 0"),
            Column("sub_qos0_max", "cpu_us_per_msg", "CPU / msg"),
        ),
    ),
    Question(
        "cpu-fixed",
        "What does a message cost at the same load?",
        "Client CPU per message when every client gets the identical offer: 2,000 msgs/s, 1,000 requests/s for round trips and 16 KiB payloads.",
        (
            Column("pub_qos1_fixed", "cpu_us_per_msg", "publish QoS 1"),
            Column("sub_qos1_fixed", "cpu_us_per_msg", "receive QoS 1"),
            Column("rtt_qos1_fixed", "cpu_us_per_msg", "round trip"),
            Column("pub_16k_fixed", "cpu_us_per_msg", "publish 16 KiB"),
        ),
    ),
    Question(
        "latency-fixed",
        "How late do messages arrive?",
        "From the stamp written at the actual publish to its arrival at the subscriber, same clock, 2,000 QoS 1 msgs/s. Publish: the client publishes, C receives. Receive: C publishes, the client's callback receives. Time a client spent behind its schedule before publishing is the next question's.",
        (
            Column("pub_qos1_fixed", "p50", "publish p50"),
            Column("pub_qos1_fixed", "p99", "publish p99"),
            Column("sub_qos1_fixed", "p50", "receive p50"),
            Column("sub_qos1_fixed", "p99", "receive p99"),
        ),
    ),
    Question(
        "schedule-lag",
        "Does each client publish on time?",
        "How late each publish left against its due time in the fixed offer, on the same clock. Latency starts at the actual publish; this is the wait before it, which a client that stalls or runs out of in-flight slots cannot hide. Up to 1 ms is the harness's pacing tick, the same for every client.",
        (
            Column("pub_qos1_fixed", "lag_p99", "publish p99"),
            Column("pub_qos1_fixed", "lag_max", "publish max"),
            Column("rtt_qos1_fixed", "lag_p99", "round trip p99"),
            Column("pub_16k_fixed", "lag_p99", "16 KiB p99"),
        ),
    ),
    Question(
        "round-trip",
        "What is the application round trip?",
        "The client publishes a request at 1,000/s and a C echo republishes it; timed in the client from publish to its callback on the reply. Two broker hops, QoS 1 both ways.",
        (
            Column("rtt_qos1_fixed", "p50", "p50"),
            Column("rtt_qos1_fixed", "p99", "p99"),
            Column("rtt_qos1_fixed", "p999", "p99.9"),
        ),
    ),
    Question(
        "memory",
        "How much memory does each client hold?",
        "Peak resident set of the client process during the window, including the interpreter.",
        (
            Column("idle_connect", "rss_peak_kb", "idle"),
            Column("pub_qos1_fixed", "rss_peak_kb", "publish 2k/s"),
            Column("sub_qos0_max", "rss_peak_kb", "receive at capacity"),
            Column("pub_16k_fixed", "rss_peak_kb", "publish 16 KiB"),
        ),
    ),
    Question(
        "idle",
        "What do connecting and idling cost?",
        "One connection with nothing to do but keep alive.",
        (
            Column("idle_connect", "connect_ms", "connect"),
            Column("idle_connect", "cpu_cores", "idle CPU"),
            Column("idle_connect", "threads", "threads"),
        ),
    ),
    Question(
        "mqtt5",
        "Does MQTT 5 cost more?",
        "The fixed-rate points again over MQTT 5 — the only rows for aiomqtt3, which speaks nothing else.",
        (
            Column("pub_qos1_fixed_v5", "cpu_us_per_msg", "publish CPU / msg"),
            Column("sub_qos1_fixed_v5", "cpu_us_per_msg", "receive CPU / msg"),
            Column("pub_qos1_fixed_v5", "p99", "publish p99"),
            Column("rtt_qos1_fixed_v5", "p99", "round trip p99"),
        ),
    ),
    # The extended suite. A question whose points were not run is skipped.
    Question(
        "load-curve",
        "How does the cost change with the load?",
        "Client CPU per message at 500, 2,000 and 5,000 QoS 1 msgs/s, the same absolute offers for every client. A client that cannot hold 5,000/s shows as not sustained there.",
        (
            Column("pub_qos1_fixed_500", "cpu_us_per_msg", "publish 500/s"),
            Column("pub_qos1_fixed", "cpu_us_per_msg", "publish 2k/s"),
            Column("pub_qos1_fixed_5k", "cpu_us_per_msg", "publish 5k/s"),
            Column("sub_qos1_fixed_500", "cpu_us_per_msg", "receive 500/s"),
            Column("sub_qos1_fixed", "cpu_us_per_msg", "receive 2k/s"),
            Column("sub_qos1_fixed_5k", "cpu_us_per_msg", "receive 5k/s"),
        ),
    ),
    Question(
        "load-curve-latency",
        "Does latency hold as the load grows?",
        "Publish-to-delivery p99 at the same three offers.",
        (
            Column("pub_qos1_fixed_500", "p99", "publish 500/s"),
            Column("pub_qos1_fixed", "p99", "publish 2k/s"),
            Column("pub_qos1_fixed_5k", "p99", "publish 5k/s"),
            Column("sub_qos1_fixed_5k", "p99", "receive 5k/s"),
        ),
    ),
    Question(
        "qos2",
        "What does exactly-once (QoS 2) cost?",
        "Four packets per message instead of two. Capacity counts a message once its PUBCOMP is back, 64 in flight. Clients whose QoS 2 does not complete the exchange are refused.",
        (
            Column("pub_qos2_max", "msgs_per_s", "publish capacity"),
            Column("pub_qos2_fixed", "cpu_us_per_msg", "publish 2k/s CPU"),
            Column("pub_qos2_fixed", "p99", "publish 2k/s p99"),
            Column("sub_qos2_fixed", "cpu_us_per_msg", "receive 2k/s CPU"),
        ),
    ),
    Question(
        "dispatch",
        "What do many topics cost?",
        "2,000 QoS 1 msgs/s spread over 1,000 topics: published round-robin, received through one wildcard, or dispatched to 100 per-filter callbacks (only libraries that match filters natively).",
        (
            Column("pub_fanout_fixed", "cpu_us_per_msg", "publish, 1,000 topics"),
            Column("sub_fanin_fixed", "cpu_us_per_msg", "receive, wildcard"),
            Column("sub_filters_fixed", "cpu_us_per_msg", "receive, 100 callbacks"),
            Column("sub_filters_fixed", "p99", "callbacks p99"),
        ),
    ),
    Question(
        "duplex",
        "Can one client publish and receive at once?",
        "The client publishes 1,000 QoS 1 msgs/s to a C sink while a C source sends it 1,000 msgs/s. CPU is per message handled in either direction.",
        (
            Column("duplex_qos1_fixed", "cpu_us_per_msg", "CPU / msg"),
            Column("duplex_qos1_fixed", "p99", "publish p99"),
            Column("duplex_qos1_fixed", "rx_p99", "receive p99"),
        ),
    ),
    Question(
        "tls",
        "What does TLS cost?",
        "The same 2,000 QoS 1 msgs/s over a TLS listener (the C peer stays on plain TCP), and the connect including the handshake.",
        (
            Column("pub_qos1_fixed_tls", "cpu_us_per_msg", "publish CPU / msg"),
            Column("sub_qos1_fixed_tls", "cpu_us_per_msg", "receive CPU / msg"),
            Column("pub_qos1_fixed_tls", "p99", "publish p99"),
            Column("idle_connect_tls", "connect_ms", "connect"),
        ),
    ),
    Question(
        "mqtt5-features",
        "What do MQTT 5 features cost?",
        "Five PUBLISH properties on every message, a 200-byte topic replaced by a topic alias (broker byte counters confirm it), and receive capacity when the client allows only 16 unacknowledged deliveries.",
        (
            Column("pub_qos1_fixed_v5_props", "cpu_us_per_msg", "publish, properties"),
            Column("sub_qos1_fixed_v5_props", "cpu_us_per_msg", "receive, properties"),
            Column("pub_qos1_fixed_v5_alias", "cpu_us_per_msg", "publish, alias"),
            Column("sub_qos1_max_v5", "msgs_per_s", "receive capacity"),
            Column("sub_qos1_max_v5_rm16", "msgs_per_s", "receive, Receive Maximum 16"),
        ),
    ),
    Question(
        "payloads",
        "How do large payloads behave?",
        "QoS 1 at 500 msgs/s of 64 KiB and 50 msgs/s of 1 MiB, and packets on both sides of each remaining-length step up to 2 MiB, every length checked by the C sink.",
        (
            Column("pub_64k_fixed", "cpu_us_per_msg", "64 KiB CPU / msg"),
            Column("pub_64k_fixed", "p99", "64 KiB p99"),
            Column("pub_1m_fixed", "cpu_us_per_msg", "1 MiB CPU / msg"),
            Column("pub_1m_fixed", "p99", "1 MiB p99"),
            Column("pub_rl_boundaries", "p99", "RL boundaries p99"),
        ),
    ),
]


def point_columns(point: dict) -> List[str]:
    """Every metric a point's own page tabulates, most important first."""
    kind, fixed = point["kind"], bool(point["rate"])
    if kind == "idle":
        return ["connect_ms", "cpu_cores", "rss_peak_kb", "threads"]
    # The user / sys split and thread count unfold with each run's counts.
    resources = ["cpu_us_per_msg", "rss_peak_kb", "ctx_switches_per_1k_msgs"]
    if fixed and kind == "duplex":
        return ["p50", "p99", "rx_p50", "rx_p99", "lag_p99", *resources]
    if fixed and kind in ("pub", "rtt"):
        return ["p50", "p99", "p999", "max", "lag_p99", *resources]
    if fixed:
        return ["p50", "p99", "p999", "max", *resources]
    extra = ["undelivered_at_stop"] if kind == "sub" else []
    return ["msgs_per_s", "cpu_cores", *resources, *extra]


def sort_key(metric: str, value: Optional[float]) -> Tuple[int, float]:
    if value is None:
        return (1, 0.0)
    return (0, -value if METRICS[metric].better == "higher" else value)


def rate_label(point: dict) -> str:
    if point["kind"] == "idle":
        return "idle"
    if not point["rate"]:
        return "capacity" + (f", {point['window']} in flight" if point["kind"] == "pub" and point["qos"] else "")
    unit = {"rtt": "requests/s", "duplex": "msgs/s each way"}.get(point["kind"], "msgs/s")
    return f"{point['rate']:,} {unit}"


def payload_label(n: int) -> str:
    if n >= 1 << 20 and n % (1 << 20) == 0:
        return f"{n >> 20} MiB"
    return f"{n // 1024} KiB" if n >= 1024 and n % 1024 == 0 else f"{n} B"


def point_payload(point: dict) -> str:
    lengths = point.get("remaining_lengths")
    if lengths:
        return "packets of " + ", ".join(payload_label(n) for n in lengths)
    return payload_label(point["payload"])


def point_features(point: dict) -> List[str]:
    """The extended knobs a point sets, for its spec line."""
    out = []
    if point.get("topics", 1) > 1:
        out.append(f"{point['topics']:,} topics")
    if point.get("filters"):
        out.append(f"{point['filters']} filter callbacks")
    if point.get("tls"):
        out.append("TLS")
    if point.get("properties", "none") != "none":
        out.append(f"{point['properties']} properties")
    if point.get("topic_alias"):
        out.append("topic alias")
    if point.get("receive_maximum"):
        out.append(f"Receive Maximum {point['receive_maximum']}")
    return out
