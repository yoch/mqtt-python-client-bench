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
]


def point_columns(point: dict) -> List[str]:
    """Every metric a point's own page tabulates, most important first."""
    kind, fixed = point["kind"], bool(point["rate"])
    if kind == "idle":
        return ["connect_ms", "cpu_cores", "rss_peak_kb", "threads"]
    # The user / sys split and thread count unfold with each run's counts.
    resources = ["cpu_us_per_msg", "rss_peak_kb", "ctx_switches_per_1k_msgs"]
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
    unit = "requests/s" if point["kind"] == "rtt" else "msgs/s"
    return f"{point['rate']:,} {unit}"


def payload_label(n: int) -> str:
    return f"{n // 1024} KiB" if n >= 1024 and n % 1024 == 0 else f"{n} B"
