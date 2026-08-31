"""Result documents: load ``results/*.json`` and turn them into renderable facts.

This module is the report's data layer. It holds no HTML: everything here is
about *what a number is allowed to mean* — which runs may enter a median, which
peer group a client belongs to, why a cell is empty — so the page modules can
stay about layout. The comparability gates live in :func:`_run_rankable` and
:func:`classify_payload`; changing them changes what the site publishes.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-").lower()
    return cleaned or "result"


def _fmt_num(value: Any, *, digits: int = 1) -> str:
    if value is None:
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if digits == 0:
        return f"{number:,.0f}"
    return f"{number:,.{digits}f}"


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


_CLIENT_ORDER = (
    "awscrt",
    "gmqtt",
    "mqttium",
    "paho",
    "amqtt",
    "aiomqtt",
    "zmqtt",
    "aiomqtt3",
    "mqttium-compat",
)

# Stability as the report should present it, where that differs from what the
# committed results declare. mqttium is on the point of its stable release, so
# it is ranked and ordered as stable here rather than being sorted behind the
# released libraries for one more campaign.
#
# This is deliberately a report-layer exception, not a change to
# ``adapters/mqttium.py``: the registry's ``experimental`` grouping still drives
# the install extras and the ``--suite experimental`` selection, and every
# committed result already carries ``stability: experimental`` in its
# ``client_identity``. When the adapter declares the release, this entry becomes
# a no-op and should be deleted.
_STABILITY_OVERRIDES = {"mqttium": "stable"}

# Rate-capped / functional / niche scenarios: primary msg/s either echoes an
# injected ceiling or is only meaningful for one client (paho-native callbacks).
# Keep them in the matrix (last); omit them from the overview throughput chart.
_CHART_EXCLUDED_SCENARIOS = frozenset(
    {
        "session_resume_qos1",
        "reconnect_ordering",
        "duplex_gateway",
        "e2e_integrity",
        "sub_callback_matching",
        "remaining_length_boundaries",
        "broker_ceiling_ingress",
        "client_ceiling_ingress",
        "queue_rejection",
        "retained_bootstrap",
        "mqttv5_flow_control",
        # Fraction-of-own-capacity latency is an intra-client question, not a
        # cross-client ranking. The public latency comparison is
        # puback_latency_fixed_rate (equal absolute offered rates).
        "puback_latency_qos1",
        "application_rtt_qos1",
    }
)
_CHART_EXCLUDED_ORDER = (
    "session_resume_qos1",
    "reconnect_ordering",
    "duplex_gateway",
    "e2e_integrity",
    "sub_callback_matching",
    "remaining_length_boundaries",
    "broker_ceiling_ingress",
    "client_ceiling_ingress",
    "queue_rejection",
    "retained_bootstrap",
    "mqttv5_flow_control",
    "puback_latency_qos1",
    "application_rtt_qos1",
)

# The set of intra-client latency scenarios used to be declared here and never
# read, while the same two names were written out again in the matrix hint text.
# ``catalog.ScenarioFacts.intra_client_only`` is the single declaration now, and
# every page that has to warn about the distinction asks the catalogue.

# Failures that reflect how the SUT behaved under the offered load (or its
# protocol/API limits). These must stay visible in the report — excluding them
# from the throughput chart is fine; burying them is not.
_CLIENT_LOAD_REASON_PREFIXES = (
    "open_loop_rate_out_of_tolerance",
    "open_loop_backpressure_misses",
    "protocol_failed",
    "timed_out_mids",
    "rtt_timeouts",
    "warmup_drain_timeout",
    "no_delivery_despite_load",
    "integrity_mismatch",
    "worker_error:",
)
_CLIENT_CAPABILITY_PREFIX = "not_implemented:"
_ENVIRONMENT_REASON_PREFIXES = (
    "container_cpu_high:",
    "broker_telemetry_missing",
    "loadgen_emitted_nothing",
    "loadgen_below_half_nominal",
    "loadgen_unconfirmed_by_broker",
    "barrier_failed",
    "sys_publish_dropped",
    "delivery_below_half_offer",
    # The broker's forwarding was the constraint, not the client. Environment
    # rather than capability: it says something about the machine.
    "broker_fanout_limited",
    # Host state at T0. Without these the governor and loadavg gates fired but
    # the run only ever showed up as "other", so a machine-invalidated result
    # never reached the environment banner.
    "cpu_governor_unknown",
    "cpu_governor_not_performance:",
    "host_busy_at_start:",
)

# Values within this relative tolerance of the row/series maximum are treated
# as tied. Medians carry float noise (e.g. a 1000 msg/s rate cap surfaces as
# 999.9944...), so a strict `==` comparison against the max silently picks a
# single "winner" among values that are displayed identically.
_TIE_RELATIVE_TOLERANCE = 1e-3


def _is_tied_with_best(value: float, best: float) -> bool:
    if value == best:
        return True
    scale = max(abs(best), 1e-9)
    return abs(value - best) / scale <= _TIE_RELATIVE_TOLERANCE


def _reason_kind(reason: str) -> str:
    if reason.startswith(_CLIENT_CAPABILITY_PREFIX):
        return "capability"
    if any(reason.startswith(p) for p in _CLIENT_LOAD_REASON_PREFIXES):
        return "load"
    if any(reason.startswith(p) for p in _ENVIRONMENT_REASON_PREFIXES):
        return "environment"
    return "other"


def _short_reason(reason: str) -> str:
    if reason.startswith(_CLIENT_CAPABILITY_PREFIX):
        return reason[len(_CLIENT_CAPABILITY_PREFIX) :]
    if reason.startswith("container_cpu_high:"):
        return "broker_cpu"
    if reason.startswith("sys_publish_dropped"):
        return "broker_drops"
    if reason.startswith("delivery_below_half_offer"):
        return "delivery_lt_half_offer"
    if reason.startswith("broker_fanout_limited"):
        return "broker_fanout"
    if reason.startswith("worker_error:"):
        return "worker_error"
    if reason.startswith("barrier_failed"):
        return "barrier"
    return reason


def _order_matrix_scenarios(scenarios: Sequence[str]) -> List[str]:
    """Throughput scenarios first; rate-capped / niche rows last (stable order)."""
    primary = [s for s in scenarios if _scenario_base(s) not in _CHART_EXCLUDED_SCENARIOS]
    trailing = [s for s in scenarios if _scenario_base(s) in _CHART_EXCLUDED_SCENARIOS]
    rank = {name: i for i, name in enumerate(_CHART_EXCLUDED_ORDER)}
    trailing.sort(key=lambda s: (rank.get(_scenario_base(s), 99), s))
    return primary + trailing


def _matrix_row_id(scenario: str, protocol: Optional[str] = None) -> str:
    proto = protocol or "MQTTv311"
    return f"{scenario} · {proto}"


def _scenario_base(row_id: str) -> str:
    if " · " in row_id:
        return row_id.rsplit(" · ", 1)[0]
    return row_id



# Peer-group order for display. io_model itself is read from client_identity in
# the result JSON (see ClientMeta), never from a table in this file.
_IO_MODEL_ORDER = ("sync", "asyncio_bridged", "crt_event_loop")
_STABILITY_ORDER = ("stable", "experimental")


@dataclass
class ClientMeta:
    """Per-client facts, taken from the results rather than hard-coded here."""

    name: str
    io_model: str = "unknown"
    stability: str = "unknown"
    version: Optional[str] = None
    private_api: Dict[str, str] = field(default_factory=dict)

    @property
    def peer_group(self) -> str:
        """The set of clients this one may be ranked against.

        The I/O model alone. Stability used to split the groups as well, on the
        grounds that a pre-release library should not take a crown from a
        released one — but that put clients doing identical work in separate
        charts, and a library moving from experimental to stable would have
        silently changed which numbers it was being compared with. Stability now
        orders clients inside a group (stable first) and is shown as a badge, so
        the reader still sees it; it no longer decides who competes.
        """
        return self.io_model


def _registry_meta(name: str) -> tuple:
    """Fall back to the adapter registry for results predating client_identity.

    Still not a table in this file: the registry is the same source the harness
    stamps into new results, so the two can never disagree.
    """
    try:
        from mqtt_client_bench.adapters.registry import get_adapter_class

        caps = get_adapter_class(name).capabilities()
        return caps.io_model, caps.stability
    except Exception:  # noqa: BLE001 - unknown client, or adapter deps missing
        return None, None


def _client_has_native_async_path(client: Optional[str]) -> bool:
    if not client:
        return False
    try:
        from mqtt_client_bench.adapters.registry import has_async_adapter

        return bool(has_async_adapter(client))
    except Exception:  # noqa: BLE001
        return client in {
            "gmqtt",
            "aiomqtt",
            "amqtt",
            "zmqtt",
            "aiomqtt3",
            "mqttium",
        }


def _run_rankable(run: Dict[str, Any], *, client: Optional[str] = None) -> bool:
    """True when a run may enter a cross-client median or latency aggregate.

    Native and facade publish paths measure different harness tax; a
    sync_facade result for a client that has a native path is diagnostic even
    when an older JSON omitted non_comparable.
    """
    if run.get("status") != "valid" or run.get("non_comparable"):
        return False
    if run.get("publish_path") == "sync_facade" and _client_has_native_async_path(client):
        return False
    return True


def _client_meta(docs: Sequence["ResultDoc"]) -> Dict[str, ClientMeta]:
    """Collect client metadata from the documents' client_identity blocks."""
    meta: Dict[str, ClientMeta] = {}
    for doc in docs:
        if doc.kind != "scenario" or not doc.client:
            continue
        entry = meta.setdefault(doc.client, ClientMeta(name=doc.client))
        if doc.io_model:
            entry.io_model = doc.io_model
        if doc.stability:
            entry.stability = doc.stability
        if doc.client_version:
            entry.version = doc.client_version
        if doc.private_api:
            entry.private_api.update(doc.private_api)
    for name, entry in meta.items():
        if entry.io_model != "unknown" and entry.stability != "unknown":
            continue
        io_model, stability = _registry_meta(name)
        if entry.io_model == "unknown" and io_model:
            entry.io_model = io_model
        if entry.stability == "unknown" and stability:
            entry.stability = stability
    # Applied last so it wins over both the committed results and the registry.
    for name, stability in _STABILITY_OVERRIDES.items():
        if name in meta:
            meta[name].stability = stability
    return meta


def _sort_clients(clients: Sequence[str], meta: Optional[Dict[str, ClientMeta]] = None) -> List[str]:
    """Order by peer group, then stable before experimental, then display order.

    Stability no longer splits the groups, but it still orders them: a released
    library reads first, and a pre-release one is badged rather than exiled to a
    chart of its own.
    """
    rank = {name: i for i, name in enumerate(_CLIENT_ORDER)}

    def key(client: str):
        info = (meta or {}).get(client)
        stability = info.stability if info else "unknown"
        io_model = info.io_model if info else "unknown"
        return (
            _IO_MODEL_ORDER.index(io_model) if io_model in _IO_MODEL_ORDER else len(_IO_MODEL_ORDER),
            _STABILITY_ORDER.index(stability) if stability in _STABILITY_ORDER else len(_STABILITY_ORDER),
            rank.get(client, len(_CLIENT_ORDER)),
            client,
        )

    return sorted(clients, key=key)
def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


# Axes that a scenario can sweep. A label built from a fixed subset collapsed
# distinct points into identical rows (pub_qos1_inflight showed three identical
# lines for inflight 1/20/100), which silently hid the very thing being swept.
_LABEL_AXES = (
    ("payload", "payload"),
    ("qos_publish", "qos"),
    ("qos_subscribe", "sub_qos"),
    ("protocol", "proto"),
    ("topology", "topo"),
    ("inflight", "inflight"),
    ("load_fraction", "load"),
    ("subscription", "sub"),
    ("topic_topology", "topics"),
    ("callback_filters", "filters"),
    ("subscription_count", "subs"),
    ("subscribers", "n_sub"),
    ("loadgen_clients", "gen"),
    ("cadence", "cadence"),
    ("properties_profile", "props"),
    ("connect_mode", "connect"),
    ("fleet_size", "fleet"),
    ("network", "net"),
    ("tls", "tls"),
)


def _point_label(point: Dict[str, Any], varying: Optional[Sequence[str]] = None) -> str:
    """Label a measurement point.

    ``varying`` restricts the label to the axes that actually differ inside the
    scenario, keeping labels short while guaranteeing they stay distinct.
    """
    parts = []
    for key, alias in _LABEL_AXES:
        if varying is not None and key not in varying:
            continue
        value = point.get(key)
        if value is None:
            continue
        if key == "tls" and not value:
            continue
        if key == "network" and value == "localhost":
            continue
        parts.append(f"{alias}={value}")
    return ", ".join(parts) if parts else "default"


def _varying_axes(points: Sequence[Dict[str, Any]]) -> List[str]:
    """Axes whose value is not constant across a scenario's points."""
    if len(points) <= 1:
        return [k for k, _ in _LABEL_AXES if points and points[0].get(k) is not None][:5]
    varying = []
    for key, _ in _LABEL_AXES:
        seen = {repr(p.get(key)) for p in points}
        if len(seen) > 1:
            varying.append(key)
    if not varying:
        # Genuinely identical points (repeat runs): fall back to a stable subset.
        varying = ["payload", "qos_publish", "protocol", "topology"]
    return varying


def _dominant(runs: Sequence[Dict[str, Any]], key: str) -> Optional[str]:
    """Most common non-empty value of ``key`` across a point's runs."""
    counts: Dict[str, int] = {}
    for run in runs:
        value = run.get(key)
        if value:
            counts[str(value)] = counts.get(str(value), 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _max_of(runs: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    values = [float(r[key]) for r in runs if r.get(key) is not None]
    return max(values) if values else None


def _ratio_of(runs: Sequence[Dict[str, Any]]) -> Optional[float]:
    values = [
        float((r.get("broker_reconciliation") or {}).get("ratio"))
        for r in runs
        if (r.get("broker_reconciliation") or {}).get("ratio") is not None
    ]
    return (sum(values) / len(values)) if values else None


def _cost_of(runs: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    values = [
        float((r.get("cost_per_message") or {}).get(key))
        for r in runs
        if (r.get("cost_per_message") or {}).get(key) is not None
    ]
    return (sum(values) / len(values)) if values else None


# What an empty cell means. Collapsing four different outcomes into one em-dash
# made "the client cannot do this" indistinguishable from "we never ran it".
EMPTY_GLYPHS = {
    "refused": ("⊘", "refused — client lacks the capability"),
    "failed": ("✕", "failed under load"),
    "environment": ("!", "invalidated by the environment (broker or host)"),
    "missing": ("–", "not run"),
}


def _empty_cell_reason(
    runs: Sequence[Dict[str, Any]], median: Optional[float]
) -> tuple:
    """Classify why a point has no comparable number."""
    if median is not None:
        return None, ""
    if not runs:
        return "missing", ""
    reasons = [str(r) for run in runs for r in (run.get("reasons") or [])]
    if not reasons:
        return "missing", ""
    kinds = {_reason_kind(r) for r in reasons}
    shorts = sorted({_short_reason(r) for r in reasons})
    detail = ", ".join(shorts[:3])
    if "capability" in kinds:
        return "refused", detail
    if "environment" in kinds:
        return "environment", detail
    if "load" in kinds:
        return "failed", detail
    return "missing", detail


def _collect_latency(runs: Sequence[Dict[str, Any]], *, client: Optional[str] = None) -> Dict[str, Optional[float]]:
    p50: List[float] = []
    p95: List[float] = []
    p99: List[float] = []
    p99_gated = False
    for run in runs:
        if not _run_rankable(run, client=client):
            continue
        for worker in run.get("workers") or []:
            summary = worker.get("latency_summary") or {}
            if summary.get("p50_ms") is not None:
                p50.append(float(summary["p50_ms"]))
            if summary.get("p95_ms") is not None:
                p95.append(float(summary["p95_ms"]))
            if summary.get("p99_ms") is not None and summary.get("p99_published"):
                p99.append(float(summary["p99_ms"]))
                p99_gated = True
    def median(values: List[float]) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        return ordered[len(ordered) // 2]

    return {
        "p50_ms": median(p50),
        "p95_ms": median(p95),
        "p99_ms": median(p99),
        "p99_gated": p99_gated and bool(p99),
    }


def _collect_integrity(runs: Sequence[Dict[str, Any]], *, client: Optional[str] = None) -> Optional[Dict[str, Any]]:
    totals = {
        "expected": 0,
        "received": 0,
        "unique": 0,
        "missing": 0,
        "duplicates": 0,
        "out_of_order": 0,
        "unexpected": 0,
    }
    worst_missing = 0
    found = False
    for run in runs:
        # Only valid, comparable runs enter integrity medians — the same rule
        # as throughput. Including inconclusive runs used to paper over
        # mismatches that validate_run now refuses.
        if not _run_rankable(run, client=client):
            continue
        for worker in run.get("workers") or []:
            integ = worker.get("integrity")
            if not integ:
                continue
            found = True
            for key in totals:
                totals[key] += int(integ.get(key) or 0)
            worst_missing = max(worst_missing, int(integ.get("missing") or 0))
    if not found:
        return None
    totals["worst_missing"] = worst_missing
    return totals


def _observed_rate(
    runs: Sequence[Dict[str, Any]], summary: Dict[str, Any]
) -> Optional[float]:
    """The rate the harness saw, publishable or not.

    Distinct from the median on purpose: this number may never enter a ranking,
    and every caller renders it as such. It exists so a page can say "the client
    delivered 15,319 msgs/s and the broker dropped 246,373 publishes doing it"
    instead of leaving a blank that reads as "never run".
    """
    rates = [float(r) for r in (summary.get("inconclusive_rates") or []) if isinstance(r, (int, float))]
    if not rates:
        rates = [
            float(run["primary_msgs_per_s"])
            for run in runs
            if isinstance(run.get("primary_msgs_per_s"), (int, float))
        ]
    if not rates:
        return None
    return sorted(rates)[len(rates) // 2]


def _representative_run(runs: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The run whose series should speak for the point: a valid one if there is one."""
    for run in runs:
        if run.get("status") == "valid":
            return run
    return runs[0] if runs else None


def _broker_cpu_series(runs: Sequence[Dict[str, Any]]) -> List[float]:
    """Broker CPU percentage per telemetry sample.

    The cgroup source reports a cumulative ``cpu_usage_usec`` and leaves
    ``cpu_pct`` null, so the percentage has to be differenced against the sample
    timestamps rather than read off.
    """
    run = _representative_run(runs)
    samples = (run or {}).get("telemetry") or []
    series: List[float] = []
    previous: Optional[tuple] = None
    for sample in samples:
        containers = sample.get("containers") or {}
        if not containers:
            continue
        entry = next(iter(containers.values()))
        ts = sample.get("ts")
        usage = entry.get("cpu_usage_usec")
        direct = entry.get("cpu_pct")
        if direct is not None:
            series.append(float(direct))
            continue
        if ts is None or usage is None:
            continue
        if previous is not None:
            d_ts = float(ts) - previous[0]
            d_usage = float(usage) - previous[1]
            if d_ts > 0:
                series.append(max(0.0, d_usage / (d_ts * 1e6) * 100.0))
        previous = (float(ts), float(usage))
    return series


def _worker_rss_series(runs: Sequence[Dict[str, Any]]) -> List[float]:
    """Peak resident memory across the SUT workers, in MiB, per sample."""
    run = _representative_run(runs)
    samples = (run or {}).get("telemetry") or []
    series: List[float] = []
    for sample in samples:
        processes = (sample.get("processes") or {}).values()
        peaks = [float(p.get("rss_kb") or 0.0) for p in processes]
        if peaks:
            series.append(max(peaks) / 1024.0)
    return series


def _loadgen_rate_series(runs: Sequence[Dict[str, Any]]) -> List[float]:
    """The injector's own observed rate over the window, when there was one."""
    run = _representative_run(runs)
    parsed = ((run or {}).get("loadgen") or {}).get("parsed") or {}
    rates = parsed.get("rates") or []
    return [float(r) for r in rates if isinstance(r, (int, float))]


def _run_status_counts(runs: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    valid = sum(1 for r in runs if r.get("status") == "valid")
    return {"valid": valid, "total": len(runs), "inconclusive": len(runs) - valid}


@dataclass
class PointRow:
    label: str
    median_msgs_per_s: Optional[float]
    status: str
    valid_runs: int
    total_runs: int
    non_comparable: bool
    latency: Dict[str, Optional[float]] = field(default_factory=dict)
    # Set when this point's completion-latency percentiles were measured at a
    # non-socket QoS0 boundary (client_identity.qos0_boundary, e.g. "queue").
    # Throughput stays broker-reconciled and comparable; the latency samples do
    # not, because they time admission rather than the socket write.
    latency_boundary: Optional[str] = None
    integrity: Optional[Dict[str, Any]] = None
    spread_low: Optional[float] = None
    spread_high: Optional[float] = None
    protocol: Optional[str] = None
    # Attribution and confidence signals. All of these were already produced by
    # the harness and dropped on the floor by the report; without them a reader
    # cannot tell a client-limited number from a broker-limited one, nor a
    # repeatable median from a noisy one.
    bottleneck: Optional[str] = None
    mad: Optional[float] = None
    relative_spread_pct: Optional[float] = None
    broker_cpu_max_pct: Optional[float] = None
    broker_reconcile_ratio: Optional[float] = None
    delivery_offer_ratio: Optional[float] = None
    effective_offer: Optional[float] = None
    cost_us_per_message: Optional[float] = None
    rss_peak_kb: Optional[float] = None
    # What the run actually reported when nothing was publishable. Two whole
    # scenarios came back with no valid run on any client, and showing only the
    # refusal made it look as though nothing had been measured at all; the
    # number exists, it simply may not enter a ranking.
    observed_msgs_per_s: Optional[float] = None
    # Why the cell is empty, when it is: refused | failed | environment | missing
    empty_reason: Optional[str] = None
    reason_detail: str = ""
    # Time series the harness already sampled and the report never read. Taken
    # from one representative run, they are a shape rather than a reading: a
    # broker that ramps to saturation mid-window looks nothing like one that sat
    # flat, and the medians alone cannot tell those apart.
    broker_cpu_series: List[float] = field(default_factory=list)
    worker_rss_series: List[float] = field(default_factory=list)
    loadgen_rate_series: List[float] = field(default_factory=list)


@dataclass
class ResultDoc:
    source_name: str
    slug: str
    kind: str
    title: str
    client: Optional[str]
    scenario: Optional[str]
    profile: Optional[str]
    non_comparable: bool
    status: str
    median_msgs_per_s: Optional[float]
    points: List[PointRow]
    environment: Dict[str, Any]
    broker: Dict[str, Any]
    verdict: Optional[Dict[str, Any]]
    raw_meta: Dict[str, Any]
    spread_low: Optional[float] = None
    spread_high: Optional[float] = None
    # Aggregated inconclusive-run signals for the index page. Keys are reason
    # strings; values are run counts. Split by attribution so load/capability
    # failures stay visible even when excluded from the throughput chart.
    load_reasons: Dict[str, int] = field(default_factory=dict)
    capability_reasons: Dict[str, int] = field(default_factory=dict)
    # True when a run of this document was allowed despite an unreadable CPU
    # governor, on a host whose profile declares the clock unpinned.
    clock_unpinned: bool = False
    environment_reasons: Dict[str, int] = field(default_factory=dict)
    inconclusive_runs: int = 0
    total_runs: int = 0
    # Taken from client_identity in the JSON rather than a hard-coded table, so a
    # newly added client is grouped and labelled correctly with no report edit.
    io_model: Optional[str] = None
    stability: Optional[str] = None
    client_version: Optional[str] = None
    private_api: Dict[str, str] = field(default_factory=dict)
    interleaved_with: List[str] = field(default_factory=list)


def classify_payload(data: Dict[str, Any], source_name: str) -> ResultDoc:
    slug = _slug(Path(source_name).stem)
    if "scenarios" in data and "suite" in data:
        nested = data.get("scenarios") or []
        scenario_names = [s.get("scenario", "?") for s in nested]
        clients = sorted({s.get("client") for s in nested if s.get("client")})
        scenario_entries = []
        for s in nested:
            medians = []
            for block in s.get("results") or []:
                median = (block.get("summary") or {}).get("median")
                if median is not None:
                    medians.append(float(median))
            scenario_entries.append(
                {
                    "scenario": s.get("scenario"),
                    "client": s.get("client"),
                    "profile": s.get("profile"),
                    "median_msgs_per_s": (sorted(medians)[len(medians) // 2] if medians else None),
                    "source_hint": f"{s.get('client')}-{s.get('scenario')}",
                }
            )
        return ResultDoc(
            source_name=source_name,
            slug=slug,
            kind="suite",
            title=f"Suite {data.get('suite', '?')}",
            client=", ".join(clients) if clients else None,
            scenario=", ".join(scenario_names[:6]) + ("…" if len(scenario_names) > 6 else ""),
            profile=None,
            non_comparable=False,
            status="suite",
            median_msgs_per_s=None,
            points=[],
            environment={},
            broker={},
            verdict=None,
            raw_meta={
                "suite": data.get("suite"),
                "estimate": data.get("estimate"),
                "scenario_count": len(scenario_names),
                "scenario_names": scenario_names,
                "scenario_entries": scenario_entries,
            },
        )

    if data.get("verdict") is not None and data.get("order") is not None:
        verdict = data.get("verdict") or {}
        return ResultDoc(
            source_name=source_name,
            slug=slug,
            kind="compare",
            title=f"Compare {data.get('baseline_client', '?')} vs {data.get('candidate_client', '?')}",
            client=f"{data.get('baseline_client')} / {data.get('candidate_client')}",
            scenario=data.get("scenario"),
            profile=data.get("profile"),
            non_comparable=False,
            status=str(verdict.get("verdict", "inconclusive")),
            median_msgs_per_s=None,
            points=[],
            environment=data.get("environment") or {},
            broker=data.get("broker") or {},
            verdict=verdict if isinstance(verdict, dict) else {"verdict": verdict},
            raw_meta={
                "order": data.get("order"),
                "cooldown_s": data.get("cooldown_s"),
                "baseline_identity": data.get("baseline_identity"),
                "candidate_identity": data.get("candidate_identity"),
                "loadgen": data.get("loadgen"),
                "points": data.get("points"),
            },
        )

    if "capacity_msgs_per_s" in data and "fractions" in data:
        return ResultDoc(
            source_name=source_name,
            slug=slug,
            kind="calibrate",
            title=f"Calibrate {data.get('client', '?')}",
            client=data.get("client"),
            scenario="calibration",
            profile=data.get("profile"),
            non_comparable=False,
            status="calibrate",
            median_msgs_per_s=data.get("capacity_msgs_per_s"),
            points=[],
            environment=data.get("environment") or {},
            broker=data.get("broker") or {},
            verdict=None,
            raw_meta={
                "fractions": data.get("fractions"),
                "rtt_capacity_msgs_per_s": data.get("rtt_capacity_msgs_per_s"),
                "rtt_fractions": data.get("rtt_fractions"),
                "protocol_capacities": data.get("protocol_capacities"),
            },
        )

    points: List[PointRow] = []
    # (median, min, max) per comparable point; used both for the scenario's
    # headline value and for the observed run-to-run range behind it.
    median_min_max: List[tuple] = []
    any_non_comparable = False
    overall_valid = 0
    overall_total = 0
    load_reasons: Dict[str, int] = {}
    capability_reasons: Dict[str, int] = {}
    environment_reasons: Dict[str, int] = {}
    inconclusive_runs = 0
    identity = data.get("client_identity") or {}
    client_name = data.get("client") or identity.get("client")
    blocks = data.get("results") or []
    varying = _varying_axes([b.get("point") or {} for b in blocks])
    for block in blocks:
        point = block.get("point") or {}
        runs = block.get("runs") or []
        summary = block.get("summary") or {}
        counts = _run_status_counts(runs)
        overall_valid += counts["valid"]
        overall_total += counts["total"]
        for run in runs:
            if run.get("status") == "valid":
                continue
            inconclusive_runs += 1
            for reason in run.get("reasons") or []:
                kind = _reason_kind(str(reason))
                short = _short_reason(str(reason))
                if kind == "load":
                    load_reasons[short] = load_reasons.get(short, 0) + 1
                elif kind == "capability":
                    capability_reasons[short] = capability_reasons.get(short, 0) + 1
                elif kind == "environment":
                    environment_reasons[short] = environment_reasons.get(short, 0) + 1
        stale_facade = any(
            r.get("publish_path") == "sync_facade" for r in runs
        ) and _client_has_native_async_path(client_name)
        non_comparable = (
            any(bool(r.get("non_comparable")) for r in runs)
            or bool(point.get("non_comparable"))
            or stale_facade
        )
        any_non_comparable = any_non_comparable or non_comparable
        # Prefer summary computed from valid runs only; drop stale facade rates.
        rankable_rates = [
            float(r["primary_msgs_per_s"])
            for r in runs
            if _run_rankable(r, client=client_name) and r.get("primary_msgs_per_s") is not None
        ]
        if rankable_rates:
            ordered_rates = sorted(rankable_rates)
            median_rate = ordered_rates[len(ordered_rates) // 2]
            point_min = ordered_rates[0]
            point_max = ordered_rates[-1]
        else:
            median_rate = None if non_comparable else summary.get("median")
            point_min = summary.get("min")
            point_max = summary.get("max")
        if median_rate is not None and not non_comparable:
            median_min_max.append(
                (
                    float(median_rate),
                    float(point_min) if point_min is not None else float(median_rate),
                    float(point_max) if point_max is not None else float(median_rate),
                )
            )
        status = "valid" if counts["valid"] == counts["total"] and counts["total"] else (
            "partial" if counts["valid"] else "inconclusive"
        )
        mad = summary.get("mad")
        relative_spread = None
        if median_rate and point_min is not None and point_max is not None and float(median_rate) > 0:
            relative_spread = 100.0 * (float(point_max) - float(point_min)) / float(median_rate)
        empty_reason, reason_detail = _empty_cell_reason(runs, median_rate)
        if stale_facade and empty_reason == "missing":
            empty_reason, reason_detail = "failed", "sync_facade publish path (not comparable)"
        point_latency = _collect_latency(runs, client=client_name)
        latency_boundary = None
        qos0_boundary = identity.get("qos0_boundary")
        if (
            int(point.get("qos_publish", 0) or 0) == 0
            and qos0_boundary not in (None, "socket")
            and any(point_latency.get(k) is not None for k in ("p50_ms", "p95_ms", "p99_ms"))
        ):
            latency_boundary = str(qos0_boundary)
        points.append(
            PointRow(
                label=_point_label(point, varying),
                median_msgs_per_s=median_rate if not non_comparable else None,
                status=status,
                valid_runs=counts["valid"],
                total_runs=counts["total"],
                non_comparable=non_comparable,
                latency=point_latency,
                latency_boundary=latency_boundary,
                integrity=_collect_integrity(runs, client=client_name),
                spread_low=float(point_min) if point_min is not None and not non_comparable else None,
                spread_high=float(point_max) if point_max is not None and not non_comparable else None,
                protocol=str(point.get("protocol") or "MQTTv311"),
                bottleneck=_dominant(runs, "bottleneck"),
                mad=float(mad) if mad is not None else None,
                relative_spread_pct=relative_spread,
                broker_cpu_max_pct=_max_of(runs, "broker_cpu_max_pct"),
                broker_reconcile_ratio=_ratio_of(runs),
                delivery_offer_ratio=_max_of(runs, "delivery_offer_ratio"),
                effective_offer=_max_of(runs, "effective_offer_msgs_per_s"),
                cost_us_per_message=_cost_of(runs, "cpu_us_per_message"),
                rss_peak_kb=_cost_of(runs, "rss_peak_kb"),
                observed_msgs_per_s=_observed_rate(runs, summary),
                empty_reason=empty_reason,
                reason_detail=reason_detail,
                broker_cpu_series=_broker_cpu_series(runs),
                worker_rss_series=_worker_rss_series(runs),
                loadgen_rate_series=_loadgen_rate_series(runs),
            )
        )

    if overall_total == 0:
        status = "empty"
    elif overall_valid == overall_total:
        status = "valid"
    elif overall_valid == 0:
        status = "inconclusive"
    else:
        status = "partial"

    primary_median = None
    spread_low = None
    spread_high = None
    if median_min_max:
        ordered_mmm = sorted(median_min_max, key=lambda t: t[0])
        primary_median, spread_low, spread_high = ordered_mmm[len(ordered_mmm) // 2]

    return ResultDoc(
        source_name=source_name,
        slug=slug,
        kind="scenario",
        title=data.get("scenario") or source_name,
        client=data.get("client"),
        scenario=data.get("scenario"),
        profile=data.get("profile"),
        non_comparable=any_non_comparable,
        status=status,
        median_msgs_per_s=primary_median,
        points=points,
        environment=data.get("environment") or {},
        broker=data.get("broker") or {},
        verdict=None,
        raw_meta={
            "runs": data.get("runs"),
            "seed": data.get("seed"),
            "client_identity": data.get("client_identity"),
        },
        spread_low=spread_low,
        spread_high=spread_high,
        clock_unpinned=any(
            bool(run.get("clock_unpinned"))
            for entry in (data.get("results") or [])
            for run in (entry.get("runs") or [])
        ),
        load_reasons=load_reasons,
        capability_reasons=capability_reasons,
        environment_reasons=environment_reasons,
        inconclusive_runs=inconclusive_runs,
        total_runs=overall_total,
        io_model=identity.get("io_model"),
        stability=identity.get("stability"),
        client_version=identity.get("client_version"),
        private_api=dict(identity.get("private_api") or {}),
        interleaved_with=list(data.get("interleaved_with") or []),
    )


# Sentinel: "look the reference host up in hosts/". Distinct from None, which
# means "publish everything", so a caller can turn the filter off explicitly
# instead of by arranging for the lookup to fail.
_AUTO_REFERENCE = object()


def load_results(input_dir: Path, *, reference: Any = _AUTO_REFERENCE) -> List[ResultDoc]:
    docs, _ = load_results_with_skips(input_dir, reference=reference)
    return docs


def load_results_with_skips(
    input_dir: Path, *, reference: Any = _AUTO_REFERENCE
) -> Tuple[List[ResultDoc], Dict[str, int]]:
    """Documents for the published host, plus a count of what was left out.

    The site publishes exactly one machine. Numbers from another host are not
    wrong, they are answers to a different question — its harness cost, its
    broker's fan-out rate, its core topology — and pooling them into one median
    is the failure this whole mechanism exists to prevent. So a run from a
    `runner` host is skipped and *counted*, never silently dropped.

    Anything measured before host profiles existed carries no fingerprint. It is
    identified from its `environment` block instead and attached to the
    reference host when the machine facts match, because that corpus did come
    off that machine — it simply has no record of the ceilings it ran against.
    """
    from mqtt_client_bench.hostcal import matches_reference, reference_profile, result_host_key

    docs: List[ResultDoc] = []
    skipped: Dict[str, int] = {}
    if reference is _AUTO_REFERENCE:
        # Raises on two reference profiles: that is a repository error, not a
        # runtime choice, and picking one at random would publish half a corpus
        # without saying so.
        reference = reference_profile()
    # Skip ephemeral local artefacts (gitignored ``_*.json`` / smoke probes).
    paths = sorted(
        path
        for path in input_dir.glob("*.json")
        if not path.name.startswith("_") and not path.name.endswith("-smoke.json")
    )
    for path in paths:
        data = _load_json(path)
        if data is None:
            continue
        key = result_host_key(data)
        if not matches_reference(key, reference):
            label = key.get("fingerprint") or key.get("hostname") or "unknown"
            skipped[str(label)] = skipped.get(str(label), 0) + 1
            continue
        docs.append(classify_payload(data, path.name))
    # Ensure unique slugs.
    seen: Dict[str, int] = {}
    for doc in docs:
        base = doc.slug
        count = seen.get(base, 0)
        seen[base] = count + 1
        if count:
            doc.slug = f"{base}-{count + 1}"
    return docs, skipped


def _doc_point_count(doc: "ResultDoc") -> int:
    """Points a document actually measured.

    Compare docs keep ``points`` empty because their per-point payload has a
    different shape from a scenario's, so counting ``doc.points`` reported 0 for
    every A/B run.
    """
    if doc.kind == "compare":
        return len(doc.raw_meta.get("points") or [])
    return len(doc.points)

