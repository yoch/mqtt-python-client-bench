"""Turn result documents into chart-ready series.

The page modules should decide layout, not comparability. Every function here
answers one question — what belongs on this axis, which clients may share it,
what the spread was — and applies the same gate: a point that is not comparable
never becomes a data value, only a gap with a reason attached.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import catalog
from .model import (
    _IO_MODEL_ORDER,
    ClientMeta,
    PointRow,
    ResultDoc,
    _CHART_EXCLUDED_SCENARIOS,
    _matrix_row_id,
    _scenario_base,
    _sort_clients,
)


def scenario_docs(docs: Sequence[ResultDoc]) -> List[ResultDoc]:
    """Documents that produced a comparable headline number."""
    return [
        doc
        for doc in docs
        if doc.kind == "scenario" and doc.median_msgs_per_s is not None and not doc.non_comparable
    ]


def protocol_aggregates(doc: ResultDoc) -> Dict[str, Tuple[float, float, float]]:
    """protocol -> (median, spread low, spread high) over comparable points.

    Protocols are bucketed rather than pooled: MQTT 3.1.1 and MQTT 5 are
    different experiments and a median across both is a number about neither.
    """
    buckets: Dict[str, List[PointRow]] = {}
    for point in doc.points:
        if point.non_comparable or point.median_msgs_per_s is None:
            continue
        buckets.setdefault(point.protocol or "MQTTv311", []).append(point)
    out: Dict[str, Tuple[float, float, float]] = {}
    for proto, points in buckets.items():
        ordered = sorted(points, key=lambda p: float(p.median_msgs_per_s or 0.0))
        mid = ordered[len(ordered) // 2]
        lows = [p.spread_low if p.spread_low is not None else p.median_msgs_per_s for p in points]
        highs = [p.spread_high if p.spread_high is not None else p.median_msgs_per_s for p in points]
        out[proto] = (
            float(mid.median_msgs_per_s or 0.0),
            float(min(v for v in lows if v is not None)),
            float(max(v for v in highs if v is not None)),
        )
    if not out and doc.median_msgs_per_s is not None:
        low = doc.spread_low if doc.spread_low is not None else doc.median_msgs_per_s
        high = doc.spread_high if doc.spread_high is not None else doc.median_msgs_per_s
        out["MQTTv311"] = (float(doc.median_msgs_per_s), float(low), float(high))
    return out


class Overview:
    """The index's cross-scenario view, assembled once and read by several panels."""

    def __init__(self, docs: Sequence[ResultDoc], meta: Dict[str, ClientMeta]):
        self.meta = meta
        self.row_ids: List[str] = []
        self.median: Dict[Tuple[str, str], Optional[float]] = {}
        self.low: Dict[Tuple[str, str], Optional[float]] = {}
        self.high: Dict[Tuple[str, str], Optional[float]] = {}
        self.cells: Dict[Tuple[str, str], PointRow] = {}
        self.all_clients: List[str] = []
        self.clients: List[str] = []

        for doc in scenario_docs(docs):
            scenario = doc.scenario or doc.title
            client = doc.client or "?"
            for proto, (median, low, high) in protocol_aggregates(doc).items():
                row_id = _matrix_row_id(scenario, proto)
                if row_id not in self.row_ids:
                    self.row_ids.append(row_id)
                self.median[(row_id, client)] = median
                self.low[(row_id, client)] = low
                self.high[(row_id, client)] = high
            for point in doc.points:
                row_id = _matrix_row_id(scenario, point.protocol or "MQTTv311")
                existing = self.cells.get((row_id, client))
                # Prefer a point that produced a number; otherwise keep the first
                # refusal so the empty cell can still explain itself.
                if existing is None or (
                    existing.median_msgs_per_s is None and point.median_msgs_per_s is not None
                ):
                    self.cells[(row_id, client)] = point

        # Clients whose every point was refused produce no aggregate, so their
        # columns would otherwise vanish from the matrix entirely.
        for doc in docs:
            if doc.kind != "scenario" or not doc.client:
                continue
            scenario = doc.scenario or doc.title
            for point in doc.points:
                row_id = _matrix_row_id(scenario, point.protocol or "MQTTv311")
                self.cells.setdefault((row_id, doc.client), point)

        # ABBA compare docs carry a composite "a / b" label in `client` and must
        # not be counted as a library.
        for doc in docs:
            if doc.kind == "scenario" and doc.client and doc.client not in self.all_clients:
                self.all_clients.append(doc.client)
        for doc in scenario_docs(docs):
            name = doc.client or "?"
            if name not in self.clients:
                self.clients.append(name)
        for doc in docs:
            if doc.kind != "scenario":
                continue
            if doc.client and doc.client not in self.clients:
                self.clients.append(doc.client)
            if not (doc.capability_reasons or doc.load_reasons):
                continue
            scenario = doc.scenario or doc.title
            protos = list(protocol_aggregates(doc)) if doc.points else ["MQTTv311"]
            for proto in protos or ["MQTTv311"]:
                row_id = _matrix_row_id(scenario, proto)
                if row_id not in self.row_ids:
                    self.row_ids.append(row_id)
        self.clients = _sort_clients(self.clients, meta)

    @property
    def chart_rows(self) -> List[str]:
        """Rows a throughput chart may carry: rate-capped scenarios are excluded."""
        return [r for r in self.row_ids if _scenario_base(r) not in _CHART_EXCLUDED_SCENARIOS]

    def series_for(self, rows: Sequence[str], clients: Sequence[str]) -> List[Dict[str, Any]]:
        return [
            {
                "client": client,
                "values": [self.median.get((row, client)) for row in rows],
                "low": [self.low.get((row, client)) for row in rows],
                "high": [self.high.get((row, client)) for row in rows],
            }
            for client in clients
        ]


def peer_groups(clients: Sequence[str], meta: Dict[str, ClientMeta]) -> List[Tuple[str, List[str]]]:
    """Clients bucketed into the groups that may be compared with each other.

    A ranking only means something inside one I/O model, so that is the split
    every chart on the site is built around. Members come back with the stable
    libraries first; ``clients`` is expected to be pre-sorted by
    :func:`_sort_clients`, which already applies that order.
    """
    grouped: Dict[str, List[str]] = {}
    for client in clients:
        info = meta.get(client)
        grouped.setdefault(info.peer_group if info else "unknown", []).append(client)
    order = {name: i for i, name in enumerate(clients)}

    def group_rank(io_model: str) -> tuple:
        return (
            _IO_MODEL_ORDER.index(io_model)
            if io_model in _IO_MODEL_ORDER
            else len(_IO_MODEL_ORDER),
            io_model,
        )

    return [
        (group, sorted(members, key=lambda c: order.get(c, len(order))))
        for group, members in sorted(grouped.items(), key=lambda kv: group_rank(kv[0]))
    ]


def trim_empty_rows(
    rows: Sequence[str], series: Sequence[Dict[str, Any]]
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Drop rows no member of this group measured, instead of drawing gaps."""
    keep = [
        i
        for i in range(len(rows))
        if any((s.get("values") or [None] * len(rows))[i] is not None for s in series)
    ]
    if not keep:
        return [], []

    def pick(serie: Dict[str, Any], key: str) -> List[Any]:
        source = serie.get(key) or []
        return [source[i] if i < len(source) else None for i in keep]

    trimmed = [
        {
            "client": s.get("client"),
            "values": pick(s, "values"),
            "low": pick(s, "low"),
            "high": pick(s, "high"),
        }
        for s in series
    ]
    return [rows[i] for i in keep], trimmed


# --------------------------------------------------------------------------
# Per-scenario views
# --------------------------------------------------------------------------

def docs_for_scenario(docs: Sequence[ResultDoc], scenario: str) -> List[ResultDoc]:
    return [d for d in docs if d.kind == "scenario" and (d.scenario or d.title) == scenario]


def point_axes(docs: Sequence[ResultDoc]) -> List[str]:
    """Axes that vary across a scenario's points, pooled over every client."""
    axes: List[str] = []
    for doc in docs:
        for point in doc.points:
            for part in str(point.label or "").split(", "):
                if "=" in part:
                    key = part.split("=", 1)[0]
                    if key not in axes:
                        axes.append(key)
    return axes


def sweep(
    docs: Sequence[ResultDoc],
    *,
    protocol: Optional[str] = None,
) -> Tuple[List[str], Dict[str, Dict[str, PointRow]]]:
    """Ordered point labels plus ``client -> label -> point`` for one scenario.

    Labels are ordered along the scenario's own axis. ``sorted()`` on the raw
    label put ``blob1m`` before ``empty0`` and interleaved the two protocols, so
    a monotone sweep rendered as noise; ordering here is what makes the curve
    readable.
    """
    axis = _dominant_axis(docs)
    by_client: Dict[str, Dict[str, PointRow]] = {}
    labels: List[str] = []
    for doc in docs:
        client = doc.client or "?"
        for point in doc.points:
            if protocol is not None and (point.protocol or "MQTTv311") != protocol:
                continue
            label = _axis_label(point, axis, drop_protocol=protocol is not None)
            by_client.setdefault(client, {})[label] = point
            if label not in labels:
                labels.append(label)
    labels.sort(key=lambda lbl: catalog.axis_sort_key(axis, _axis_value(lbl, axis)))
    return labels, by_client


def _dominant_axis(docs: Sequence[ResultDoc]) -> str:
    """The axis a sweep should be plotted against, ordinal preferred."""
    axes = point_axes(docs)
    ordinal = catalog.primary_ordinal_axis(axes)
    if ordinal:
        return ordinal
    for axis in axes:
        if axis != "proto":
            return axis
    return axes[0] if axes else ""


def _axis_label(point: PointRow, axis: str, *, drop_protocol: bool) -> str:
    label = str(point.label or "default")
    parts = label.split(", ")
    if drop_protocol:
        parts = [p for p in parts if not p.startswith("proto=")]
    # "payload=empty0" under an axis already titled "payload size" says the key
    # twice and costs the width that clipped the last tick. Strip it when the
    # label carries nothing else.
    if len(parts) == 1 and parts[0].startswith(f"{axis}="):
        parts = [parts[0].split("=", 1)[1]]
    return ", ".join(parts) or "default"


def _axis_value(label: str, axis: str):
    """Recover the raw axis value from a rendered label, stripped or not."""
    parts = str(label).split(", ")
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            if key == axis or (axis == "qos_publish" and key == "qos"):
                return value
    return parts[0] if len(parts) == 1 else label


def protocols_in(docs: Sequence[ResultDoc]) -> List[str]:
    seen: List[str] = []
    for doc in docs:
        for point in doc.points:
            proto = point.protocol or "MQTTv311"
            if proto not in seen:
                seen.append(proto)
    return sorted(seen)


def cost_points(docs: Sequence[ResultDoc], clients: Sequence[str]) -> List[Dict[str, Any]]:
    """Throughput against CPU cost per message, one dot per client."""
    out: List[Dict[str, Any]] = []
    for client in clients:
        rates: List[float] = []
        costs: List[float] = []
        for doc in docs:
            if (doc.client or "?") != client:
                continue
            for point in doc.points:
                if point.non_comparable or point.status != "valid":
                    continue
                if point.median_msgs_per_s is None or point.cost_us_per_message is None:
                    continue
                rates.append(float(point.median_msgs_per_s))
                costs.append(float(point.cost_us_per_message))
        if not rates:
            continue
        out.append(
            {
                "client": client,
                "x": sorted(rates)[len(rates) // 2],
                "y": sorted(costs)[len(costs) // 2],
            }
        )
    return out


def coverage(docs: Sequence[ResultDoc]) -> Tuple[List[str], List[str], Dict[Tuple[str, str], Optional[float]], Dict[Tuple[str, str], Tuple[int, int]]]:
    """Valid-run fraction per (client, scenario), for the corpus heatmap."""
    clients: List[str] = []
    scenarios: List[str] = []
    counts: Dict[Tuple[str, str], Tuple[int, int]] = {}
    for doc in docs:
        if doc.kind != "scenario" or not doc.client:
            continue
        scenario = doc.scenario or doc.title
        if doc.client not in clients:
            clients.append(doc.client)
        if scenario not in scenarios:
            scenarios.append(scenario)
        valid = sum(p.valid_runs for p in doc.points)
        total = sum(p.total_runs for p in doc.points)
        prev = counts.get((doc.client, scenario), (0, 0))
        counts[(doc.client, scenario)] = (prev[0] + valid, prev[1] + total)
    values: Dict[Tuple[str, str], Optional[float]] = {}
    for key, (valid, total) in counts.items():
        values[key] = (valid / total) if total else None
    return clients, sorted(scenarios), values, counts
