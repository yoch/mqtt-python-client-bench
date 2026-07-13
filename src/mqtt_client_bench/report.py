"""Build a static HTML report site from committed benchmark JSON results."""

from __future__ import annotations

import html
import json
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ASSETS_DIR = Path(__file__).resolve().parent / "report_assets"


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


_CLIENT_ORDER = ("awscrt", "gmqtt", "paho", "amqtt", "aiomqtt", "zmqtt", "aiomqtt3")

# Rate-capped / functional scenarios: primary msg/s just echoes the injected
# ceiling, so they flatten the throughput chart. Keep them in the matrix (last)
# until we have a better integrity-oriented presentation.
_CHART_EXCLUDED_SCENARIOS = frozenset({"duplex_gateway", "e2e_integrity"})

# Failures that reflect how the SUT behaved under the offered load (or its
# protocol/API limits). These must stay visible in the report — excluding them
# from the throughput chart is fine; burying them is not.
_CLIENT_LOAD_REASON_PREFIXES = (
    "open_loop_rate_out_of_tolerance",
    "protocol_failed",
    "timed_out_mids",
    "rtt_timeouts",
    "warmup_drain_timeout",
    "no_delivery_despite_load",
    "worker_error:",
)
_CLIENT_CAPABILITY_PREFIX = "not_implemented:"
_ENVIRONMENT_REASON_PREFIXES = (
    "container_cpu_high:",
    "broker_telemetry_missing",
    "loadgen_emitted_nothing",
    "loadgen_below_half_nominal",
    "barrier_failed",
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
    if reason.startswith("worker_error:"):
        return "worker_error"
    if reason.startswith("barrier_failed"):
        return "barrier"
    return reason


def _order_matrix_scenarios(scenarios: Sequence[str]) -> List[str]:
    """Throughput scenarios first; rate-capped / functional rows last."""
    primary = [s for s in scenarios if _scenario_base(s) not in _CHART_EXCLUDED_SCENARIOS]
    trailing = [s for s in scenarios if _scenario_base(s) in _CHART_EXCLUDED_SCENARIOS]
    return primary + trailing


def _matrix_row_id(scenario: str, protocol: Optional[str] = None) -> str:
    proto = protocol or "MQTTv311"
    return f"{scenario} · {proto}"


def _scenario_base(row_id: str) -> str:
    if " · " in row_id:
        return row_id.rsplit(" · ", 1)[0]
    return row_id


def _protocol_from_row_id(row_id: str) -> str:
    if " · " in row_id:
        return row_id.rsplit(" · ", 1)[1]
    return "MQTTv311"

# One stable colour per known client, shared by the matrix swatches, the
# results table, and the overview chart so the same client always reads the
# same colour anywhere on the site.
_CLIENT_COLORS = {
    "paho": "#0f6e56",
    "gmqtt": "#245b7a",
    "aiomqtt": "#9a5b12",
    "amqtt": "#6b4f7a",
    "awscrt": "#8b3a3a",
    "zmqtt": "#3f6b4d",
    "aiomqtt3": "#4a5a78",
}
_FALLBACK_PALETTE = ["#5c6b64", "#7a6a4f", "#4f6b7a", "#7a4f5c"]


def _sort_clients(clients: Sequence[str]) -> List[str]:
    rank = {name: i for i, name in enumerate(_CLIENT_ORDER)}
    return sorted(clients, key=lambda c: (rank.get(c, len(_CLIENT_ORDER)), c))


def _client_colors(clients: Sequence[str]) -> Dict[str, str]:
    colors: Dict[str, str] = {}
    fallback_idx = 0
    for client in clients:
        if client in _CLIENT_COLORS:
            colors[client] = _CLIENT_COLORS[client]
        else:
            colors[client] = _FALLBACK_PALETTE[fallback_idx % len(_FALLBACK_PALETTE)]
            fallback_idx += 1
    return colors


def _client_swatch(name: str, colors: Dict[str, str]) -> str:
    color = colors.get(name, "#5c6b64")
    return f'<span class="swatch" style="background:{_esc(color)}"></span>{_esc(name)}'


def _performance_matrix_html(
    scenarios: Sequence[str],
    clients: Sequence[str],
    by_key: Dict[tuple, Optional[float]],
    colors: Dict[str, str],
) -> str:
    """Compact scenario × client table for immediate reading on the index page."""
    if not scenarios or not clients:
        return ""
    ordered = _sort_clients(clients)
    head = "".join(f'<th scope="col" class="num">{_client_swatch(c, colors)}</th>' for c in ordered)
    body_rows: List[str] = []
    for scenario in scenarios:
        cells = [by_key.get((scenario, c)) for c in ordered]
        numeric = [v for v in cells if v is not None]
        best = max(numeric) if numeric else None
        tied_count = sum(1 for v in numeric if best is not None and _is_tied_with_best(v, best))
        # A tie across every populated cell isn't a "winner" — it means the
        # scenario is rate-capped, not that one client outperformed the rest.
        all_tied = bool(numeric) and tied_count == len(numeric)
        tds = []
        for value in cells:
            if value is None:
                tds.append('<td class="num muted">—</td>')
            elif all_tied:
                tds.append(f'<td class="num">{_esc(_fmt_num(value))}</td>')
            elif best is not None and _is_tied_with_best(value, best):
                tds.append(f'<td class="num best">{_esc(_fmt_num(value))}</td>')
            else:
                tds.append(f'<td class="num">{_esc(_fmt_num(value))}</td>')
        body_rows.append(
            f'<tr><th scope="row" class="scenario">{_esc(scenario)}</th>{"".join(tds)}</tr>'
        )
    return f"""
      <section class="panel">
        <div class="panel-head">
          <h2>Performance matrix</h2>
          <p class="hint">Median msg/s per scenario × MQTT protocol × client, comparable runs only. Rows are never mixed across protocols. Best result in each row is highlighted, unless every client ties (rate-capped scenario). Rate-capped checks (<code>duplex_gateway</code>, <code>e2e_integrity</code>) are listed last and omitted from the chart above. Client load misses and capability gaps are listed in Client issues at the bottom.</p>
        </div>
        <div class="table-wrap table-wrap-sticky-col">
          <table class="matrix">
            <thead>
              <tr>
                <th scope="col" class="scenario-head">Scenario</th>
                {head}
              </tr>
            </thead>
            <tbody>
              {"".join(body_rows)}
            </tbody>
          </table>
        </div>
      </section>
"""


def _client_signals_html(docs: Sequence[ResultDoc], colors: Dict[str, str]) -> str:
    """Single dedicated table for SUT-attributable failures and capability gaps."""
    rows: List[tuple] = []
    for doc in docs:
        if doc.kind != "scenario":
            continue
        client = doc.client or "?"
        scenario = doc.scenario or doc.title
        if doc.load_reasons:
            detail = ", ".join(f"{name}×{count}" for name, count in sorted(doc.load_reasons.items()))
            rows.append(("load", client, scenario, detail, doc.inconclusive_runs, doc.total_runs, doc.slug))
        if doc.capability_reasons:
            detail = ", ".join(sorted(doc.capability_reasons))
            rows.append(("capability", client, scenario, detail, doc.inconclusive_runs, doc.total_runs, doc.slug))
    if not rows:
        return ""

    kind_rank = {"load": 0, "capability": 1}
    client_rank = {name: i for i, name in enumerate(_CLIENT_ORDER)}
    rows.sort(
        key=lambda r: (
            kind_rank.get(r[0], 9),
            client_rank.get(r[1], len(_CLIENT_ORDER)),
            r[1],
            r[2],
        )
    )

    body = []
    for kind, client, scenario, detail, failed, total, slug in rows:
        swatch = _client_swatch(client, colors) if client in colors else _esc(client)
        kind_label = "under load" if kind == "load" else "capability"
        body.append(
            f"<tr>"
            f"<td><span class=\"badge badge-{'partial' if kind == 'load' else 'inconclusive'}\">{_esc(kind_label)}</span></td>"
            f"<td>{swatch}</td>"
            f"<td class=\"mono\">{_esc(scenario)}</td>"
            f"<td class=\"mono\">{_esc(detail)}</td>"
            f"<td class=\"num\">{_esc(failed)}/{_esc(total)}</td>"
            f"<td><a href=\"runs/{_esc(slug)}.html\">detail</a></td>"
            f"</tr>"
        )
    return f"""
      <section class="panel panel-signal">
        <div class="panel-head">
          <h2>Client issues</h2>
          <p class="hint">SUT-attributable problems kept out of the throughput median on purpose: missed open-loop targets / protocol failures under load, and points refused for missing adapter capabilities. Environment issues (broker CPU, loadgen, barriers) stay in All results only.</p>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Kind</th>
                <th>Client</th>
                <th>Scenario</th>
                <th>Signal</th>
                <th class="num">Failed runs</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {"".join(body)}
            </tbody>
          </table>
        </div>
      </section>
"""


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _point_label(point: Dict[str, Any]) -> str:
    parts = []
    if point.get("payload") is not None:
        parts.append(f"payload={point['payload']}")
    if point.get("qos_publish") is not None:
        parts.append(f"qos={point['qos_publish']}")
    if point.get("qos_subscribe") is not None:
        parts.append(f"sub_qos={point['qos_subscribe']}")
    if point.get("protocol") is not None:
        parts.append(f"proto={point['protocol']}")
    if point.get("topology") is not None:
        parts.append(f"topo={point['topology']}")
    return ", ".join(parts) if parts else "default"


def _collect_latency(runs: Sequence[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    p50: List[float] = []
    p95: List[float] = []
    p99: List[float] = []
    p99_gated = False
    for run in runs:
        if run.get("status") != "valid" or run.get("non_comparable"):
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


def _collect_integrity(runs: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
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
    integrity: Optional[Dict[str, Any]] = None
    chart_rates: List[Optional[float]] = field(default_factory=list)
    spread_low: Optional[float] = None
    spread_high: Optional[float] = None
    protocol: Optional[str] = None


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
    environment_reasons: Dict[str, int] = field(default_factory=dict)
    inconclusive_runs: int = 0
    total_runs: int = 0


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
    for block in data.get("results") or []:
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
        non_comparable = any(bool(r.get("non_comparable")) for r in runs) or bool(point.get("non_comparable"))
        any_non_comparable = any_non_comparable or non_comparable
        # Prefer summary computed from valid runs only.
        median_rate = summary.get("median")
        if non_comparable or (point.get("profile") == "smoke"):
            # Keep value for display but mark non-comparable.
            pass
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
        chart_rates = [
            r.get("primary_msgs_per_s")
            for r in runs
            if r.get("status") == "valid" and not r.get("non_comparable")
        ]
        points.append(
            PointRow(
                label=_point_label(point),
                median_msgs_per_s=median_rate,
                status=status,
                valid_runs=counts["valid"],
                total_runs=counts["total"],
                non_comparable=non_comparable,
                latency=_collect_latency(runs),
                integrity=_collect_integrity(runs),
                chart_rates=chart_rates,
                spread_low=float(point_min) if point_min is not None else None,
                spread_high=float(point_max) if point_max is not None else None,
                protocol=str(point.get("protocol") or "MQTTv311"),
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
        load_reasons=load_reasons,
        capability_reasons=capability_reasons,
        environment_reasons=environment_reasons,
        inconclusive_runs=inconclusive_runs,
        total_runs=overall_total,
    )


def load_results(input_dir: Path) -> List[ResultDoc]:
    docs: List[ResultDoc] = []
    paths = sorted(input_dir.glob("*.json"))
    for path in paths:
        data = _load_json(path)
        if data is None:
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
    return docs


def _page_shell(title: str, body: str, *, relative_root: str = ".") -> str:
    css_href = f"{relative_root}/assets/style.css".replace("/./", "/")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{_esc(title)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,700&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="{_esc(css_href)}" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.8/dist/chart.umd.min.js" defer></script>
  <script src="{_esc(f'{relative_root}/assets/app.js')}" defer></script>
</head>
<body>
  <div class="page">
    <header class="site-header">
      <a class="brand" href="{_esc(f'{relative_root}/index.html')}">MQTT Python client bench</a>
      <p class="tagline">Comparative publish/subscribe results against Mosquitto</p>
    </header>
    {body}
    <footer class="site-footer">
      <p>Generated locally from committed <code>results/*.json</code>. Raw JSON stays in the repository, not on this site.</p>
    </footer>
  </div>
</body>
</html>
"""


def _status_badge(status: str, non_comparable: bool = False) -> str:
    label = status
    if non_comparable:
        label = f"{status} · non-comparable"
    return f'<span class="badge badge-{_esc(status)}">{_esc(label)}</span>'


def render_index(docs: Sequence[ResultDoc], generated_at: str) -> str:
    if not docs:
        body = f"""
    <main>
      <section class="hero">
        <h1>Benchmark reports</h1>
        <p>No result files yet. Run scenarios locally with <code>--output results/&lt;name&gt;.json</code>, commit the JSON, and push to refresh this site.</p>
        <p class="meta">Generated { _esc(generated_at) }</p>
      </section>
    </main>
"""
        return _page_shell("Benchmark reports", body)

    # Grouped bar chart + matrix: one x-tick / row per scenario·protocol, one
    # series / column per client. Never merge medians across MQTT protocols.
    scenario_docs = [
        doc
        for doc in docs
        if doc.kind == "scenario" and doc.median_msgs_per_s is not None and not doc.non_comparable
    ]

    def _protocol_aggregates(doc: ResultDoc) -> Dict[str, tuple]:
        """protocol -> (median, spread_low, spread_high) from comparable points."""
        buckets: Dict[str, List[PointRow]] = {}
        for point in doc.points:
            if point.non_comparable or point.median_msgs_per_s is None:
                continue
            proto = point.protocol or "MQTTv311"
            buckets.setdefault(proto, []).append(point)
        out: Dict[str, tuple] = {}
        for proto, pts in buckets.items():
            ordered = sorted(pts, key=lambda p: float(p.median_msgs_per_s or 0.0))
            mid = ordered[len(ordered) // 2]
            lows = [p.spread_low if p.spread_low is not None else p.median_msgs_per_s for p in pts]
            highs = [p.spread_high if p.spread_high is not None else p.median_msgs_per_s for p in pts]
            out[proto] = (
                float(mid.median_msgs_per_s or 0.0),
                float(min(v for v in lows if v is not None)),
                float(max(v for v in highs if v is not None)),
            )
        if not out and doc.median_msgs_per_s is not None:
            out["MQTTv311"] = (
                float(doc.median_msgs_per_s),
                float(doc.spread_low if doc.spread_low is not None else doc.median_msgs_per_s),
                float(doc.spread_high if doc.spread_high is not None else doc.median_msgs_per_s),
            )
        return out

    row_ids: List[str] = []
    by_key: Dict[tuple, Optional[float]] = {}
    by_key_low: Dict[tuple, Optional[float]] = {}
    by_key_high: Dict[tuple, Optional[float]] = {}
    for doc in scenario_docs:
        scenario = doc.scenario or doc.title
        client = doc.client or "?"
        for proto, (median_v, low_v, high_v) in _protocol_aggregates(doc).items():
            row_id = _matrix_row_id(scenario, proto)
            if row_id not in row_ids:
                row_ids.append(row_id)
            by_key[(row_id, client)] = median_v
            by_key_low[(row_id, client)] = low_v
            by_key_high[(row_id, client)] = high_v

    # Single-library scenario clients only. ABBA compare docs store a composite
    # "a / b" label in `client` and must not inflate the Clients stat.
    all_clients: List[str] = []
    for doc in docs:
        if doc.kind != "scenario" or not doc.client:
            continue
        if doc.client not in all_clients:
            all_clients.append(doc.client)
    scenario_clients: List[str] = []
    for doc in scenario_docs:
        name = doc.client or "?"
        if name not in scenario_clients:
            scenario_clients.append(name)
    colors = _client_colors(_sort_clients(all_clients))
    # Include clients/scenarios that only produced capability or load signals so
    # the issues table and matrix columns stay aligned with the full campaign.
    for doc in docs:
        if doc.kind != "scenario":
            continue
        if doc.client and doc.client not in scenario_clients:
            scenario_clients.append(doc.client)
        if not (doc.capability_reasons or doc.load_reasons):
            continue
        scenario = doc.scenario or doc.title
        protos = list(_protocol_aggregates(doc)) if doc.points else ["MQTTv311"]
        if not protos:
            protos = ["MQTTv311"]
        for proto in protos:
            row_id = _matrix_row_id(scenario, proto)
            if row_id not in row_ids:
                row_ids.append(row_id)
    chart_scenarios = [s for s in row_ids if _scenario_base(s) not in _CHART_EXCLUDED_SCENARIOS]
    matrix_scenarios = _order_matrix_scenarios(row_ids)
    overview_series = [
        {
            "client": client,
            "color": colors.get(client, "#5c6b64"),
            "values": [by_key.get((scenario, client)) for scenario in chart_scenarios],
            "low": [by_key_low.get((scenario, client)) for scenario in chart_scenarios],
            "high": [by_key_high.get((scenario, client)) for scenario in chart_scenarios],
        }
        for client in _sort_clients(scenario_clients)
    ]
    overview_payload = {"scenarios": chart_scenarios, "series": overview_series}
    matrix_html = _performance_matrix_html(matrix_scenarios, scenario_clients, by_key, colors)
    signals_html = _client_signals_html(docs, colors)

    non_comparable_n = sum(1 for doc in docs if doc.non_comparable)
    stats_html = f"""
      <div class="stats">
        <article>
          <p class="stat-label">Clients</p>
          <p class="stat-value">{_esc(len(all_clients))}</p>
        </article>
        <article>
          <p class="stat-label">Scenario rows</p>
          <p class="stat-value">{_esc(len(row_ids))}</p>
        </article>
        <article>
          <p class="stat-label">Result files</p>
          <p class="stat-value">{_esc(len(docs))}<span> {_esc(non_comparable_n)} non-comparable</span></p>
        </article>
      </div>
"""

    client_rank = {name: i for i, name in enumerate(_CLIENT_ORDER)}
    rows = []
    for doc in sorted(
        docs,
        key=lambda d: (
            client_rank.get(d.client or "", len(_CLIENT_ORDER)),
            d.client or "~",
            d.kind,
            d.scenario or d.title,
        ),
    ):
        client_cell = _client_swatch(doc.client, colors) if doc.client in colors else _esc(doc.client or "—")
        rows.append(
            f"""<tr>
  <td><a href="runs/{_esc(doc.slug)}.html">{_esc(doc.title)}</a></td>
  <td>{_esc(doc.kind)}</td>
  <td>{client_cell}</td>
  <td>{_esc(doc.profile or "—")}</td>
  <td>{_status_badge(doc.status, doc.non_comparable)}</td>
  <td class="num">{_esc(_fmt_num(doc.median_msgs_per_s))}</td>
  <td class="mono muted">{_esc(doc.source_name)}</td>
</tr>"""
        )

    body = f"""
    <main>
      <section class="hero">
        <h1>Benchmark reports</h1>
        <p>Readable summaries of local MQTT client runs. Higher throughput is better; latency and integrity appear on each detail page.</p>
        <p class="meta">generated { _esc(generated_at) }</p>
        {stats_html}
      </section>

      <section class="panel">
        <div class="panel-head">
          <h2>Throughput snapshot</h2>
          <p class="hint">Grouped by scenario · MQTT protocol, one colour per client. Whiskers show the observed run-to-run min/max. Comparable only within the same protocol. Rate-capped checks, smoke, and non-comparable results are omitted.</p>
        </div>
        <div class="chart-wrap chart-wrap-wide">
          <canvas id="overview-chart" data-overview='{_esc(json.dumps(overview_payload))}'></canvas>
        </div>
      </section>

      {matrix_html}

      <section class="panel">
        <div class="panel-head">
          <h2>All results</h2>
          <p class="hint">Every committed result file, including diagnostics and smoke runs excluded from the charts above.</p>
        </div>
        <div class="table-wrap table-wrap-scroll">
          <table>
            <thead>
              <tr>
                <th>Result</th>
                <th>Kind</th>
                <th>Client</th>
                <th>Profile</th>
                <th>Status</th>
                <th class="num">Median msg/s</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows)}
            </tbody>
          </table>
        </div>
      </section>

      {signals_html}
    </main>
"""
    return _page_shell("Benchmark reports", body)


def render_detail(doc: ResultDoc, generated_at: str, related: Optional[Dict[str, str]] = None) -> str:
    related = related or {}
    env_bits = []
    for key in ("hostname", "platform", "python", "cpu_count"):
        if doc.environment.get(key) is not None:
            env_bits.append(f"<li><strong>{_esc(key)}</strong> {_esc(doc.environment[key])}</li>")
    versions = (doc.environment.get("client_versions") or {}) if isinstance(doc.environment, dict) else {}
    if versions:
        env_bits.append(
            "<li><strong>client_versions</strong> "
            + _esc(", ".join(f"{k}={v}" for k, v in sorted(versions.items()) if v))
            + "</li>"
        )

    broker_bits = []
    for key, value in (doc.broker or {}).items():
        broker_bits.append(f"<li><strong>{_esc(key)}</strong> {_esc(value)}</li>")

    point_rows = []
    for idx, point in enumerate(doc.points):
        lat = point.latency
        integ = point.integrity or {}
        point_rows.append(
            f"""<tr>
  <td>{_esc(point.label)}</td>
  <td>{_status_badge(point.status, point.non_comparable)}</td>
  <td class="num">{_esc(_fmt_num(point.median_msgs_per_s))}</td>
  <td class="num">{_esc(_fmt_num(lat.get('p50_ms'), digits=2))}</td>
  <td class="num">{_esc(_fmt_num(lat.get('p99_ms'), digits=2))}{' *' if lat.get('p99_gated') else ''}</td>
  <td class="num">{_esc(integ.get('missing', '—'))} / {_esc(integ.get('duplicates', '—'))} (worst {_esc(integ.get('worst_missing', '—'))})</td>
  <td>{_esc(point.valid_runs)}/{_esc(point.total_runs)}</td>
</tr>"""
        )

    chart_block = ""
    if doc.points:
        labels = [p.label for p in doc.points]
        values = [p.median_msgs_per_s for p in doc.points]
        lows = [p.spread_low for p in doc.points]
        highs = [p.spread_high for p in doc.points]
        chart_block = f"""
      <section class="panel">
        <h2>Per-point throughput</h2>
        <p class="hint">Whiskers show the observed run-to-run min/max at each point. Dual-protocol scenarios list MQTTv311 and MQTTv5 points separately (labels include <code>proto=</code>).</p>
        <div class="chart-wrap">
          <canvas class="detail-chart" data-labels='{_esc(json.dumps(labels))}' data-values='{_esc(json.dumps(values))}' data-low='{_esc(json.dumps(lows))}' data-high='{_esc(json.dumps(highs))}'></canvas>
        </div>
      </section>
"""

    compare_block = ""
    if doc.kind == "compare" and doc.verdict:
        identity_bits = []
        for label, key in (("baseline", "baseline_identity"), ("candidate", "candidate_identity")):
            ident = doc.raw_meta.get(key) or {}
            identity_bits.append(
                f"<li><strong>{label}</strong> {_esc(ident.get('client'))} "
                f"v{_esc(ident.get('client_version'))} "
                f"({_esc(ident.get('stability'))}/{_esc(ident.get('implementation_language'))})</li>"
            )
        point_cal = []
        for point in doc.raw_meta.get("points") or []:
            cals = point.get("calibrations") or {}
            if cals:
                point_cal.append(
                    f"<li>point {_esc(point.get('point_index'))}: {_esc(json.dumps(cals))}</li>"
                )
        loadgen = doc.raw_meta.get("loadgen") or {}
        compare_block = f"""
      <section class="panel">
        <h2>A/B verdict</h2>
        <p>{_status_badge(str(doc.verdict.get('verdict', 'inconclusive')))}</p>
        <ul class="kv">
          <li><strong>profile</strong> {_esc(doc.profile)}</li>
          <li><strong>cooldown_s</strong> {_esc(doc.raw_meta.get('cooldown_s'))}</li>
          <li><strong>order</strong> {_esc(doc.raw_meta.get('order'))}</li>
          <li><strong>median ratio</strong> {_esc(_fmt_num(doc.verdict.get('median_ratio'), digits=3))}</li>
          <li><strong>effect %</strong> {_esc(_fmt_num(doc.verdict.get('absolute_effect_pct'), digits=2))}</li>
          <li><strong>CI</strong> {_esc(_fmt_num(doc.verdict.get('ci_low'), digits=3))} … {_esc(_fmt_num(doc.verdict.get('ci_high'), digits=3))}</li>
          <li><strong>loadgen</strong> {_esc(loadgen.get('image'))} digest={_esc(loadgen.get('image_digest'))}</li>
          {''.join(identity_bits)}
        </ul>
        {"<h3>Per-client calibrations</h3><ul>" + ''.join(point_cal) + "</ul>" if point_cal else ""}
      </section>
"""

    calibrate_block = ""
    if doc.kind == "calibrate":
        fractions = doc.raw_meta.get("fractions")
        rtt_fractions = doc.raw_meta.get("rtt_fractions")
        rtt_capacity = doc.raw_meta.get("rtt_capacity_msgs_per_s")
        protocol_capacities = doc.raw_meta.get("protocol_capacities")
        proto_block = ""
        if protocol_capacities:
            proto_block = f"""
        <h3>Per-protocol capacities</h3>
        <pre class="code-block">{_esc(json.dumps(protocol_capacities, indent=2))}</pre>
"""
        calibrate_block = f"""
      <section class="panel">
        <h2>Calibration</h2>
        <p>Publish capacity (primary): <strong>{_esc(_fmt_num(doc.median_msgs_per_s))}</strong> msg/s</p>
        <p>RTT capacity (primary): <strong>{_esc(_fmt_num(rtt_capacity))}</strong> pairs/s</p>
        {proto_block}
        <h3>Publish fractions</h3>
        <pre class="code-block">{_esc(json.dumps(fractions, indent=2))}</pre>
        <h3>RTT fractions</h3>
        <pre class="code-block">{_esc(json.dumps(rtt_fractions, indent=2))}</pre>
      </section>
"""

    suite_block = ""
    if doc.kind == "suite":
        scenario_links = []
        for entry in doc.raw_meta.get("scenario_entries") or []:
            name = entry.get("scenario") or "?"
            client = entry.get("client") or ""
            href = related.get(f"{client}:{name}") or related.get(name)
            label = f"{name}" + (f" ({client})" if client else "")
            median = entry.get("median_msgs_per_s")
            median_txt = f" — {_fmt_num(median)} msg/s" if median is not None else ""
            if href:
                scenario_links.append(
                    f'<li><a href="{_esc(href)}">{_esc(label)}</a>{_esc(median_txt)}</li>'
                )
            else:
                scenario_links.append(f"<li>{_esc(label)}{_esc(median_txt)}</li>")
        if not scenario_links:
            for name in doc.raw_meta.get("scenario_names") or []:
                href = related.get(name)
                if href:
                    scenario_links.append(f'<li><a href="{_esc(href)}">{_esc(name)}</a></li>')
                else:
                    scenario_links.append(f"<li>{_esc(name)}</li>")
        suite_block = f"""
      <section class="panel">
        <h2>Suite overview</h2>
        <ul class="kv">
          <li><strong>suite</strong> {_esc(doc.raw_meta.get('suite'))}</li>
          <li><strong>scenarios</strong> {_esc(doc.raw_meta.get('scenario_count'))}</li>
        </ul>
        <h3>Scenario results</h3>
        <ul>{''.join(scenario_links)}</ul>
        <pre class="code-block">{_esc(json.dumps(doc.raw_meta.get('estimate'), indent=2))}</pre>
      </section>
"""

    points_table = ""
    if point_rows:
        points_table = f"""
      <section class="panel">
        <h2>Measurement points</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Point</th>
                <th>Status</th>
                <th>Median msg/s</th>
                <th>Latency p50 ms</th>
                <th>Latency p99 ms</th>
                <th>Missing / dup</th>
                <th>Valid runs</th>
              </tr>
            </thead>
            <tbody>
              {''.join(point_rows)}
            </tbody>
          </table>
        </div>
        <p class="hint">* p99 marked gated when sample coverage is incomplete. Smoke / non-comparable points are excluded from global comparative charts.</p>
      </section>
"""

    body = f"""
    <main>
      <p class="crumb"><a href="../index.html">← All results</a></p>
      <section class="hero">
        <h1>{_esc(doc.title)}</h1>
        <p>
          {_status_badge(doc.status, doc.non_comparable)}
          · client <strong>{_esc(doc.client or '—')}</strong>
          · profile <strong>{_esc(doc.profile or '—')}</strong>
        </p>
        <p class="meta">Source <code>{_esc(doc.source_name)}</code> · generated { _esc(generated_at) }</p>
      </section>

      <section class="stats">
        <article>
          <p class="stat-label">Median throughput</p>
          <p class="stat-value">{_esc(_fmt_num(doc.median_msgs_per_s))} <span>msg/s</span></p>
        </article>
        <article>
          <p class="stat-label">Kind</p>
          <p class="stat-value">{_esc(doc.kind)}</p>
        </article>
        <article>
          <p class="stat-label">Points</p>
          <p class="stat-value">{_esc(len(doc.points))}</p>
        </article>
      </section>

      {chart_block}
      {points_table}
      {compare_block}
      {calibrate_block}
      {suite_block}

      <section class="panel two-col">
        <div>
          <h2>Environment</h2>
          <ul class="kv">{''.join(env_bits) or '<li>—</li>'}</ul>
        </div>
        <div>
          <h2>Broker</h2>
          <ul class="kv">{''.join(broker_bits) or '<li>—</li>'}</ul>
        </div>
      </section>
    </main>
"""
    return _page_shell(doc.title, body, relative_root="..")


def build_site(input_dir: Path | str, output_dir: Path | str) -> Dict[str, Any]:
    """Generate the static site under output_dir from JSON files in input_dir."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    docs = load_results(input_path)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if output_path.exists():
        shutil.rmtree(output_path)
    runs_dir = output_path / "runs"
    assets_out = output_path / "assets"
    runs_dir.mkdir(parents=True, exist_ok=True)
    assets_out.mkdir(parents=True, exist_ok=True)

    for name in ("style.css", "app.js"):
        shutil.copy2(ASSETS_DIR / name, assets_out / name)

    (output_path / "index.html").write_text(render_index(docs, generated_at), encoding="utf-8")
    related: Dict[str, str] = {}
    for doc in docs:
        if doc.kind == "scenario" and doc.scenario:
            related[doc.scenario] = f"{doc.slug}.html"
            if doc.client:
                related[f"{doc.client}:{doc.scenario}"] = f"{doc.slug}.html"
    for doc in docs:
        (runs_dir / f"{doc.slug}.html").write_text(
            render_detail(doc, generated_at, related=related),
            encoding="utf-8",
        )

    # No raw JSON copied into site/.
    return {
        "input": str(input_path),
        "output": str(output_path),
        "results": len(docs),
        "generated_at": generated_at,
    }
