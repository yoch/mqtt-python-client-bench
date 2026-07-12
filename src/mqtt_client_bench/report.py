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
    for run in runs:
        for worker in run.get("workers") or []:
            summary = worker.get("latency_summary") or {}
            if summary.get("p50_ms") is not None:
                p50.append(float(summary["p50_ms"]))
            if summary.get("p95_ms") is not None:
                p95.append(float(summary["p95_ms"]))
            if summary.get("p99_ms") is not None and summary.get("p99_published"):
                p99.append(float(summary["p99_ms"]))
    def median(values: List[float]) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        return ordered[len(ordered) // 2]

    return {"p50_ms": median(p50), "p95_ms": median(p95), "p99_ms": median(p99)}


def _collect_integrity(runs: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for run in runs:
        for worker in run.get("workers") or []:
            if worker.get("integrity"):
                return worker["integrity"]
    return None


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


def classify_payload(data: Dict[str, Any], source_name: str) -> ResultDoc:
    slug = _slug(Path(source_name).stem)
    if "scenarios" in data and "suite" in data:
        scenario_names = [s.get("scenario", "?") for s in data.get("scenarios") or []]
        clients = sorted({s.get("client") for s in data.get("scenarios") or [] if s.get("client")})
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
            raw_meta={"suite": data.get("suite"), "estimate": data.get("estimate"), "scenario_count": len(scenario_names)},
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
            raw_meta={"order": data.get("order")},
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
            raw_meta={"fractions": data.get("fractions")},
        )

    points: List[PointRow] = []
    medians: List[float] = []
    any_non_comparable = False
    overall_valid = 0
    overall_total = 0
    for block in data.get("results") or []:
        point = block.get("point") or {}
        runs = block.get("runs") or []
        summary = block.get("summary") or {}
        counts = _run_status_counts(runs)
        overall_valid += counts["valid"]
        overall_total += counts["total"]
        non_comparable = any(bool(r.get("non_comparable")) for r in runs) or bool(point.get("non_comparable"))
        any_non_comparable = any_non_comparable or non_comparable
        median_rate = summary.get("median")
        if median_rate is not None:
            medians.append(float(median_rate))
        status = "valid" if counts["valid"] == counts["total"] and counts["total"] else (
            "partial" if counts["valid"] else "inconclusive"
        )
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
                chart_rates=[r.get("primary_msgs_per_s") for r in runs],
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
    if medians:
        medians_sorted = sorted(medians)
        primary_median = medians_sorted[len(medians_sorted) // 2]

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

    rows = []
    for doc in docs:
        rows.append(
            f"""<tr>
  <td><a href="runs/{_esc(doc.slug)}.html">{_esc(doc.title)}</a></td>
  <td>{_esc(doc.kind)}</td>
  <td>{_esc(doc.client or "—")}</td>
  <td>{_esc(doc.profile or "—")}</td>
  <td>{_status_badge(doc.status, doc.non_comparable)}</td>
  <td class="num">{_esc(_fmt_num(doc.median_msgs_per_s))}</td>
  <td class="mono">{_esc(doc.source_name)}</td>
</tr>"""
        )

    chart_labels = [doc.title for doc in docs if doc.kind == "scenario" and doc.median_msgs_per_s is not None]
    chart_values = [doc.median_msgs_per_s for doc in docs if doc.kind == "scenario" and doc.median_msgs_per_s is not None]
    chart_clients = [doc.client or "?" for doc in docs if doc.kind == "scenario" and doc.median_msgs_per_s is not None]

    body = f"""
    <main>
      <section class="hero">
        <h1>Benchmark reports</h1>
        <p>Readable summaries of local MQTT client runs. Higher throughput is better; latency and integrity appear on each detail page.</p>
        <p class="meta">{_esc(len(docs))} file(s) · generated { _esc(generated_at) }</p>
      </section>

      <section class="panel">
        <h2>Throughput snapshot</h2>
        <div class="chart-wrap">
          <canvas id="overview-chart" data-labels='{_esc(json.dumps(chart_labels))}' data-values='{_esc(json.dumps(chart_values))}' data-clients='{_esc(json.dumps(chart_clients))}'></canvas>
        </div>
        <p class="hint">Median messages/s across points for each scenario result. Smoke profiles are marked non-comparable on detail pages.</p>
      </section>

      <section class="panel">
        <h2>All results</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Result</th>
                <th>Kind</th>
                <th>Client</th>
                <th>Profile</th>
                <th>Status</th>
                <th>Median msg/s</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows)}
            </tbody>
          </table>
        </div>
      </section>
    </main>
"""
    return _page_shell("Benchmark reports", body)


def render_detail(doc: ResultDoc, generated_at: str) -> str:
    env_bits = []
    for key in ("hostname", "platform", "python", "cpu_count"):
        if doc.environment.get(key) is not None:
            env_bits.append(f"<li><strong>{_esc(key)}</strong> {_esc(doc.environment[key])}</li>")

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
  <td class="num">{_esc(_fmt_num(lat.get('p99_ms'), digits=2))}</td>
  <td class="num">{_esc(integ.get('missing', '—'))} / {_esc(integ.get('duplicates', '—'))}</td>
  <td>{_esc(point.valid_runs)}/{_esc(point.total_runs)}</td>
</tr>"""
        )

    chart_block = ""
    if doc.points:
        labels = [p.label for p in doc.points]
        values = [p.median_msgs_per_s for p in doc.points]
        chart_block = f"""
      <section class="panel">
        <h2>Per-point throughput</h2>
        <div class="chart-wrap">
          <canvas class="detail-chart" data-labels='{_esc(json.dumps(labels))}' data-values='{_esc(json.dumps(values))}'></canvas>
        </div>
      </section>
"""

    compare_block = ""
    if doc.kind == "compare" and doc.verdict:
        compare_block = f"""
      <section class="panel">
        <h2>A/B verdict</h2>
        <p>{_status_badge(str(doc.verdict.get('verdict', 'inconclusive')))}</p>
        <ul class="kv">
          <li><strong>median ratio</strong> {_esc(_fmt_num(doc.verdict.get('median_ratio'), digits=3))}</li>
          <li><strong>effect %</strong> {_esc(_fmt_num(doc.verdict.get('absolute_effect_pct'), digits=2))}</li>
          <li><strong>CI</strong> {_esc(_fmt_num(doc.verdict.get('ci_low'), digits=3))} … {_esc(_fmt_num(doc.verdict.get('ci_high'), digits=3))}</li>
        </ul>
      </section>
"""

    calibrate_block = ""
    if doc.kind == "calibrate":
        fractions = doc.raw_meta.get("fractions")
        calibrate_block = f"""
      <section class="panel">
        <h2>Calibration</h2>
        <p>Baseline capacity: <strong>{_esc(_fmt_num(doc.median_msgs_per_s))}</strong> msg/s</p>
        <pre class="code-block">{_esc(json.dumps(fractions, indent=2))}</pre>
      </section>
"""

    suite_block = ""
    if doc.kind == "suite":
        suite_block = f"""
      <section class="panel">
        <h2>Suite overview</h2>
        <ul class="kv">
          <li><strong>suite</strong> {_esc(doc.raw_meta.get('suite'))}</li>
          <li><strong>scenarios</strong> {_esc(doc.raw_meta.get('scenario_count'))}</li>
        </ul>
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
    for doc in docs:
        (runs_dir / f"{doc.slug}.html").write_text(render_detail(doc, generated_at), encoding="utf-8")

    # No raw JSON copied into site/.
    return {
        "input": str(input_path),
        "output": str(output_path),
        "results": len(docs),
        "generated_at": generated_at,
    }
