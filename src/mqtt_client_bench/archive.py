"""Split a result document into a committed summary and a full-fidelity archive.

Raw per-message sample arrays — ``latencies_ns``, ``scheduler_lags_ns`` and the
sequence vectors — are 97-98 % of a result file's bytes, and they are consumed
exactly once: ``validate_run()`` turns them into ``latency_summary`` and
``integrity``, both of which are stored alongside. Nothing reads them afterwards,
not the report (which only reads ``latency_summary``) and not the rankings. So
committing them bought a repository GitHub refuses outright — two ABBA documents
exceeded the 100 MB blob limit — in exchange for nothing that gets published.

They keep their value for *re-analysis*: other quantiles, a histogram, a
bootstrap over latency rather than over throughput. That is why this archives
them rather than dropping them. The committed document records how many samples
were archived, so a summary can never be mistaken for a complete record.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path
from typing import Dict, Iterable, Tuple

from mqtt_client_bench.metrics import latency_summary

# Per-message vectors. Every one of these is an input to a statistic that is
# itself stored in the document.
RAW_SAMPLE_KEYS = ("latencies_ns", "scheduler_lags_ns", "sequences", "sent_sequences")

ARCHIVED_KEY = "raw_samples_archived"


def _slim_in_place(node) -> Dict[str, int]:
    dropped: Dict[str, int] = {}
    if isinstance(node, dict):
        # Derive before discarding: a document that reached here without its
        # latency summary must not lose the percentiles with the samples.
        if isinstance(node.get("latencies_ns"), list) and not node.get("latency_summary"):
            node["latency_summary"] = latency_summary(node["latencies_ns"])
        counts = {}
        for key in RAW_SAMPLE_KEYS:
            value = node.get(key)
            if isinstance(value, list):
                counts[key] = len(value)
                del node[key]
        if counts:
            node[ARCHIVED_KEY] = counts
            for key, n in counts.items():
                dropped[key] = dropped.get(key, 0) + n
        for value in node.values():
            for key, n in _slim_in_place(value).items():
                dropped[key] = dropped.get(key, 0) + n
    elif isinstance(node, list):
        for value in node:
            for key, n in _slim_in_place(value).items():
                dropped[key] = dropped.get(key, 0) + n
    return dropped


def slim_document(doc: dict) -> Tuple[dict, Dict[str, int]]:
    """Return a copy without raw sample vectors, plus per-key dropped counts."""
    slim = copy.deepcopy(doc)
    return slim, _slim_in_place(slim)


def archive_results(
    input_dir: str | Path,
    archive_dir: str | Path,
    *,
    dry_run: bool = False,
) -> dict:
    """Archive raw samples out of every result file, in place.

    The archive keeps the document exactly as measured, gzipped; the file left in
    ``input_dir`` is the same document minus the sample vectors. Re-running is
    safe: a document with nothing left to archive is not rewritten, and its
    archive is not replaced, so an already-archived run cannot be overwritten by
    its own summary.
    """
    input_path, archive_path = Path(input_dir), Path(archive_dir)
    report = {"archived": [], "skipped": [], "bytes_before": 0, "bytes_after": 0}
    for path in sorted(input_path.glob("*.json")):
        raw = path.read_bytes()
        try:
            doc = json.loads(raw)
        except Exception:  # noqa: BLE001
            report["skipped"].append({"file": path.name, "reason": "unparseable"})
            continue
        slim, dropped = slim_document(doc)
        if not dropped:
            report["skipped"].append({"file": path.name, "reason": "no raw samples"})
            continue
        target = archive_path / f"{path.stem}.json.gz"
        after = json.dumps(slim, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        report["bytes_before"] += len(raw)
        report["bytes_after"] += len(after)
        report["archived"].append(
            {
                "file": path.name,
                "archive": str(target),
                "dropped": dropped,
                "bytes_before": len(raw),
                "bytes_after": len(after),
            }
        )
        if dry_run:
            continue
        archive_path.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            with gzip.open(target, "wb", compresslevel=9) as fh:
                fh.write(raw)
        path.write_bytes(after)
    report["files"] = len(report["archived"])
    report["saved_bytes"] = report["bytes_before"] - report["bytes_after"]
    return report


def _initiator_worker(run: dict) -> dict:
    for worker in run.get("workers") or []:
        if worker.get("role") in ("rtt_initiator", "publisher"):
            return worker
    return {}


def extract_run_row(run: dict, *, document: dict | None = None) -> dict:
    """One reviewable row per run: enough to rebuild medians, ABBA and A/A.

    Raw sample vectors stay out. Percentiles come from ``latency_summary``,
    which ``slim_document`` derives before dropping ``latencies_ns``.
    """
    doc = document or {}
    worker = _initiator_worker(run)
    latency = worker.get("latency_summary") or {}
    point = run.get("point") or {}
    offered = worker.get("offered")
    submitted = worker.get("submitted")
    if submitted is None:
        submitted = worker.get("sent_in_window")
    completed = worker.get("completed_in_window")
    completion = worker.get("completion")
    if completion is None and offered and completed is not None:
        completion = completed / float(offered)
    host = run.get("host_profile") or {}
    env = run.get("environment") or {}
    identity = worker or doc.get("client_identity") or {}
    return {
        "client": run.get("client") or doc.get("client"),
        "client_version": identity.get("client_version"),
        "adapter": identity.get("adapter"),
        "harness_fingerprint": run.get("harness_fingerprint") or doc.get("harness_fingerprint"),
        "harness_git_sha": run.get("harness_git_sha") or doc.get("harness_git_sha"),
        "host_fingerprint": host.get("fingerprint"),
        "host_role": host.get("role"),
        "machine": env.get("machine"),
        "architecture": env.get("machine"),
        "python": env.get("python"),
        "broker_version": (
            (run.get("broker") or {}).get("version")
            or (doc.get("broker") or {}).get("version")
            or ((run.get("sys_counters") or {}).get("broker") or {}).get("version")
        ),
        "scaling_governor": env.get("scaling_governor") or (run.get("host_state") or {}).get("scaling_governor"),
        "scenario": point.get("scenario") or doc.get("scenario"),
        "protocol": point.get("protocol") or run.get("protocol_effective"),
        "shared_load_fraction": point.get("shared_load_fraction"),
        "target_rate": point.get("target_rate"),
        "C_common": point.get("shared_capacity_msgs_per_s"),
        "run_id": run.get("run_id"),
        "run_index": run.get("run_index"),
        "matrix_slot": run.get("matrix_slot"),
        "matrix_rotation": run.get("matrix_rotation"),
        "ab_label": run.get("ab_label"),
        "slot": run.get("slot"),
        "order": run.get("order") or doc.get("order"),
        "status": run.get("status"),
        "reasons": list(run.get("reasons") or []),
        "primary_msgs_per_s": run.get("primary_msgs_per_s"),
        "p50_ms": latency.get("p50_ms"),
        "p95_ms": latency.get("p95_ms"),
        "p99_ms": latency.get("p99_ms"),
        "offered": offered,
        "submitted": submitted,
        "completed": completed,
        "completion": completion,
        "missed_due_to_backpressure": worker.get("missed_due_to_backpressure"),
        "backlog_at_end": worker.get("backlog_at_end") if worker.get("backlog_at_end") is not None else worker.get("timeouts"),
        "publish_path": run.get("publish_path") or worker.get("publish_path"),
        "native_async": run.get("native_async") if run.get("native_async") is not None else worker.get("native_async"),
        "io_model": identity.get("io_model"),
        "completion_mechanism": identity.get("completion_mechanism"),
        "comparison_metric": run.get("comparison_metric") or doc.get("comparison_metric"),
        "comparison_direction": run.get("comparison_direction") or doc.get("comparison_direction"),
        "comparison_value": run.get("comparison_value"),
        "managed_broker": run.get("managed_broker"),
        "non_comparable": run.get("non_comparable"),
    }


def extract_run_rows(doc: dict) -> list:
    """Flatten a scenario or compare document into persistable per-run rows."""
    rows = []
    blocks = list(doc.get("results") or [])
    if not blocks and doc.get("points"):
        blocks = list(doc.get("points") or [])
    if not blocks and doc.get("runs"):
        blocks = [{"point": doc.get("point") or {}, "runs": doc.get("runs")}]
    for block in blocks:
        point = block.get("point") or {}
        for run in block.get("runs") or []:
            merged = dict(run)
            merged.setdefault("point", point)
            if block.get("order") and not merged.get("order"):
                merged["order"] = block["order"]
            rows.append(extract_run_row(merged, document=doc))
    return rows


def extract_compare_summaries(doc: dict) -> list:
    """Persist ABBA / A/A verdicts without raw samples."""
    if doc.get("baseline_client") is None or not doc.get("points"):
        return []
    summaries = []
    for block in doc.get("points") or []:
        verdict = block.get("verdict") or {}
        point = block.get("point") or {}
        summaries.append(
            {
                "kind": "compare_point",
                "scenario": doc.get("scenario"),
                "baseline_client": doc.get("baseline_client"),
                "candidate_client": doc.get("candidate_client"),
                "protocol": point.get("protocol"),
                "shared_load_fraction": point.get("shared_load_fraction"),
                "target_rate": point.get("target_rate"),
                "C_common": point.get("shared_capacity_msgs_per_s"),
                "order": block.get("order") or doc.get("order"),
                "n_blocks": verdict.get("n_blocks"),
                "block_ratios": verdict.get("block_ratios") or block.get("block_ratios"),
                "median_ratio": verdict.get("median_ratio"),
                "ci_low": verdict.get("ci_low"),
                "ci_high": verdict.get("ci_high"),
                "absolute_effect_pct": verdict.get("absolute_effect_pct"),
                "excludes_zero_effect": verdict.get("excludes_zero_effect"),
                "verdict": verdict.get("verdict"),
                "n_valid_baseline": len(block.get("baseline_rates") or []),
                "n_valid_candidate": len(block.get("candidate_rates") or []),
                "comparison_metric": (
                    verdict.get("comparison_metric")
                    or block.get("comparison_metric")
                    or doc.get("comparison_metric")
                ),
                "comparison_direction": (
                    verdict.get("comparison_direction")
                    or block.get("comparison_direction")
                    or doc.get("comparison_direction")
                ),
                "publish_path_baseline": (doc.get("baseline_identity") or {}).get("publish_path"),
                "publish_path_candidate": (doc.get("candidate_identity") or {}).get("publish_path"),
            }
        )
    return summaries
