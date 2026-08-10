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
