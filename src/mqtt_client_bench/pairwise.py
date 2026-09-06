"""Official pairwise RTT campaign gates.

Capacity that sizes ``C_common`` must be 5/5 valid on the official path.
Dispersion is recorded so a wild median cannot hide in a compact summary.

Smoke is path proof only. ``PROFILE=smoke`` tags every run ``non_comparable``,
and a contended runner may also void a completed native run with an exclusive
broker-CPU reason. Those runs may size a *non-comparable* calibration so
matched-load / ABBA machinery can be exercised. They never size an official
``C_common``.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

from mqtt_client_bench.metrics import sanitize_number, summarize_runs


REQUIRED_OFFICIAL_CAPACITY_RUNS = 5
REQUIRED_PROTOCOLS = ("MQTTv311", "MQTTv5")

# Exclusive environmental voids that a smoke path-proof may still admit.
# Timeouts, worker errors, host_busy and mixed reasons stay refused.
_PATH_PROOF_ENV_REASONS = frozenset(
    {
        "broker_headroom_low",
        "container_cpu_high",
        "cpu_governor_unknown",
        "clock_unpinned",
        "non_comparable",
    }
)


def _spread(values: list[float]) -> dict:
    summary = summarize_runs(values)
    med = summary.get("median")
    mn = summary.get("min")
    mx = summary.get("max")
    spread_pct = None
    if med and med > 0 and mn is not None and mx is not None:
        spread_pct = sanitize_number(100.0 * (mx - mn) / med)
    mean = summary.get("mean")
    cv = None
    if mean and mean > 0 and len(values) >= 2:
        var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        cv = sanitize_number(math.sqrt(var) / mean)
    return {
        "values": summary["values"],
        "n": summary["n"],
        "median": med,
        "min": mn,
        "max": mx,
        "mean": mean,
        "spread_pct": spread_pct,
        "cv": cv,
    }


def _reason_keys(run: dict) -> set[str]:
    keys = {str(reason).split(":", 1)[0] for reason in (run.get("reasons") or [])}
    if run.get("non_comparable"):
        keys.add("non_comparable")
    return {key for key in keys if key}


def _completed_measurement(run: dict) -> bool:
    if run.get("ok") is False or run.get("error"):
        return False
    return run.get("primary_msgs_per_s") is not None


def _officially_valid(run: dict) -> bool:
    return (
        run.get("status") == "valid"
        and not run.get("non_comparable")
        and _completed_measurement(run)
    )


def _path_proof_admitted(run: dict) -> bool:
    """Smoke-only: completed native runs, including exclusive broker-CPU voids."""
    if not _completed_measurement(run):
        return False
    if run.get("status") == "valid":
        return True
    if run.get("status") != "inconclusive":
        return False
    extra = _reason_keys(run) - _PATH_PROOF_ENV_REASONS
    return not extra


def official_rtt_capacity_block(
    block: dict,
    client: str,
    *,
    required_valid: int = REQUIRED_OFFICIAL_CAPACITY_RUNS,
    allow_non_comparable: bool = False,
) -> dict:
    """Inspect one protocol block. Fail closed unless every requested run is valid.

    Official: ``status=valid`` and not ``non_comparable``. Smoke path-proof may
    also admit exclusive environmental voids so the rest of the campaign can
    run as ``non_comparable``.
    """
    proto = str((block.get("point") or {}).get("protocol") or "")
    runs = list(block.get("runs") or [])
    admitted = []
    valid = []
    invalid = []
    for run in runs:
        take = (
            _path_proof_admitted(run)
            if allow_non_comparable
            else _officially_valid(run)
        )
        if take:
            admitted.append(run)
            if run.get("status") == "valid":
                valid.append(run)
        else:
            invalid.append(run)
    rates = [
        float(r["primary_msgs_per_s"])
        for r in admitted
        if r.get("primary_msgs_per_s") is not None
    ]
    paths = {r.get("publish_path") for r in runs}
    errors = []
    if client in ("mqttium", "gmqtt") and paths - {None, "native_async"}:
        errors.append(f"{client}:{proto}:non_native_rtt_path:{sorted(paths)}")
    if client == "paho" and paths - {None, "sync_facade"}:
        errors.append(f"{client}:{proto}:non_sync_rtt_path:{sorted(paths)}")
    if len(admitted) < required_valid:
        reasons = []
        for run in invalid:
            extra = list(run.get("reasons") or [])
            if run.get("non_comparable") and "non_comparable" not in extra:
                extra.append("non_comparable")
            reasons.extend(extra or [run.get("status") or "invalid"])
        errors.append(
            f"{client}:{proto}:insufficient_valid_rtt_capacity:"
            f"{len(admitted)}/{required_valid}:{','.join(reasons) or 'none'}"
        )
    if len(runs) < required_valid:
        errors.append(f"{client}:{proto}:incomplete_rtt_capacity:{len(runs)}/{required_valid}")
    stats = _spread(rates)
    admission = "official_valid"
    if allow_non_comparable:
        admission = "smoke_path_proof"
    return {
        "protocol": proto,
        "n_runs": len(runs),
        "n_valid": len(valid),
        "n_admitted": len(admitted),
        "n_invalid": len(invalid),
        "admission": admission,
        "publish_paths": sorted(p for p in paths if p),
        "errors": errors,
        **stats,
        "capacity_msgs_per_s": stats["median"] if not errors else None,
    }


def official_rtt_capacities(
    doc: dict,
    client: str,
    *,
    required_valid: int = REQUIRED_OFFICIAL_CAPACITY_RUNS,
    required_protocols: tuple[str, ...] = REQUIRED_PROTOCOLS,
    allow_non_comparable: bool = False,
) -> dict:
    """Build a calibration from a matrix document, or list the refusals."""
    errors = []
    protocols = {}
    seen = set()
    for block in doc.get("results") or []:
        inspected = official_rtt_capacity_block(
            block,
            client,
            required_valid=required_valid,
            allow_non_comparable=allow_non_comparable,
        )
        proto = inspected["protocol"]
        if proto in required_protocols:
            seen.add(proto)
            protocols[proto] = inspected
            errors.extend(inspected["errors"])
    missing = [p for p in required_protocols if p not in seen]
    if missing:
        errors.append(f"{client}:missing_valid_rtt_capacity:{','.join(missing)}")
    return {
        "client": client,
        "ok": not errors,
        "errors": errors,
        "protocols": protocols,
        "path_proof": bool(allow_non_comparable),
    }


def write_official_rtt_calibrations(
    matrix_dir: str | Path,
    cal_dir: str | Path,
    clients: list[str] | tuple[str, ...],
    *,
    required_valid: int = REQUIRED_OFFICIAL_CAPACITY_RUNS,
    allow_non_comparable: bool = False,
) -> dict:
    """Write per-client load profiles. Raises SystemExit-style ValueError on refusal."""
    matrix_dir = Path(matrix_dir)
    cal_dir = Path(cal_dir)
    cal_dir.mkdir(parents=True, exist_ok=True)
    errors = []
    all_caps = {}
    for client in clients:
        doc = json.loads((matrix_dir / f"{client}-rtt_capacity_qos1.json").read_text())
        inspected = official_rtt_capacities(
            doc,
            client,
            required_valid=required_valid,
            allow_non_comparable=allow_non_comparable,
        )
        errors.extend(inspected["errors"])
        caps = {
            proto: block["capacity_msgs_per_s"]
            for proto, block in inspected["protocols"].items()
            if block.get("capacity_msgs_per_s") is not None
        }
        profile = {
            "schema_version": 1,
            "source": "interleaved_rtt_capacity_matrix",
            "client": client,
            "required_valid_runs": required_valid,
            "path_proof": bool(allow_non_comparable),
            "non_comparable": bool(allow_non_comparable),
            "capacity_blocks": inspected["protocols"],
            "protocol_capacities": {
                proto: {"rtt_capacity_msgs_per_s": value}
                for proto, value in caps.items()
            },
        }
        (cal_dir / f"{client}-load.json").write_text(json.dumps(profile, indent=2) + "\n")
        all_caps[client] = inspected
    payload = {"valid_rtt_capacities": all_caps, "path_proof": bool(allow_non_comparable)}
    if errors:
        raise ValueError("strict pairwise RTT capacity gate failed: " + "; ".join(errors))
    return payload
