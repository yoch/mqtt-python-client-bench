"""Official pairwise RTT campaign gates.

Capacity that sizes ``C_common`` must be 5/5 valid on the official path.
Dispersion is recorded so a wild median cannot hide in a compact summary.

Smoke is path proof only. ``PROFILE=smoke`` tags every run ``non_comparable``,
and a contended runner may also void a completed native run with an exclusive
broker-CPU reason. Those runs may size a *non-comparable* calibration so
matched-load / ABBA machinery can be exercised. They never size an official
``C_common``.

Same-client A/A is a fail-closed publication gate, not a decorative verdict.
Capacity, then A/A, then enforce. Ranking ABBA A/B runs only after the
control passes. A missing CI, a single complementary pair, or an
incomplete block all fail closed.

``PROFILE=standard`` cannot weaken those invariants through environment
overrides. ``validate_official_pairwise_policy`` is the auditable check;
smoke and targeted paths are left free to use four A/A blocks or skip ranking.
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
        "process_cpu_high",
        "cpu_governor_unknown",
        "clock_unpinned",
        "non_comparable",
        "broker_telemetry_missing",
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

    Official: every requested run must be executed and ``status=valid``.
    ``6`` runs of which ``5`` are valid is not ``5/5``. Smoke path-proof may
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
    elif not allow_non_comparable and len(admitted) != required_valid:
        errors.append(
            f"{client}:{proto}:insufficient_valid_rtt_capacity:"
            f"{len(admitted)}/{required_valid}:extra_runs"
        )
    if len(runs) < required_valid:
        errors.append(f"{client}:{proto}:incomplete_rtt_capacity:{len(runs)}/{required_valid}")
    elif not allow_non_comparable and len(runs) != required_valid:
        errors.append(
            f"{client}:{proto}:executed_rtt_capacity:{len(runs)}/{required_valid}"
        )
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
    if errors:
        raise ValueError("strict pairwise RTT capacity gate failed: " + "; ".join(errors))
    common = _pair_common_capacities(all_caps, clients)
    aa_dir = cal_dir / "aa"
    aa_dir.mkdir(parents=True, exist_ok=True)
    for client in clients:
        aa_profile = {
            "schema_version": 1,
            "source": "pairwise_c_common_for_aa",
            "client": client,
            "c_common_of": list(clients),
            "path_proof": bool(allow_non_comparable),
            "non_comparable": bool(allow_non_comparable),
            "protocol_capacities": {
                proto: {"rtt_capacity_msgs_per_s": value}
                for proto, value in common.items()
            },
        }
        (aa_dir / f"{client}-load.json").write_text(json.dumps(aa_profile, indent=2) + "\n")
    payload = {
        "valid_rtt_capacities": all_caps,
        "path_proof": bool(allow_non_comparable),
        "c_common": common,
        "aa_load_dir": str(aa_dir),
    }
    return payload


def _pair_common_capacities(all_caps: dict, clients: list[str] | tuple[str, ...]) -> dict:
    """C_common is min of the pair. A/A of the faster client must use this, not its own ceiling."""
    common = {}
    for proto in REQUIRED_PROTOCOLS:
        values = []
        for client in clients:
            block = (all_caps[client].get("protocols") or {}).get(proto) or {}
            value = block.get("capacity_msgs_per_s")
            if value is None:
                raise ValueError(f"{client}:{proto}:missing_c_common_capacity")
            values.append(float(value))
        common[proto] = min(values)
    return common


# Official A/A: 25 % MQTTv311 (ARM-observed non-neutrality) and 75 % MQTTv311
# (representative load). expand_scenario crosses fraction then protocol.
OFFICIAL_AA_VARIANT_INDEXES = (0, 4)
OFFICIAL_AA_BLOCKS = 6
AA_CONTROL_MAX_ABS_EFFECT_PCT = 3.0
MIN_AA_BLOCKS = 4
MIN_AA_REPLICATIONS_PER_DESIGN = 2


def _canonical_flag(value) -> str:
    if value is True or value == 1:
        return "1"
    if value is False or value == 0:
        return "0"
    return str(value).strip()


def parse_aa_variant_indexes(raw) -> list:
    """Parse ``0,4`` / ``[0, 4]`` into ints. Empty or unparsable → []."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        values = list(raw)
    else:
        values = [part for part in str(raw).replace(" ", "").split(",") if part]
    indexes = []
    for item in values:
        try:
            indexes.append(int(item))
        except (TypeError, ValueError):
            return []
    return indexes


def validate_official_pairwise_policy(
    *,
    profile: str,
    run_aa="1",
    aa_control_enforce="1",
    aa_blocks=OFFICIAL_AA_BLOCKS,
    aa_variant_indexes=None,
) -> dict:
    """Refuse a weakened official campaign. Non-standard profiles always pass.

    Insufficient official values error out; they are never silently raised
    to the legal minimum. Smoke / targeted may keep ``AA_BLOCKS=4``,
    ``RUN_ABBA=0`` and ``RUN_LOAD_MATRIX=0``.
    """
    official = str(profile).strip() == "standard"
    payload = {
        "ok": True,
        "official": official,
        "violations": [],
    }
    if not official:
        return payload
    run_flag = _canonical_flag(run_aa)
    enforce_flag = _canonical_flag(aa_control_enforce)
    if run_flag != "1":
        payload["violations"].append(f"official_policy_violation:RUN_AA={run_flag}")
    if enforce_flag != "1":
        payload["violations"].append(
            f"official_policy_violation:AA_CONTROL_ENFORCE={enforce_flag}"
        )
    raw_blocks = aa_blocks
    try:
        n_blocks = int(str(aa_blocks).strip())
    except (TypeError, ValueError):
        n_blocks = None
        payload["violations"].append(
            f"official_policy_violation:AA_BLOCKS={raw_blocks}"
        )
    if n_blocks is not None:
        if n_blocks % 2 != 0:
            payload["violations"].append(
                f"official_policy_violation:AA_BLOCKS_odd={n_blocks}"
            )
        if n_blocks < OFFICIAL_AA_BLOCKS:
            payload["violations"].append(
                f"official_policy_violation:AA_BLOCKS={n_blocks}"
            )
        per_design = n_blocks // 2 if n_blocks >= 0 else 0
        if per_design < MIN_AA_REPLICATIONS_PER_DESIGN:
            payload["violations"].append(
                "official_policy_violation:insufficient_replication_per_design:"
                f"AA_BLOCKS={n_blocks}"
            )
    indexes = parse_aa_variant_indexes(aa_variant_indexes)
    if aa_variant_indexes not in (None, "") and not indexes:
        payload["violations"].append(
            f"official_policy_violation:AA_VARIANT_INDEXES={aa_variant_indexes}"
        )
    required = set(OFFICIAL_AA_VARIANT_INDEXES)
    present = set(indexes)
    for missing in sorted(required - present):
        payload["violations"].append(
            f"official_policy_violation:missing_AA_variant:{missing}"
        )
    payload["ok"] = not payload["violations"]
    return payload


def continue_to_ab_ranking(aa_gate_passed: bool) -> bool:
    """A/B ranking runs only after a successful A/A gate."""
    return bool(aa_gate_passed)


def ab_compare_outputs(root: str | Path) -> list:
    """Compare documents that are not same-client A/A controls."""
    root = Path(root)
    names = []
    for path in sorted(root.rglob("compare-*.json")):
        if path.name.startswith("compare-aa-"):
            continue
        names.append(str(path.relative_to(root)))
    return names


def _design_counts(designs: list) -> dict:
    counts = {"ABBA": 0, "BAAB": 0}
    for design in designs:
        key = str(design) if design else ""
        if key in counts:
            counts[key] += 1
    return counts


def evaluate_aa_control(
    doc: dict,
    *,
    max_abs_effect_pct: float = AA_CONTROL_MAX_ABS_EFFECT_PCT,
    min_blocks: int = MIN_AA_BLOCKS,
    min_replications_per_design: int = MIN_AA_REPLICATIONS_PER_DESIGN,
) -> dict:
    """Fail-closed gate for a same-client compare document.

    Pass requires every requested block to be complete, both designs
    present with at least ``min_replications_per_design`` each, a real
    CI that contains ratio 1, and an absolute effect at most
    ``max_abs_effect_pct``. A missing CI is a failure, never a silent
    ``excludes_zero_effect=false``.
    """
    baseline = doc.get("baseline_client")
    candidate = doc.get("candidate_client")
    points = list(doc.get("points") or [])
    variant = None
    if points:
        point = points[0].get("point") or {}
        variant = {
            "shared_load_fraction": point.get("shared_load_fraction"),
            "protocol": point.get("protocol"),
            "target_rate": point.get("target_rate"),
        }
    first = (points[0].get("verdict") or {}) if points else {}
    order = list(doc.get("order") or (points[0].get("order") if points else []) or [])
    requested = doc.get("blocks_requested")
    if requested is None and order:
        requested = len(order) // 4
    complete = int(first.get("n_blocks") or 0) if points else 0
    designs = list(first.get("block_designs") or [])
    counts = _design_counts(designs)
    ci_low = first.get("ci_low") if points else None
    ci_high = first.get("ci_high") if points else None
    explicit_ci = first.get("ci_available") if points else None
    if explicit_ci is False:
        ci_available = False
    else:
        ci_available = ci_low is not None and ci_high is not None
    payload = {
        "aa_control_pass": False,
        "aa_control_reason": None,
        "aa_absolute_effect_pct": first.get("absolute_effect_pct") if points else None,
        "aa_ci": {
            "ci_low": ci_low,
            "ci_high": ci_high,
            "excludes_zero_effect": first.get("excludes_zero_effect"),
            "median_ratio": first.get("median_ratio"),
            "ci_available": ci_available,
        }
        if points
        else None,
        "aa_variant": variant,
        "aa_n_blocks": complete,
        "aa_designs": designs,
        "aa_blocks_requested": requested,
        "aa_blocks_complete": complete,
        "aa_design_counts": counts,
        "aa_ci_available": ci_available,
    }
    if baseline != candidate:
        payload["aa_control_pass"] = None
        payload["aa_control_reason"] = "not_same_client"
        return payload
    if not points:
        payload["aa_control_reason"] = "no_compare_points"
        return payload
    reasons = []
    for index, block in enumerate(points):
        verdict = block.get("verdict") or {}
        n_blocks = int(verdict.get("n_blocks") or 0)
        effect = verdict.get("absolute_effect_pct")
        point_designs = list(verdict.get("block_designs") or [])
        point_counts = _design_counts(point_designs)
        point_requested = requested
        point_order = list(block.get("order") or order)
        if point_requested is None and point_order:
            point_requested = len(point_order) // 4
        point_ci_low = verdict.get("ci_low")
        point_ci_high = verdict.get("ci_high")
        point_ci_available = verdict.get("ci_available")
        if point_ci_available is None:
            point_ci_available = point_ci_low is not None and point_ci_high is not None
        if point_ci_available is False or point_ci_low is None or point_ci_high is None:
            point_ci_available = False
        if point_requested is not None and n_blocks != int(point_requested):
            reasons.append(
                f"point{index}:incomplete_blocks:{n_blocks}/{point_requested}"
            )
        if n_blocks < min_blocks:
            reasons.append(f"point{index}:insufficient_blocks:{n_blocks}/{min_blocks}")
        if point_counts["ABBA"] < 1 or point_counts["BAAB"] < 1:
            reasons.append(
                f"point{index}:missing_design:ABBA={point_counts['ABBA']},BAAB={point_counts['BAAB']}"
            )
        if (
            point_counts["ABBA"] < min_replications_per_design
            or point_counts["BAAB"] < min_replications_per_design
        ):
            reasons.append(
                f"point{index}:insufficient_replication:"
                f"ABBA={point_counts['ABBA']},BAAB={point_counts['BAAB']}"
            )
        if not point_ci_available:
            reasons.append(f"point{index}:ci_unavailable")
        else:
            if not (float(point_ci_low) <= 0.0 <= float(point_ci_high)):
                reasons.append(f"point{index}:ci_excludes_zero")
        if effect is None:
            reasons.append(f"point{index}:missing_effect")
        elif abs(float(effect)) > max_abs_effect_pct:
            reasons.append(
                f"point{index}:abs_effect_{float(effect):.4f}_exceeds_{max_abs_effect_pct}"
            )
    if reasons:
        payload["aa_control_pass"] = False
        payload["aa_control_reason"] = ";".join(reasons)
        return payload
    payload["aa_control_pass"] = True
    payload["aa_control_reason"] = "neutral"
    return payload


def attach_aa_control(doc: dict) -> dict:
    """Persist A/A gate fields on a compare document (no-op for A/B)."""
    control = evaluate_aa_control(doc)
    doc.update(control)
    return doc


def enforce_aa_controls(
    root: str | Path,
    *,
    enforce: bool = True,
    require_files: bool = False,
) -> dict:
    """Scan ``compare-aa-*.json`` under ``root`` and optionally fail closed."""
    root = Path(root)
    reports = []
    failures = []
    files = sorted(root.rglob("compare-aa-*.json"))
    for path in files:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            reports.append({"file": str(path), "aa_control_pass": False, "aa_control_reason": str(exc)})
            failures.append(f"{path}:unreadable")
            continue
        control = evaluate_aa_control(doc)
        reports.append({"file": str(path), **control})
        if control.get("aa_control_pass") is not True:
            failures.append(
                f"{path.name}:{control.get('aa_control_reason') or 'aa_control_failed'}"
            )
    if require_files and not files:
        failures.append("missing_aa_compare_documents")
    payload = {
        "ok": not failures,
        "n_files": len(files),
        "failures": failures,
        "reports": reports,
        "enforced": bool(enforce),
    }
    if enforce and failures:
        raise ValueError("A/A control failed: " + "; ".join(failures))
    return payload


def maybe_run_ab_ranking_after_aa(
    root: str | Path,
    *,
    enforce: bool,
    rank,
) -> dict:
    """Call ``rank`` only when the A/A gate passed.

    A failing enforced control raises before ``rank``. A failed or missing
    gate never produces a new A/B compare.
    """
    payload = enforce_aa_controls(root, enforce=enforce, require_files=enforce)
    if not continue_to_ab_ranking(payload.get("ok") is True):
        return {
            "ranked": False,
            "aa": payload,
            "ab_outputs": ab_compare_outputs(root),
        }
    rank()
    return {
        "ranked": True,
        "aa": payload,
        "ab_outputs": ab_compare_outputs(root),
    }

