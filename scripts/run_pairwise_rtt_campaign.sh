#!/usr/bin/env bash
# Official pairwise application-RTT campaign.
#
# Two independent grids. Paho never sizes the asyncio peer comparison.
#
#   A. mqttium ↔ gmqtt   C_common_async = min(C_mqttium, C_gmqtt)
#   B. mqttium ↔ paho    C_common_paho  = min(C_mqttium, C_paho)
#      Paho is sync_reference / inter-I/O-model only.
#
# RTT roles take the native asyncio path when the library exposes one.
# Previous three-way ARM results (Paho-sized C_common, bridged RTT) are
# historical / asyncio_bridged / not evidence of a native asyncio ranking.
#
# Same-client A/A is a fail-closed publication gate, not a decorative verdict.
# Capacity, then A/A, then enforce. Ranking ABBA A/B runs only after the
# control passes. Completeness, practical bias, and pair-unit stability
# fail closed. A bootstrap CI with two pair units is not an equivalence
# test. Default A/A variants are 25 % and 75 %
# MQTTv311 (indexes 0,4). Targeted ARM validation uses AA_BLOCKS=4.
#
# PROFILE=standard refuses weakened overrides (RUN_AA=0, AA_CONTROL_ENFORCE=0,
# AA_BLOCKS<6 or odd, missing variant 0 or 4) before any measurement. Smoke
# and targeted paths may keep AA_BLOCKS=4 and skip ranking.
#
# Usage:
#   bash scripts/run_pairwise_rtt_campaign.sh
#   PROFILE=smoke bash scripts/run_pairwise_rtt_campaign.sh   # functional only
#   bash scripts/run_targeted_aa_validation.sh                  # A/A recheck
#   BENCH_BROKER=127.0.0.1:11883 BENCH_BROKER_PID=$pid \\
#     bash scripts/run_pairwise_rtt_campaign.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc13}"
GMQTT_VER="${GMQTT_VER:-0.7.0}"
PAHO_VER="${PAHO_VER:-2.1.0}"
PROFILE="${PROFILE:-standard}"
MATRIX_RUNS="${MATRIX_RUNS:-5}"
ABBA_BLOCKS="${ABBA_BLOCKS:-6}"
AA_BLOCKS="${AA_BLOCKS:-6}"
RUN_ABBA="${RUN_ABBA:-1}"
RUN_AA="${RUN_AA:-1}"
RUN_LOAD_MATRIX="${RUN_LOAD_MATRIX:-1}"
RUN_ASYNCIO_PAIR="${RUN_ASYNCIO_PAIR:-1}"
RUN_SYNC_REFERENCE_PAIR="${RUN_SYNC_REFERENCE_PAIR:-1}"
# Empty = every expanded point. Smoke may set e.g. 0,1 (25% v311 and 25% v5).
ABBA_VARIANT_INDEXES="${ABBA_VARIANT_INDEXES:-}"
# expand order: fraction then protocol → 0 = 25% MQTTv311, 4 = 75% MQTTv311.
# AA_VARIANT_INDEX remains a one-point override when AA_VARIANT_INDEXES is unset.
AA_VARIANT_INDEXES="${AA_VARIANT_INDEXES:-${AA_VARIANT_INDEX:-0,4}}"
if [ -z "${AA_CONTROL_ENFORCE:-}" ]; then
  if [ "$PROFILE" = "standard" ]; then
    AA_CONTROL_ENFORCE=1
  else
    AA_CONTROL_ENFORCE=0
  fi
fi

if [ "${CLIENTS:-}" = "mqttium,gmqtt,paho" ] || [ "${CLIENTS:-}" = "mqttium,paho,gmqtt" ]; then
  echo "three-way CLIENTS= is refused: Paho must not size the asyncio grid" >&2
  exit 2
fi

python - "$PROFILE" "$RUN_AA" "$AA_CONTROL_ENFORCE" "$AA_BLOCKS" "$AA_VARIANT_INDEXES" <<'PY'
import json
import sys

from mqtt_client_bench.pairwise import validate_official_pairwise_policy

payload = validate_official_pairwise_policy(
    profile=sys.argv[1],
    run_aa=sys.argv[2],
    aa_control_enforce=sys.argv[3],
    aa_blocks=sys.argv[4],
    aa_variant_indexes=sys.argv[5],
)
if not payload["ok"]:
    raise SystemExit(";".join(payload["violations"]))
print(json.dumps(payload))
PY

echo "=== pin exact measured versions ==="
pip install --force-reinstall --no-cache-dir \
  "mqttium==${MQTTIUM_VER}" \
  "gmqtt==${GMQTT_VER}" \
  "paho-mqtt==${PAHO_VER}"
python - <<'PY'
from importlib.metadata import version
assert version("mqttium") == "1.0.0rc13", version("mqttium")
assert version("gmqtt") == "0.7.0", version("gmqtt")
assert version("paho-mqtt") == "2.1.0", version("paho-mqtt")
print("mqttium", version("mqttium"))
print("gmqtt", version("gmqtt"))
print("paho-mqtt", version("paho-mqtt"))
PY

HOST_DIR="${RESULTS_DIR:-$(python - <<'PY'
from mqtt_client_bench.hostcal import resolve_host_profile, results_dir_for
print(results_dir_for(resolve_host_profile()))
PY
)}"
OUT="${HOST_DIR}/pairwise-rtt-native"
CAL_ROOT="${CAL_DIR:-$OUT/calibrations}"
LOG_DIR="${LOG_DIR:-$OUT/logs}"
ARCHIVE_DIR="${ARCHIVE_DIR:-$OUT/archive}"
mkdir -p "$OUT" "$CAL_ROOT" "$LOG_DIR"

BROKER_ARGS=()
if [ -n "${BENCH_BROKER:-}" ]; then
  BROKER_ARGS=(--broker "$BENCH_BROKER")
  if [ -n "${BENCH_BROKER_PID:-}" ]; then
    BROKER_ARGS+=(--broker-pid "$BENCH_BROKER_PID")
  elif [ "$PROFILE" = "standard" ]; then
    echo "standard campaign with BENCH_BROKER requires BENCH_BROKER_PID so broker CPU/headroom can be verified" >&2
    exit 2
  fi
else
  python -m mqtt_client_bench.run broker up
fi

run_capacity() {
  local pair="$1"
  local label="$2"
  local pair_dir="$OUT/$label"
  local cal_dir="$CAL_ROOT/$label"
  mkdir -p "$pair_dir" "$cal_dir"

  echo "==> [$label] interleaved RTT capacity ($pair) runs=${MATRIX_RUNS}"
  python -m mqtt_client_bench.run matrix \
    --clients "$pair" \
    --scenario rtt_capacity_qos1 \
    --profile "$PROFILE" \
    --runs "$MATRIX_RUNS" \
    "${BROKER_ARGS[@]}" \
    --output-dir "$pair_dir" \
    >"$LOG_DIR/matrix-${label}-rtt_capacity_qos1.log" 2>&1

  python - "$pair_dir" "$cal_dir" "$pair" "$MATRIX_RUNS" "$PROFILE" <<'PY'
import json
import sys
from pathlib import Path

from mqtt_client_bench.pairwise import write_official_rtt_calibrations

out = Path(sys.argv[1])
cal = Path(sys.argv[2])
clients = tuple(sys.argv[3].split(","))
required = int(sys.argv[4])
allow_non_comparable = sys.argv[5] == "smoke"
try:
    payload = write_official_rtt_calibrations(
        out,
        cal,
        clients,
        required_valid=required,
        allow_non_comparable=allow_non_comparable,
    )
except ValueError as exc:
    raise SystemExit(str(exc)) from exc
print(json.dumps(payload, indent=2, default=str))
PY
}

run_aa() {
  local pair="$1"
  local label="$2"
  local pair_dir="$OUT/$label"
  local cal_dir="$CAL_ROOT/$label"
  local a_client="${pair%%,*}"
  local b_client="${pair#*,}"
  IFS=',' read -ra aa_idxs <<< "$AA_VARIANT_INDEXES"
  for idx in "${aa_idxs[@]}"; do
    idx="${idx// /}"
    [ -n "$idx" ] || continue
    echo "==> [$label] A/A ${a_client} variant=${idx} blocks=${AA_BLOCKS}"
    python -m mqtt_client_bench.run compare \
      --clients "${a_client},${a_client}" \
      --scenario application_rtt_fixed_rate \
      --profile "$PROFILE" \
      --blocks "$AA_BLOCKS" \
      --variant-index "$idx" \
      "${BROKER_ARGS[@]}" \
      --load-profile-dir "$cal_dir/aa" \
      --output "${pair_dir}/compare-aa-${a_client}-application_rtt_fixed_rate-v${idx}.json" \
      >"$LOG_DIR/aa-${label}-${a_client}-v${idx}.log" 2>&1
    if [ "$a_client" != "$b_client" ]; then
      echo "==> [$label] A/A ${b_client} variant=${idx} blocks=${AA_BLOCKS}"
      python -m mqtt_client_bench.run compare \
        --clients "${b_client},${b_client}" \
        --scenario application_rtt_fixed_rate \
        --profile "$PROFILE" \
        --blocks "$AA_BLOCKS" \
        --variant-index "$idx" \
        "${BROKER_ARGS[@]}" \
        --load-profile-dir "$cal_dir/aa" \
        --output "${pair_dir}/compare-aa-${b_client}-application_rtt_fixed_rate-v${idx}.json" \
        >"$LOG_DIR/aa-${label}-${b_client}-v${idx}.log" 2>&1
    fi
  done
}

run_ranking() {
  local pair="$1"
  local label="$2"
  local pair_dir="$OUT/$label"
  local cal_dir="$CAL_ROOT/$label"

  echo "==> [$label] matched-load application RTT ($pair)"
  if [ "$RUN_LOAD_MATRIX" = "1" ]; then
    python -m mqtt_client_bench.run matrix \
      --clients "$pair" \
      --scenario application_rtt_fixed_rate \
      --profile "$PROFILE" \
      --runs "$MATRIX_RUNS" \
      "${BROKER_ARGS[@]}" \
      --load-profile-dir "$cal_dir" \
      --output-dir "$pair_dir" \
      >"$LOG_DIR/matrix-${label}-application_rtt_fixed_rate.log" 2>&1
  else
    echo "==> [$label] skipping matched-load matrix (RUN_LOAD_MATRIX=0)"
  fi

  if [ "$RUN_ABBA" != "1" ]; then
    echo "==> [$label] skipping A/B ABBA ranking (RUN_ABBA=0)"
    return 0
  fi
  echo "==> [$label] ABBA $pair application_rtt_fixed_rate blocks=${ABBA_BLOCKS}"
  if [ -n "$ABBA_VARIANT_INDEXES" ]; then
    IFS=',' read -ra idxs <<< "$ABBA_VARIANT_INDEXES"
    for idx in "${idxs[@]}"; do
      python -m mqtt_client_bench.run compare \
        --clients "$pair" \
        --scenario application_rtt_fixed_rate \
        --profile "$PROFILE" \
        --blocks "$ABBA_BLOCKS" \
        --variant-index "$idx" \
        "${BROKER_ARGS[@]}" \
        --load-profile-dir "$cal_dir" \
        --output "${pair_dir}/compare-${label}-application_rtt_fixed_rate-v${idx}.json" \
        >"$LOG_DIR/abba-${label}-application_rtt_fixed_rate-v${idx}.log" 2>&1
    done
  else
    python -m mqtt_client_bench.run compare \
      --clients "$pair" \
      --scenario application_rtt_fixed_rate \
      --profile "$PROFILE" \
      --blocks "$ABBA_BLOCKS" \
      "${BROKER_ARGS[@]}" \
      --load-profile-dir "$cal_dir" \
      --output "${pair_dir}/compare-${label}-application_rtt_fixed_rate.json" \
      >"$LOG_DIR/abba-${label}-application_rtt_fixed_rate.log" 2>&1
  fi
}

persist_evidence() {
  python -m mqtt_client_bench.run results archive --input "$OUT/asyncio" --archive "$ARCHIVE_DIR/asyncio" || true
  python -m mqtt_client_bench.run results archive --input "$OUT/sync_reference" --archive "$ARCHIVE_DIR/sync_reference" || true
  python scripts/persist_pairwise_evidence.py "$OUT" --output "$OUT/pairwise-run-table.json"
}

enforce_aa() {
  python - "$OUT" "$AA_CONTROL_ENFORCE" "$RUN_AA" "$PROFILE" "$AA_BLOCKS" "$AA_VARIANT_INDEXES" <<'PY'
import json
import sys
from pathlib import Path

from mqtt_client_bench.pairwise import (
    enforce_aa_controls,
    validate_official_pairwise_policy,
)

root = Path(sys.argv[1])
enforce = sys.argv[2] == "1"
run_aa = sys.argv[3] == "1"
policy = validate_official_pairwise_policy(
    profile=sys.argv[4],
    run_aa=sys.argv[3],
    aa_control_enforce=sys.argv[2],
    aa_blocks=sys.argv[5],
    aa_variant_indexes=sys.argv[6],
)
if not policy["ok"]:
    raise SystemExit(";".join(policy["violations"]))
if not run_aa:
    raise SystemExit(0)
try:
    payload = enforce_aa_controls(root, enforce=enforce, require_files=enforce)
except ValueError as exc:
    raise SystemExit(str(exc)) from exc
print(json.dumps(payload, indent=2, default=str))
PY
}

[ "$RUN_ASYNCIO_PAIR" = "1" ] && run_capacity "mqttium,gmqtt" "asyncio"
[ "$RUN_SYNC_REFERENCE_PAIR" = "1" ] && run_capacity "mqttium,paho" "sync_reference"

if [ "$RUN_AA" = "1" ]; then
  [ "$RUN_ASYNCIO_PAIR" = "1" ] && run_aa "mqttium,gmqtt" "asyncio"
  [ "$RUN_SYNC_REFERENCE_PAIR" = "1" ] && run_aa "mqttium,paho" "sync_reference"
fi

persist_evidence
enforce_aa
echo "AA_GATE_PASSED profile=${PROFILE} aa_indexes=${AA_VARIANT_INDEXES} aa_blocks=${AA_BLOCKS}"

python - <<'PY'
from mqtt_client_bench.pairwise import continue_to_ab_ranking

# Reached only if enforce_aa exited 0 (set -e). Standard ranking is forbidden
# unless that A/A gate passed.
if not continue_to_ab_ranking(True):
    raise SystemExit("ab_ranking_refused_after_aa")
PY

if [ "$RUN_ABBA" = "1" ] || [ "$RUN_LOAD_MATRIX" = "1" ]; then
  [ "$RUN_ASYNCIO_PAIR" = "1" ] && run_ranking "mqttium,gmqtt" "asyncio"
  [ "$RUN_SYNC_REFERENCE_PAIR" = "1" ] && run_ranking "mqttium,paho" "sync_reference"
  persist_evidence
fi

echo "PAIRWISE_NATIVE_RTT_DONE profile=${PROFILE} out=${OUT} aa_indexes=${AA_VARIANT_INDEXES} aa_enforce=${AA_CONTROL_ENFORCE}"
