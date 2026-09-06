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
# Usage:
#   bash scripts/run_pairwise_rtt_campaign.sh
#   PROFILE=smoke bash scripts/run_pairwise_rtt_campaign.sh   # functional only
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
# Empty = every expanded point. Smoke may set e.g. 0,1 (25% v311 and 25% v5).
ABBA_VARIANT_INDEXES="${ABBA_VARIANT_INDEXES:-}"
# expand order: fraction then protocol → index 4 is 75% MQTTv311.
AA_VARIANT_INDEX="${AA_VARIANT_INDEX:-4}"

if [ "${CLIENTS:-}" = "mqttium,gmqtt,paho" ] || [ "${CLIENTS:-}" = "mqttium,paho,gmqtt" ]; then
  echo "three-way CLIENTS= is refused: Paho must not size the asyncio grid" >&2
  exit 2
fi

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

run_pair() {
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

  echo "==> [$label] matched-load application RTT ($pair)"
  python -m mqtt_client_bench.run matrix \
    --clients "$pair" \
    --scenario application_rtt_fixed_rate \
    --profile "$PROFILE" \
    --runs "$MATRIX_RUNS" \
    "${BROKER_ARGS[@]}" \
    --load-profile-dir "$cal_dir" \
    --output-dir "$pair_dir" \
    >"$LOG_DIR/matrix-${label}-application_rtt_fixed_rate.log" 2>&1

  if [ "$RUN_ABBA" = "1" ]; then
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
  fi

  if [ "$RUN_AA" = "1" ]; then
    local a_client="${pair%%,*}"
    local b_client="${pair#*,}"
    echo "==> [$label] A/A ${a_client} variant=${AA_VARIANT_INDEX}"
    python -m mqtt_client_bench.run compare \
      --clients "${a_client},${a_client}" \
      --scenario application_rtt_fixed_rate \
      --profile "$PROFILE" \
      --blocks "$AA_BLOCKS" \
      --variant-index "$AA_VARIANT_INDEX" \
      "${BROKER_ARGS[@]}" \
      --load-profile-dir "$cal_dir/aa" \
      --output "${pair_dir}/compare-aa-${a_client}-application_rtt_fixed_rate-v${AA_VARIANT_INDEX}.json" \
      >"$LOG_DIR/aa-${label}-${a_client}-v${AA_VARIANT_INDEX}.log" 2>&1
    echo "==> [$label] A/A ${b_client} variant=${AA_VARIANT_INDEX}"
    python -m mqtt_client_bench.run compare \
      --clients "${b_client},${b_client}" \
      --scenario application_rtt_fixed_rate \
      --profile "$PROFILE" \
      --blocks "$AA_BLOCKS" \
      --variant-index "$AA_VARIANT_INDEX" \
      "${BROKER_ARGS[@]}" \
      --load-profile-dir "$cal_dir/aa" \
      --output "${pair_dir}/compare-aa-${b_client}-application_rtt_fixed_rate-v${AA_VARIANT_INDEX}.json" \
      >"$LOG_DIR/aa-${label}-${b_client}-v${AA_VARIANT_INDEX}.log" 2>&1
  fi
}

run_pair "mqttium,gmqtt" "asyncio"
run_pair "mqttium,paho" "sync_reference"

python -m mqtt_client_bench.run results archive --input "$OUT/asyncio" --archive "$ARCHIVE_DIR/asyncio" || true
python -m mqtt_client_bench.run results archive --input "$OUT/sync_reference" --archive "$ARCHIVE_DIR/sync_reference" || true
python scripts/persist_pairwise_evidence.py "$OUT" --output "$OUT/pairwise-run-table.json"

echo "PAIRWISE_NATIVE_RTT_DONE profile=${PROFILE} out=${OUT}"
