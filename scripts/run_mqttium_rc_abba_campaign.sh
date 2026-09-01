#!/usr/bin/env bash
# Targeted rc10↔rc11 ABBA diagnostic campaign (same host, interleaved).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

BLOCKS="${MQTTIUM_ABBA_BLOCKS:-5}"
PROFILE="${MQTTIUM_ABBA_PROFILE:-standard}"
HOST_PROFILE="${MQTTIUM_HOST_PROFILE:-hosts/cursor-ee5b909b92c756ad.json}"
OUTDIR="${MQTTIUM_ABBA_OUTDIR:-results/mqttium-rc-ab}"
LOGDIR="${MQTTIUM_ABBA_LOGDIR:-logs/mqttium-rc-ab}"
mkdir -p "$OUTDIR" "$LOGDIR"

run_one() {
  local scenario="$1"
  shift
  local out="$OUTDIR/${scenario}.json"
  local log="$LOGDIR/${scenario}.log"
  echo "==> $scenario $(date -Is)" | tee -a "$LOGDIR/campaign.log"
  python scripts/mqttium_rc_abba.py \
    --scenario "$scenario" \
    --blocks "$BLOCKS" \
    --profile "$PROFILE" \
    --host-profile "$HOST_PROFILE" \
    --output "$out" \
    "$@" \
    >"$log" 2>&1
  echo "done $scenario" | tee -a "$LOGDIR/campaign.log"
}

run_one puback_latency_fixed_rate
run_one rtt_capacity_qos1
run_one pub_qos_sweep_telemetry
run_one pub_qos1_inflight
run_one pub_payload_sweep_qos0 --variant-index 2

echo "MQTTIUM_RC_ABBA_DONE $(date -Is)" | tee -a "$LOGDIR/campaign.log"
