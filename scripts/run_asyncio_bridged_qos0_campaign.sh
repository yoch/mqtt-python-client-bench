#!/usr/bin/env bash
# Re-bench all asyncio_bridged clients after QoS0 schedule_call adapter work.
# Clients: gmqtt, aiomqtt, amqtt, zmqtt, mqttium (+ aiomqtt3 via .venv-aiomqtt3).
# Usage: bash scripts/run_asyncio_bridged_qos0_campaign.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MAIN_VENV="${MAIN_VENV:-$ROOT/.venv}"
AIOMQTT3_VENV="${AIOMQTT3_VENV:-$ROOT/.venv-aiomqtt3}"

# shellcheck disable=SC1091
source "$MAIN_VENV/bin/activate"
export PYTHONPATH=src

SCENARIOS=(
  pub_payload_sweep_qos0
  pub_qos_sweep_telemetry
  pub_qos1_inflight
  remaining_length_boundaries
  sub_exact_telemetry
  sub_hierarchy_telemetry
  sub_callback_matching
  duplex_gateway
  burst_recovery
  e2e_integrity
  rtt_capacity_qos1
  puback_latency_qos1
  application_rtt_qos1
)

# Main venv bridged clients (aiomqtt3 needs its own env).
CLIENTS=(gmqtt aiomqtt amqtt zmqtt mqttium)

mkdir -p calibrations results logs
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="results/_archive_asyncio_bridged_${STAMP}"
mkdir -p "$ARCHIVE"
shopt -s nullglob
for client in "${CLIENTS[@]}" aiomqtt3; do
  for f in results/"${client}"-*.json; do
    mv -v "$f" "$ARCHIVE/" || true
  done
done
shopt -u nullglob

python -m mqtt_client_bench.run broker up

run_client() {
  local client="$1"
  local py="$2"
  local load="calibrations/${client}-load.json"
  echo "=== calibrate ${client} ($(date -Is)) ==="
  "$py" -m mqtt_client_bench.run calibrate --client "$client" --profile standard \
    --output "$load" | tee "logs/calibrate-${client}.log"
  for s in "${SCENARIOS[@]}"; do
    echo "==> ${client} ${s} $(date -Is)"
    "$py" -m mqtt_client_bench.run run \
      --scenario "$s" \
      --client "$client" \
      --profile standard \
      --runs 3 \
      --load-profile "$load" \
      --output "results/${client}-${s}.json" \
      >"logs/${client}-${s}.log" 2>&1
  done
}

for client in "${CLIENTS[@]}"; do
  run_client "$client" python
done

if [[ -x "$AIOMQTT3_VENV/bin/python" ]]; then
  # shellcheck disable=SC1091
  source "$AIOMQTT3_VENV/bin/activate"
  export PYTHONPATH=src
  # Ensure bench package is importable from aiomqtt3 env (editable or path).
  run_client aiomqtt3 "$AIOMQTT3_VENV/bin/python"
  # shellcheck disable=SC1091
  source "$MAIN_VENV/bin/activate"
  export PYTHONPATH=src
else
  echo "SKIP aiomqtt3: missing $AIOMQTT3_VENV"
fi

python -m mqtt_client_bench.run report build --input results --output site
echo "ASYNC_BRIDGED_CAMPAIGN_DONE $(date -Is)"
