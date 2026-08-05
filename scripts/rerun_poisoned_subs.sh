#!/usr/bin/env bash
# Re-run client×scenario pairs whose existing JSON contain valid runs that the
# new fail-closed $SYS-drop rules would invalidate.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

mkdir -p logs results calibrations
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="results/_archive_poisoned_subs_${STAMP}"
mkdir -p "$ARCHIVE"

PAIRS=(
  "aiomqtt:sub_exact_telemetry"
  "aiomqtt:sub_hierarchy_telemetry"
  "amqtt:sub_exact_telemetry"
  "amqtt:sub_hierarchy_telemetry"
  "awscrt:sub_exact_telemetry"
  "gmqtt:sub_exact_telemetry"
  "gmqtt:sub_hierarchy_telemetry"
  "mqttium:sub_exact_telemetry"
  "paho:sub_exact_telemetry"
  "zmqtt:sub_exact_telemetry"
  "zmqtt:sub_hierarchy_telemetry"
)

echo "=== loadavg $(cat /proc/loadavg) ==="
python -m mqtt_client_bench.run broker up

for pair in "${PAIRS[@]}"; do
  client="${pair%%:*}"
  scenario="${pair##*:}"
  src="results/${client}-${scenario}.json"
  if [[ -f "$src" ]]; then
    mv -v "$src" "$ARCHIVE/"
  fi
done

for pair in "${PAIRS[@]}"; do
  client="${pair%%:*}"
  scenario="${pair##*:}"
  load="calibrations/${client}-load.json"
  if [[ ! -f "$load" ]]; then
    echo "MISSING load profile $load" >&2
    exit 1
  fi
  echo "==> ${client} ${scenario} $(date -Is)"
  python -m mqtt_client_bench.run run \
    --scenario "$scenario" \
    --client "$client" \
    --profile standard \
    --runs 3 \
    --load-profile "$load" \
    --output "results/${client}-${scenario}.json" \
    >"logs/${client}-${scenario}.rerun.log" 2>&1
done

python -m mqtt_client_bench.run report build --input results --output site
echo "POISON_SUB_RERUN_DONE $(date -Is)"
