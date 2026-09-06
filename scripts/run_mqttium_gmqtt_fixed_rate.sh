#!/usr/bin/env bash
# Matched-load latency campaign: mqttium vs gmqtt at the same absolute rates.
#
# Default is the short remake: application_rtt_fixed_rate matrix only, 3
# interleaved runs, reuse existing calibrations, no ABBA / A/A.
# The overnight shape is opt-in: FULL=1.
#
# Usage:
#   bash scripts/run_mqttium_gmqtt_fixed_rate.sh
#   FULL=1 bash scripts/run_mqttium_gmqtt_fixed_rate.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc13}"
FULL="${FULL:-0}"
RECALIBRATE="${RECALIBRATE:-0}"
if [ "$FULL" = "1" ]; then
  MATRIX_RUNS="${MATRIX_RUNS:-5}"
  RUN_ABBA="${RUN_ABBA:-1}"
  RUN_AA="${RUN_AA:-1}"
  MATRIX_SCENARIOS_DEFAULT="application_rtt_fixed_rate puback_latency_fixed_rate"
else
  MATRIX_RUNS="${MATRIX_RUNS:-3}"
  RUN_ABBA="${RUN_ABBA:-0}"
  RUN_AA="${RUN_AA:-0}"
  MATRIX_SCENARIOS_DEFAULT="application_rtt_fixed_rate"
fi
ABBA_BLOCKS="${ABBA_BLOCKS:-6}"
AA_BLOCKS="${AA_BLOCKS:-4}"

HOST_DIR="${RESULTS_DIR:-$(python - <<'PYEOF'
import sys
sys.path.insert(0, "src")
from mqtt_client_bench.hostcal import resolve_host_profile, results_dir_for
print(results_dir_for(resolve_host_profile()))
PYEOF
)}"
OUT="${HOST_DIR}/mqttium-gmqtt"
mkdir -p "$OUT" calibrations logs
echo "writing to $OUT (matrix ${MATRIX_RUNS} runs, FULL=${FULL}, ABBA=${RUN_ABBA}, AA=${RUN_AA})"

echo "=== pin mqttium==${MQTTIUM_VER} ==="
pip install --force-reinstall --no-cache-dir "mqttium==${MQTTIUM_VER}"
python - <<'PY'
from importlib.metadata import version
import mqttium
from mqttium.api import AsyncClient
from pathlib import Path
assert "site-packages" in str(Path(mqttium.__file__).resolve())
assert hasattr(AsyncClient, "publish_nowait")
print("OK mqttium", version("mqttium"), mqttium.__file__)
import gmqtt
print("OK gmqtt", getattr(gmqtt, "__version__", "?"))
PY

python -m mqtt_client_bench.run broker up

for client in mqttium gmqtt; do
  cal="calibrations/${client}-load.json"
  if [ "$RECALIBRATE" = "1" ] || [ ! -f "$cal" ]; then
    echo "=== calibrate ${client} ==="
    python -m mqtt_client_bench.run calibrate --client "$client" --profile standard \
      --output "$cal" | tee "logs/calibrate-${client}-matched.log"
  else
    echo "=== reuse ${cal} ==="
  fi
done

# shellcheck disable=SC2206
MATRIX_SCENARIOS=(${MATRIX_SCENARIOS:-$MATRIX_SCENARIOS_DEFAULT})

for s in "${MATRIX_SCENARIOS[@]}"; do
  echo "==> matrix ${s} mqttium,gmqtt runs=${MATRIX_RUNS} $(date -Is)"
  python -m mqtt_client_bench.run matrix \
    --clients mqttium,gmqtt \
    --scenario "$s" \
    --profile standard \
    --runs "$MATRIX_RUNS" \
    --load-profile-dir calibrations \
    --output-dir "$OUT" \
    >"logs/matrix-mqttium-gmqtt-${s}.log" 2>&1 || echo "FAILED matrix ${s}" | tee -a logs/mqttium-gmqtt-fixed-rate.log
done

if [ "$RUN_ABBA" = "1" ]; then
  for s in "${MATRIX_SCENARIOS[@]}"; do
    echo "==> ABBA gmqtt,mqttium ${s} blocks=${ABBA_BLOCKS} $(date -Is)"
    python -m mqtt_client_bench.run compare \
      --clients gmqtt,mqttium \
      --scenario "$s" \
      --profile standard \
      --blocks "$ABBA_BLOCKS" \
      --load-profile-dir calibrations \
      --output "${OUT}/compare-gmqtt-mqttium-${s}.json" \
      >"logs/abba-gmqtt-mqttium-${s}.log" 2>&1 || echo "FAILED ABBA ${s}" | tee -a logs/mqttium-gmqtt-fixed-rate.log
  done
fi

if [ "$RUN_AA" = "1" ]; then
  # A/A at 75 % of C_common, MQTTv311.
  # application_rtt_fixed_rate expand order: fraction then protocol
  #   4 = 0.75 MQTTv311
  # puback_latency_fixed_rate expand order:
  #   6 = 10000 MQTTv311
  echo "==> A/A mqttium application_rtt_fixed_rate 75pct/v311 $(date -Is)"
  python -m mqtt_client_bench.run compare \
    --clients mqttium,mqttium \
    --scenario application_rtt_fixed_rate \
    --profile standard \
    --blocks "$AA_BLOCKS" \
    --variant-index 4 \
    --load-profile-dir calibrations \
    --output "${OUT}/compare-aa-mqttium-application_rtt_fixed_rate-75-v311.json" \
    >"logs/aa-mqttium-application_rtt_fixed_rate.log" 2>&1 || echo "FAILED A/A mqttium RTT" | tee -a logs/mqttium-gmqtt-fixed-rate.log

  echo "==> A/A gmqtt application_rtt_fixed_rate 75pct/v311 $(date -Is)"
  python -m mqtt_client_bench.run compare \
    --clients gmqtt,gmqtt \
    --scenario application_rtt_fixed_rate \
    --profile standard \
    --blocks "$AA_BLOCKS" \
    --variant-index 4 \
    --load-profile-dir calibrations \
    --output "${OUT}/compare-aa-gmqtt-application_rtt_fixed_rate-75-v311.json" \
    >"logs/aa-gmqtt-application_rtt_fixed_rate.log" 2>&1 || echo "FAILED A/A gmqtt RTT" | tee -a logs/mqttium-gmqtt-fixed-rate.log

  echo "==> A/A mqttium puback_latency_fixed_rate 10k/v311 $(date -Is)"
  python -m mqtt_client_bench.run compare \
    --clients mqttium,mqttium \
    --scenario puback_latency_fixed_rate \
    --profile standard \
    --blocks "$AA_BLOCKS" \
    --variant-index 6 \
    --load-profile-dir calibrations \
    --output "${OUT}/compare-aa-mqttium-puback_latency_fixed_rate-10k-v311.json" \
    >"logs/aa-mqttium-puback_latency_fixed_rate.log" 2>&1 || echo "FAILED A/A mqttium PUBACK" | tee -a logs/mqttium-gmqtt-fixed-rate.log

  echo "==> A/A gmqtt puback_latency_fixed_rate 10k/v311 $(date -Is)"
  python -m mqtt_client_bench.run compare \
    --clients gmqtt,gmqtt \
    --scenario puback_latency_fixed_rate \
    --profile standard \
    --blocks "$AA_BLOCKS" \
    --variant-index 6 \
    --load-profile-dir calibrations \
    --output "${OUT}/compare-aa-gmqtt-puback_latency_fixed_rate-10k-v311.json" \
    >"logs/aa-gmqtt-puback_latency_fixed_rate.log" 2>&1 || echo "FAILED A/A gmqtt PUBACK" | tee -a logs/mqttium-gmqtt-fixed-rate.log
fi

python scripts/summarize_mqttium_gmqtt.py "$OUT" | tee "${OUT}/summary.json"
echo "MATCHED_LOAD_DONE $(date -Is)"
