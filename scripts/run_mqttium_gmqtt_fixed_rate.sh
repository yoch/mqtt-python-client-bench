#!/usr/bin/env bash
# Matched-load latency campaign: mqttium vs gmqtt at the same absolute rates.
#
# application_rtt_fixed_rate uses shared_load_fraction × C_common =
# min(client RTT capacities). puback_latency_fixed_rate uses catalogue
# target_rate values. Per-client load_fraction scenarios are refused.
#
# Usage:
#   bash scripts/run_mqttium_gmqtt_fixed_rate.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc13}"
MATRIX_RUNS="${MATRIX_RUNS:-5}"
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
echo "writing to $OUT (matrix ${MATRIX_RUNS} runs, ABBA ${ABBA_BLOCKS} blocks, A/A ${AA_BLOCKS} blocks)"

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
  echo "=== calibrate ${client} ==="
  python -m mqtt_client_bench.run calibrate --client "$client" --profile standard \
    --output "calibrations/${client}-load.json" | tee "logs/calibrate-${client}-matched.log"
done

MATRIX_SCENARIOS=(
  application_rtt_fixed_rate
  puback_latency_fixed_rate
)

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

for s in "${MATRIX_SCENARIOS[@]}"; do
  echo "==> ABBA gmqtt,mqttium ${s} blocks=${ABBA_BLOCKS} $(date -Is)"
  python -m mqtt_client_bench.run compare \
    --clients gmqtt,mqttium \
    --scenario "$s" \
    --profile standard \
    --blocks "$ABBA_BLOCKS" \
    --output "${OUT}/compare-gmqtt-mqttium-${s}.json" \
    >"logs/abba-gmqtt-mqttium-${s}.log" 2>&1 || echo "FAILED ABBA ${s}" | tee -a logs/mqttium-gmqtt-fixed-rate.log
done

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
  --output "${OUT}/compare-aa-mqttium-application_rtt_fixed_rate-75-v311.json" \
  >"logs/aa-mqttium-application_rtt_fixed_rate.log" 2>&1 || echo "FAILED A/A mqttium RTT" | tee -a logs/mqttium-gmqtt-fixed-rate.log

echo "==> A/A gmqtt application_rtt_fixed_rate 75pct/v311 $(date -Is)"
python -m mqtt_client_bench.run compare \
  --clients gmqtt,gmqtt \
  --scenario application_rtt_fixed_rate \
  --profile standard \
  --blocks "$AA_BLOCKS" \
  --variant-index 4 \
  --output "${OUT}/compare-aa-gmqtt-application_rtt_fixed_rate-75-v311.json" \
  >"logs/aa-gmqtt-application_rtt_fixed_rate.log" 2>&1 || echo "FAILED A/A gmqtt RTT" | tee -a logs/mqttium-gmqtt-fixed-rate.log

echo "==> A/A mqttium puback_latency_fixed_rate 10k/v311 $(date -Is)"
python -m mqtt_client_bench.run compare \
  --clients mqttium,mqttium \
  --scenario puback_latency_fixed_rate \
  --profile standard \
  --blocks "$AA_BLOCKS" \
  --variant-index 6 \
  --output "${OUT}/compare-aa-mqttium-puback_latency_fixed_rate-10k-v311.json" \
  >"logs/aa-mqttium-puback_latency_fixed_rate.log" 2>&1 || echo "FAILED A/A mqttium PUBACK" | tee -a logs/mqttium-gmqtt-fixed-rate.log

echo "==> A/A gmqtt puback_latency_fixed_rate 10k/v311 $(date -Is)"
python -m mqtt_client_bench.run compare \
  --clients gmqtt,gmqtt \
  --scenario puback_latency_fixed_rate \
  --profile standard \
  --blocks "$AA_BLOCKS" \
  --variant-index 6 \
  --output "${OUT}/compare-aa-gmqtt-puback_latency_fixed_rate-10k-v311.json" \
  >"logs/aa-gmqtt-puback_latency_fixed_rate.log" 2>&1 || echo "FAILED A/A gmqtt PUBACK" | tee -a logs/mqttium-gmqtt-fixed-rate.log

python scripts/summarize_mqttium_gmqtt.py "$OUT" | tee "${OUT}/summary.json"
echo "MATCHED_LOAD_DONE $(date -Is)"
