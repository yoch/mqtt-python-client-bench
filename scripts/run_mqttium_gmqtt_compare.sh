#!/usr/bin/env bash
# Official same-host interleaved mqttium vs gmqtt comparison.
#
# Stability:
#   - `run matrix` interleaves clients *within each point* (5 runs, rotated)
#   - `run compare` ABBA (6 blocks = 12A+12B) on contested scenarios, with
#     per-client calibration so load_fraction is not a shared ceiling
#   - gmqtt is A (established peer); mqttium is B (candidate)
#
# Usage:
#   bash scripts/run_mqttium_gmqtt_compare.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc13}"
MATRIX_RUNS="${MATRIX_RUNS:-5}"
ABBA_BLOCKS="${ABBA_BLOCKS:-6}"

HOST_DIR="${RESULTS_DIR:-$(python - <<'PYEOF'
import sys
sys.path.insert(0, "src")
from mqtt_client_bench.hostcal import resolve_host_profile, results_dir_for
print(results_dir_for(resolve_host_profile()))
PYEOF
)}"
OUT="${HOST_DIR}/mqttium-gmqtt"
mkdir -p "$OUT" calibrations logs
echo "writing to $OUT (matrix ${MATRIX_RUNS} runs, ABBA ${ABBA_BLOCKS} blocks)"

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
    --output "calibrations/${client}-load.json" | tee "logs/calibrate-${client}-compare.log"
done

MATRIX_SCENARIOS=(
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

for s in "${MATRIX_SCENARIOS[@]}"; do
  echo "==> matrix ${s} mqttium,gmqtt runs=${MATRIX_RUNS} $(date -Is)"
  python -m mqtt_client_bench.run matrix \
    --clients mqttium,gmqtt \
    --scenario "$s" \
    --profile standard \
    --runs "$MATRIX_RUNS" \
    --load-profile-dir calibrations \
    --output-dir "$OUT" \
    >"logs/matrix-mqttium-gmqtt-${s}.log" 2>&1 || echo "FAILED matrix ${s}" | tee -a logs/mqttium-gmqtt-compare.log
done

# ABBA: do not pass a shared --load-profile. compare_clients calibrates each
# client so latency fractions stay on that client's own capacity.
ABBA_SCENARIOS=(
  pub_qos_sweep_telemetry
  pub_payload_sweep_qos0
  rtt_capacity_qos1
  application_rtt_qos1
  puback_latency_qos1
)

for s in "${ABBA_SCENARIOS[@]}"; do
  echo "==> ABBA gmqtt,mqttium ${s} blocks=${ABBA_BLOCKS} $(date -Is)"
  python -m mqtt_client_bench.run compare \
    --clients gmqtt,mqttium \
    --scenario "$s" \
    --profile standard \
    --blocks "$ABBA_BLOCKS" \
    --output "${OUT}/compare-gmqtt-mqttium-${s}.json" \
    >"logs/abba-gmqtt-mqttium-${s}.log" 2>&1 || echo "FAILED ABBA ${s}" | tee -a logs/mqttium-gmqtt-compare.log
done

python scripts/summarize_mqttium_gmqtt.py "$OUT" | tee "${OUT}/summary.json"
echo "COMPARE_DONE $(date -Is)"
