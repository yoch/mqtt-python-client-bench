#!/usr/bin/env bash
# Official same-host interleaved mqttium vs gmqtt comparison.
#
# Stability:
#   - `run matrix` interleaves clients *within each point* (5 runs, rotated)
#   - `run compare` ABBA (6 blocks = 12A+12B) on contested scenarios, with
#     per-client calibration so load_fraction is not a shared ceiling
#   - gmqtt is A (established peer); mqttium is B (candidate)
#
# mqttium source:
#   MQTTIUM_VER=1.0.0rc14          PyPI pin when no git/checkout is set
#   MQTTIUM_CLIENT_PATH=...        Use this --target install via --client-path
#   MQTTIUM_GIT_REF=branch         Clone yoch/mqttium@ref into a --target tree
#   MQTTIUM_GIT_SHA=commit         Checkout exact commit (overrides branch tip)
#   MQTTIUM_RUN_LABEL=name         Write under $RESULTS_DIR/name/mqttium-gmqtt
#                                  (git installs default to mqttium-git)
#   SKIP_ABBA=1                    Matrix + calibrate only (no run compare)
#   MATRIX_RUNS / ABBA_BLOCKS      Defaults 5 and 6

#
# Usage:
#   bash scripts/run_mqttium_gmqtt_compare.sh
#   MQTTIUM_GIT_SHA=<sha> MQTTIUM_RUN_LABEL=mqttium-pr460 \
#     bash scripts/run_mqttium_gmqtt_compare.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc14}"
MQTTIUM_GIT_REF="${MQTTIUM_GIT_REF:-}"
MQTTIUM_GIT_SHA="${MQTTIUM_GIT_SHA:-}"
MQTTIUM_CLIENT_PATH="${MQTTIUM_CLIENT_PATH:-}"
MATRIX_RUNS="${MATRIX_RUNS:-5}"
ABBA_BLOCKS="${ABBA_BLOCKS:-6}"
SKIP_ABBA="${SKIP_ABBA:-0}"

if [[ -n "${MQTTIUM_GIT_REF}" || -n "${MQTTIUM_GIT_SHA}" ]]; then
  MQTTIUM_RUN_LABEL="${MQTTIUM_RUN_LABEL:-mqttium-git}"
else
  MQTTIUM_RUN_LABEL="${MQTTIUM_RUN_LABEL:-}"
fi

HOST_DIR="${RESULTS_DIR:-$(python - <<'PYEOF'
import sys
sys.path.insert(0, "src")
from mqtt_client_bench.hostcal import resolve_host_profile, results_dir_for
print(results_dir_for(resolve_host_profile()))
PYEOF
)}"
if [[ -n "${MQTTIUM_RUN_LABEL}" ]]; then
  OUT="${HOST_DIR}/${MQTTIUM_RUN_LABEL}/mqttium-gmqtt"
  CAL_DIR="calibrations/${MQTTIUM_RUN_LABEL}"
else
  OUT="${HOST_DIR}/mqttium-gmqtt"
  CAL_DIR="calibrations"
fi
mkdir -p "$OUT" "$CAL_DIR" logs
if [[ "$SKIP_ABBA" == "1" ]]; then
  echo "writing to $OUT (matrix ${MATRIX_RUNS} runs, ABBA skipped, calib ${CAL_DIR})"
else
  echo "writing to $OUT (matrix ${MATRIX_RUNS} runs, ABBA ${ABBA_BLOCKS} blocks, calib ${CAL_DIR})"
fi

CLIENT_PATH_ARGS=()

if [[ -n "${MQTTIUM_GIT_REF}" || -n "${MQTTIUM_GIT_SHA}" ]]; then
  INSTALL_ROOT="${MQTTIUM_CLIENT_PATH:-$ROOT/.mqttium-${MQTTIUM_RUN_LABEL}}"
  SRC_DIR="${INSTALL_ROOT}-src"
  MQTTIUM_CLIENT_PATH="$INSTALL_ROOT"
  if [[ -n "${MQTTIUM_GIT_SHA}" ]]; then
    echo "=== clone mqttium + checkout ${MQTTIUM_GIT_SHA} -> ${MQTTIUM_CLIENT_PATH} ==="
    rm -rf "$SRC_DIR" "$MQTTIUM_CLIENT_PATH"
    git clone --filter=blob:none https://github.com/yoch/mqttium.git "$SRC_DIR"
    git -C "$SRC_DIR" checkout --quiet "${MQTTIUM_GIT_SHA}"
  else
    echo "=== clone mqttium@${MQTTIUM_GIT_REF} -> ${SRC_DIR} ==="
    rm -rf "$SRC_DIR" "$MQTTIUM_CLIENT_PATH"
    git clone --depth 1 --branch "$MQTTIUM_GIT_REF" https://github.com/yoch/mqttium.git "$SRC_DIR"
  fi
  pip install --no-cache-dir --force-reinstall --target "$MQTTIUM_CLIENT_PATH" "$SRC_DIR"
  MQTTIUM_RESOLVED_SHA="$(git -C "$SRC_DIR" rev-parse HEAD)"
  echo "mqttium source ${MQTTIUM_RESOLVED_SHA}"
  printf '%s\n' "$MQTTIUM_RESOLVED_SHA" >"${OUT}/MQTTIUM_GIT_SHA"
elif [[ -n "$MQTTIUM_CLIENT_PATH" && -f "$MQTTIUM_CLIENT_PATH/pyproject.toml" ]]; then
  echo "=== install mqttium from ${MQTTIUM_CLIENT_PATH} (--target) ==="
  TARGET="${MQTTIUM_CLIENT_PATH}-installed"
  pip install --no-cache-dir --force-reinstall --target "$TARGET" "$MQTTIUM_CLIENT_PATH"
  MQTTIUM_CLIENT_PATH="$TARGET"
fi

if [[ -n "$MQTTIUM_CLIENT_PATH" ]]; then
  CLIENT_PATH_ARGS=(--client-path "mqttium=${MQTTIUM_CLIENT_PATH}")
  python - <<PY
import sys
sys.path.insert(0, "${MQTTIUM_CLIENT_PATH}")
import mqttium
from pathlib import Path
from mqttium.api import AsyncClient
path = Path(mqttium.__file__).resolve()
assert str(path).startswith(str(Path("${MQTTIUM_CLIENT_PATH}").resolve())), path
assert hasattr(AsyncClient, "publish_nowait"), "publish_nowait required"
print("OK", getattr(mqttium, "__version__", "?"), path)
import gmqtt
print("OK gmqtt", getattr(gmqtt, "__version__", "?"))
PY
else
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
fi

python -m mqtt_client_bench.run broker up

for client in mqttium gmqtt; do
  echo "=== calibrate ${client} ==="
  extra=()
  if [[ "$client" == "mqttium" && -n "$MQTTIUM_CLIENT_PATH" ]]; then
    extra=(--client-path "$MQTTIUM_CLIENT_PATH")
  fi
  python -m mqtt_client_bench.run calibrate --client "$client" --profile standard \
    "${extra[@]}" \
    --output "${CAL_DIR}/${client}-load.json" | tee "logs/calibrate-${client}-compare.log"
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
  puback_latency_fixed_rate
  application_rtt_fixed_rate
)

for s in "${MATRIX_SCENARIOS[@]}"; do
  echo "==> matrix ${s} mqttium,gmqtt runs=${MATRIX_RUNS} $(date -Is)"
  python -m mqtt_client_bench.run matrix \
    --clients mqttium,gmqtt \
    --scenario "$s" \
    --profile standard \
    --runs "$MATRIX_RUNS" \
    --load-profile-dir "$CAL_DIR" \
    "${CLIENT_PATH_ARGS[@]}" \
    --output-dir "$OUT" \
    >"logs/matrix-mqttium-gmqtt-${s}.log" 2>&1 || echo "FAILED matrix ${s}" | tee -a logs/mqttium-gmqtt-compare.log
done

# ABBA: do not pass a shared --load-profile. Matched-load latency calibrates
# each client, then offers C_common = min(capacities) × shared_load_fraction.
# Per-client load_fraction scenarios are refused by the harness.
ABBA_SCENARIOS=(
  pub_qos_sweep_telemetry
  pub_payload_sweep_qos0
  rtt_capacity_qos1
  application_rtt_fixed_rate
  puback_latency_fixed_rate
)

if [[ "$SKIP_ABBA" == "1" ]]; then
  echo "SKIP_ABBA=1: not running run compare"
else
  for s in "${ABBA_SCENARIOS[@]}"; do
    echo "==> ABBA gmqtt,mqttium ${s} blocks=${ABBA_BLOCKS} $(date -Is)"
    python -m mqtt_client_bench.run compare \
      --clients gmqtt,mqttium \
      --scenario "$s" \
      --profile standard \
      --blocks "$ABBA_BLOCKS" \
      --load-profile-dir "$CAL_DIR" \
      "${CLIENT_PATH_ARGS[@]}" \
      --output "${OUT}/compare-gmqtt-mqttium-${s}.json" \
      >"logs/abba-gmqtt-mqttium-${s}.log" 2>&1 || echo "FAILED ABBA ${s}" | tee -a logs/mqttium-gmqtt-compare.log
  done
fi

python scripts/summarize_mqttium_gmqtt.py "$OUT" | tee "${OUT}/summary.json"
echo "COMPARE_DONE $(date -Is)"
