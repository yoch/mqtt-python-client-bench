#!/usr/bin/env bash
# Prepare + run mqttium (and optionally mqttium-compat) against PyPI or a checkout.
#
# Usage:
#   bash scripts/run_mqttium_campaign.sh
#
# Environment:
#   MQTTIUM_VER=1.0.0rc12          PyPI pin when MQTTIUM_CLIENT_PATH is unset
#   MQTTIUM_CLIENT_PATH=...        Use this checkout via --client-path (skip PyPI)
#   MQTTIUM_GIT_REF=branch         Clone yoch/mqttium@ref into MQTTIUM_CLIENT_PATH
#   MQTTIUM_GIT_SHA=commit         Checkout exact commit (overrides branch tip)
#   MQTTIUM_RUN_LABEL=name         Write under \$RESULTS_DIR/name/ (paired A/B runs)
#   MQTTIUM_SKIP_ARCHIVE=1         Do not move prior mqttium-*.json
#   Baseline reuse:
#   - post-#422 (d520edf) ≡ PR #422 campaign → post422-baseline/
#   - post-#423 (1d14bfca) ≡ PR #423 campaign → pr423/
#   MQTTIUM_PREP_ONLY=1            Install/archive only
#
# Does NOT run automatically — invoke explicitly when ready.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

RESULTS_DIR="${RESULTS_DIR:-$(python - <<'PYEOF'
import sys
sys.path.insert(0, "src")
from mqtt_client_bench.hostcal import resolve_host_profile, results_dir_for
print(results_dir_for(resolve_host_profile()))
PYEOF
)}"
mkdir -p "$RESULTS_DIR"
if [[ "$RESULTS_DIR" != "results" ]]; then
  echo "note: this host is not the published reference; writing to $RESULTS_DIR"
fi
if [[ -n "${MQTTIUM_RUN_LABEL:-}" ]]; then
  RESULTS_DIR="${RESULTS_DIR}/${MQTTIUM_RUN_LABEL}"
  mkdir -p "$RESULTS_DIR"
  echo "note: run label ${MQTTIUM_RUN_LABEL} -> ${RESULTS_DIR}"
fi

CLIENT_PATH_ARGS=()
MQTTIUM_CLIENT_PATH="${MQTTIUM_CLIENT_PATH:-}"

if [[ -n "${MQTTIUM_GIT_REF:-}" || -n "${MQTTIUM_GIT_SHA:-}" ]]; then
  INSTALL_ROOT="${MQTTIUM_CLIENT_PATH:-$ROOT/.mqttium-${MQTTIUM_RUN_LABEL:-git}}"
  SRC_DIR="${INSTALL_ROOT}-src"
  MQTTIUM_CLIENT_PATH="$INSTALL_ROOT"
  if [[ -n "${MQTTIUM_GIT_SHA:-}" ]]; then
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
  echo "mqttium source $(git -C "$SRC_DIR" rev-parse --short HEAD)"
elif [[ -n "$MQTTIUM_CLIENT_PATH" && -f "$MQTTIUM_CLIENT_PATH/pyproject.toml" ]]; then
  echo "=== install mqttium from ${MQTTIUM_CLIENT_PATH} (--target) ==="
  TARGET="${MQTTIUM_CLIENT_PATH}-installed"
  pip install --no-cache-dir --force-reinstall --target "$TARGET" "$MQTTIUM_CLIENT_PATH"
  MQTTIUM_CLIENT_PATH="$TARGET"
fi

if [[ -n "$MQTTIUM_CLIENT_PATH" ]]; then
  CLIENT_PATH_ARGS=(--client-path "$MQTTIUM_CLIENT_PATH")
  python - <<PY
import sys
sys.path.insert(0, "${MQTTIUM_CLIENT_PATH}")
import mqttium
from pathlib import Path
from mqttium.api import AsyncClient
path = Path(mqttium.__file__).resolve()
assert str(path).startswith(str(Path("${MQTTIUM_CLIENT_PATH}").resolve())), path
assert hasattr(AsyncClient, "publish_nowait"), "publish_nowait required"
print("OK", mqttium.__version__, path)
PY
else
  MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc12}"
  echo "=== ensure mqttium==${MQTTIUM_VER} (site-packages, no editable) ==="
  pip install --force-reinstall --no-cache-dir "mqttium==${MQTTIUM_VER}"
  python - <<'PY'
from importlib.metadata import version
import mqttium
from pathlib import Path
from mqttium.api import AsyncClient
v = version("mqttium")
path = Path(mqttium.__file__).resolve()
assert "site-packages" in str(path), path
assert hasattr(AsyncClient, "publish_nowait"), "publish_nowait required"
print("OK", v, path)
PY
fi

mkdir -p calibrations logs
if [[ "${MQTTIUM_SKIP_ARCHIVE:-0}" != "1" ]]; then
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="${RESULTS_DIR}/_archive_mqttium_${STAMP}"
mkdir -p "$ARCHIVE"
shopt -s nullglob
for f in "${RESULTS_DIR}"/mqttium-compat-*.json; do
  mv -v "$f" "$ARCHIVE/"
done
for f in "${RESULTS_DIR}"/mqttium-*.json; do
  mv -v "$f" "$ARCHIVE/"
done
shopt -u nullglob
fi

if [[ "${MQTTIUM_PREP_ONLY:-0}" == "1" ]]; then
  echo "PREP_ONLY: install+archive done. Skipping broker/calibrate/run/report."
  exit 0
fi

python -m mqtt_client_bench.run broker up

CAL_LOAD="calibrations/mqttium-load.json"
if [[ -n "${MQTTIUM_RUN_LABEL:-}" ]]; then
  CAL_LOAD="calibrations/${MQTTIUM_RUN_LABEL}-mqttium-load.json"
fi

echo "=== calibrate native ==="
python -m mqtt_client_bench.run calibrate --client mqttium --profile standard \
  "${CLIENT_PATH_ARGS[@]}" \
  --output "$CAL_LOAD" | tee "logs/calibrate-mqttium-${MQTTIUM_RUN_LABEL:-default}.log"

if [[ "${MQTTIUM_COMPAT:-0}" == "1" ]]; then
  echo "=== calibrate compat ==="
  python -m mqtt_client_bench.run calibrate --client mqttium-compat --profile standard \
    --output calibrations/mqttium-compat-load.json | tee logs/calibrate-mqttium-compat.log
fi

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

CLIENTS=(mqttium)
if [[ "${MQTTIUM_COMPAT:-0}" == "1" ]]; then
  CLIENTS+=(mqttium-compat)
fi

for client in "${CLIENTS[@]}"; do
  load="$CAL_LOAD"
  path_args=()
  if [[ "$client" == "mqttium" && -n "$MQTTIUM_CLIENT_PATH" ]]; then
    path_args=(--client-path "$MQTTIUM_CLIENT_PATH")
  fi
  for s in "${SCENARIOS[@]}"; do
    echo "==> ${client} ${s} $(date -Is)"
    python -m mqtt_client_bench.run run \
      --scenario "$s" \
      --client "$client" \
      --profile standard \
      --runs 3 \
      --load-profile "$load" \
      "${path_args[@]}" \
      --output "${RESULTS_DIR}/${client}-${s}.json" \
      >"logs/${client}-${s}.log" 2>&1
  done
done

SITE_OUT="${SITE_OUT:-site-mqttium}"
python -m mqtt_client_bench.run report build --input "$RESULTS_DIR" --output "$SITE_OUT"
echo "CAMPAIGN_DONE $(date -Is)"
