#!/usr/bin/env bash
# Prepare + run mqttium (and optionally mqttium-compat) against a pinned PyPI release.
# Usage: bash scripts/run_mqttium_campaign.sh
#   MQTTIUM_ONLY=1          — skip mqttium-compat (default)
#   MQTTIUM_COMPAT=1        — also run mqttium-compat
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

mkdir -p calibrations logs
# Archive prior mqttium* JSON in this results dir so medians are not mixed.
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="${RESULTS_DIR}/_archive_mqttium_${STAMP}"
mkdir -p "$ARCHIVE"
shopt -s nullglob
# Two loops, not one glob list: `mqttium-*.json` also matches
# `mqttium-compat-*.json`, and with `set -e` the second copy of a name
# that the first glob already moved aborts the campaign.
for f in "${RESULTS_DIR}"/mqttium-compat-*.json; do
  mv -v "$f" "$ARCHIVE/"
done
for f in "${RESULTS_DIR}"/mqttium-*.json; do
  mv -v "$f" "$ARCHIVE/"
done
shopt -u nullglob

# --- load-bearing steps below (calibrate + scenarios). Run only when ready. ---
if [[ "${MQTTIUM_PREP_ONLY:-0}" == "1" ]]; then
  echo "PREP_ONLY: install+archive done. Skipping broker/calibrate/run/report."
  exit 0
fi

python -m mqtt_client_bench.run broker up

echo "=== calibrate native ==="
python -m mqtt_client_bench.run calibrate --client mqttium --profile standard \
  --output calibrations/mqttium-load.json | tee logs/calibrate-mqttium.log

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
  load="calibrations/${client}-load.json"
  for s in "${SCENARIOS[@]}"; do
    echo "==> ${client} ${s} $(date -Is)"
    python -m mqtt_client_bench.run run \
      --scenario "$s" \
      --client "$client" \
      --profile standard \
      --runs 3 \
      --load-profile "$load" \
      --output "${RESULTS_DIR}/${client}-${s}.json" \
      >"logs/${client}-${s}.log" 2>&1
  done
done

python -m mqtt_client_bench.run report build --input "$RESULTS_DIR" --output site-mqttium-rc12
echo "CAMPAIGN_DONE $(date -Is)"
