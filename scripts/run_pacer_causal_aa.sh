#!/usr/bin/env bash
# Causal A/A: in_loop vs external-process pacer on the same frozen C_common.
#
# Does NOT produce an official ranking. PROFILE is smoke / NON_COMPARABLE.
# One interleaved RTT-capacity matrix sizes C_common once. Both pacer modes
# then reuse that directory so target rates cannot drift between arms.
#
# Counterbalance (not all in_loop then all external):
#   mqttium 25%  in_loop → external
#   gmqtt   25%  external → in_loop
#   mqttium 75%  in_loop → external
#   gmqtt   75%  external → in_loop
#
# AA_CONTROL_ENFORCE=0 so both arms complete; compare still records the 3%
# bias+stability gate. Token loss / sequence gap invalidates a run.
#
# Usage on the RPi5 runner (isolated native broker already up):
#   PROFILE=smoke MATRIX_RUNS=1 AA_CONTROL_ENFORCE=0 \
#     bash scripts/run_pacer_causal_aa.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc13}"
GMQTT_VER="${GMQTT_VER:-0.7.0}"
PAHO_VER="${PAHO_VER:-2.1.0}"
PROFILE="${PROFILE:-smoke}"
MATRIX_RUNS="${MATRIX_RUNS:-1}"
AA_BLOCKS="${AA_BLOCKS:-4}"
AA_CONTROL_ENFORCE="${AA_CONTROL_ENFORCE:-0}"

if [ "$PROFILE" = "standard" ]; then
  echo "run_pacer_causal_aa.sh refuses PROFILE=standard (NO OFFICIAL RANKING)" >&2
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
OUT="${HOST_DIR}/pacer-causal-aa"
CAL_ROOT="${CAL_DIR:-$OUT/calibrations}"
LOG_DIR="${LOG_DIR:-$OUT/logs}"
TRACE_DIR="${TRACE_DIR:-$OUT/temporal-traces}"
mkdir -p "$OUT" "$CAL_ROOT" "$LOG_DIR" "$TRACE_DIR"

BROKER_ARGS=()
if [ -n "${BENCH_BROKER:-}" ]; then
  BROKER_ARGS=(--broker "$BENCH_BROKER")
  if [ -n "${BENCH_BROKER_PID:-}" ]; then
    BROKER_ARGS+=(--broker-pid "$BENCH_BROKER_PID")
  fi
else
  python -m mqtt_client_bench.run broker up
fi

PAIR="mqttium,gmqtt"
LABEL="asyncio"
PAIR_DIR="$OUT/$LABEL"
CAL_DIR="$CAL_ROOT/$LABEL"
mkdir -p "$PAIR_DIR" "$CAL_DIR"

echo "==> [pacer-causal] interleaved RTT capacity ($PAIR) once — freeze C_common"
python -m mqtt_client_bench.run matrix \
  --clients "$PAIR" \
  --scenario rtt_capacity_qos1 \
  --profile "$PROFILE" \
  --runs "$MATRIX_RUNS" \
  "${BROKER_ARGS[@]}" \
  --output-dir "$PAIR_DIR" \
  >"$LOG_DIR/matrix-${LABEL}-rtt_capacity_qos1.log" 2>&1

python - "$PAIR_DIR" "$CAL_DIR" "$PAIR" "$MATRIX_RUNS" "$PROFILE" <<'PY'
import json
import sys
from pathlib import Path

from mqtt_client_bench.pairwise import write_official_rtt_calibrations

out = Path(sys.argv[1])
cal = Path(sys.argv[2])
clients = tuple(sys.argv[3].split(","))
required = int(sys.argv[4])
allow_non_comparable = sys.argv[5] == "smoke"
payload = write_official_rtt_calibrations(
    out,
    cal,
    clients,
    required_valid=required,
    allow_non_comparable=allow_non_comparable,
)
print(json.dumps(payload, indent=2, default=str))
PY

AA_DIR="$CAL_DIR/aa"
FROZEN="$OUT/frozen_target_rates.json"
echo "==> [pacer-causal] C_common frozen at $AA_DIR"

# cell: client, variant index (0=25% MQTTv311, 4=75% MQTTv311), first pacer, second pacer
run_cell() {
  local client="$1"
  local idx="$2"
  local first="$3"
  local second="$4"
  local mode
  for mode in "$first" "$second"; do
    echo "==> [pacer-causal] A/A ${client} variant=${idx} pacer=${mode} blocks=${AA_BLOCKS}"
    python -m mqtt_client_bench.run compare \
      --clients "${client},${client}" \
      --scenario application_rtt_fixed_rate \
      --profile "$PROFILE" \
      --blocks "$AA_BLOCKS" \
      --variant-index "$idx" \
      --pacer-mode "$mode" \
      "${BROKER_ARGS[@]}" \
      --load-profile-dir "$AA_DIR" \
      --output "${PAIR_DIR}/compare-aa-${client}-application_rtt_fixed_rate-v${idx}-${mode}.json" \
      >"$LOG_DIR/aa-${client}-v${idx}-${mode}.log" 2>&1
  done
}

run_cell mqttium 0 in_loop external
run_cell gmqtt 0 external in_loop
run_cell mqttium 4 in_loop external
run_cell gmqtt 4 external in_loop

python - "$PAIR_DIR" "$FROZEN" "$AA_DIR" <<'PY'
import json
import sys
from pathlib import Path

pair = Path(sys.argv[1])
rows = []
rates = set()
for path in sorted(pair.glob("compare-aa-*.json")):
    doc = json.loads(path.read_text())
    point = (doc.get("points") or [{}])[0].get("point") or {}
    row = {
        "file": path.name,
        "pacer_mode": doc.get("pacer_mode") or point.get("pacer_mode"),
        "client": doc.get("baseline_client"),
        "target_rate": point.get("target_rate"),
        "C_common": point.get("shared_capacity_msgs_per_s"),
        "shared_load_fraction": point.get("shared_load_fraction"),
        "protocol": point.get("protocol"),
        "aa_control_pass": doc.get("aa_control_pass"),
        "aa_bias_pct": doc.get("aa_bias_pct"),
        "aa_stability_pct": doc.get("aa_stability_pct"),
    }
    rows.append(row)
    if row["target_rate"] is not None and row["shared_load_fraction"] is not None:
        rates.add((row["shared_load_fraction"], row["protocol"], round(float(row["target_rate"]), 6)))

# Same fraction×protocol must share one absolute rate across pacer modes and clients.
by_cell = {}
for row in rows:
    key = (row["shared_load_fraction"], row["protocol"])
    by_cell.setdefault(key, set()).add(row["target_rate"])
mismatched = {str(k): sorted(v) for k, v in by_cell.items() if len(v) > 1}
payload = {
    "rows": rows,
    "mismatched_target_rates": mismatched,
    "ok": not mismatched,
}
Path(sys.argv[2]).write_text(json.dumps(payload, indent=2) + "\n")
print(json.dumps(payload, indent=2))
if mismatched:
    raise SystemExit("pacer causal arms saw different target rates; C_common was not frozen")
PY

python -m mqtt_client_bench.temporal_trace --input "$PAIR_DIR" --output "$TRACE_DIR"
echo "PACER_CAUSAL_AA_DONE profile=${PROFILE} out=${OUT} traces=${TRACE_DIR} enforce=${AA_CONTROL_ENFORCE}"
