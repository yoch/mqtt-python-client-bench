#!/usr/bin/env bash
# Focused three-way performance campaign for a stable ARM64 runner.
#
# Primary comparison: mqttium 1.0.0rc13, gmqtt 0.7.0 and Paho 2.1.0.
# Paho is a synchronous contextual reference, not an asyncio peer.
#
# Fairness rules:
# - capacity scenarios give every client the same workload and measure its ceiling;
# - latency scenarios give every client the exact same absolute target_rate;
# - application_rtt_fixed_rate derives one C_common=min(RTT capacities) across
#   ALL THREE clients, then resolves one shared target_rate grid;
# - this runner campaign deliberately refuses the observed-lower-bound fallback:
#   every client/protocol needs a fresh official RTT capacity or the campaign
#   stops before the matched-load matrix.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

MQTTIUM_VER="${MQTTIUM_VER:-1.0.0rc13}"
GMQTT_VER="${GMQTT_VER:-0.7.0}"
PAHO_VER="${PAHO_VER:-2.1.0}"
MATRIX_RUNS="${MATRIX_RUNS:-5}"
CLIENTS=(mqttium gmqtt paho)
CLIENT_CSV="mqttium,gmqtt,paho"

HOST_DIR="${RESULTS_DIR:-$(python - <<'PY'
from mqtt_client_bench.hostcal import resolve_host_profile, results_dir_for
print(results_dir_for(resolve_host_profile()))
PY
)}"
OUT="${HOST_DIR}/mqttium-gmqtt-paho-arm64"
CAL_DIR="${CAL_DIR:-$OUT/calibrations}"
LOG_DIR="${LOG_DIR:-$OUT/logs}"
mkdir -p "$OUT" "$CAL_DIR" "$LOG_DIR"

echo "=== exact client versions ==="
pip install --force-reinstall --no-cache-dir \
  "mqttium==${MQTTIUM_VER}" \
  "gmqtt==${GMQTT_VER}" \
  "paho-mqtt==${PAHO_VER}"
python - <<'PY'
from importlib.metadata import version
assert version("mqttium") == "1.0.0rc13"
assert version("gmqtt") == "0.7.0"
assert version("paho-mqtt") == "2.1.0"
print("mqttium", version("mqttium"))
print("gmqtt", version("gmqtt"))
print("paho-mqtt", version("paho-mqtt"))
PY

python -m mqtt_client_bench.run broker up

# Fresh calibration on this ARM64 machine. Do not reuse workstation profiles.
for client in "${CLIENTS[@]}"; do
  echo "=== calibrate ${client} ==="
  python -m mqtt_client_bench.run calibrate \
    --client "$client" \
    --profile standard \
    --output "$CAL_DIR/${client}-load.json" \
    | tee "$LOG_DIR/calibrate-${client}.log"
done

# Fail closed: for this official three-way run we do NOT accept an arbitrary
# inconclusive delivered rate as a capacity lower bound. Every client must have
# an official positive RTT capacity for both protocols. This guarantees that the
# shared grid is derived only from valid calibrations.
python - "$CAL_DIR" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
clients = ("mqttium", "gmqtt", "paho")
protocols = ("MQTTv311", "MQTTv5")
errors = []
rows = {}
for client in clients:
    path = root / f"{client}-load.json"
    data = json.loads(path.read_text())
    rows[client] = {}
    for proto in protocols:
        bucket = (data.get("protocol_capacities") or {}).get(proto) or {}
        cap = bucket.get("rtt_capacity_msgs_per_s")
        rows[client][proto] = cap
        if cap is None or float(cap) <= 0:
            errors.append(f"{client}:{proto}:missing_valid_rtt_capacity")
print(json.dumps({"official_rtt_capacities": rows}, indent=2))
if errors:
    raise SystemExit("strict calibration gate failed: " + ", ".join(errors))
PY

# Capacity + headline publish comparison. Same point/config for all three;
# different ceilings are the measurement.
CAPACITY_SCENARIOS=(
  pub_qos_sweep_telemetry
  pub_payload_sweep_qos0
  rtt_capacity_qos1
)
for scenario in "${CAPACITY_SCENARIOS[@]}"; do
  echo "==> three-way capacity matrix ${scenario}"
  python -m mqtt_client_bench.run matrix \
    --clients "$CLIENT_CSV" \
    --scenario "$scenario" \
    --profile standard \
    --runs "$MATRIX_RUNS" \
    --output-dir "$OUT" \
    >"$LOG_DIR/matrix-${scenario}.log" 2>&1
done

# Matched-load latency. The application RTT point is resolved ONCE from
# C_common=min(mqttium,gmqtt,paho) for each protocol, so all three receive the
# exact same target_rate. PUBACK uses explicit catalogue target_rate values.
LATENCY_SCENARIOS=(
  application_rtt_fixed_rate
  puback_latency_fixed_rate
)
for scenario in "${LATENCY_SCENARIOS[@]}"; do
  echo "==> three-way matched-load matrix ${scenario}"
  python -m mqtt_client_bench.run matrix \
    --clients "$CLIENT_CSV" \
    --scenario "$scenario" \
    --profile standard \
    --runs "$MATRIX_RUNS" \
    --load-profile-dir "$CAL_DIR" \
    --output-dir "$OUT" \
    >"$LOG_DIR/matrix-${scenario}.log" 2>&1
done

# Machine-readable campaign summary: versions, official capacities, and the
# exact common target_rate grid recorded by the matrix documents.
python - "$OUT" "$CAL_DIR" <<'PY'
import json
import sys
from importlib.metadata import version
from pathlib import Path

out = Path(sys.argv[1])
cal = Path(sys.argv[2])
clients = ("mqttium", "gmqtt", "paho")
summary = {
    "clients": {
        "mqttium": {"version": version("mqttium"), "io_model": "asyncio"},
        "gmqtt": {"version": version("gmqtt"), "io_model": "asyncio"},
        "paho": {"version": version("paho-mqtt"), "io_model": "sync_reference"},
    },
    "official_rtt_capacities": {},
    "shared_application_rtt_grid": [],
}
for client in clients:
    data = json.loads((cal / f"{client}-load.json").read_text())
    summary["official_rtt_capacities"][client] = {
        proto: ((data.get("protocol_capacities") or {}).get(proto) or {}).get("rtt_capacity_msgs_per_s")
        for proto in ("MQTTv311", "MQTTv5")
    }

probe = json.loads((out / "mqttium-application_rtt_fixed_rate.json").read_text())
for block in probe.get("results") or []:
    point = block.get("point") or {}
    summary["shared_application_rtt_grid"].append({
        "protocol": point.get("protocol"),
        "shared_load_fraction": point.get("shared_load_fraction"),
        "target_rate": point.get("target_rate"),
        "C_common": point.get("shared_capacity_msgs_per_s"),
        "shared_capacities": point.get("shared_capacities"),
        "shared_capacity_sources": point.get("shared_capacity_sources"),
    })
(out / "campaign-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY

echo "THREE_WAY_ARM64_MATCHED_LOAD_DONE"
