#!/usr/bin/env bash
# Focused three-way RTT campaign for the stable ARM64 runner.
#
# Primary comparison: mqttium 1.0.0rc13, gmqtt 0.7.0 and Paho 2.1.0.
# Paho is a synchronous contextual reference, not an asyncio peer.
#
# This ARM64 runner has native Mosquitto but no Docker. Therefore this script
# deliberately limits the official ARM64 campaign to application-RTT scenarios:
# publisher-only capacity/PUBACK rankings require the harness's managed-broker
# $SYS + broker-headroom gates and would be inconclusive on an external broker.
#
# Fairness rules:
# - rtt_capacity_qos1 gives every client the same workload and measures its ceiling;
# - the capacity matrix is interleaved across ALL THREE clients on this host;
# - application_rtt_fixed_rate derives one C_common=min(valid RTT capacities)
#   across ALL THREE clients, then every client receives the exact same target_rate;
# - no fraction-of-own-capacity result is used as a cross-client comparison;
# - no observed/inconclusive lower-bound fallback is accepted.
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
BENCH_BROKER="${BENCH_BROKER:-}"
CLIENTS=(mqttium gmqtt paho)
CLIENT_CSV="mqttium,gmqtt,paho"

if [ -z "$BENCH_BROKER" ]; then
  echo "BENCH_BROKER is required for the native ARM64 campaign" >&2
  exit 2
fi

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

# First measure the RTT ceiling on this exact host/broker in one interleaved
# three-client matrix. These are the ONLY capacities allowed to define C_common.
echo "==> three-way RTT capacity matrix"
python -m mqtt_client_bench.run matrix \
  --clients "$CLIENT_CSV" \
  --scenario rtt_capacity_qos1 \
  --profile standard \
  --runs "$MATRIX_RUNS" \
  --broker "$BENCH_BROKER" \
  --output-dir "$OUT" \
  >"$LOG_DIR/matrix-rtt_capacity_qos1.log" 2>&1

# Convert only VALID matrix medians into the minimal load-profile shape consumed
# by shared_load_fraction. Any missing protocol/median fails closed. We do not
# read raw inconclusive rates and therefore cannot hit the permissive lower-bound
# fallback currently present in the experimental harness.
python - "$OUT" "$CAL_DIR" <<'PY'
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
cal = Path(sys.argv[2])
clients = ("mqttium", "gmqtt", "paho")
required = {"MQTTv311", "MQTTv5"}
all_caps = {}
errors = []
for client in clients:
    doc = json.loads((out / f"{client}-rtt_capacity_qos1.json").read_text())
    caps = {}
    for block in doc.get("results") or []:
        point = block.get("point") or {}
        proto = point.get("protocol")
        median = (block.get("summary") or {}).get("median")
        valid_runs = [r for r in (block.get("runs") or []) if r.get("status") == "valid"]
        if proto in required and median is not None and valid_runs:
            caps[str(proto)] = float(median)
    missing = sorted(required - set(caps))
    if missing:
        errors.append(f"{client}:missing_valid_rtt_capacity:{','.join(missing)}")
    profile = {
        "schema_version": 1,
        "source": "interleaved_rtt_capacity_matrix",
        "client": client,
        "protocol_capacities": {
            proto: {"rtt_capacity_msgs_per_s": value}
            for proto, value in caps.items()
        },
    }
    (cal / f"{client}-load.json").write_text(json.dumps(profile, indent=2) + "\n")
    all_caps[client] = caps
print(json.dumps({"valid_rtt_capacities": all_caps}, indent=2))
if errors:
    raise SystemExit("strict RTT capacity gate failed: " + "; ".join(errors))
PY

# Same absolute target_rate for mqttium, gmqtt and Paho at every point.
echo "==> three-way matched-load application RTT matrix"
python -m mqtt_client_bench.run matrix \
  --clients "$CLIENT_CSV" \
  --scenario application_rtt_fixed_rate \
  --profile standard \
  --runs "$MATRIX_RUNS" \
  --broker "$BENCH_BROKER" \
  --load-profile-dir "$CAL_DIR" \
  --output-dir "$OUT" \
  >"$LOG_DIR/matrix-application_rtt_fixed_rate.log" 2>&1

# Machine-readable provenance + the exact shared grid. Paho is explicitly
# labelled as a synchronous reference; no asyncio ranking is implied.
python - "$OUT" "$CAL_DIR" "$BENCH_BROKER" <<'PY'
import json
import sys
from importlib.metadata import version
from pathlib import Path

out = Path(sys.argv[1])
cal = Path(sys.argv[2])
broker = sys.argv[3]
clients = ("mqttium", "gmqtt", "paho")
summary = {
    "broker": {"mode": "external_native", "endpoint": broker},
    "clients": {
        "mqttium": {"version": version("mqttium"), "io_model": "asyncio"},
        "gmqtt": {"version": version("gmqtt"), "io_model": "asyncio"},
        "paho": {"version": version("paho-mqtt"), "io_model": "sync_reference"},
    },
    "rtt_capacities": {},
    "shared_application_rtt_grid": [],
    "scope_note": "ARM64 corroboration: RTT capacity + matched-load application RTT only; publisher-only rankings require managed-broker gates.",
}
for client in clients:
    data = json.loads((cal / f"{client}-load.json").read_text())
    summary["rtt_capacities"][client] = {
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

echo "THREE_WAY_ARM64_MATCHED_RTT_DONE"
