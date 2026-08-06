#!/usr/bin/env bash
# Bounded campaign: calibrate (publish + RTT capacity per MQTT protocol) +
# representative core (3 runs, standard 12/3/6) + 2 ABBA + report.
#
# Scenarios run through `matrix`, which interleaves every client *within each
# point* instead of running one full client campaign after another. Sequential
# campaigns let hours of thermal drift and background load enter the ranking as
# if they were differences between libraries; interleaving removes that, and the
# client order rotates between repetitions so no client always runs first.
#
# Dual-protocol scenarios expand MQTTv311×MQTTv5 automatically.
# After pulling dual-protocol changes, re-run calibrate for every client.
# Fail closed: any step error aborts the rest.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source .venv/bin/activate
mkdir -p calibrations results logs

CLIENTS="${CLIENTS:-paho,gmqtt,aiomqtt,amqtt,awscrt}"

START=$(date +%s)
# Append, never truncate: a campaign spans hours and can be resumed, so the log
# has to keep what earlier attempts already did.
echo "CAMPAIGN start $(date -Is) clients=$CLIENTS" | tee -a logs/campaign.log

# Resumable by default: a full campaign is many hours and an interruption
# (machine sleep, session end, Ctrl-C) must not throw away completed work.
# Set FORCE=1 to redo everything from scratch.
FORCE="${FORCE:-0}"

IFS=',' read -ra CLIENT_LIST <<<"$CLIENTS"

# Calibration stays per client: it measures that client's own capacity, and its
# output is only ever compared with itself.
for c in "${CLIENT_LIST[@]}"; do
  cal="calibrations/${c}-load.json"
  # A calibration is reusable only if it carries per-protocol capacities for the
  # installed version of that client; `run` re-validates client/version/broker
  # and refuses a mismatched profile anyway.
  if [[ "$FORCE" != "1" ]] && python - "$cal" "$c" <<'PY'
import json, sys
from pathlib import Path
path, client = Path(sys.argv[1]), sys.argv[2]
if not path.exists():
    raise SystemExit(1)
try:
    data = json.loads(path.read_text())
except Exception:
    raise SystemExit(1)
if data.get("client") != client:
    raise SystemExit(1)
buckets = data.get("protocol_capacities") or {}
if not buckets:
    raise SystemExit(1)
# At least one protocol must carry a usable publish capacity.
raise SystemExit(0 if any(v.get("capacity_msgs_per_s") for v in buckets.values()) else 1)
PY
  then
    echo "==> calibrate $c (reusing $cal)" | tee -a logs/campaign.log
    continue
  fi
  echo "==> calibrate $c" | tee -a logs/campaign.log
  python -m mqtt_client_bench.run calibrate \
      --client "$c" --profile standard \
      --output "$cal" \
      >"logs/calibrate-${c}.log" 2>&1
done

REPR=(
  pub_payload_sweep_qos0
  pub_qos_sweep_telemetry
  pub_qos1_inflight
  sub_exact_telemetry
  sub_hierarchy_telemetry
  sub_callback_matching
  duplex_gateway
  burst_recovery
  e2e_integrity
  puback_latency_qos1
  application_rtt_qos1
)

for s in "${REPR[@]}"; do
  # Skip only when every client already has a result for this scenario *from the
  # current harness*. Results predating the fairness fixes carry no run
  # provenance, so they are correctly treated as missing and re-run.
  if [[ "$FORCE" != "1" ]] && python - "$s" "$CLIENTS" <<'PY'
import json, sys
from pathlib import Path
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario

scenario, clients = sys.argv[1], sys.argv[2].split(",")
expected = len(expand_scenario(SCENARIO_BY_NAME[scenario], "standard"))
for client in clients:
    path = Path("results") / f"{client}-{scenario}.json"
    if not path.exists():
        raise SystemExit(1)
    try:
        data = json.loads(path.read_text())
    except Exception:
        raise SystemExit(1)
    blocks = data.get("results") or []
    runs = [r for block in blocks for r in (block.get("runs") or [])]
    if not runs:
        raise SystemExit(1)
    # `started_at` only exists on runs produced after the fairness fixes, so
    # older results count as missing rather than as finished work.
    if not all("started_at" in r for r in runs):
        raise SystemExit(1)
    # Points are checkpointed as they finish, so a file can exist while the
    # scenario is only half measured.
    if len(blocks) < expected:
        raise SystemExit(1)
raise SystemExit(0)
PY
  then
    echo "==> matrix $s (already complete, skipping)" | tee -a logs/campaign.log
    continue
  fi
  echo "==> matrix $s ($CLIENTS)" | tee -a logs/campaign.log
  python -m mqtt_client_bench.run matrix \
      --clients "$CLIENTS" --scenario "$s" --profile standard --runs 3 \
      --load-profile-dir calibrations \
      --output-dir results \
      >"logs/matrix-${s}.log" 2>&1
done

# ABBA stays for the pairwise verdicts with bootstrap confidence intervals;
# the matrix above gives the ranking, this gives the significance.
echo "==> ABBA paho,gmqtt" | tee -a logs/campaign.log
python -m mqtt_client_bench.run compare \
  --clients paho,gmqtt --scenario pub_qos_sweep_telemetry \
  --profile standard --blocks 4 \
  --output results/compare-paho-gmqtt-pub-qos.json \
  >logs/compare-paho-gmqtt.log 2>&1

echo "==> ABBA paho,awscrt" | tee -a logs/campaign.log
python -m mqtt_client_bench.run compare \
  --clients paho,awscrt --scenario pub_qos_sweep_telemetry \
  --profile standard --blocks 4 \
  --output results/compare-paho-awscrt-pub-qos.json \
  >logs/compare-paho-awscrt.log 2>&1

python -m mqtt_client_bench.run report build --input results --output site \
  >>logs/campaign.log 2>&1
python -m mqtt_client_bench.run broker down >>logs/campaign.log 2>&1

END=$(date +%s)
echo "CAMPAIGN elapsed $((END-START))s ($(date -Is))" | tee -a logs/campaign.log
echo CAMPAIGN_DONE >>logs/campaign.log
