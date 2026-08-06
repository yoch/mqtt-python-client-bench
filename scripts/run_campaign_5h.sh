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
echo "CAMPAIGN start $(date -Is) clients=$CLIENTS" | tee logs/campaign.log

# Calibration stays per client: it measures that client's own capacity, and its
# output is only ever compared with itself.
IFS=',' read -ra CLIENT_LIST <<<"$CLIENTS"
for c in "${CLIENT_LIST[@]}"; do
  echo "==> calibrate $c" | tee -a logs/campaign.log
  python -m mqtt_client_bench.run calibrate \
      --client "$c" --profile standard \
      --output "calibrations/${c}-load.json" \
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
