#!/usr/bin/env bash
# Bounded campaign: calibrate (publish + RTT capacity) + representative core
# (3 runs) + 2 ABBA + report.
# Fail closed: any step error aborts the rest.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source .venv/bin/activate
mkdir -p calibrations results logs

START=$(date +%s)
echo "CAMPAIGN start $(date -Is)" | tee logs/campaign.log

for c in paho gmqtt aiomqtt amqtt awscrt; do
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

for c in paho gmqtt aiomqtt amqtt awscrt; do
  for s in "${REPR[@]}"; do
    out="results/${c}-${s}.json"
    if [[ -f "$out" ]]; then
      echo "skip $out" | tee -a logs/campaign.log
      continue
    fi
    echo "==> $c $s" | tee -a logs/campaign.log
    python -m mqtt_client_bench.run run \
        --scenario "$s" --client "$c" --profile standard --runs 3 \
        --load-profile "calibrations/${c}-load.json" \
        --output "$out" \
        >"logs/${c}-${s}.log" 2>&1
  done
done

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
