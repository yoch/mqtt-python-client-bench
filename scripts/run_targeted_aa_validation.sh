#!/usr/bin/env bash
# Smallest ARM revalidation of the A/A counterbalancing fix.
#
# Does NOT produce an official ranking. PROFILE is smoke / NON_COMPARABLE.
# Capacity still runs so C_common exists; matched-load matrix and ABBA are
# skipped. A/A uses four blocks (ABBA BAAB ABBA BAAB) so the complementary
# pair bootstrap has two experimental units:
#   - mqttium A/A at 25 % MQTTv311 (the ARM non-neutrality) and 75 % MQTTv311
#   - gmqtt A/A as the witness client
# Broker PID telemetry and responder gates stay on via the campaign script.
#
# Usage on the RPi5 runner (after the isolated native broker is up):
#   PROFILE=smoke MATRIX_RUNS=1 AA_CONTROL_ENFORCE=1 \
#     bash scripts/run_targeted_aa_validation.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export PROFILE="${PROFILE:-smoke}"
export MATRIX_RUNS="${MATRIX_RUNS:-1}"
export RUN_LOAD_MATRIX="${RUN_LOAD_MATRIX:-0}"
export RUN_ABBA="${RUN_ABBA:-0}"
export RUN_AA="${RUN_AA:-1}"
export AA_BLOCKS="${AA_BLOCKS:-4}"
export AA_VARIANT_INDEXES="${AA_VARIANT_INDEXES:-0,4}"
export AA_CONTROL_ENFORCE="${AA_CONTROL_ENFORCE:-1}"
export RUN_ASYNCIO_PAIR="${RUN_ASYNCIO_PAIR:-1}"
export RUN_SYNC_REFERENCE_PAIR="${RUN_SYNC_REFERENCE_PAIR:-0}"

echo "TARGETED_AA_VALIDATION profile=${PROFILE} aa_blocks=${AA_BLOCKS} aa_indexes=${AA_VARIANT_INDEXES} enforce=${AA_CONTROL_ENFORCE}"
exec bash "$ROOT/scripts/run_pairwise_rtt_campaign.sh"
