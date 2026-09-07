#!/usr/bin/env bash
# Fast day-to-day RTT regression probe.
#
# Keeps the same external-pacer and ABBA/BAAB mechanics as the quick/full
# pairwise campaign, but only samples the two MQTTv311 load points that are
# most useful for routine regression detection:
#   variant 0 = 25% of pair C_common
#   variant 4 = 75% of pair C_common
#
# Expected use: frequent regression checks. This is intentionally not
# publication-grade evidence. Use run_pairwise_rtt_quick.sh for the full
# 8-point RTT grid and run_pairwise_rtt_campaign.sh PROFILE=standard only for
# exceptional publication/deep-validation work.
set -euo pipefail

export PROFILE="${PROFILE:-smoke}"
export MATRIX_RUNS="${MATRIX_RUNS:-2}"
export RUN_AA="${RUN_AA:-0}"
export RUN_LOAD_MATRIX="${RUN_LOAD_MATRIX:-0}"
export RUN_ABBA="${RUN_ABBA:-1}"
export ABBA_BLOCKS="${ABBA_BLOCKS:-2}"
export ABBA_VARIANT_INDEXES="${ABBA_VARIANT_INDEXES:-0,4}"
export PACER_MODE="${PACER_MODE:-external}"

exec bash "$(dirname "$0")/run_pairwise_rtt_quick.sh"
