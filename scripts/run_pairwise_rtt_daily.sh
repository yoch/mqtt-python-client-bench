#!/usr/bin/env bash
# Fast day-to-day RTT regression probe.
#
# Keeps the same external-pacer and ABBA/BAAB mechanics as the quick campaign,
# but only samples the two MQTTv311 load points that are useful and normally
# sustainable for routine regression detection:
#   variant 0 = 25% of pair C_common
#   variant 2 = 50% of pair C_common
#
# 75%/90% are saturation diagnostics and are intentionally excluded by default:
# with closed-loop capacity as the denominator they can become backpressure
# tests rather than latency comparisons for some clients.
#
# Use run_pairwise_rtt_quick.sh for 25/50% across MQTTv311 + MQTTv5, and the
# standard campaign only for exceptional publication/deep-validation work.
set -euo pipefail

export PROFILE="${PROFILE:-smoke}"
export MATRIX_RUNS="${MATRIX_RUNS:-2}"
export RUN_AA="${RUN_AA:-0}"
export RUN_LOAD_MATRIX="${RUN_LOAD_MATRIX:-0}"
export RUN_ABBA="${RUN_ABBA:-1}"
export ABBA_BLOCKS="${ABBA_BLOCKS:-2}"
export ABBA_VARIANT_INDEXES="${ABBA_VARIANT_INDEXES:-0,2}"
export PACER_MODE="${PACER_MODE:-external}"

exec bash "$(dirname "$0")/run_pairwise_rtt_quick.sh"
