#!/usr/bin/env bash
# Practical pairwise RTT campaign for day-to-day regression work.
#
# Goal: useful regression signal in minutes, not publication-grade precision.
# Keeps the high-value causal safeguards (external pacing, pair-specific
# C_common, ABBA/BAAB counterbalancing) while avoiding repeated A/A campaigns.
#
# Defaults:
#   * smoke timing (short windows; results are explicitly non_comparable)
#   * 2 capacity runs per client/protocol
#   * NO A/A by default: the harness itself is already qualified; enable
#     RUN_AA=1 explicitly when revalidating a runner/methodology change
#   * full A/B variant grid, 2 blocks per point (one ABBA + one BAAB)
#   * external process pacer for every open-loop compare
#
# Override any variable explicitly if a particular investigation needs more
# replication. For publication evidence use run_pairwise_rtt_campaign.sh with
# PROFILE=standard instead.
set -euo pipefail

export PROFILE="${PROFILE:-smoke}"
export MATRIX_RUNS="${MATRIX_RUNS:-2}"
export AA_BLOCKS="${AA_BLOCKS:-4}"
export ABBA_BLOCKS="${ABBA_BLOCKS:-2}"
export AA_VARIANT_INDEXES="${AA_VARIANT_INDEXES:-4}"
export AA_CONTROL_ENFORCE="${AA_CONTROL_ENFORCE:-0}"
export RUN_AA="${RUN_AA:-0}"
export RUN_ABBA="${RUN_ABBA:-1}"
export RUN_LOAD_MATRIX="${RUN_LOAD_MATRIX:-0}"
export RUN_ASYNCIO_PAIR="${RUN_ASYNCIO_PAIR:-1}"
export RUN_SYNC_REFERENCE_PAIR="${RUN_SYNC_REFERENCE_PAIR:-1}"
export PACER_MODE="${PACER_MODE:-external}"

if [ "$PACER_MODE" != "external" ]; then
  echo "quick pairwise RTT requires PACER_MODE=external" >&2
  exit 2
fi

if [ "$PROFILE" = "standard" ]; then
  echo "quick campaign intentionally uses short/non-publication timing; use PROFILE=smoke" >&2
  exit 2
fi

exec bash "$(dirname "$0")/run_pairwise_rtt_campaign.sh"
