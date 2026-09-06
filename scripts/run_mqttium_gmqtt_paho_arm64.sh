#!/usr/bin/env bash
# SUPERSEDED. Do not use this script for an official ranking.
#
# The three-way ARM64 campaign sized C_common = min(mqttium, gmqtt, paho).
# Paho's lower ceiling under-loaded the asyncio peers, so that grid is not a
# pairwise matched-load comparison. Those runs also drove mqttium/gmqtt RTT
# through the sync facade / AsyncioBridge.
#
# Official replacement: scripts/run_pairwise_rtt_campaign.sh
# Historical three-way summary stays under
# results/rpi-4dd74b07ca00455b/mqttium-gmqtt-paho-arm64/ as
# historical / asyncio_bridged / not evidence of native asyncio RTT ranking.
set -euo pipefail

if [ "${SUPERSEDED_THREE_WAY:-}" != "1" ]; then
  echo "SUPERSEDED: scripts/run_mqttium_gmqtt_paho_arm64.sh is not an official campaign." >&2
  echo "Use scripts/run_pairwise_rtt_campaign.sh (pairwise C_common, native RTT)." >&2
  echo "Set SUPERSEDED_THREE_WAY=1 only to re-run the historical three-way shape." >&2
  exit 2
fi

echo "refusing to re-execute the historical three-way grid from this checkout" >&2
exit 2
