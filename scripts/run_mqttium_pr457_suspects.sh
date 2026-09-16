#!/usr/bin/env bash
# Closed-loop A/A then A/B for mqttium PR #457 suspects vs RC14.
# Does not modify mqttium. Does not overwrite mqttium-pr457 campaign corpus.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

OUT="results/cursor-bce5e86fefabd33d/mqttium-pr457-ab"
ARCHIVE="archive/cursor-bce5e86fefabd33d/mqttium-pr457-ab"
LOG="logs/mqttium-pr457-ab.log"
mkdir -p "$OUT" "$ARCHIVE" logs

RC14_A="$ROOT/.mqttium-pr457-ab/rc14-a"
RC14_B="$ROOT/.mqttium-pr457-ab/rc14-b"
FINAL="$ROOT/.mqttium-pr457-ab/final"
RC14_SRC="$ROOT/.mqttium-pr457-ab/rc14-a-src"
FINAL_SRC="$ROOT/.mqttium-pr457-ab/final-src"
PY="$ROOT/.venv/bin/python"
BLOCKS="${BLOCKS:-8}"

archive_one() {
  local file="$1"
  local tmp
  tmp="$(mktemp -d /tmp/pr457-ab-archive.XXXXXX)"
  cp -a "$file" "$tmp/$(basename "$file")"
  PYTHONPATH=src "$PY" -m mqtt_client_bench.run results archive --input "$tmp" --archive "$ARCHIVE" || true
  # copy slim back
  if [[ -f "$tmp/$(basename "$file")" ]]; then
    cp -a "$tmp/$(basename "$file")" "$file"
  fi
  rm -rf "$tmp"
}

run_ab() {
  local name="$1" scenario="$2" variant="$3" base="$4" cand="$5" base_src="$6" cand_src="$7"
  local extra=("${@:8}")
  echo "==> $name $(date -Is)" | tee -a "$LOG"
  "$PY" scripts/run_mqttium_capacity_ab.py \
    --mode ab \
    --name "$name" \
    --scenario "$scenario" \
    --variant-index "$variant" \
    --baseline-path "$base" \
    --candidate-path "$cand" \
    --baseline-src "$base_src" \
    --candidate-src "$cand_src" \
    --blocks "$BLOCKS" \
    --profile standard \
    --output "$OUT/${name}.json" \
    "${extra[@]}" \
    | tee -a "$LOG"
  archive_one "$OUT/${name}.json"
}

echo "PR457_AB_START $(date -Is)" | tee "$LOG"

# A. Publish QoS1 MQTT 3.1.1 (pub_qos_sweep_telemetry variant 2)
run_ab aa-qos1-v311 pub_qos_sweep_telemetry 2 "$RC14_A" "$RC14_B" "$RC14_SRC" "$RC14_SRC"
run_ab ab-qos1-v311 pub_qos_sweep_telemetry 2 "$RC14_A" "$FINAL" "$RC14_SRC" "$FINAL_SRC"

# B. Subscribe + headroom probe on the candidate, then A/A and A/B at campaign offer
echo "==> probe-plus-offer $(date -Is)" | tee -a "$LOG"
"$PY" scripts/run_mqttium_capacity_ab.py \
  --mode probe \
  --name probe-plus-offer \
  --scenario sub_hierarchy_telemetry \
  --variant-index 0 \
  --client-path "$FINAL" \
  --client-src "$FINAL_SRC" \
  --probe-offers 160000,200000,250000 \
  --probe-repeats 2 \
  --output "$OUT/probe-plus-offer.json" \
  | tee -a "$LOG"
archive_one "$OUT/probe-plus-offer.json"

run_ab aa-sub-plus sub_hierarchy_telemetry 0 "$RC14_A" "$RC14_B" "$RC14_SRC" "$RC14_SRC"
run_ab ab-sub-plus sub_hierarchy_telemetry 0 "$RC14_A" "$FINAL" "$RC14_SRC" "$FINAL_SRC"

# Specificity controls: hash and exact, A/B only (same window as plus)
run_ab ab-sub-hash sub_hierarchy_telemetry 1 "$RC14_A" "$FINAL" "$RC14_SRC" "$FINAL_SRC"
run_ab ab-sub-exact-v311 sub_exact_telemetry 0 "$RC14_A" "$FINAL" "$RC14_SRC" "$FINAL_SRC"

echo "PR457_AB_DONE $(date -Is)" | tee -a "$LOG"
