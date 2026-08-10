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
#
# Failure policy: a failing step is recorded and the campaign moves on to the
# next one, then the script exits non-zero with a summary. The measurement
# fail-closed guarantee is unaffected — it lives in validate_run(), which marks
# an untrustworthy run inconclusive whatever this loop does. Aborting the whole
# run here only meant that one broken scenario cost every scenario after it,
# which is the wrong trade for an unattended multi-hour campaign.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source .venv/bin/activate
mkdir -p calibrations results logs

CLIENTS="${CLIENTS:-paho,gmqtt,aiomqtt,amqtt,awscrt}"
# aiomqtt v3 shares an import name with v2, so it cannot be interleaved by
# `matrix` and gets a sequential pass of its own from its own venv. Set
# AIOMQTT3_VENV= to skip it.
AIOMQTT3_VENV="${AIOMQTT3_VENV-.venv-aiomqtt3}"

FAILED=()
note_failure() {
  FAILED+=("$1")
  echo "FAILED $1 ($(date -Is)) — see $2" | tee -a logs/campaign.log
}

START=$(date +%s)
# Append, never truncate: a campaign spans hours and can be resumed, so the log
# has to keep what earlier attempts already did.
echo "CAMPAIGN start $(date -Is) clients=$CLIENTS" | tee -a logs/campaign.log

# Resumable by default: a full campaign is many hours and an interruption
# (machine sleep, session end, Ctrl-C) must not throw away completed work.
# Set FORCE=1 to redo everything from scratch.
FORCE="${FORCE:-0}"

IFS=',' read -ra CLIENT_LIST <<<"$CLIENTS"

# A calibration is reusable only when the harness itself would accept it, so the
# gate calls the same validator `run`/`matrix` use. Checking only that the file
# parses is not enough: a library upgrade or a broker image pull leaves a
# readable profile that the harness then refuses mid-campaign, turning a
# recalibration into a dead campaign. broker_up() here is idempotent and passes
# no cpuset, so it never disturbs a pin the harness applied.
calibration_reusable() {  # <profile-path> <client> <python>
  "$3" - "$1" "$2" 2>>logs/campaign.log <<'PY'
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
if not any(v.get("capacity_msgs_per_s") for v in buckets.values()):
    raise SystemExit(1)
from mqtt_client_bench.broker import broker_up
from mqtt_client_bench.harness import _validate_load_profile

try:
    _validate_load_profile(data, client=client, client_path=None, broker=broker_up(wait=True))
except Exception as exc:
    print(f"    stale calibration for {client}: {exc}", file=sys.stderr)
    raise SystemExit(1)
raise SystemExit(0)
PY
}

# Calibration stays per client: it measures that client's own capacity, and its
# output is only ever compared with itself.
calibrate_client() {  # <client> <python>
  local c="$1" py="$2"
  local cal="calibrations/${c}-load.json"
  if [[ "$FORCE" != "1" ]] && calibration_reusable "$cal" "$c" "$py"; then
    echo "==> calibrate $c (reusing $cal)" | tee -a logs/campaign.log
    return 0
  fi
  echo "==> calibrate $c" | tee -a logs/campaign.log
  # Append like campaign.log: on a resume the previous attempt's log is what
  # explains why this step is being redone.
  if ! "$py" -m mqtt_client_bench.run calibrate \
      --client "$c" --profile standard \
      --output "$cal" \
      >>"logs/calibrate-${c}.log" 2>&1; then
    # Not fatal: a point that needs a capacity it cannot resolve comes back
    # inconclusive from run_point, so the other clients still get measured.
    note_failure "calibrate $c" "logs/calibrate-${c}.log"
    return 1
  fi
}

for c in "${CLIENT_LIST[@]}"; do
  calibrate_client "$c" python || true
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

# Scenarios tagged `diagnostic` are outside the representative ranking set, but
# `report build` still lists them in the front-page matrix. Leaving them on an
# older harness generation therefore publishes two generations side by side with
# nothing marking the difference, so they run in the same campaign.
DIAGNOSTIC=(
  rtt_capacity_qos1
  remaining_length_boundaries
)

SCENARIOS=("${REPR[@]}" "${DIAGNOSTIC[@]}")

# Skip only when every listed client already has a result for this scenario
# *from the current harness*. Results predating the fairness fixes carry no run
# provenance, so they are correctly treated as missing and re-run.
scenario_complete() {  # <scenario> <clients-csv> <python>
  "$3" - "$1" "$2" <<'PY'
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
}

for s in "${SCENARIOS[@]}"; do
  if [[ "$FORCE" != "1" ]] && scenario_complete "$s" "$CLIENTS" python; then
    echo "==> matrix $s (already complete, skipping)" | tee -a logs/campaign.log
    continue
  fi
  echo "==> matrix $s ($CLIENTS)" | tee -a logs/campaign.log
  if ! python -m mqtt_client_bench.run matrix \
      --clients "$CLIENTS" --scenario "$s" --profile standard --runs 3 \
      --load-profile-dir calibrations \
      --output-dir results \
      >>"logs/matrix-${s}.log" 2>&1; then
    note_failure "matrix $s" "logs/matrix-${s}.log"
  fi
done

# aiomqtt v3 shares the `aiomqtt` import name with v2, so it cannot live in the
# same interpreter and cannot be interleaved by `matrix`. It gets a sequential
# pass from its own venv instead: not interleaved with its peers (a structural
# limit of the two-venv split, already noted in the README), but at least
# produced by the same harness generation as everything else in the report.
if [[ -n "$AIOMQTT3_VENV" && -x "$AIOMQTT3_VENV/bin/python" ]]; then
  A3PY="$AIOMQTT3_VENV/bin/python"
  export PYTHONPATH="${PYTHONPATH:-src}"
  calibrate_client aiomqtt3 "$A3PY" || true
  for s in "${SCENARIOS[@]}"; do
    if [[ "$FORCE" != "1" ]] && scenario_complete "$s" aiomqtt3 "$A3PY"; then
      echo "==> aiomqtt3 $s (already complete, skipping)" | tee -a logs/campaign.log
      continue
    fi
    echo "==> aiomqtt3 $s" | tee -a logs/campaign.log
    if ! "$A3PY" -m mqtt_client_bench.run run \
        --scenario "$s" --client aiomqtt3 --profile standard --runs 3 \
        --load-profile calibrations/aiomqtt3-load.json \
        --output "results/aiomqtt3-${s}.json" \
        >>"logs/aiomqtt3-${s}.log" 2>&1; then
      note_failure "aiomqtt3 $s" "logs/aiomqtt3-${s}.log"
    fi
  done
else
  echo "==> aiomqtt3 pass skipped (no $AIOMQTT3_VENV)" | tee -a logs/campaign.log
fi

# ABBA stays for the pairwise verdicts with bootstrap confidence intervals;
# the matrix above gives the ranking, this gives the significance.
#
# `compare` writes its output once, at the end — unlike `matrix` it does not
# checkpoint per point, so a file either exists complete or does not exist and
# existence plus provenance is a complete test. `started_at` only appears on runs
# produced after the fairness fixes, exactly as in scenario_complete. grep rather
# than json.load: these files reach 100+ MB because they embed per-message
# latency arrays, and parsing one just to decide whether to skip it would cost
# seconds and gigabytes.
compare_complete() {  # <output-path>
  [[ -s "$1" ]] && grep -q '"started_at"' "$1"
}

run_abba() {  # <label> <clients> <output>
  local label="$1" clients="$2" out="$3"
  local log="logs/compare-${label}.log"
  if [[ "$FORCE" != "1" ]] && compare_complete "$out"; then
    echo "==> ABBA $clients (already complete, skipping)" | tee -a logs/campaign.log
    return 0
  fi
  echo "==> ABBA $clients" | tee -a logs/campaign.log
  if ! python -m mqtt_client_bench.run compare \
      --clients "$clients" --scenario pub_qos_sweep_telemetry \
      --profile standard --blocks 4 \
      --output "$out" \
      >>"$log" 2>&1; then
    note_failure "ABBA $clients" "$log"
  fi
}

run_abba paho-gmqtt paho,gmqtt results/compare-paho-gmqtt-pub-qos.json
run_abba paho-awscrt paho,awscrt results/compare-paho-awscrt-pub-qos.json

if ! python -m mqtt_client_bench.run report build --input results --output site \
  >>logs/campaign.log 2>&1; then
  note_failure "report build" "logs/campaign.log"
fi
python -m mqtt_client_bench.run broker down >>logs/campaign.log 2>&1 || true

END=$(date +%s)
echo "CAMPAIGN elapsed $((END-START))s ($(date -Is))" | tee -a logs/campaign.log
if ((${#FAILED[@]})); then
  # Exit 3, not 1: "ran to the end, some steps failed" is a distinct outcome
  # from "the script itself died". campaign_ctl marks 3 as a success for
  # systemd, so this does not trigger a restart loop that would keep retrying a
  # deterministic failure all weekend — while a real crash still restarts.
  printf 'CAMPAIGN_DONE_WITH_FAILURES %d: %s\n' "${#FAILED[@]}" "$(IFS='; '; echo "${FAILED[*]}")" \
    | tee -a logs/campaign.log
  exit 3
fi
echo CAMPAIGN_DONE | tee -a logs/campaign.log
