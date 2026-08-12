#!/usr/bin/env bash
# Control a running campaign: status / stop / resume.
#
# Why not SIGSTOP: measure windows use CLOCK_MONOTONIC, which keeps advancing
# while a process is suspended. Broker keepalive (60 s) drops the connections and
# the barrier/connect timeouts fire, so anything longer than a couple of seconds
# corrupts the run in flight rather than pausing it.
#
# Why SIGINT and not SIGTERM: only SIGINT unwinds the stack, so only SIGINT runs
# the harness `finally` block that terminates role workers and stops the
# emqtt-bench container. SIGTERM leaves them orphaned (verified empirically).
#
# Stopping costs the in-progress scenario only: `run_campaign_5h.sh` skips
# scenarios whose clients all already have a result from the current harness.
set -uo pipefail
cd "$(dirname "$0")/.."

# `status` imports the bench package to expand scenarios, so it needs the same
# env the campaign runs in. Without this it fails on ModuleNotFoundError unless
# the caller happens to have the venv already activated.
# shellcheck disable=SC1091
source .venv/bin/activate
export PYTHONPATH=src

UNIT="${UNIT:-mqtt-bench-campaign}"
# A point interrupted mid-measure has role workers, a loadgen container and a
# barrier server to unwind; 30 s was not always enough, and the escalation past
# it is SIGTERM, which skips that cleanup entirely.
SIGINT_GRACE_S="${SIGINT_GRACE_S:-90}"
VALIDATE_CHECK="$(cd "$(dirname "$0")" && pwd)/_validate_check.py"

find_pgid() {
  local pid
  pid=$(pgrep -f 'run_campaign_5h' | grep -v cursorsandbox | head -1) || return 1
  [[ -n "$pid" ]] || return 1
  ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' '
}

cmd_status() {
  local pgid
  if systemctl --user is-active --quiet "$UNIT" 2>/dev/null; then
    echo "campaign: running as systemd --user unit '$UNIT'"
  elif ! pgid=$(find_pgid) || [[ -z "$pgid" ]]; then
    echo "campaign: NOT running"
  else
    echo "campaign: running (process group $pgid)"
    # Match on the process group, not on a command pattern, so this listing
    # cannot pick up the pipeline that produces it.
    ps -eo pid,pgid,etime,cmd --no-headers \
      | awk -v g="$pgid" '$2==g {printf "  %s  %s  %s\n", $1, $3, substr($0, index($0,$4), 96)}' \
      | head -4
  fi
  echo
  echo "scenario progress (current harness only):"
  python - <<'PY'
import json
from pathlib import Path
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario

REPR = ["pub_payload_sweep_qos0","pub_qos_sweep_telemetry","pub_qos1_inflight",
        "sub_exact_telemetry","sub_hierarchy_telemetry","sub_callback_matching",
        "duplex_gateway","burst_recovery","e2e_integrity","puback_latency_qos1",
        "application_rtt_qos1"]
done_n = 0
for scenario in REPR:
    expected = len(expand_scenario(SCENARIO_BY_NAME[scenario], "standard"))
    fresh, partial = 0, 0
    for path in sorted(Path("results").glob(f"*-{scenario}.json")):
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        blocks = data.get("results") or []
        runs = [r for b in blocks for r in (b.get("runs") or [])]
        if not runs or not all("started_at" in r for r in runs):
            continue
        if len(blocks) >= expected:
            fresh += 1
        else:
            partial = max(partial, len(blocks))
    if fresh:
        done_n += 1
        state = "done"
    elif partial:
        state = f"partial {partial}/{expected} pts"
    else:
        state = "pending"
    print(f"  {scenario:<28} {state:>18}  ({fresh} complete client files)")
print(f"\n  {done_n}/{len(REPR)} scenarios complete")
PY
  # The outcome line is what tells a finished campaign apart from one that ended
  # with failed scenarios, which is the first thing to look at afterwards.
  local last
  last=$(grep -E 'CAMPAIGN_DONE|FAILED ' logs/campaign.log 2>/dev/null | tail -5)
  if [[ -n "$last" ]]; then
    echo
    echo "last outcome lines (logs/campaign.log):"
    sed 's/^/  /' <<<"$last"
  fi
}

cmd_stop() {
  local pgid
  # A campaign started as a systemd unit is stopped through systemd. SIGINT is
  # sent explicitly to the whole unit cgroup so the harness cleanup runs; a plain
  # `systemctl stop` would use SIGTERM, which skips it.
  if systemctl --user is-active --quiet "$UNIT" 2>/dev/null; then
    echo "stopping systemd --user unit '$UNIT' with SIGINT..."
    systemctl --user kill --signal=SIGINT "$UNIT" 2>/dev/null
    for _ in $(seq 1 "$SIGINT_GRACE_S"); do
      sleep 1
      systemctl --user is-active --quiet "$UNIT" 2>/dev/null || break
    done
    systemctl --user stop "$UNIT" >/dev/null 2>&1 || true
    systemctl --user reset-failed "$UNIT" >/dev/null 2>&1 || true
    echo "stopped. Resume with: bash scripts/campaign_ctl.sh resume"
    return 0
  fi
  if ! pgid=$(find_pgid) || [[ -z "$pgid" ]]; then
    echo "campaign is not running; nothing to stop"
    return 0
  fi
  echo "stopping campaign (process group $pgid) with SIGINT so workers are cleaned up..."
  kill -INT "-$pgid" 2>/dev/null
  for _ in $(seq 1 "$SIGINT_GRACE_S"); do
    sleep 1
    find_pgid >/dev/null 2>&1 || break
  done
  if find_pgid >/dev/null 2>&1; then
    echo "still running after ${SIGINT_GRACE_S} s; escalating to SIGTERM"
    echo "  (SIGTERM skips the harness cleanup; check for orphaned workers below)"
    kill -TERM "-$pgid" 2>/dev/null
    sleep 3
  fi
  echo "stopped. Leftover bench processes: $(pgrep -cf 'mqtt_client_bench.roles' 2>/dev/null || echo 0)"
  local stray
  stray=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -i emqtt || true)
  if [[ -n "$stray" ]]; then
    echo "stray loadgen container(s), removing: $stray"
    docker rm -f $stray >/dev/null 2>&1
  fi
  echo "Resume later with: bash scripts/campaign_ctl.sh resume"
}


# Exercise the harness for real before committing 12 h to it.
#
# Every regression this project has hit was invisible to the unit suite and
# visible in the first seconds of a run: a worker that died on a KeyError, an
# adapter broken by a library bump, a config knob nothing honoured. The suite
# passed in every one of those cases. So the gate is not "do the tests pass" but
# "does every role, on every client, still produce a result".
#
# Validity is deliberately NOT the criterion: a smoke run shares cores and
# saturates the broker, so `inconclusive` is normal and says nothing about
# correctness. A worker that produced no result at all is the failure that
# matters, and it is exactly what a code regression looks like.
VALIDATE_SCENARIOS="${VALIDATE_SCENARIOS:-pub_qos_sweep_telemetry e2e_integrity rtt_capacity_qos1 sub_exact_telemetry}"

cmd_validate() {
  local clients="${CLIENTS:-paho,gmqtt,aiomqtt,amqtt,awscrt,zmqtt,mqttium,mqttium-compat}"
  local tmp; tmp=$(mktemp -d)
  local failed=0

  echo "1/2 unit suite"
  if ! python -m unittest tests.test_unit >"$tmp/tests.log" 2>&1; then
    echo "  FAIL — see $tmp/tests.log"
    tail -15 "$tmp/tests.log" | sed 's/^/     /'
    return 1
  fi
  echo "  ok   $(grep -oE 'Ran [0-9]+ tests' "$tmp/tests.log" | head -1)"

  echo "2/2 smoke: one scenario per topology, every client, first variant only"
  for s in $VALIDATE_SCENARIOS; do
    printf '  %-28s' "$s"
    if ! python -m mqtt_client_bench.run matrix --clients "$clients" --scenario "$s" \
         --profile smoke --runs 1 --variant-index 0 --output-dir "$tmp" \
         >"$tmp/$s.log" 2>&1; then
      echo "FAIL (see $tmp/$s.log)"; failed=1; continue
    fi
    local bad
    bad=$(python "$VALIDATE_CHECK" "$tmp" "$s")
    if [[ -n "$bad" ]]; then echo "FAIL  $bad"; failed=1; else echo "ok"; fi
  done

  if (( failed )); then
    echo
    echo "validation failed — logs in $tmp"
    return 1
  fi
  rm -rf "$tmp"
  echo
  echo "validation passed: every role produced a result on every client"
}

# Refuse to start a campaign that is already doomed. A full run is ~6.5 h; every
# one of these makes the harness mark runs inconclusive or risks the host, and
# each is cheap to check now and expensive to discover afterwards.
preflight() {
  local fatal=0
  local gov
  gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)
  if [[ "$gov" != "performance" ]]; then
    echo "BLOCKER  cpu governor is '$gov', not 'performance'"
    echo "         every run would be invalidated (cpu_governor_not_performance)."
    echo "         fix: sudo cpupower frequency-set -g performance"
    echo "           or: echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"
    fatal=1
  else
    echo "ok       cpu governor: performance"
  fi

  local free_gb
  free_gb=$(df -BG --output=avail . | tail -1 | tr -dc '0-9')
  if (( free_gb < 5 )); then
    echo "BLOCKER  only ${free_gb} GB free; a campaign writes ~1 GB plus logs"
    fatal=1
  else
    echo "ok       disk free: ${free_gb} GB (campaign writes ~1 GB)"
  fi

  local avail_gb
  avail_gb=$(free -g | awk '/^Mem:/{print $7}')
  if (( avail_gb < 4 )); then
    echo "BLOCKER  only ${avail_gb} GB RAM available"
    fatal=1
  else
    echo "ok       RAM available: ${avail_gb} GB"
  fi

  local load
  load=$(awk '{print int($1)}' /proc/loadavg)
  local cpus
  cpus=$(nproc)
  if (( load > cpus )); then
    echo "WARN     load average ${load} exceeds ${cpus} CPUs; runs may be invalidated as host_busy"
  else
    echo "ok       load average: ${load} (of ${cpus} CPUs)"
  fi

  if ! docker info >/dev/null 2>&1; then
    echo "BLOCKER  docker is not usable; the broker and loadgen cannot start"
    fatal=1
  else
    echo "ok       docker reachable"
  fi

  # The managed broker uses network_mode: host, so anything already bound to its
  # ports makes `broker up` fail closed at the first step of the campaign. Cheap
  # to see now, and otherwise discovered hours later in a log.
  #
  # Our own broker binds those same ports, so a bare port check reports the
  # healthy case as a blocker. Only a listener that is *not* our running
  # container counts.
  local held="" ours=0
  if docker ps --filter "name=$(basename "$PWD")-mosquitto" --filter "status=running" \
       --format '{{.Names}}' 2>/dev/null | grep -q .; then
    ours=1
    echo "ok       managed broker already running (it owns 11883/11884)"
  else
    for port in 11883 11884; do
      if ss -lnt 2>/dev/null | grep -q ":${port}\b"; then held="$held $port"; fi
    done
  fi
  if [[ -n "$held" ]]; then
    echo "BLOCKER  broker port(s) already bound:$held"
    ss -lntp 2>/dev/null | grep -E ":(11883|11884)\b" | sed 's/^/         /'
    echo "         a foreign broker answers the health check while ours crash-loops;"
    echo "         stop that listener, or the campaign dies at 'broker up'."
    fatal=1
  else
    [[ "$ours" == "1" ]] || echo "ok       broker ports 11883/11884 free"
  fi
  return $fatal
}

cmd_preflight() { preflight; }

cmd_resume() {
  if find_pgid >/dev/null 2>&1; then
    echo "campaign is already running; nothing to do"
    return 0
  fi
  echo "validation (a 12 h run deserves a few minutes of proof):"
  if ! cmd_validate; then
    echo
    echo "refusing to start: the harness itself does not work. Fix it, or re-run with SKIP_VALIDATE=1"
    [[ "${SKIP_VALIDATE:-0}" == "1" ]] || return 1
    echo "SKIP_VALIDATE=1 set; starting anyway"
  fi
  echo
  echo "preflight:"
  if ! preflight; then
    echo
    echo "refusing to start: fix the blockers above, or re-run with SKIP_PREFLIGHT=1"
    [[ "${SKIP_PREFLIGHT:-0}" == "1" ]] || return 1
    echo "SKIP_PREFLIGHT=1 set; starting anyway"
  fi
  echo
  local clients="${CLIENTS:-paho,gmqtt,aiomqtt,amqtt,awscrt,zmqtt,mqttium,mqttium-compat}"
  local mgr_usable
  echo "resuming with clients=$clients (completed scenarios are skipped)"
  # `setsid` was not enough: the campaign died twice at session teardown, since
  # the terminal's cgroup is torn down with it whatever the process group says.
  # A transient systemd --user *service* (not --scope, which stays in the
  # caller's cgroup) is owned by the user manager and outlives the session.
  # Do not gate on `is-system-running` succeeding: it exits non-zero for
  # "degraded", which one unrelated failed unit (a stale gnome-terminal VTE
  # scope is enough) is sufficient to cause. That silently downgraded the launch
  # to the setsid path below — the one that does not survive session teardown,
  # i.e. exactly the failure this unit exists to prevent. Only a user manager we
  # cannot talk to at all disqualifies systemd.
  local mgr_state
  mgr_state=$(systemctl --user is-system-running 2>/dev/null || true)
  case "$mgr_state" in
    running|degraded|starting|maintenance|stopping) mgr_usable=1 ;;
    *) mgr_usable=0 ;;
  esac
  if command -v systemd-run >/dev/null 2>&1 && [[ "$mgr_usable" == "1" ]]; then
    systemctl --user reset-failed "$UNIT" >/dev/null 2>&1 || true
    # Restart on failure: the campaign is resumable and skips finished
    # scenarios, so a transient failure (docker hiccup, an OOM-killed worker)
    # costs one scenario instead of the rest of the weekend. The burst limit
    # stops a deterministic failure from spinning forever.
    if systemd-run --user --unit="$UNIT" --same-dir --collect \
        --property=Restart=on-failure --property=RestartSec=120 \
        --property=StartLimitIntervalSec=3600 --property=StartLimitBurst=5 \
        --property='SuccessExitStatus=3 130 SIGINT' \
        --setenv=CLIENTS="$clients" --setenv=PYTHONPATH=src \
        bash scripts/run_campaign_5h.sh >/dev/null 2>&1; then
      echo "started as systemd --user unit '$UNIT' (survives this session)"
      echo "  follow: journalctl --user -u $UNIT -f    or    tail -f logs/campaign.log"
      sleep 3
      cmd_status
      return 0
    fi
    echo "systemd-run failed; falling back to setsid (will not survive session teardown)"
  fi
  CLIENTS="$clients" setsid nohup bash scripts/run_campaign_5h.sh \
    >> logs/campaign-stdout.log 2>&1 < /dev/null &
  disown
  sleep 2
  cmd_status
}

case "${1:-status}" in
  status) cmd_status ;;
  preflight|check) cmd_preflight ;;
  validate) cmd_validate ;;
  stop|pause) cmd_stop ;;
  resume|start) cmd_resume ;;
  *) echo "usage: $0 {status|preflight|validate|stop|resume}" >&2; exit 2 ;;
esac
