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

UNIT="${UNIT:-mqtt-bench-campaign}"

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
}

cmd_stop() {
  local pgid
  # A campaign started as a systemd unit is stopped through systemd. SIGINT is
  # sent explicitly to the whole unit cgroup so the harness cleanup runs; a plain
  # `systemctl stop` would use SIGTERM, which skips it.
  if systemctl --user is-active --quiet "$UNIT" 2>/dev/null; then
    echo "stopping systemd --user unit '$UNIT' with SIGINT..."
    systemctl --user kill --signal=SIGINT "$UNIT" 2>/dev/null
    for _ in $(seq 1 30); do
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
  for _ in $(seq 1 30); do
    sleep 1
    find_pgid >/dev/null 2>&1 || break
  done
  if find_pgid >/dev/null 2>&1; then
    echo "still running after 30 s; escalating to SIGTERM"
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

cmd_resume() {
  if find_pgid >/dev/null 2>&1; then
    echo "campaign is already running; nothing to do"
    return 0
  fi
  local clients="${CLIENTS:-paho,gmqtt,aiomqtt,amqtt,awscrt,zmqtt,mqttium,mqttium-compat}"
  echo "resuming with clients=$clients (completed scenarios are skipped)"
  # `setsid` was not enough: the campaign died twice at session teardown, since
  # the terminal's cgroup is torn down with it whatever the process group says.
  # A transient systemd --user *service* (not --scope, which stays in the
  # caller's cgroup) is owned by the user manager and outlives the session.
  if command -v systemd-run >/dev/null 2>&1 && systemctl --user is-system-running >/dev/null 2>&1; then
    systemctl --user reset-failed "$UNIT" >/dev/null 2>&1 || true
    if systemd-run --user --unit="$UNIT" --same-dir --collect \
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
  stop|pause) cmd_stop ;;
  resume|start) cmd_resume ;;
  *) echo "usage: $0 {status|stop|resume}" >&2; exit 2 ;;
esac
