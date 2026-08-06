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

find_pgid() {
  local pid
  pid=$(pgrep -f 'run_campaign_5h' | grep -v cursorsandbox | head -1) || return 1
  [[ -n "$pid" ]] || return 1
  ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' '
}

cmd_status() {
  local pgid
  if ! pgid=$(find_pgid) || [[ -z "$pgid" ]]; then
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
  echo "scenarios completed by the current harness:"
  python - <<'PY'
import json
from pathlib import Path
REPR = ["pub_payload_sweep_qos0","pub_qos_sweep_telemetry","pub_qos1_inflight",
        "sub_exact_telemetry","sub_hierarchy_telemetry","sub_callback_matching",
        "duplex_gateway","burst_recovery","e2e_integrity","puback_latency_qos1",
        "application_rtt_qos1"]
done_n = 0
for scenario in REPR:
    files = sorted(Path("results").glob(f"*-{scenario}.json"))
    fresh = 0
    for path in files:
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        runs = [r for b in (data.get("results") or []) for r in (b.get("runs") or [])]
        if runs and all("started_at" in r for r in runs):
            fresh += 1
    mark = "done" if fresh else "pending"
    if fresh:
        done_n += 1
    print(f"  {scenario:<28} {mark:>8}  ({fresh} client files)")
print(f"\n  {done_n}/{len(REPR)} scenarios complete")
PY
}

cmd_stop() {
  local pgid
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
