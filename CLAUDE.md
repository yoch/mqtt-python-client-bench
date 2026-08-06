# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Language

**Everything in this repository is written in English** — documentation, code
comments, docstrings, scenario descriptions, commit messages, CLI output, report
content, and shell script comments. `SCENARIOS.md` and `docs/CEILING_PROBES.md`
were originally French and have been translated; do not reintroduce French in any
artefact, including when editing those files. Conversation with the maintainer may
be in French; the repository content may not.

## What this repo is

A comparative end-to-end benchmark for Python MQTT client libraries (paho, gmqtt,
aiomqtt, amqtt, awscrt, zmqtt, aiomqtt3, mqttium, mqttium-compat) against a local
Dockerized Mosquitto. Raw run outputs are **committed** under `results/*.json`;
GitHub Pages rebuilds the static report site from them. **CI never runs benchmarks** —
suites are executed locally by the maintainer, only unit tests and the report build
run in Actions.

Reference docs: `README.md` (catalogue, comparability matrix, limitations),
`SCENARIOS.md` (per-scenario wiring — in French), `docs/CEILING_PROBES.md`
(broker-vs-client ceiling runbook).

## Commands

```bash
# Env (two venvs: aiomqtt v2 and v3 share an import name and cannot cohabit)
source .venv/bin/activate            # pip install -e ".[dev,all,zmqtt,mqttium]"
source .venv-aiomqtt3/bin/activate   # pip install -e ".[dev,aiomqtt3]"  (aiomqtt3 only)
export PYTHONPATH=src

python -m mqtt_client_bench.run broker up        # docker compose Mosquitto + TLS certs
python -m mqtt_client_bench.run clients -v       # adapter capability matrix
python -m mqtt_client_bench.run list --suite core

# Calibrate first: open-loop scenarios (load_fraction) are refused without it
python -m mqtt_client_bench.run calibrate --client paho --profile standard \
    --output calibrations/paho-load.json

python -m mqtt_client_bench.run run --scenario pub_qos_sweep_telemetry --client paho \
    --profile standard --load-profile calibrations/paho-load.json \
    --output results/paho-pub_qos_sweep_telemetry.json

# Published rankings use `matrix`: it interleaves clients within each point
# instead of running one client campaign after another (see Fairness below).
python -m mqtt_client_bench.run matrix --clients paho,gmqtt,aiomqtt \
    --scenario pub_qos_sweep_telemetry --profile standard --runs 3 \
    --load-profile-dir calibrations --output-dir results

python -m mqtt_client_bench.run compare --clients paho,gmqtt \
    --scenario pub_qos_sweep_telemetry --blocks 4 --output results/compare-....json
python -m mqtt_client_bench.run report build --input results --output site
python -m mqtt_client_bench.run broker down
```

Committed result files follow `results/<client>-<scenario>.json`
(`compare-*.json` for ABBA); see `scripts/run_campaign_5h.sh` for the canonical
campaign loop and the other `scripts/run_*.sh` for targeted re-runs.

Tests (no broker or Docker needed):

```bash
PYTHONPATH=src python -m unittest tests.test_unit -v            # full suite (CI form)
PYTHONPATH=src python -m unittest tests.test_unit.DualProtocolTests -v          # one class
PYTHONPATH=src python -m unittest tests.test_unit.MetricsTests.test_abba_order  # one test
```

Iterate with `--profile smoke` (3 s / 1 run) — never publish smoke output; it is
tagged `non_comparable` and `report build` skips `*-smoke.json` and `_*.json`.

## Architecture

**CLI → harness → role subprocesses → adapters.**

- `run.py` — argparse CLI only; every subcommand delegates to `harness`, `broker`,
  or `report`.
- `scenarios.py` — the catalogue. `Scenario` dataclass + `variants`;
  `expand_scenario()` applies `PROFILE_SPECS` timings and expands
  `dual_protocol`-tagged scenarios into `MQTTv311` × `MQTTv5` **points**. A point
  (a plain dict) is the unit of execution.
- `harness.run_point()` — per point: refuse unsupported knobs, resolve
  `target_rate` from the load profile, spawn role workers as separate processes
  (`python -m mqtt_client_bench.roles.<role> --config <json>`), pin each to a
  disjoint physical-core group (`telemetry.allocate_cpuset`: `sut` / `orch` /
  `loadgen`), coordinate phases over a Unix-socket barrier
  (`control.BarrierServer`: broadcast T0 → collect `WARMUP_DRAINED` acks →
  broadcast `T_MEASURE`), sample telemetry + `$SYS` counters, then call
  `validate_run()`.
- `roles/` — `publisher`, `subscriber`, `rtt_initiator`, `responder`. Each reads a
  JSON config, writes a JSON result file, and talks **only** to the adapter
  protocol. They never import a client library directly.
- `adapters/` — one module per library plus `base.py` (protocol + capabilities),
  `registry.py` (name → class, `client_path` sys.path injection for A/B of the same
  library), `async_bridge.py`.
- `report.py` + `report_assets/style.css` — reads `results/*.json`, classifies each doc, and
  emits the static site (index matrix + per-result detail pages).
- `result.schema.json` — output contract; `SchemaTests` guards it.

### Adapter layer

Role workers see a **sync** facade (`MqttClientAdapter` in `adapters/base.py`) with
paho-VERSION2-shaped callbacks. Async libraries wrap `AsyncioBridge` (private loop
thread; publishes are queued and woken by one coalesced `call_soon_threadsafe` per
burst — `schedule_call` for QoS0 paths that can publish synchronously on the loop,
`schedule_coro` for await-only APIs). Never add a blocking cross-thread round-trip
per publish: that would silently penalize one adapter against its peers.

`AdapterCapabilities.missing_for_point()` is the refusal gate. Anything a library
cannot do honestly (QoS2 completion semantics, MQTT v5, `max_inflight`, native
`message_callback_add`, `TCP_NODELAY`) must be declared `False` so the point comes
back `inconclusive` with `not_implemented:<feature>` — **never approximate a
capability to make a point run**.

Adding a client touches: new `adapters/<name>.py`, `_ADAPTERS` and
`_CLIENT_MODULE_PREFIXES` in `adapters/registry.py`, an extra in `pyproject.toml`,
the `client` enum in `result.schema.json`, and README/tests. The report derives
`io_model`, `stability` and version from each result's `client_identity` (falling
back to the registry), so no table there needs editing; only `_CLIENT_ORDER` and
`_CLIENT_COLORS` are cosmetic and degrade gracefully.

Any dependency on a library's **private** API must be declared in the adapter's
`_PRIVATE_API` dict and returned from `identity()`. Reaching into internals changes
what is being measured, so it has to be visible in the result JSON and on the
methodology page — and a test pins the shape so a library release that moves them
fails the suite instead of drifting silently.

## Measurement invariants

These are the point of the project; a change that breaks one invalidates published
results.

- **Three protocols, never mixed**: capacity (closed-loop, bounded `outstanding`,
  primary metric `completed_success` in `[T0_measure, T1)`), latency (open-loop at
  calibrated fractions of *that client's* capacity *in the same regime* — publish
  capacity for PUBACK latency, RTT capacity for application RTT), integrity
  (bounded rate, sequence-header counters).
- **Publish completion contract**: QoS0 = handed to transport, QoS1 = PUBACK,
  QoS2 = PUBCOMP. An adapter that fires earlier must set the matching capability
  `False`.
- **Calibration is per client × protocol**: `load_fraction` without a matching
  `protocol_capacities` entry is refused (`load_profile_missing_protocol:*`,
  `load_fraction_without_{publish,rtt}_calibration`), not defaulted.
- **Equal in-flight window**: capacity points set `inflight = outstanding` so
  clients that expose `max_inflight` are not throttled below the ones that ignore
  it. Only `pub_qos1_inflight` sweeps the window (and requires the knob).
- **Broker reconciliation**: for single-publisher topologies, adapter-reported
  completions are compared with the broker's `$SYS` received-publish counter; a
  run the broker cannot confirm is `inconclusive` (`broker_unconfirmed`). The
  `$SYS` probe runs for every managed-broker run, not just ingress.
- **Fail closed in `validate_run()`**: worker errors, open-loop rate drift > 2 %,
  `$SYS` publish drops, broker CPU ≥ 85 % (or ≥ 70 % headroom gate), a non-
  `performance` CPU governor, a busy host at T0, loadgen below half the effective
  offer, or delivery below half the offer make a run `inconclusive` and set a
  `bottleneck` (`sut_limited` / `broker_limited` / `broker_unconfirmed` /
  `loadgen_limited` / `offer_limited`). Only `valid` runs enter medians.
- **No unequal harness tax**: per-message work in the role workers and in
  `AsyncioBridge` must cost the same for every adapter. The bridge hands publish
  coroutines to *reused* workers rather than spawning one `asyncio.Task` per
  message, because only await-only APIs would have paid that. Telemetry reads
  cgroup counters instead of spawning `docker stats` inside the measure window,
  and the orchestrator is pinned to the `orch` cpuset.
- **Interleaving**: published rankings come from `run matrix`, which rotates
  clients within each point. Sequential per-client campaigns let hours of drift
  enter the ranking as if it were a library difference.
- **Peer grouping**: rankings compare within the same `io_model` (`sync` /
  `asyncio_bridged` / `crt_event_loop`) and the same MQTT protocol; `stable` and
  `experimental` clients are ranked separately.
- Ingress offer accounting: emqtt-bench double-counts QoS0 publishes — compare
  against `effective_offer_msgs_per_s` / `observed_pub_rate`, never the raw
  parsed rate (`docs/CEILING_PROBES.md`).

## Gotchas

- `standard` profile requires at least one physical core group per role and errors
  out otherwise; `smoke` shares cores.
- Netem profiles (`--network lan|wan|edge`) need `tc` + `CAP_NET_ADMIN` and are
  diagnostic/`non_comparable`.
- `certs/`, `calibrations/`, `logs/`, `site/` are gitignored; certs are regenerated
  by `ensure_certs()`.
- Scenarios tagged `planned` stay in the catalogue but are refused by
  `unsupported_features()` — keep them listed rather than deleting them.
