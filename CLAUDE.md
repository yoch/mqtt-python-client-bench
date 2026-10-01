# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Language

**Everything in this repository is written in English**: documentation, code
comments, docstrings, point questions, commit messages, CLI output and report
content. Conversation with the maintainer may be in French; the repository
content may not.

## What this repo is

A comparative benchmark for Python MQTT client libraries (paho, gmqtt, aiomqtt,
aiomqtt3, amqtt, awscrt, zmqtt, mqttium) against a local Dockerized Mosquitto.
Every published number is either a **count confirmed by a third party** (the C
peer or the broker's `$SYS`) or a **resource read from `/proc` by the
orchestrator**. Campaigns are run locally by the maintainer and committed under
`results/v2/<campaign>/`, and GitHub Pages rebuilds the static report from
them. **CI never runs benchmarks**: it runs only the unit tests and the report
build.

Reference docs: `README.md` (clients, statuses, quick start, limitations) and
`SCENARIOS.md` (per-point wiring and checks). The v1 harness is
archived under the `corpus-v1` tag; its result corpus was removed from git
history and is kept only in an offline bundle. Do not resurrect v1 concepts (load
profiles, calibration, fractions of a client's own capacity, ABBA, host
profiles); they were removed because they could not be verified.

## Commands

```bash
uv sync --extra dev                                   # orchestrator: stdlib only
export PYTHONPATH=src
python -m mqtt_client_bench.run envs --sync           # .venvs/<extra>, one per client
python -m mqtt_client_bench.run list --suites core,v5 # points + campaign estimate
python -m mqtt_client_bench.run run --clients paho --points pub_qos1_fixed   # smoke by default
python -m mqtt_client_bench.run campaign              # standard, core+v5, all clients, ~1 h
python -m mqtt_client_bench.run campaign --resume results/v2/<campaign>
python -m mqtt_client_bench.run report --input results/v2 --output site
python -m mqtt_client_bench.run harness-cost          # null-client floor, exits 1 over budget
```

Commands that start the broker need Docker; the C peer (`peer/mqtt_peer.c`)
is compiled on first use into `build/mqtt_peer-<source sha>`.

Tests (no broker or Docker needed):

```bash
uv sync --extra dev --extra paho --extra gmqtt --extra aiomqtt --extra amqtt \
  --extra awscrt --extra zmqtt --extra mqttium
PYTHONPATH=src uv run --no-sync python -m unittest discover -s tests -v
PYTHONPATH=src uv run --no-sync python -m unittest tests.test_bench.CheckTests -v
```

`tests/test_bench.py` covers the v2 core (histogram parity with C, checks,
drive shapes, null client, catalogue, campaign plan, `$SYS` parsing).
`tests/test_adapters.py` pins adapter contracts and library private-API shapes,
and skips itself when the client libraries are not installed.

## Architecture

**CLI → session → run → (worker, peer, broker) → checks → campaign store.**

- `bench/cli.py` holds argparse only; `run.py` is a shim kept for
  `python -m mqtt_client_bench.run`.
- `bench/catalog.py` is the catalogue. A `Point` is one workload shape
  (`kind` = pub | sub | rtt | duplex | idle, QoS 0–2, payload, `rate` where 0
  means capacity, `window`, protocol) with the one question it answers. The
  extended knobs (`topics`, `filters`, `tls`, `properties`, `topic_alias`,
  `receive_maximum`, `remaining_lengths`) are left out of `as_dict()` at their
  defaults. Suites are `core`, `v5` (fixed-rate points over MQTT 5) and
  `extended` (load curve, QoS 2, dispatch, duplex, TLS, MQTT 5 features,
  payload sizes). Profiles are `standard` (comparable) and `smoke`. A test
  keeps a standard campaign over every suite and client under 95 % of 3 h.
  What is not measured yet is in `TODO.md`.
- `bench/session.py` runs once per campaign:
  - allocates one physical core per role (`bench/cpus.py`: broker, sut, peer,
    orch);
  - starts Mosquitto on its cpuset, pins the orchestrator, builds the peer and
    starts the `$SYS` probe;
  - records the harness floor and the broker's C→C ceiling. The receive offer
    is 90 % of that ceiling.
- `bench/runner.py:run_once()` runs one point for one client:
  - starts the C peers and the worker, listeners first (duplex runs a sink
    and a source);
  - reads `$SYS`, sends one absolute `GO t_start t_measure t_end t_stop` to
    both;
  - reads `/proc` at `t_measure` and `t_end` and `$SYS` after `t_stop`;
  - calls `checks.evaluate()`.
- `bench/worker.py` runs under **the client's own interpreter**
  (`.venvs/<extra>/bin/python`). It connects, prints `ready`, drives the
  adapter with one of the `bench/drive.py` shapes, and snapshots integer
  counters from a timer thread. It never samples resources itself.
- `peer/mqtt_peer.c` is the neutral sink / source / echo. It completes QoS 2,
  spreads a source over N topics (`--topics`), attaches MQTT 5 properties
  (`--props`), and a sink counts user properties and unexpected payload
  lengths (`--sizes`). It histograms latency with the same log-linear buckets
  as `bench/histogram.py`.
- `bench/checks.py` decides what a run means (`valid` / `not_sustained` /
  `invalid`), its flags and its metrics. Its docstring is the contract.
- `bench/campaign.py` plans runs, interleaving clients within every point and
  run, and skipping refused pairs. It rewrites `manifest.json` and
  `<client>.json` after every run, so a campaign can be resumed. An `invalid`
  run is retried once.
- `report/` reads `results/v2/<campaign>/` and emits a static site with no
  CDN, organised by question: one table per point, all clients together.

### Adapter layer

`adapters/` has one module per library, plus `base.py` (the
`MqttClientAdapter` protocol and `AdapterCapabilities`), `registry.py` (name →
class) and `async_bridge.py` (the `a*` coroutine methods of the asyncio
adapters, plus the sync facade that older callers use).

**Drive shapes** (`worker.drive_shape`), resolved from capabilities:

- `sync` (paho, awscrt): publish from a plain thread.
- `nowait` (gmqtt, mqttium): libraries that admit a publish synchronously on
  the loop run one coroutine with a completion callback.
- `awaited` (aiomqtt, aiomqtt3, amqtt, zmqtt): `window` reused worker
  coroutines, because awaiting serially would pin the in-flight window at 1.

`test_drive_shapes` pins the mapping.

**Refusals.** Anything a library cannot do honestly is declared `False` in its
capabilities, and the pair comes back `unsupported` with
`not_implemented:<feature>`. `AdapterCapabilities.missing_for_point()` maps
a point's fields to features: MQTT 5 (amqtt), MQTT 3.1.1 (aiomqtt3), `qos2`
(gmqtt, aiomqtt3, awscrt), `native_message_callback_add` (all but paho and
mqttium), `v5_publish_properties`, `v5_topic_alias` and `v5_receive_maximum`.
`test_extended_feature_declarations` pins the table. **Never approximate a
capability to make a point run.**

**Private API.** Any dependency on a library's private API must be declared in
the adapter's `_PRIVATE_API` dict and returned from `identity()`, which is
recorded in every campaign. A test pins the shape, so a library release that
moves internals fails the suite instead of drifting silently.

**Adding a client** touches:

- a new `adapters/<name>.py`;
- `_ADAPTERS` and `_CLIENT_MODULE_PREFIXES` in `adapters/registry.py`;
- an extra in `pyproject.toml` and `CLIENT_EXTRAS` in `bench/envs.py`;
- the expected tables in `tests/test_adapters.py`;
- the README client table.

## Measurement invariants

A change that breaks one of these invalidates published results.

- **Counts are confirmed by a party that shares no code with the client.**
  - The broker's `$SYS` received and sent counters must reconcile with what
    the client and the peer report, within `5 + 0.05 %`.
  - The peer's counts are the deliveries on publish points.
  - An unconfirmed count makes the run `invalid`, never "probably fine".
- **Resources are read from outside.** CPU is the sum of per-thread
  `schedstat` from `/proc`, falling back to ticks when threads come and go
  (`cpu_source` records which). RSS peak is reset at `t_measure`. The worker
  must not import anything that samples itself.
- **Fixed offers are identical for every client.** Cost and latency compare
  only at fixed-rate points, where every client gets the same absolute rate
  (2,000/s, or 1,000/s for RTT and 16 KiB; the extended load curve adds 500/s
  and 5,000/s). Never offer a client a fraction of
  its own capacity: a faster client would sit further along its own
  latency-versus-load curve, and the ranking would penalise headroom.
- **A backlog is not a latency.** A client that cannot hold a fixed offer is
  `not_sustained`. The run is reported, but kept out of cost and latency
  medians.
- **The client under test is the only Python in a run.** The other side is
  always the C peer. The receive offer comes from C at 90 % of the measured
  C→C broker ceiling, so Mosquitto is not the first limit. A capacity point
  where the broker still saturated carries `broker_bound`, and one where the
  client absorbed the whole offer carries `offer_bound`: neither is a library
  ranking by itself.
- **The harness tax is small and equal.** The worker does integer increments
  and one histogram store per message, plus one send-time store per publish
  on fixed-rate points. `harness-cost` measures each drive
  shape against a null client against a **1 µs per message** budget (measured
  0.4–0.6 µs), and the floor is recorded in every campaign. Never add a
  per-message cross-thread round trip, allocation or clock read on one shape
  that the others do not pay. Extended points that change every publish
  (topics, properties, alias) route it through one `drive.routed_publish`
  call, the same for every client at that point.
- **Each library is driven the fastest way its own API allows.** Fairness is
  equal harness cost, not the slowest common shape.
- **Interleaving.** Campaigns rotate clients within every point and run.
  Sequential per-client campaigns let host drift enter the ranking as if it
  were a library difference.
- **One schedule, one clock.** Worker and peer receive the same absolute
  `CLOCK_MONOTONIC` schedule. Latency stamps are the first 8 payload bytes on
  the same clock, and histograms are bucketed identically in C and Python
  (`HistogramTests` asserts parity).
- **Latency is transit, and lag is reported beside it.** The stamp is the
  actual publish time. On fixed-rate publish and round-trip points, message
  `n` is due at `t_start + ceil((n + 1) / rate)`, and send minus due is the
  schedule lag. It is computed after the run from stored send times, never
  in the drive loop, and reported separately. Never fold lag into latency:
  the 1 ms pacing tick would put the same floor under every client's median.
- **Fail closed.** The standard profile requires a physical core per role, a
  quiet host (`host_quiet`: at most 0.5 cores busy outside client, peer and
  broker) and broker headroom (< 85 % of its core) on fixed points. `smoke` is
  `non_comparable`: it turns a noisy host into a flag and is never published.
- **Only `valid` runs enter medians.** `invalid` and `unsupported` pairs are
  listed in the report's coverage section, never silently dropped.

## Gotchas

- `aiomqtt` v2 and v3 share an import name. They have separate environments
  (declared as a uv conflict), and the adapter tests read aiomqtt3's
  capabilities without importing it.
- The broker listens on 127.0.0.1:11883 (plain) and 11884 (TLS) with
  `network_mode: host`, so a broker from another checkout can hold the port.
  `broker_up` fails loudly in that case. It generates the throwaway TLS
  certificates into `build/certs` before `compose up`, because a missing
  bind-mount source would be created empty.
- A client's Receive Maximum cannot be read back by a third party. Like the
  capacity window, it is an adapter setting pinned by the adapter tests, while
  the delivered count stays broker-confirmed.
- The i7-3770 development desktop runs `schedutil` with a noisy session, so
  it is not a reference host. Standard campaigns there fail `host_quiet`;
  each run's `resources.host` (`top`, `states`, `per_cpu`, `unattributed_cores`)
  and the campaign's closing `noise_summary` say which process or kernel state
  was behind it. That pass over `/proc` (~50 ms for 1,300 processes, at each window
  boundary) runs on the orchestrator's core, counts as 0.006 cores of host
  noise and is labelled `orchestrator` in the list.
- `build/`, `.venvs/`, `site/` and `results/v2/*-smoke/` are gitignored.
