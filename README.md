# MQTT Python client comparative benchmark

End-to-end harness that measures popular **Python MQTT client libraries** under
realistic publish/subscribe workloads against a local Mosquitto broker.

Extracted from the Eclipse Paho MQTT Python client benchmark suite and
generalized behind a per-library adapter layer.

**Live reports:** [yoch.github.io/mqtt-python-client-bench](https://yoch.github.io/mqtt-python-client-bench/)
(generated automatically from committed `results/*.json`).

## Clients

### Stable catalogue

| Client | Notes |
|---|---|
| `paho` | Eclipse Paho MQTT Python (sync callbacks) — reference |
| `gmqtt` | asyncio + callbacks; sync facade via `AsyncioBridge` (QoS2 refused: PUBREC≠PUBCOMP) |
| `aiomqtt` | asyncio idiomatic API **v2.x** (paho backend); sync facade |
| `amqtt` | asyncio client only (MQTT 3.1.1; v5 refused) |
| `awscrt` | AWS Common Runtime (`aws-c-mqtt`) — **native** engine, not pure Python |

### Experimental catalogue (separate rankings)

| Client | Notes |
|---|---|
| `zmqtt` | Pure asyncio MQTT 3.1.1/5 (Alpha) — `pip install 'mqtt-client-bench[zmqtt]'` |
| `aiomqtt3` | aiomqtt **v3** alpha (mqtt5 sans-io, MQTT5 only). **Cannot** share an env with `aiomqtt` v2 |

```bash
python -m mqtt_client_bench.run clients -v
```

Unsupported scenario knobs for a given adapter are refused with
`not_implemented:...` instead of silently measuring something else.

### Watchlist (not in catalogue yet)

`mqttproto`, `ohmqtt` — too early / no stable PyPI story.
Wrappers of Paho/gmqtt (`fastapi-mqtt`, `jmqtt`, …) are intentionally excluded.

### Suites

| Suite | Purpose |
|---|---|
| `core` | Stable publication suite (experimental clients **refused**) |
| `full` | Extended stable scenarios |
| `experimental` | Same contracts as `core`, for `zmqtt` / `aiomqtt3` rankings |

### Comparability matrix (high level)

| Scenario family | Comparable across | Notes |
|---|---|---|
| Publisher capacity / QoS0–1 | stable clients with matching caps | QoS2 excluded for gmqtt |
| `pub_qos1_inflight` | paho, aiomqtt | requires `max_inflight` |
| `sub_callback_matching` | **paho only** | native `message_callback_add` |
| Fleet idle | sync clients only | async_bridged refused (1 loop/thread per conn) |
| MQTT v5 properties | paho, gmqtt, aiomqtt, awscrt, zmqtt | amqtt / aiomqtt3 constraints apply |
| Netem (`lan`/`wan`/`edge`) | diagnostic only | marked `non_comparable` on loopback |
| Smoke profile | never | always `non_comparable` |

## Quick start

```bash
# From the project root
pip install -e ".[paho]"

# Generate TLS certs and start Mosquitto (Docker required)
python -m mqtt_client_bench.run broker up

# List scenarios
python -m mqtt_client_bench.run list --suite core

# Standard run with Paho (default profile is standard)
python -m mqtt_client_bench.run run \
  --scenario pub_qos_sweep_telemetry \
  --client paho \
  --output results/paho-pub-qos-sweep-telemetry.json

# Smoke (short, non-comparable) — must be requested explicitly
python -m mqtt_client_bench.run run \
  --scenario pub_qos_sweep_telemetry \
  --profile smoke \
  --client paho

# Stop broker
python -m mqtt_client_bench.run broker down
```

Optional extras: `.[gmqtt]`, `.[aiomqtt]`, `.[amqtt]`, `.[awscrt]`, or `.[all]`.
Experimental: `.[zmqtt]` or `.[aiomqtt3]` (separate environments).

## Commands

| Command | Purpose |
|---|---|
| `broker up` / `broker down` | Local Mosquitto via docker compose (`network_mode: host`) |
| `clients` | Adapter catalogue / capability matrix |
| `list [--suite core\|full]` | Scenario catalogue |
| `run --scenario NAME --client LIB` | Run one scenario (default `--profile standard`) |
| `run --suite core\|full --client LIB` | Run a suite |
| `calibrate --client LIB --output load.json` | Baseline capacity → open-loop fractions |
| `compare --clients A,B --scenario NAME` | ABBA A/B comparison (all variants by default) |
| `report build [--input results] [--output site]` | Build static HTML reports for GitHub Pages |

Useful flags:

- `--profile smoke|standard` — smoke is short and marked `non_comparable` (default: **standard**)
- `--client …` — system under test
- `--client-path` — optional checkout/worktree for A/B of the same library
- `--broker host:port` — external broker (`managed_broker=false`)
- `--network localhost|lan|wan|edge` — netem profiles need `tc` + `CAP_NET_ADMIN` (diagnostic / non-comparable)
- `--variant-index N` — compare a single scenario variant
- `--load-profile` — JSON produced by `calibrate` (must match client/version/broker)
- `--output` — write full JSON result

## What is measured

Three protocols are never mixed:

1. **Capacity** — closed-loop bounded outstanding window; primary metric is
   `completed_success` in `[T0_measure, T1)`.
2. **Latency** — open-loop at calibrated fractions of **that client's** baseline capacity.
3. **Integrity** — bounded-rate sequence checks (missing/duplicate/out-of-order).

### Publish completion contract

| QoS | `on_publish` means |
|---|---|
| 0 | Packet handed to the transport |
| 1 | PUBACK received |
| 2 | PUBCOMP received (adapters that fire earlier must set `qos2=False`) |

Counters: `offered`, `submitted`, `sync_rejected`, `completed_success`,
`completed_failed`, `missed_due_to_backpressure`. Only `completed_success`
feeds the primary throughput.

Async libraries use a sync facade (`AsyncioBridge`). That cost is assumed and
documented; scenarios where it is not representative (`fleet`, native callback
matching) are refused for bridged clients.

Mosquitto provides a local broker on `127.0.0.1:11883` (TCP) and
`127.0.0.1:11884` (TLS — established TLS, no TLS 1.3 guarantee claimed).
`emqtt-bench` is used only as an ingress load generator (MQTT version aligned
to `point.protocol`).

## Adapter architecture

Role workers (publisher / subscriber / RTT / responder) talk only to
`MqttClientAdapter`. Library-specific code lives under
`src/mqtt_client_bench/adapters/`.

## Publishing results

Benchmarks always run **locally** (Docker Mosquitto, host networking). GitHub
Actions does **not** execute the suites; it only rebuilds the report site.

1. Run with `--profile standard` and write JSON into `results/`.
2. Preview with `report build`.
3. Commit JSON under `results/` and push to `main`.

## Comparative runs

```bash
python -m mqtt_client_bench.run compare \
  --clients paho,gmqtt \
  --scenario pub_qos_sweep_telemetry \
  --blocks 4 \
  --profile standard \
  --output /tmp/ab.json
```

ABBA blocks bootstrap per-block `median(B)/median(A)` ratios. Only fully valid
slots enter the verdict. Load-fraction scenarios auto-calibrate each client
against its own QoS1 capacity. Fixed 5 s cooldown between slots.

## Planned (not executable yet)

- `fleet4k_zipf` / `fleet100k` topic cardinality in the loadgen
- `wan_cut` controlled blackhole outage
- `session_resume` persistent-session outage drain

## Layout

```
src/mqtt_client_bench/
  run.py              CLI
  harness.py          orchestration / barriers / drain
  scenarios.py        catalogue
  adapters/           paho, gmqtt, aiomqtt, amqtt, awscrt, zmqtt, aiomqtt3
  roles/              worker processes
docker-compose.yml    Mosquitto
mosquitto/ certs/     broker config + TLS material
tests/                unit tests
results/              committed raw JSON outputs
```

## Tests

```bash
PYTHONPATH=src python tests/test_unit.py
```

## Known limitations

- Harness-level gaps (`receive_maximum`, retained bootstrap, session outage, …)
  still refuse with `not_implemented:*`.
- `aiomqtt` v2 and v3 cannot cohabit in one environment.
- `amqtt` has no MQTT v5 client path in this bench (`mqtt_v5=false`).
- `gmqtt` QoS2 completion is at PUBREC in 0.7 (`qos2=false`).
- Sync facade overhead for asyncio clients is intentional and documented.
