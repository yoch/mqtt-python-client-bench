# MQTT Python client comparative benchmark

End-to-end harness that measures popular **Python MQTT client libraries** under
realistic publish/subscribe workloads against a local Mosquitto broker.

Extracted from the Eclipse Paho MQTT Python client benchmark suite and
generalized behind a per-library adapter layer.

**Live reports:** [yoch.github.io/mqtt-python-client-bench](https://yoch.github.io/mqtt-python-client-bench/)
(generated automatically from committed `results/*.json`).

## Clients

### Stable catalogue

| Client | Repository | Notes |
|---|---|---|
| `paho` | [eclipse-paho/paho.mqtt.python](https://github.com/eclipse-paho/paho.mqtt.python) | Eclipse Paho MQTT Python (sync callbacks) — reference |
| `gmqtt` | [wialon/gmqtt](https://github.com/wialon/gmqtt) | asyncio + callbacks; sync facade via `AsyncioBridge` (QoS2 refused: PUBREC≠PUBCOMP) |
| `aiomqtt` | [empicano/aiomqtt](https://github.com/empicano/aiomqtt) | asyncio idiomatic API **v2.x** (paho backend); sync facade |
| `amqtt` | [Yakifo/amqtt](https://github.com/Yakifo/amqtt) | asyncio client only (MQTT 3.1.1; v5 refused) |
| `awscrt` | [awslabs/aws-crt-python](https://github.com/awslabs/aws-crt-python) | AWS Common Runtime (`aws-c-mqtt`) — **native** engine, not pure Python |

### Experimental catalogue (separate rankings)

| Client | Repository | Notes |
|---|---|---|
| `zmqtt` | [faststream-community/zMQTT](https://github.com/faststream-community/zMQTT) | Pure asyncio MQTT 3.1.1/5 (Alpha) — `pip install 'mqtt-client-bench[zmqtt]'` |
| `aiomqtt3` | [empicano/aiomqtt](https://github.com/empicano/aiomqtt) | aiomqtt **v3** alpha (mqtt5 sans-io, MQTT5 only). **Cannot** share an env with `aiomqtt` v2 |

```bash
python -m mqtt_client_bench.run clients -v
```

Unsupported scenario knobs for a given adapter are refused with
`not_implemented:...` instead of silently measuring something else.

### Watchlist (not in catalogue yet)

[`mqttproto`](https://github.com/agronholm/mqttproto), [`ohmqtt`](https://github.com/ohmqtt/ohmqtt_python) — too early / no stable PyPI story.
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
| Application RTT | stable clients with RTT calibration | same lib on both ends; fractions of `rtt_capacity_qos1` |
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
| `calibrate --client LIB --output load.json` | Publish + RTT closed-loop baselines → open-loop fractions |
| `compare --clients A,B --scenario NAME` | ABBA A/B comparison (all variants by default) |
| `report build [--input results] [--output site]` | Build static HTML reports for GitHub Pages |

Useful flags:

- `--profile smoke|standard` — smoke is short and marked `non_comparable` (default: **standard**: 20 s measure / 5 s warmup / 3 runs; smoke: 3 s / 1 s / 1 run)
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
2. **Latency** — open-loop at calibrated fractions of **that client's** baseline
   capacity *in the same regime* (publish capacity for PUBACK latency; RTT
   capacity for application RTT).
3. **Integrity** — bounded-rate sequence checks (missing/duplicate/out-of-order).

### Application RTT

`application_rtt_qos1` measures a **homogeneous product loop**: the SUT library
drives both the initiator (`sut` cpuset) and the responder (`orch` cpuset). The
primary sample is one completed request/response pair. That amplifies stack
cost relative to a single-sided client benchmark — intentional for “gateway /
peer of the same stack” questions; it is not a neutral peer RTT.

Open-loop RTT fractions are sized from `rtt_capacity_qos1` (closed-loop max
completed pairs/s for that client), **not** from publisher-only capacity. A
publish QoS1 baseline understates the RTT ceiling (two publishes + two
deliveries per sample) and would mark high fractions inconclusive.

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
matching) are refused for bridged clients. All bridged adapters share the same
submission discipline: `publish()` allocates a synthetic mid, schedules the
coroutine on the loop (`create_task`, non-blocking) and completion is reported
via `on_publish` — no adapter pays a per-publish blocking bridge round-trip
that its peers do not.

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
against its own regime capacity (publish or RTT). Fixed 5 s cooldown between
slots.

## Planned (not executable yet)

Niche/functional scenarios stay in the catalogue but are tagged `planned` and
excluded from suite execution — they probe protocol corner cases, not everyday
client performance, so they are deliberately not implemented for now:

- `session_resume_qos1` persistent-session outage drain
- `mqttv5_flow_control` (`receive_maximum`)
- `retained_bootstrap` (broker-sensitive snapshot)
- `queue_rejection` accounting protocol
- `fleet4k_zipf` / `fleet100k` topic cardinality in the loadgen
- `wan_cut` controlled blackhole outage
- `mqttv5_rich` variants `topic_alias` / `subscription_identifier` and
  `connect_latency_and_churn` variants `tls_resume` / `tcp_concurrent` refuse
  per-point with `not_implemented:*`; the other variants of those scenarios run.

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

- Niche scenarios (`receive_maximum`, retained bootstrap, session outage,
  queue rejection) are tagged `planned`: skipped by suites, refused with
  `not_implemented:*` if forced — see “Planned”.
- `aiomqtt` v2 and v3 cannot cohabit in one environment.
- `amqtt` has no MQTT v5 client path in this bench (`mqtt_v5=false`).
- `gmqtt` QoS2 completion is at PUBREC in 0.7 (`qos2=false`).
- Sync facade overhead for asyncio clients is intentional and documented.
