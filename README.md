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
| `mqttium` | [yoch/mqttium](https://github.com/yoch/mqttium) / [PyPI](https://pypi.org/project/mqttium/) | Native `AsyncClient` (beta ≥0.2.0b4, `publish_nowait` on bridge loop) — `pip install 'mqtt-client-bench[mqttium]'` + `--suite experimental` |
| `mqttium-compat` | same | Paho VERSION2 façade only (`mqttium.compat.paho`) — ranked separately from `mqttium` |

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
| `experimental` | Same contracts as `core`, for `zmqtt` / `aiomqtt3` / `mqttium` / `mqttium-compat` rankings |

### Comparability matrix (high level)

| Scenario family | Comparable across | Notes |
|---|---|---|
| Dual-protocol core (pub qos sweep, sub_exact, puback, RTT) | clients at the **same MQTT protocol** | Expanded as `MQTTv311` and `MQTTv5` rows; never mix protocols in a ranking cell |
| Other publisher capacity / QoS0–1 | stable clients with matching caps | still MQTTv311-only; QoS2 excluded for gmqtt |
| `pub_qos1_inflight` | paho, aiomqtt | requires `max_inflight` |
| Application RTT | same protocol + RTT calibration | fractions of that protocol’s `rtt_capacity`; awscrt refused (no `TCP_NODELAY`) |
| `sub_callback_matching` | **paho only** | native `message_callback_add` |
| Fleet idle | sync clients only | async_bridged refused (1 loop/thread per conn) |
| MQTT v5 properties | paho, gmqtt, aiomqtt, awscrt, zmqtt | amqtt / aiomqtt3 constraints apply |
| `aiomqtt3` rankings | **MQTTv5 peers only** | v5-only client; calibrate via `protocol_capacities.MQTTv5` |
| Netem (`lan`/`wan`/`edge`) | diagnostic only | marked `non_comparable` on loopback |
| Smoke profile | never | always `non_comparable` |
| I/O peer groups | within the same `io_model` | `sync` (paho, mqttium-compat) ≠ `asyncio_bridged` (aiomqtt, gmqtt, mqttium, …) ≠ `crt_event_loop` (awscrt); `mqttium` ≠ `mqttium-compat` |

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
Experimental: `.[zmqtt]`, `.[aiomqtt3]`, or `.[mqttium]` (aiomqtt3 needs a separate env).

## Commands

| Command | Purpose |
|---|---|
| `broker up` / `broker down` | Local Mosquitto via docker compose (`network_mode: host`) |
| `clients` | Adapter catalogue / capability matrix |
| `list [--suite core\|full]` | Scenario catalogue |
| `run --scenario NAME --client LIB` | Run one scenario (default `--profile standard`) |
| `run --suite core\|full --client LIB` | Run a suite |
| `calibrate --client LIB --output load.json` | Publish + RTT closed-loop baselines → open-loop fractions |
| `matrix --clients A,B,C [--scenario NAME]` | **Recommended for published rankings** — runs every client interleaved within each point, rotating the order between repetitions |
| `compare --clients A,B --scenario NAME` | ABBA A/B comparison (all variants by default) |
| `report build [--input results] [--output site]` | Build static HTML reports for GitHub Pages |

Useful flags:

- `--profile smoke|standard` — smoke is short and marked `non_comparable` (default: **standard**: 12 s measure / 3 s warmup / 3 runs; smoke: 3 s / 1 s / 1 run)
- `--client …` — system under test
- `--client-path` — optional checkout/worktree for A/B of the same library
- `--broker host:port` — external broker (`managed_broker=false`)
- `--network localhost|lan|wan|edge` — netem profiles need `tc` + `CAP_NET_ADMIN` (diagnostic / non-comparable)
- `--variant-index N` — compare a single scenario variant
- `--load-profile` — JSON produced by `calibrate` (must match client/version/broker)
- `--output` — write full JSON result

## Scenario design

How each scenario is wired (topology, cadence, primary metric, caps, refusals):
see **[SCENARIOS.md](SCENARIOS.md)**. Broker vs client ceiling probes:
**[docs/CEILING_PROBES.md](docs/CEILING_PROBES.md)**.

## What is measured

Three protocols are never mixed:

1. **Capacity** — closed-loop bounded outstanding window; primary metric is
   `completed_success` in `[T0_measure, T1)`.
2. **Latency** — open-loop at calibrated fractions of **that client's** baseline
   capacity *in the same regime* (publish capacity for PUBACK latency; RTT
   capacity for application RTT).
3. **Integrity** — bounded-rate sequence checks (missing/duplicate/out-of-order).

Worker-owned measurement memory is bounded independently of elapsed message
count. Latencies and scheduler lag use deterministic reservoir sampling
(50,000 samples by default); sequence integrity uses bounded exact detail plus
two online 64-bit commutative fingerprints. Result metadata records observed
and retained sample counts so percentile quality remains auditable.

Publisher payload backlog is separately capped at 64 MiB by default by reducing
the effective outstanding window for large payloads. A single payload larger
than the cap is still admitted alone and reported explicitly. Periodic worker
telemetry includes RSS, RSS high-water, USS and PSS; abnormal exits retain the
return code, signal and a `possible_oom_or_sigkill` marker instead of looking
like an unexplained missing result.

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

RTT scenarios require `TCP_NODELAY` end to end: without it, ping-pong traffic
measures a deterministic Nagle+delayed-ACK plateau (~40 ms/hop ≈ 84 ms/pair on
loopback), not the client. The broker sets `set_tcp_nodelay true`; paho and
aiomqtt set it on their sockets; asyncio clients get it from the runtime.
`awscrt` (aws-c-io) exposes no such knob, so its RTT points are refused with
`not_implemented:tcp_nodelay` rather than published as a TCP artifact.

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
matching) are refused for bridged clients. Bridged adapters share one submission
discipline: `publish()` allocates a synthetic mid, enqueues work through a
**coalesced** cross-thread wake (one `call_soon_threadsafe` per burst), and
reports completion via `on_publish`. QoS0 paths that can publish synchronously
on the loop use `schedule_call` (no `asyncio.Task` per message: mqttium
`publish_nowait`, gmqtt `_connection.publish`); await-only APIs keep
`schedule_coro`. No adapter pays a per-publish *blocking* bridge round-trip
that its peers do not. Rankings remain peer-grouped by `io_model` (sync vs
asyncio_bridged vs CRT); do not treat paho and aiomqtt as interchangeable.

`mqttium` uses ``AsyncClient.publish_nowait`` on the bridge event-loop thread
(PyPI ≥0.2.0b4; loop-bound, not cross-thread). QoS≥1 completion is
``receipt.wait()``, which also re-raises an admission failure so a refused
publish is not counted as a completion. The adapter installs no
``AsyncClient.on_publish``: mqttium takes its direct QoS0 transport write only
while that callback is unset, so setting one would benchmark the slower path.
The Paho façade remains a separate client id (`mqttium-compat`). Bench
``max_queued`` maps to ``max_pending_outbound_messages``
(``EngineConfig.max_queued`` was removed in 0.1.0a2). Through 0.2.0b4 the façade
does not expose ``max_outbound_inflight`` and the engine refuses it once
attached; the compat adapter rebuilds the inner ``AsyncClient`` before connect so
QoS≥1 scenarios stay comparable. Campaign helpers:
`scripts/run_mqttium_campaign.sh`,
`scripts/run_asyncio_bridged_qos0_campaign.sh`.

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
slots enter the verdict. Load-fraction scenarios auto-calibrate each client once
against its own MQTT 3.1.1 and MQTT 5 regime capacities (publish or RTT), then
execute every client × protocol × 50/75/90/100 % point. Fixed 5 s cooldown
between slots.

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
  adapters/           paho, gmqtt, aiomqtt, amqtt, awscrt, zmqtt, aiomqtt3, mqttium, mqttium-compat
  roles/              worker processes
docker-compose.yml    Mosquitto
mosquitto/ certs/     broker config + TLS material
tests/                unit tests
results/              committed raw JSON outputs
```

## Tests

```bash
PYTHONPATH=src python -m unittest tests.test_unit -v
```

## Contributing

All repository content is written in **English**: documentation, comments,
docstrings, scenario descriptions, commit messages and report output.

## Known limitations

- Niche scenarios (`receive_maximum`, retained bootstrap, session outage,
  queue rejection) are tagged `planned`: skipped by suites, refused with
  `not_implemented:*` if forced — see “Planned”.
- `aiomqtt` v2 and v3 cannot cohabit in one environment.
- `amqtt` has no MQTT v5 client path in this bench (`mqtt_v5=false`).
- `gmqtt` QoS2 completion is at PUBREC in 0.7 (`qos2=false`).
- `awscrt` cannot set `TCP_NODELAY` (aws-c-io hides the fd) → RTT scenarios
  refused; its publish/ingress numbers are unaffected (pipelined writes).
- Sync facade overhead for asyncio clients is intentional and documented.
- The `sub_*` scenarios are **load-generator bound on the reference host**, not
  client bound: every client lands within a few tens of msgs/s of the same
  ~30 300 ceiling, which is the ingress offer rather than a property of the
  library. Read them as a delivery-correctness check, not as a ranking.
- The 64 KiB and 1 MiB points of `pub_payload_sweep_qos0` are **broker bound**:
  Mosquitto saturates before most clients do, so a valid median survives mainly
  for the clients too slow to saturate it. That inverts the ranking at those two
  sizes, and they should not be read as a comparison until the broker has more
  headroom than the clients.
