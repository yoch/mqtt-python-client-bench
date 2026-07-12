# MQTT Python client comparative benchmark

End-to-end harness that measures popular **Python MQTT client libraries** under
realistic publish/subscribe workloads against a local Mosquitto broker.

Extracted from the Eclipse Paho MQTT Python client benchmark suite and
generalized behind a per-library adapter layer.

**Live reports:** [yoch.github.io/mqtt-python-client-bench](https://yoch.github.io/mqtt-python-client-bench/)
(generated automatically from committed `results/*.json`).

## Clients

| Client | Status | Notes |
|---|---|---|
| `paho` | implemented | Eclipse Paho MQTT Python (sync callbacks) |
| `gmqtt` | implemented | asyncio + callbacks; sync facade via `AsyncioBridge` |
| `aiomqtt` | implemented | asyncio idiomatic API (v2.x); sync facade via `AsyncioBridge` |
| `amqtt` | implemented | asyncio client only (broker unused); sync facade via `AsyncioBridge` |

List adapters and capability flags:

```bash
python -m mqtt_client_bench.run clients -v
```

Unsupported scenario knobs for a given adapter are refused with
`not_implemented:...` instead of silently measuring something else.

## Quick start

```bash
# From the project root
pip install -e ".[paho]"

# Generate TLS certs and start Mosquitto (Docker required)
python -m mqtt_client_bench.run broker up

# List scenarios
python -m mqtt_client_bench.run list --suite core

# Smoke run with Paho (short, non-comparable)
python -m mqtt_client_bench.run run \
  --scenario pub_qos_sweep_telemetry \
  --profile smoke \
  --client paho

# Stop broker
python -m mqtt_client_bench.run broker down
```

Optional extras: `.[gmqtt]`, `.[aiomqtt]`, `.[amqtt]`, or `.[all]`.

## Commands

| Command | Purpose |
|---|---|
| `broker up` / `broker down` | Local Mosquitto via docker compose (`network_mode: host`) |
| `clients` | Adapter catalogue / capability matrix |
| `list [--suite core\|full]` | Scenario catalogue |
| `run --scenario NAME --client LIB` | Run one scenario |
| `run --suite core\|full --client LIB` | Run a suite |
| `calibrate --client LIB --output load.json` | Baseline capacity → open-loop fractions |
| `compare --clients A,B --scenario NAME` | ABBA A/B comparison between two adapters |
| `report build [--input results] [--output site]` | Build static HTML reports for GitHub Pages |

Useful flags:

- `--profile smoke|standard` — smoke is short and marked `non_comparable`
- `--client paho|gmqtt|aiomqtt|amqtt` — system under test
- `--client-path` — optional checkout/worktree for A/B of the same library
- `--broker host:port` — external broker (`managed_broker=false`)
- `--network localhost|lan|wan|edge|wan_cut` — netem profiles need `tc` + `CAP_NET_ADMIN`
- `--load-profile` — JSON produced by `calibrate`
- `--output` — write full JSON result

## What is measured

Three protocols are never mixed:

1. **Capacity** — closed-loop bounded outstanding window; primary metric is completions in `[T0,T1)`.
2. **Latency** — open-loop at calibrated fractions of baseline capacity.
3. **Integrity** — bounded-rate sequence checks (missing/duplicate/out-of-order).

Mosquitto provides a local broker on `127.0.0.1:11883` (TCP) and
`127.0.0.1:11884` (TLS). `emqtt-bench` is used only as an ingress load generator.

## Adapter architecture

Role workers (publisher / subscriber / RTT / responder) talk only to
`MqttClientAdapter`. Library-specific code lives under
`src/mqtt_client_bench/adapters/`.

Async libraries (`gmqtt`, `aiomqtt`, `amqtt`) expose the same sync facade
by driving an asyncio loop on a dedicated thread (`AsyncioBridge`).

## Publishing results

Benchmarks always run **locally** (Docker Mosquitto, host networking). GitHub
Actions does **not** execute the suites; it only rebuilds the report site.

1. Run a scenario and write JSON into `results/`:

```bash
python -m mqtt_client_bench.run run \
  --scenario pub_qos_sweep_telemetry \
  --profile standard \
  --client paho \
  --output results/paho-pub-qos-sweep-telemetry.json
```

2. Preview the site locally (optional):

```bash
python -m mqtt_client_bench.run report build --input results --output site
# open site/index.html
```

3. Commit the JSON under `results/` and push to `main`. The Pages workflow
   regenerates the HTML reports automatically. Raw JSON stays in the
   repository and is **not** copied into the published site.

## Comparative runs

```bash
python -m mqtt_client_bench.run compare \
  --clients paho,gmqtt \
  --scenario pub_qos_sweep_telemetry \
  --blocks 4 \
  --profile smoke \
  --output /tmp/ab.json
```

Compare runs between any two implemented adapters use the same ABBA protocol
as same-library A/B comparisons.

## Layout

```
src/mqtt_client_bench/
  run.py              CLI
  harness.py          orchestration / barriers / drain
  scenarios.py        catalogue
  adapters/           paho, gmqtt, aiomqtt, amqtt
  roles/              worker processes
docker-compose.yml    Mosquitto
mosquitto/ certs/     broker config + TLS material
tests/                unit tests
results/              committed raw JSON outputs
.github/workflows/    unit tests + GitHub Pages report deploy
```

## Tests

```bash
PYTHONPATH=src python tests/test_unit.py
```

## Known limitations

- Harness-level gaps (`receive_maximum`, retained bootstrap, session outage, …)
  still refuse with `not_implemented:*` as in the original suite.
- `aiomqtt` v3 (mqtt5 backend) is out of scope; this bench targets v2.x.
- `amqtt` does not expose MQTT v5 publish properties (`v5_publish_properties=false`).
