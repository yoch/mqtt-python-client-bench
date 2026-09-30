# MQTT Python client comparative benchmark

Measures popular **Python MQTT client libraries** against a local Mosquitto
broker and reports only what can be checked: how many messages a client sent,
how many arrived, how much CPU and memory it used to do it, and how late the
messages were — each count confirmed by a party that does not share code with
the client.

**Live report:** [yoch.github.io/mqtt-python-client-bench](https://yoch.github.io/mqtt-python-client-bench/)
(rebuilt from the committed campaigns under `results/v2/`).

## Clients

| Client | Library | Drive shape | Notes |
|---|---|---|---|
| `paho` | [eclipse-paho/paho.mqtt.python](https://github.com/eclipse-paho/paho.mqtt.python) | sync | Reference; network thread + callbacks |
| `awscrt` | [awslabs/aws-crt-python](https://github.com/awslabs/aws-crt-python) | sync | Native engine (`aws-c-mqtt`), not pure Python |
| `gmqtt` | [wialon/gmqtt](https://github.com/wialon/gmqtt) | nowait | asyncio; publish admitted synchronously on the loop |
| `mqttium` | [yoch/mqttium](https://github.com/yoch/mqttium) | nowait | asyncio; `publish_nowait` + public receipts (≥ 1.1.0) |
| `aiomqtt` | [empicano/aiomqtt](https://github.com/empicano/aiomqtt) v2 | awaited | asyncio, paho backend |
| `aiomqtt3` | [empicano/aiomqtt](https://github.com/empicano/aiomqtt) v3 alpha | awaited | MQTT 5 only; own environment (same import name as v2) |
| `amqtt` | [Yakifo/amqtt](https://github.com/Yakifo/amqtt) | awaited | MQTT 3.1.1 only |
| `zmqtt` | [faststream-community/zMQTT](https://github.com/faststream-community/zMQTT) | awaited | asyncio, alpha |

Each library is driven the fastest way its own API allows:

- **sync**: from a plain thread;
- **nowait**: one coroutine on the worker's loop with a completion callback;
- **awaited**: `window` reused coroutines, so awaiting never pins the in-flight
  window at 1.

The drive shape is derived from the adapter's declared capabilities
(`bench/worker.py:drive_shape`) and a test pins it per client. A capability a
library lacks (MQTT 5 for amqtt, MQTT 3.1.1 for aiomqtt3) is refused, never
approximated; refused pairs appear as `unsupported` in the report's coverage
section.

## What a run measures

Each run has three parties, each in its own process and pinned to its own
physical core:

- **the client worker**, which runs the library under test in that client's
  own uv environment and only drives it and counts;
- **the C peer** (`peer/mqtt_peer.c`), the neutral party on the other side of
  the broker, acting as a sink, source or echo;
- **Mosquitto**, whose `$SYS` counters are read fresh before and after the run.

The orchestrator reads CPU time, RSS and context switches from `/proc` from
outside the worker, so the worker never samples itself.

| Point | Kind | Question |
|---|---|---|
| `pub_qos0_max` | publish capacity | QoS 0 messages published per second |
| `pub_qos1_max` | publish capacity | QoS 1 messages completed (PUBACK) per second, 64 in flight |
| `pub_qos1_fixed` | fixed 2,000/s | cost per message and publish→delivery latency |
| `sub_qos0_max` | receive capacity | QoS 0 messages received per second |
| `sub_qos1_fixed` | fixed 2,000/s | receive cost per message and delivery latency |
| `rtt_qos1_fixed` | fixed 1,000 req/s | application round trip against the C echo |
| `pub_16k_fixed` | fixed 1,000/s | cost of 16 KiB payloads |
| `idle_connect` | idle | connect time and idle footprint |

The `v5` suite repeats the three fixed-rate points over MQTT 5, and
`extended` adds more capacity and QoS 0 variants. [SCENARIOS.md](SCENARIOS.md)
describes the wiring of every point.

Capacity points rank throughput. **Fixed-rate points** give every client the
identical absolute offer, so CPU µs per message, RSS and latency compare across
all libraries. Latency is measured from a `CLOCK_MONOTONIC` stamp in the first
8 payload bytes to its arrival. The peer measures it on publish points and
the client callback on receive and round-trip points. It is recorded in the
same log-linear histogram in C and Python (16 buckets per power of two, at
most 6.25 % wide).

### Statuses

- **`valid`**: every check passed.
- **`not_sustained`**: the client could not hold the fixed offer. This is a
  real finding about the client, but the run stays out of cost and latency
  tables, because a backlog's latency is queueing time.
- **`invalid`**: the harness, the peer, the broker or the host failed. The run
  says nothing about the client and is retried once.

The checks compare counts across parties with a tolerance of `5 + 0.05 %`:

- the broker confirms what the client published and what was delivered;
- QoS 1 loses nothing;
- the offer was actually produced and absorbed;
- the broker kept headroom on fixed points;
- the rest of the host stayed quiet.

Flags qualify a valid run without invalidating it:

- `broker_bound`: Mosquitto was at its core limit on a capacity point;
- `offer_bound`: the client received the whole receive offer, so its capacity
  is higher still;
- `broker_queue_overflow`: `sub_qos1_max` exceeded the broker queue;
- `host_noisy`: only on non-comparable profiles.

### What makes the numbers trustworthy

- **Harness cost is measured, not assumed.** `harness-cost` drives every
  shape against a null client. The floor is 0.4–0.6 µs per message and about
  20 MiB of RSS, against a 1 µs budget. Both are recorded in every campaign.
- **Receive capacity is offered by C.** The offer is 90 % of the broker's
  measured C→C ceiling, so the broker is not the first limit.
- **Clients are interleaved.** A campaign rotates clients within every point
  and run, so host drift spreads over all libraries instead of landing on
  whichever ran last.
- **Cross-validation against v1.** On a 134-run smoke, 134 of 134 client
  counts matched `$SYS` exactly. Capacity rose where the v1 harness had taxed
  the fast clients: gmqtt went from 28.9k to 40k QoS 0 msgs/s, and paho from
  11.3k to 11.7k. Worker RSS dropped from 42 MiB to 22–26 MiB.

## Quick start

```bash
# One lockfile, one environment per client under .venvs/<extra>
uv sync --extra dev
PYTHONPATH=src python -m mqtt_client_bench.run envs --sync

# Mosquitto in Docker; the C peer is compiled on first use (cc required)
PYTHONPATH=src python -m mqtt_client_bench.run list --suites core,v5
PYTHONPATH=src python -m mqtt_client_bench.run run --clients paho,gmqtt --points pub_qos1_fixed

# The published form: every client x point x run, interleaved and resumable
PYTHONPATH=src python -m mqtt_client_bench.run campaign
PYTHONPATH=src python -m mqtt_client_bench.run campaign --resume results/v2/<campaign>

PYTHONPATH=src python -m mqtt_client_bench.run report --input results/v2 --output site
PYTHONPATH=src python -m mqtt_client_bench.run harness-cost
```

A standard campaign over `core,v5` for all eight clients takes about an hour
(`list` prints the estimate from the real plan). The `smoke` profile (0.5 s
warm-up, 2 s window, one run) is for development only. It is tagged
`non_comparable` and never published.

The standard profile needs one physical core each for the broker, the client,
the peer and the orchestrator, plus the `performance` CPU governor and a quiet
host. Otherwise runs fail `host_quiet`.

## Layout

```
peer/mqtt_peer.c              neutral C sink / source / echo
mosquitto/mosquitto.conf      one plaintext listener, $SYS every second
src/mqtt_client_bench/
  adapters/                   one module per library + registry + capabilities
  bench/
    catalog.py                points, suites, profiles
    worker.py, drive.py       the client process: drive shapes and counters
    runner.py, session.py     one run; one broker + peer + probe session
    checks.py                 what a run's counts may be used for
    campaign.py               interleaved, resumable campaigns
    sysprobe.py, procstat.py  $SYS and /proc readers
    histogram.py              latency histogram shared with the peer
    harness_cost.py           null-client floor
  report/                     static site generator
results/v2/<campaign>/        manifest.json + one <client>.json per client
```

The v1 harness and its corpus are archived under the `corpus-v1` tag.

## Tests

```bash
uv sync --extra dev --extra paho --extra gmqtt --extra aiomqtt --extra amqtt \
  --extra awscrt --extra zmqtt --extra mqttium
PYTHONPATH=src uv run --no-sync python -m unittest discover -s tests -v
```

No broker or Docker is needed. The histogram parity test compiles the peer
when a C compiler is available.

## Known limitations

- One host, loopback only: results describe CPU cost and latency without a
  network in the way, on the machine named in each campaign's manifest.
- `awscrt` cannot set `TCP_NODELAY` (aws-c-io hides the socket). Its ~25 ms
  round trip at 1,000 req/s is Nagle's algorithm meeting delayed ACKs. That is
  real behaviour of the library as shipped, reported as measured.
- `aiomqtt` v2 and v3 cannot share an environment. Each client has its own,
  so this only matters for the adapter tests, which skip aiomqtt3.
- CPU per message at a fixed rate is higher than at capacity, because a
  mostly idle process pays for wake-ups. Compare fixed-rate costs with each
  other, never with capacity costs.

## Contributing

All repository content is written in **English**: documentation, comments,
docstrings, commit messages and report output.
