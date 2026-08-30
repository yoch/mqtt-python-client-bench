# Ceiling probes — broker vs client

Diagnostic runbook to push ingress offer past the core `sub_*` default
(200k msgs/s, paced mqtt_hammer `--rate`) and separate **Mosquitto ceiling**
from **SUT client ceiling**. These points are `suite=full`, tagged
`diagnostic`, and **non_comparable** (not ranking core). The catalogue
ceiling grid still uses `loadgen_clients` 32 / 64 / 128 (`I=1` nominal).
QoS0 exact-topic steps ride paced mqtt_hammer; emqtt-bench cannot hold 128k
on one loadgen core. For *why* the broker sits where it does — the per-byte
header `read(2)` in 2.0.x and what 2.1 replaced it with — see
`MOSQUITTO_PROFILING.md`.

## Calibrating a host

`run calibrate-host` measures this machine's ceilings into `hosts/`. Once per
host: the profile is committed and every campaign reads it.

```bash
# Reference host (the one the site publishes). ~300 s, machine must be quiet.
python -m mqtt_client_bench.run calibrate-host --role reference

# A remote runner. Same command, different role: runners are never published.
python -m mqtt_client_bench.run calibrate-host --role runner
```

The probe brings the broker up if none is listening and puts the machine back
as it found it. A ceiling of zero is a failure, not a slow host, and no profile
is written.

Two things that will bite otherwise:

- **Leave ~2 minutes between two calibrations.** The idleness gate reads a
  one-minute load average, and 300 s of hammering leaves a tail well above its
  threshold — a back-to-back second run is refused, correctly.
- **After changing `docker-compose.yml`, run `broker down && broker up`.** A
  container started before a volume was added does not have it, and the symptom
  looks like the feature is broken rather than the container being stale.

### Running on a remote runner

1. **Preflight** — `docker info`, `gcc --version`, `emqtt_bench --help`,
   `run clients -v`, then `broker up` and `broker down`. Any gap is a blocker.
2. **Calibrate the host** on a quiet machine, then commit `hosts/<host>-<fp>.json`.
3. **Calibrate the clients** with `run calibrate`, as on the reference host.
4. **Run the campaign.** `run matrix` writes to `results/<host>-<fp>/` by
   default on a runner — the campaign files are named `<client>-<scenario>.json`,
   so writing to `results/` would overwrite the published corpus file by file.
5. **Read it** with `report build --input results/<host>-<fp> --reference none`.

A host with no cpufreq governor (a VM, a container) is refused unless its
committed profile declares `frequency_policy: "unpinned"`. The declaration is
what permits it — never the absence of the sysfs file — and such runs carry
`clock_unpinned` and are never published.

## The unpaced ingress rate is bimodal, and the broker is read-bound

Repeated identical hammer runs against a subscriber-less broker land near either
230k or 700k msgs/s, at every thread count, on a quiet machine. Not noise: two
stable regimes.

Counting the broker's own syscalls (`/proc/1/io` inside a container given
`--cap-add SYS_PTRACE`) explains it completely. Eight consecutive 5 s runs,
2 publisher threads, 256-byte QoS0:

| msgs/s | µs/msg | broker reads/s | msgs per read | bytes per read |
|---:|---:|---:|---:|---:|
| 734,257 | 1.36 | 92,865 | 7.9 | 2076 |
| 235,584 | 4.24 | 96,692 | 2.4 | 639 |
| 233,886 | 4.28 | 97,899 | 2.4 | 626 |
| 595,624 | 1.68 | 94,897 | 6.3 | 1645 |
| 702,899 | 1.42 | 91,077 | 7.7 | 2023 |
| 233,626 | 4.28 | 100,959 | 2.3 | 607 |
| 332,861 | 3.00 | 94,021 | 3.5 | 929 |
| 559,437 | 1.79 | 80,992 | 6.9 | 1811 |

**The read rate is the invariant.** It spans 25% across runs whose throughput
spans 214%. `reads/s × msgs-per-read` predicts the measured rate to within 1.5%
on every row.

So Mosquitto is **read-syscall-bound, not message-bound**: it sustains roughly
95k reads per second and the message rate is whatever batching hands it. What
varies is how much has accumulated in the socket receive queue when it calls
`read()` — 2.3 packets in the slow regime, 7.9 in the fast one. Note that even
the fast regime reads ~2 KB, well under `packet_buffer_size` (4096), so the
buffer is not the constraint; arrival timing is.

Both states are self-sustaining, which is why it is bimodal rather than noisy.
If the broker falls momentarily behind, data piles up, reads get fatter,
per-message cost falls and it catches up — and stays efficient. If it keeps up
perfectly, each read returns only what trickled in since the last one, which is
expensive per message and keeps it exactly where it was.

### What follows from it

- **An unpaced ingress rate is not a ceiling.** It is a property of how the
  writer and the broker's read loop happen to couple, and it moves 3x between
  runs of the same command. Do not derive a campaign offer from it.
- **A paced offer does not have this problem**: `--rate` fixes arrivals, so the
  batching is set by the rate rather than by the coupling. That is the shape the
  ranking uses, and it is one more reason to keep it.
- The quantity worth recording about a host, if one is wanted, is the broker's
  **read rate** (~95k/s here), not a message rate.

## One subscriber is worth two thirds of the pipeline

Measured on the reference host (i7-3770, loadgen cpuset = one physical group,
256-byte QoS0, `scripts/mqtt_hammer`):

| shape | msgs/s |
|---|---:|
| hammer alone, 2 threads, `--rate 200000` | 198,842 |
| hammer alone, 2 threads, unpaced | 220,738 |
| hammer alone, 4 threads, unpaced | 636,761 |
| hammer alone, 8 threads, unpaced | 216,900 (oversubscribed) |
| **hammer 2 threads `--rate 200000`, one subscriber attached** | **73,120** |

With no subscriber Mosquitto only decodes. With one it must also fan out, and
that roughly triples its per-message cost; the publisher is then back-pressured
to whatever the broker can forward. The loadgen's own capability is therefore
irrelevant to any `sub_*` scenario — the broker's fan-out is the binding
constraint.

Two consequences worth stating plainly, because both are easy to get backwards:

- A ceiling probe that reads the **subscriber's** delivery is measuring the
  broker, not the loadgen. `broker_ceiling_ingress` attaches a reference
  subscriber, so `primary_msgs_per_s` from it is a fan-out number. Measuring the
  loadgen means running it with nothing consuming, which is what
  `hostcal.measure_loadgen_ceiling()` does.
- `DEFAULT_INGRESS_OFFER_MSGS_PER_S = 200000` is **not** a sustainable rate and
  was never meant to be. It is an over-offer whose job is to make the SUT client
  the bottleneck. Deriving it from a sustainable ceiling would set it near 76k
  here and turn the fastest clients into neighbours of the constraint.

`HAMMER_PUB_CLIENTS = 2` is a hand-tuned constant; the peak thread count is a
property of the core group and is swept per host by `calibrate-host`.

## The 64k trap (emqtt-bench QoS0 double counting)

**That was never a real 64k offer** — it is a counting artefact.

For a QoS0 publish that returns `ok`, upstream `emqtt_bench.erl`:

1. `publish/2` calls `inc_counter(..., pub)`;
2. `loop/5` calls `inc_counter(..., pub)` again for the same success.

So every QoS0 PUBLISH is counted **twice** in the `pub total=… rate=…` series.
The parsed rate is ≈ **2 × the real rate**.

| JSON field | Meaning |
|---|---|
| `loadgen.engine` | `hammer` or `emqtt` |
| `loadgen.nominal_rate` | Configured offer (hammer `--rate`, or `clients × 1000 / interval_ms`) |
| `loadgen.effective_offer_msgs_per_s` | **Offer reference** |
| `loadgen.parsed_pub_rate_raw` / `parsed.median_rate` | Raw generator rate (**do not compare** emqtt QoS0) |
| `loadgen.observed_pub_rate` | Corrected rate (`raw / 2` at emqtt QoS0; 1:1 for hammer) |
| `loadgen.qos0_pub_counter_double_count` | `true` when the emqtt correction applies |

With `-c 32 -I 1`: the real offer is **32k**, not 64k. Do not widen the broker
cpuset (Mosquitto 2.1 is single-threaded). emqtt-bench above 100 clients
does not hold the nominal: 150×`I=1` measured ~98k `$SYS received` here.

`mqtt_hammer` (in-tree, `scripts/mqtt_hammer.c`) counts one completed
`write(2)` of a PUBLISH packet. On this host, `--rate 200000` with no
subscriber was **189,777** msgs/s with `$SYS received` ratio **1.000**.
Pub+sub counts matched 1:1 across hammer pub, `$SYS received`, `$SYS sent`,
and hammer sub. A slow subscriber back-pressures TCP and the observed pub
rate falls with the pipeline — that is not a counting error.

## Preconditions

- Local managed broker (the repo's Mosquitto 2.1.2, `sys_interval 1`).
- `gcc` to build `scripts/mqtt_hammer` (lazy, on first hammer run).
- emqtt-bench image available for templated / QoS>0 paths; Docker host networking.
- The `paho` extra installed (used by the `$SYS` probe).
- `smoke` profile to iterate; `standard` for a more stable verdict.

## Matrix

| Scenario | Topology | Offer (`loadgen_clients` / target) | Primary |
|---|---|---|---|
| `broker_ceiling_ingress` | `broker_ceiling` (pub + emqtt sub) | 32 / 64 / 128 → 32k / 64k / 128k | reference sub `recv` |
| `client_ceiling_ingress` | `subscriber_ingress` + `--client` | same grid | SUT delivered |

Publish capacity stays covered by `pub_qos_sweep_telemetry` (already SUT-limited).

## Commands

```bash
# Broker ceiling (no Python SUT — --client is ignored by the workers)
mqtt-client-bench run \
  --suite full \
  --scenario broker_ceiling_ingress \
  --profile smoke \
  --client paho \
  --output results/broker-ceiling-smoke.json

# Client ceiling (substitute gmqtt / awscrt / …)
mqtt-client-bench run \
  --suite full \
  --scenario client_ceiling_ingress \
  --profile smoke \
  --client gmqtt \
  --output results/client-ceiling-gmqtt-smoke.json
```

A single offer step:

```bash
# Through the catalogue: the variants set ingress_target_msgs_per_s = clients×1000.
# Filter afterwards on point.loadgen_clients in the JSON, or re-run with the
# scenario variants temporarily edited.
```

## Reading

1. **Offer** = `effective_offer_msgs_per_s` / `nominal_rate` — never QoS0 `parsed.median_rate` for emqtt-bench.
2. **Delivered** = `primary_msgs_per_s` (SUT or `loadgen_ref_sub.observed_recv_rate`).
3. **Ratio** = `delivery_offer_ratio` (delivered / offer).
4. **`$SYS`** = `sys_counters.dropped_delta` (plus sent/received) over the measure window. Ingress fails closed if loadgen and `$SYS received` diverge (`loadgen_unconfirmed_by_broker`).
5. **CPU** = `broker_cpu_max_pct` and the `telemetry` samples (Mosquitto container + SUT processes).

## Overriding the core offer (`MQTT_BENCH_INGRESS_OFFER`)

The 200k default is a property of the reference host, not of the harness: on a
4-core KVM guest with Mosquitto pinned to one core, a paced hammer sweep
(pub 256 B on core 2, hammer sub on core 0, `$SYS` probe on core 3) measured
the full pipeline ceiling at **~206k msgs/s** — rx and tx both, broker CPU
pegged at ~100 %, zero `$SYS` drops, and no further gain from a 320k cap or an
unpaced firehose. 200k is therefore ~97 % of that ceiling; a client delivering
at the offer (`offer_limited`) cannot be separated further **on that host** by
raising the offer.

On a larger host, set `MQTT_BENCH_INGRESS_OFFER=<msgs/s>` to replace the
default core `sub_*` offer (it also lifts the hammer `--rate` clamp to the
requested value). Fail-closed semantics:

- every point that takes the override is forced `non_comparable` and carries
  `ingress_offer_overridden: true` — the numbers answer "what does this host's
  pipeline do at offer X", never "how does this client rank";
- explicit `ingress_target_msgs_per_s` variants (the ceiling grids) and
  `per_publisher` fan-in offers are untouched;
- an unparseable or non-positive value is refused, not defaulted.

## Verdicts

| Verdict | Typical criteria |
|---|---|
| **VERIFIED broker ceiling** | On `broker_ceiling_ingress`, recv plateaus while the offer rises (64k→128k); and/or material `dropped_delta`; bottleneck `broker_limited`. |
| **VERIFIED client ceiling** | On `client_ceiling_ingress`, the SUT plateaus **below** the `broker_ceiling` recv at the same offer; low `$SYS` drops; bottleneck `sut_limited`. |
| **offer_limited** | Delivered ≥ ~90 % of the effective offer — raise the offer before concluding. |
| **INCONCLUSIVE** | Loadgen unconfirmed by `$SYS`, barrier/worker errors, missing `$SYS` probe, or contradictory signals. |

Out of scope: changing the broker cpuset, replacing Mosquitto, or including
these points in the core ranking.
