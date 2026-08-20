# Ceiling probes — broker vs client

Diagnostic runbook to push ingress offer past the core `sub_*` default
(200k msgs/s, paced mqtt_hammer `--rate`) and separate **Mosquitto ceiling**
from **SUT client ceiling**. These points are `suite=full`, tagged
`diagnostic`, and **non_comparable** (not ranking core). The catalogue
ceiling grid still uses `loadgen_clients` 32 / 64 / 128 (`I=1` nominal).
QoS0 exact-topic steps ride paced mqtt_hammer; emqtt-bench cannot hold 128k
on one loadgen core.

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

## Verdicts

| Verdict | Typical criteria |
|---|---|
| **VERIFIED broker ceiling** | On `broker_ceiling_ingress`, recv plateaus while the offer rises (64k→128k); and/or material `dropped_delta`; bottleneck `broker_limited`. |
| **VERIFIED client ceiling** | On `client_ceiling_ingress`, the SUT plateaus **below** the `broker_ceiling` recv at the same offer; low `$SYS` drops; bottleneck `sut_limited`. |
| **offer_limited** | Delivered ≥ ~90 % of the effective offer — raise the offer before concluding. |
| **INCONCLUSIVE** | Loadgen unconfirmed by `$SYS`, barrier/worker errors, missing `$SYS` probe, or contradictory signals. |

Out of scope: changing the broker cpuset, replacing Mosquitto, or including
these points in the core ranking.
