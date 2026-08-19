# Ceiling probes — broker vs client

Diagnostic runbook to push ingress offer past the core `sub_*` default
(150k msgs/s with `loadgen_clients=150`) and separate **Mosquitto ceiling**
from **SUT client ceiling**. These points are `suite=full`, tagged
`diagnostic`, and **non_comparable** (not ranking core).

Why the old 32k plateau was not a Mosquitto CPU cap, and what the
read-ahead rebuild changes: [MOSQUITTO_PROFILING.md](MOSQUITTO_PROFILING.md).

## The 64k trap (emqtt-bench QoS0 double counting)

**That was never a real 64k offer** — it is a counting artefact.

For a QoS0 publish that returns `ok`, upstream `emqtt_bench.erl`:

1. `publish/2` calls `inc_counter(..., pub)`;
2. `loop/5` calls `inc_counter(..., pub)` again for the same success.

So every QoS0 PUBLISH is counted **twice** in the `pub total=… rate=…` series.
The parsed rate is ≈ **2 × the real rate**.

| JSON field | Meaning |
|---|---|
| `loadgen.nominal_rate` | Configured offer ≈ `clients × 1000 / interval_ms` |
| `loadgen.effective_offer_msgs_per_s` | **Offer reference** (= `nominal_rate` for pub) |
| `loadgen.parsed_pub_rate_raw` / `parsed.median_rate` | Raw emqtt-bench rate (**do not compare** at QoS0) |
| `loadgen.observed_pub_rate` | Corrected rate (`raw / 2` at QoS0) |
| `loadgen.qos0_pub_counter_double_count` | `true` when the correction applies |

With `-c 32 -I 1`: the real offer is **32k**, not 64k. A client at ~30.5k already
tracks ~95 % of the offer — you must **raise the nominal** (more clients) to look
for a higher ceiling. Do not widen the broker cpuset (Mosquitto 2.0 is
single-threaded).

## Preconditions

- Local managed broker (the repo's Mosquitto, `sys_interval 1`).
- emqtt-bench image available; Docker host networking.
- The `paho` extra installed (used by the `$SYS` probe).
- `smoke` profile to iterate; `standard` for a more stable verdict.

## Matrix

| Scenario | Topology | Offer (`loadgen_clients` / target) | Primary |
|---|---|---|---|
| `broker_ceiling_ingress` | `broker_ceiling` (emqtt pub + emqtt sub) | 32 / 64 / 128 → 32k / 64k / 128k | reference sub `recv` |
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

1. **Offer** = `effective_offer_msgs_per_s` / `nominal_rate` — never QoS0 `parsed.median_rate`.
2. **Delivered** = `primary_msgs_per_s` (SUT or `loadgen_ref_sub.observed_recv_rate`).
3. **Ratio** = `delivery_offer_ratio` (delivered / offer).
4. **`$SYS`** = `sys_counters.dropped_delta` (plus sent/received) over the measure window.
5. **CPU** = `broker_cpu_max_pct` and the `telemetry` samples (Mosquitto container + SUT processes).

## Verdicts

| Verdict | Typical criteria |
|---|---|
| **VERIFIED broker ceiling** | On `broker_ceiling_ingress`, recv plateaus while the offer rises (64k→128k); and/or material `dropped_delta`; bottleneck `broker_limited`. |
| **VERIFIED client ceiling** | On `client_ceiling_ingress`, the SUT plateaus **below** the `broker_ceiling` recv at the same offer; low `$SYS` drops; bottleneck `sut_limited`. |
| **offer_limited** | Delivered ≥ ~90 % of the effective offer — raise `loadgen_clients` before concluding. |
| **INCONCLUSIVE** | Loadgen &lt; half the offer, barrier/worker errors, missing `$SYS` probe, or contradictory signals. |

Out of scope: changing the broker cpuset, replacing Mosquitto, or including
these points in the core ranking.
