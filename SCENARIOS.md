# Scenario design

This document describes **what each scenario measures**, how the bench is wired
(topology, cadence, primary metric), and what is comparable or not. The catalogue
itself is the source of truth: `src/mqtt_client_bench/scenarios.py`; the harness
lives in `harness.py`.

## Measurement model (three protocols, never mixed)

| Protocol | Question | How load is applied | Primary metric |
|---|---|---|---|
| **Capacity** | What throughput does the client sustain? | Closed loop: bounded `outstanding` window, no pacing | `completed_success` / s within `[T0, T1)` |
| **Latency** | What latency at X % of *its own* capacity? | Open loop at calibrated fractions (`load_fraction`) | Latency distribution (PUBACK or application RTT) |
| **Integrity** | Missing / duplicate / out of order? | Bounded rate + sequence header | Integrity counters (not a throughput ranking) |

Calibration (`calibrate`): for each client and **each supported MQTT protocol**,
a QoS1 publish capacity and an RTT capacity are measured and stored under
`protocol_capacities`. Open-loop scenarios derive
`target_rate = capacity[protocol] × load_fraction`. Without a compatible
calibration profile (same client / version / protocol), fraction points are refused.

Timing profiles (`PROFILE_SPECS`):

| Profile | Measure / warmup / drain | Runs | Comparable |
|---|---|---|---|
| `standard` | 12 s / 3 s / 6 s | 3 | yes |
| `smoke` | 3 s / 1 s / 2 s | 1 | no (`non_comparable`) |

### Dual protocol (`dual_protocol`)

A minimal **core** subset is expanded into both `MQTTv311` **and** `MQTTv5`:

- `pub_qos_sweep_telemetry`, `sub_exact_telemetry`
- `puback_latency_qos1`, `rtt_capacity_qos1`, `application_rtt_qos1`

Rankings and the HTML matrix use `scenario · protocol` rows — **never** a
cross-protocol comparison. `aiomqtt3` (v5 only) is compared with peers on the
`MQTTv5` rows; `amqtt` skips v5.

Open loop (`puback` / `application_rtt`): the full matrix of fractions **`0.50`,
`0.75`, `0.90`, `1.00`** for every client and every supported protocol. A single
per-client calibration carries both protocol capacities and is reused across the
whole matrix, so the baseline does not move between fractions.

### Fairness invariants enforced by the catalogue

- **In-flight window.** Every capacity point sets `inflight = outstanding`, so
  clients that expose `max_inflight` run the same effective pipelining window as
  clients that do not expose it and are bounded only by the harness gate. Only
  `pub_qos1_inflight` sweeps the window on purpose (and keeps
  `require_max_inflight`, so clients without the knob are refused rather than
  measured at a different window). `max_queued` is kept clear of the gate so it
  is never the binding constraint.
- **Broker reconciliation.** For single-publisher topologies the broker's `$SYS`
  received-publish counter is compared against what the adapter reported as
  completed; a run whose completions the broker cannot confirm is
  `inconclusive` (`broker_received_below_completed`), not a published number.
- **Broker headroom.** Peak broker CPU is recorded per run
  (`broker_cpu_max_pct`); above 70 % the run is `broker_limited` and does not
  enter a ranking, and 85 % remains the hard saturation signal.

## Topologies

| Topology | Actors | What the primary rate means |
|---|---|---|
| `publisher_only` | 1 SUT publisher | SUT publish completions |
| `publisher_with_oracle` | 1 SUT publisher + N subscribers (same library) | Publish rate (integrity checked subscriber-side) |
| `fanout` | 1 SUT publisher + N subscribers | Publish rate under broker fan-out cost |
| `subscriber_ingress` | 1 SUT subscriber + **emqtt-bench** (N clients) | Messages **delivered** to the subscriber |
| `application_rtt` | SUT initiator + responder (same library, `orch` cpuset) | Completed request/response pairs |
| `duplex_gateway` | SUT publisher + SUT subscriber + emqtt-bench injection | Publisher throughput (commands at 200/s) |
| `broker_ceiling` | emqtt-bench pub + emqtt-bench sub, no Python SUT | Reference subscriber receive rate |
| `connect` | Probe inside the orchestrator (pinned to the `sut` cpuset) | Connect latency / success |
| `fleet` | N idle connections (orchestrator, `sut` cpuset) | Keepalive / RSS / CPU cost |

## Cadences

| Cadence | Behaviour |
|---|---|
| `capacity` | Closed-loop maximum (no `target_rate`) |
| `loaded75` / fractions | Open loop; the point carries `load_fraction` → calibrated `target_rate` |
| `steady50` | Open loop at 50 % of a base (default 2000 → **1000 msg/s**) |
| `burst` | Ingress: bounded burst (`-L`), then silence; recovery observed during drain |
| `microburst` | Like burst with `-L 1000` |
| `periodic10` | Ingress at **10 msg/s** aggregate |

## Ingress offer — emqtt-bench vs paced hammer

For QoS0 exact-topic `subscriber_ingress` capacity the harness targets
`DEFAULT_INGRESS_OFFER_MSGS_PER_S` (**200,000** msg/s) with **paced
mqtt_hammer** (`scripts/mqtt_hammer.c`, `--rate 200000`, two publisher
threads). Confirmed on the reference host: hammer `--rate 200000` with no
subscriber → **189,777** msgs/s and `$SYS received` matched 1.000 (the
writes are decoded PUBLISHes, not a TCP-buffer fiction). With a subscriber,
TCP back-pressure may slow the publisher; the SUT score is still
callback deliveries.

emqtt-bench `-I` is milliseconds: 150×`I=1` does not hold 150k (`$SYS
received` ~98k here). Templated topics (`%i`) and QoS>0 stay on emqtt-bench,
capped at **100,000**. `clamp_emqtt_offer` **raises** the publisher count
so that I=1 can hold that cap — 32 catalogue clients must not silently
stay at 32k after the ranking default moved to 200k. Burst recovery is
an exception: it keeps the I=1 offer of the configured client count
(32k), not the ranking target. Compare against `effective_offer_msgs_per_s`
/ `observed_pub_rate` — emqtt-bench QoS0 `pub` rates are double-counted (see
[docs/CEILING_PROBES.md](docs/CEILING_PROBES.md)). Ceiling probes still pin
32k / 64k / 128k (hammer can hold those; emqtt cannot hold 128k).

If delivered ≈ offer, the point is **offer_limited**. A slow client well
below half the offer stays **valid** + **sut_limited** on core capacity
points. Mosquitto 2.1 is single-threaded: a 200k offer routinely pegs broker
CPU while clients still differentiate; that CPU is recorded
(`broker_cpu_max_pct`) but does not invalidate ranking `sub_*`. Diagnostic
and `broker_ceiling` points still fail closed at ≥ 70 % / 85 %.

Mosquitto 2.1 is single-threaded: do not widen the broker cpuset.

---

## Suite `core`

### `pub_payload_sweep_qos0`

- **Goal**: QoS0 publisher capacity across payload sizes.
- **Topology**: `publisher_only` · **Cadence**: `capacity`.
- **Variants**: `empty0`, `binary64`, `telemetry256`, `event1k`, `record16k`, `block64k`, `blob1m`.
- **Primary**: publish completions / s.
- **Reading**: size → throughput curve; `blob1m` can also saturate broker CPU / network.

### `pub_qos_sweep_telemetry`

- **Goal**: publisher capacity for QoS 0 / 1 / 2, fixed `telemetry256` payload.
- **Topology**: `publisher_only` · **Cadence**: `capacity` · tag `dual_protocol`.
- **Variants**: `qos_publish ∈ {0,1,2}`.
- **Primary**: completions / s (QoS0 = handed to transport; QoS1 = PUBACK; QoS2 = PUBCOMP).
- **Refusals**: clients without correct QoS2 (`gmqtt`, `awscrt` → `not_implemented:qos2`).

### `pub_qos1_inflight`

- **Goal**: effect of the client in-flight window on QoS1 capacity.
- **Topology**: `publisher_only` · **Cadence**: `capacity` · tag `diagnostic`.
- **Variants**: `inflight ∈ {1,20,100}` (+ `max_queued = 10×`, `outstanding = max(n,8)`).
- **Requires**: `max_inflight` / `max_queued` adapter support (`require_max_*`).
- **Refusals**: gmqtt, awscrt, amqtt and others without the knobs → `not_implemented:max_inflight`.

### `remaining_length_boundaries`

- **Goal**: exact MQTT *Remaining Length* encoding-width transitions (1 vs 2 bytes).
- **Topology**: `publisher_only` · **Cadence**: `capacity` · tag `diagnostic`.
- **Variants**: payloads `rl_126` … `rl_16384` (sized to hit those thresholds).
- **Reading**: protocol/encoder diagnostic, not a product ranking.

### `sub_exact_telemetry`

- **Goal**: **ingress** capacity: N external publishers → 1 exact topic → 1 SUT subscriber.
- **Topology**: `subscriber_ingress` · **Cadence**: `capacity` · tag `dual_protocol`.
- **Loadgen**: paced mqtt_hammer `--rate 200000` (two QoS0 publishers). emqtt-bench cannot hold 150k on one loadgen core.
- **Primary**: messages delivered to the subscriber callback / s.
- **Reading**: compare with **`loadgen.effective_offer_msgs_per_s`**. Do not use QoS0 `parsed.median_rate` as the offer. If delivered ≈ offer, the point is **offer_limited**. Campaign JSON measured at 32k / Mosquitto 2.0.20 is not comparable.
- **`$SYS`**: `sys_counters` (drops/sent/received) recorded over the measure window; loadgen vs `$SYS received` must reconcile.

### `sub_hierarchy_telemetry`

- **Goal**: same ingress, but with a broker wildcard subscription (`+` or `#`) over the `fleet4k_uniform` topology.
- **Variants**: `subscription=plus` and `subscription=hash`.
- **Primary**: deliveries / s.
- **Reading**: broker + client matching cost; same offer-ceiling caveat as `sub_exact`.

### `sub_callback_matching`

- **Goal**: cost of **local** `message_callback_add` matching (Paho).
- **Topology**: `subscriber_ingress` · broker subscription `#`, loadgen publishes on `cb/<i>/…` · tag `diagnostic`.
- **Loadgen**: templated `cb/%i/data` → emqtt-bench. Offer is the 200k ranking default clamped to **100k** (emqtt I=1 cap); publisher count is raised to 100 so variants 1 / 16 / 256 share the same offer. Campaign JSON that still shows 32k for filters=1/16 and 100k for 256 predates this equalization and is not internally comparable across the filter sweep.
- **Variants**: `callback_filters ∈ {1,16,256}`.
- **Refusals**: clients without `native_message_callback_add` (everything except paho / mqttium-compat).

### `duplex_gateway`

- **Goal**: "gateway" load: the SUT publishes telemetry **and** receives injected commands.
- **Topology**: `duplex_gateway` (pub + sub on the `sut` cpuset).
- **Injection**: emqtt-bench → `bench/<run>/commands` at **200 msg/s** aggregate (2 clients).
- **Variants**: publisher cadence `steady50` or `burst`.
- **Primary**: publisher throughput (usually capped by the cadence); **not** a pure capacity ranking.
- **Report**: excluded from the throughput chart (rate-capped scenario). Note this exclusion lives in the report layer, not in the catalogue.

### `burst_recovery`

- **Goal**: behaviour and recovery under an ingress burst followed by silence.
- **Topology**: `subscriber_ingress` · **Cadence**: `burst` · `#` subscription · fleet topics.
- **Loadgen**: starts at `T_MEASURE`, `-L ≈ target × duration`, `I=1`.
- **Primary**: delivered rate during the window; drain exposes the backlog.
- **Reading**: same ceiling family as the other ingress capacity scenarios.

### `e2e_integrity`

- **Goal**: sequence integrity (`PMQ1` header) publisher → subscriber, same library.
- **Topology**: `publisher_with_oracle` · **Cadence**: `steady50` (~**1000** msg/s) · tag `functional`.
- **Variants**: QoS 0/1/2 + empty QoS0 payload; `force_header=True`.
- **Primary**: achieved throughput (always ~capped); the substance is missing/dup/ooo.
- **Report**: excluded from the throughput chart (report-layer exclusion).

### `puback_latency_qos1`

- **Goal**: open-loop PUBACK latency at fractions of the client's **publish capacity**.
- **Topology**: `publisher_only` · fractions `0.50 / 0.75 / 0.90 / 1.00` · tag `dual_protocol`.
- **Requires**: `--load-profile` (or automatic calibration in `compare`).
- **Invalidation**: `open_loop_rate_out_of_tolerance` when the achieved rate deviates > 2 % from target.

### `puback_latency_fixed_rate`

- **Goal**: PUBACK latency at **absolute** offered rates, identical for every client.
- **Topology**: `publisher_only` · rates `1,000 / 2,500 / 5,000 / 10,000` msg/s · tag `dual_protocol`.
- **Requires**: nothing — the rate is absolute, so no calibration is involved.
- **Reading**: this is the scenario for comparing latency *between* clients. The
  fraction-based `puback_latency_qos1` is not: it paces each client at a share of
  its own ceiling, so a client with more headroom is offered a higher absolute
  rate and compares unfavourably for that reason alone.
- **Refusals**: a client that cannot sustain a rate is `offer_limited`, which is
  the honest outcome and is more informative than a number.

### `rtt_capacity_qos1`

- **Goal**: closed-loop capacity of application RTT pairs (same library on both sides).
- **Topology**: `application_rtt` · **Cadence**: `capacity` · `outstanding=32` · tags `diagnostic`, `dual_protocol`.
- **Primary**: completed pairs / s → baseline for `application_rtt_qos1`.
- **Note**: amplifies stack cost (two publishes and two deliveries per sample).

### `application_rtt_qos1`

- **Goal**: open-loop application RTT latency at fractions of **that** RTT capacity.
- **Topology**: `application_rtt` · fractions `0.50 / 0.75 / 0.90 / 1.00` · tag `dual_protocol`.
- **Requires**: end-to-end `TCP_NODELAY` (broker + client); otherwise a Nagle artefact of ~84 ms/pair.
- **Refusals**: `awscrt` → `not_implemented:tcp_nodelay`.

---

## Suite `full`

### `pub_segment_threshold_16k` / `pub_segment_block_64k` / `pub_segment_blob_1m`

- **Goal**: publisher capacity at "segmented" sizes (16 KiB / 64 KiB / 1 MiB), QoS0.
- **Topology**: `publisher_only` · **Cadence**: `capacity`.
- **Reading**: fragmentation / copy diagnostic; one point each.

### `payload_stress`

- **Goal**: payload stress (8 MiB, str encoding, large QoS1).
- **Variants**: `blob8m` QoS0, `telemetry256_str` QoS0, `block64k`/`blob1m` QoS1.

### `topic_stress`

- **Goal**: topic stress (depth, length, unicode) plus extreme callback matching.
- **Topology**: `subscriber_ingress` · 16 loadgen clients.
- **Variants**: `deep32`, `long_topic_{256,1024}`, `unicode`, `callback_filters=4096`, overlapping × 8.

### `sub_multi_subscribe`

- **Goal**: N exact subscriptions on a single client.
- **Variants**: `subscription_count ∈ {16,256}` · `subscription=multi_exact`.

### `fanin_scaling`

- **Goal**: fan-in scaling, publishers → 1 subscriber.
- **Modes**:
  - `constant_aggregate`: fixed aggregate target (~40k), 1 / 16 / 128 clients;
  - `per_publisher`: ~1000 msg/s **per** client → aggregate = `clients × 1000`.
- **Reading**: connection storm vs throughput; useful to see whether the ceiling moves with N.

### `fanout_scaling`

- **Goal**: 1 SUT publisher → N subscribers (same library).
- **Topology**: `publisher_with_oracle` · **Variants**: `subscribers ∈ {1,8,32}`.
- **Primary**: publisher side (broker fan-out cost + N client stacks).

### `periodic_and_microburst`

- **Goal**: extreme traffic shapes (very slow / micro-burst).
- **Variants**: `periodic10` (10/s), `microburst` (`-L 1000`).

### `mqttv5_properties`

- **Goal**: cost of "realistic" v5 PUBLISH properties vs v3.1.1 / v5 without properties.
- **Topology**: `publisher_with_oracle`.
- **Variants**: `MQTTv311/none`, `MQTTv5/none`, `MQTTv5/realistic`.

### `mqttv5_rich`

- **Goal**: heavy properties. The `topic_alias` / `subscription_identifier` variants are usually refused (`not_implemented:*`) until the adapters implement them.

### `qos_asymmetric`

- **Goal**: asymmetric pub/sub QoS pairs at bounded rate (`steady50`).
- **Variants**: (1,0), (2,1), (0,1).

### `session_resume_qos1`

- **Goal**: does a persistent session actually replay the QoS 1 backlog that piled up while the subscriber was away?
- **Topology**: `publisher_with_oracle` · **Cadence**: `steady50` (~1000 msg/s) · tag `functional`.
- **Outage**: the subscriber issues a plain `DISCONNECT` mid-window, stays offline for `outage_s`, then reconnects with the same client id and `clean_session=0` — **without re-subscribing**, since replaying the subscription would defeat the point. MQTT retains session state whenever Clean Session = 0, and Mosquitto keeps it in memory even with `persistence false` (that setting only disables the on-disk copy; sessions are lost on *broker restart*, which this scenario never does).
- **Requires**: an adapter that can `connect()` again after `disconnect()` (`AdapterCapabilities.reconnect`); otherwise the point is refused with `not_implemented:reconnect`.
- **Primary**: publisher throughput, pinned by the cadence — **not** a ranking. The substance is the integrity counters plus `delivered_after_resume` and `session_present_on_resume` on the subscriber worker.
- **Reading**: `missing ≈ outage_s × rate` means the session was **not** resumed and the backlog was dropped. `delivered_after_resume = 0` means nothing at all came back after reconnect. Both are results, not run failures, so the run stays `valid` and the counters carry the verdict.
- **Caveat**: a loss here is not automatically a verdict on the library — it can equally be the bench adapter rebuilding its client on reconnect. Attribute before quoting.
- **Report**: excluded from the throughput chart (rate-capped).

### `reconnect_ordering`

- **Goal**: message loss and ordering across a reconnect, at two outage lengths.
- **Topology**: same as above · **Variants**: `outage_s ∈ {1.0, 3.0}`.
- **Primary**: `out_of_order` and `duplicates` from `integrity_counts`, on top of `missing`.
- **Report**: excluded from the throughput chart.

#### Outage variants deliberately left out

The graceful disconnect above needs no privilege and stays comparable everywhere.
Two stronger outages were considered and are **not** implemented:

| Variant | Extra coverage | Why it is not here |
|---|---|---|
| Abrupt drop (`SO_LINGER=0` + `close()` → RST) | QoS 1 messages unacknowledged *at* the break, and their DUP retransmission | Needs access to the live socket. Paho exposes it publicly; asyncio clients hide it behind a transport, so it would have to become an adapter capability with reduced client coverage. |
| Silent blackhole (`network.blackhole()`) | How long a client takes to notice a dead link | Needs `tc` + `CAP_NET_ADMIN`, so it cannot run everywhere — and it mostly measures the configured keepalive rather than the library. |

### `network_matrix`

- **Goal**: the same publish load under `localhost` / `lan` / `wan` / `edge` netem profiles.
- **Marking**: any `network ≠ localhost` → `non_comparable` (machine/kernel diagnostic).

### `tls_steady_state`

- **Goal**: QoS1 publish capacity over an **already established** TLS session (not mass handshakes).
- **Topology**: `publisher_only` · `tls=True`.

### `connect_latency_and_churn`

- **Goal**: TCP/TLS connect latency and connection storms.
- **Topology**: `connect` (runs inside the orchestrator, pinned to the `sut` cpuset).
- **Variants**: serial TCP/TLS, `tls_resume`, concurrent 32/256.
- **Partial refusals**: some variants are `not_implemented:*` depending on the adapter.

### `client_fleet_idle`

- **Goal**: cost of an idle fleet (30 s keepalive): RSS / CPU.
- **Topology**: `fleet` · sizes 1 / 32 / 256.
- **Refusals**: `async_bridged` clients (one loop/thread per connection) → `fleet_async_bridged`.

### `broker_ceiling_ingress`

- **Goal**: **Mosquitto** ceiling probe without a Python SUT (emqtt pub + emqtt sub).
- **Suite**: `full` · tags `diagnostic` · `non_comparable`.
- **Variants**: `loadgen_clients ∈ {32,64,128}` → effective offer 32k / 64k / 128k (`I=1`).
- **Primary**: reference subscriber `recv` rate.
- **Runbook**: [docs/CEILING_PROBES.md](docs/CEILING_PROBES.md).

### `client_ceiling_ingress`

- **Goal**: same offer grid with a SUT subscriber — does the client break before the broker?
- **Topology**: `subscriber_ingress` · same offer as `broker_ceiling_ingress`.
- **Primary**: SUT delivered vs `effective_offer` + `$SYS`.
- **Runbook**: [docs/CEILING_PROBES.md](docs/CEILING_PROBES.md).

---

## `planned` scenarios (catalogue only)

Not executed by the suites; forcing one yields `not_implemented:planned_scenario`:

| Name | Intent |
|---|---|
| `mqttv5_flow_control` | Broker `Receive Maximum` vs client in-flight interaction |
| `queue_rejection` | Accept/reject accounting under queue pressure |
| `retained_bootstrap` | Massive retained snapshot (very broker-sensitive) |

---

## Suite `experimental`

Same measurement contract as `core`, but with separate rankings for experimental
clients (`zmqtt`, `aiomqtt3`, `mqttium`, `mqttium-compat`). See the README.

---

## How to read a result

1. Look at `status` / `reasons` (capability refusal, broker CPU, open loop out of tolerance, broker reconciliation).
2. Look at `bottleneck` (`sut_limited` / `broker_limited` / `broker_unconfirmed` / `loadgen_limited` / `offer_limited`) — a heuristic, not absolute truth.
3. For ingress: compare the primary metric with `loadgen.effective_offer_msgs_per_s` (or `nominal_rate`). Do **not** treat QoS0 `parsed.median_rate` as real msgs/s (emqtt-bench double counting).
4. Do not read `duplex_gateway` / `e2e_integrity` as throughput races: they are **deliberately rate-capped**.
5. Latency: only compare points at the **same fraction** and calibrated from the **same** client.
6. Broker/client ceilings: see [docs/CEILING_PROBES.md](docs/CEILING_PROBES.md) (`broker_ceiling_ingress`, `client_ceiling_ingress`).
7. Rankings are only meaningful within a peer group: same `io_model`, same MQTT protocol, and stable clients separate from experimental ones.

## Related files

| File | Role |
|---|---|
| `src/mqtt_client_bench/scenarios.py` | Declarations + profiles + expansion |
| `src/mqtt_client_bench/harness.py` | Orchestration, loadgen, validation, broker reconciliation |
| `src/mqtt_client_bench/loadgen.py` | emqtt-bench, `nominal_rate`, parsing |
| `src/mqtt_client_bench/sys_probe.py` | `$SYS` probe: dropped / sent / received |
| `src/mqtt_client_bench/workloads.py` | Payloads, topics, integrity header |
| `src/mqtt_client_bench/roles/` | publisher / subscriber / RTT / responder workers |
| `docs/CEILING_PROBES.md` | Broker / client ceiling runbook |
| `README.md` | Bench overview and CLI commands |
