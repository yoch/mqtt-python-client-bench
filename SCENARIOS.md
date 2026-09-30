# Scenarios

How every point in `src/mqtt_client_bench/bench/catalog.py` is wired, what
each party counts, and which checks decide its status. The catalogue is the
source of truth; this file explains it.

## Common to every run

- **Topics.** `bench/<run_id>/data` and `bench/<run_id>/reply`, unique per
  run, so a straggler from a previous run can never be counted.
- **Payload.** 256 bytes unless stated. The first 8 bytes are a little-endian
  `CLOCK_MONOTONIC` stamp; the rest is filler.
- **Processes.** Each party runs in its own process on its own physical core
  (`taskset`): the client worker (the client's own uv environment), the C peer,
  Mosquitto (a Docker cpuset) and the orchestrator.
- **Start order.** The listening party connects first: the peer on publish and
  round-trip points, the worker on receive points. Both print `ready`, then
  receive the same absolute schedule `GO t_start t_measure t_end t_stop`
  (`CLOCK_MONOTONIC` ns), sent 300 ms ahead.
- **Phases.**
  - `[t_start, t_measure)` is warm-up.
  - `[t_measure, t_end)` is the measured window: rates, costs and latency come
    only from here.
  - `[t_end, t_stop)` drains in-flight traffic, so totals can be reconciled.
- **Counters.** The worker snapshots its counters at `t_measure`, `t_end` and
  `t_stop` from a timer thread: sent, completed, failed, rejected, received and
  skipped. Nothing is sampled from inside the drive loop.
- **Resources.** The orchestrator reads the worker's and the peer's `/proc`
  (per-thread `schedstat` CPU, utime/stime, RSS, peak RSS reset at
  `t_measure`, context switches) at `t_measure` and `t_end`. It reads the
  broker's CPU from its cgroup and host CPU from `/proc/stat`.
- **Broker.** A raw-socket `$SYS` probe takes a fresh reading before GO and
  after `t_stop`. The probe's own traffic is subtracted.

## Publish points (`pub_*`)

The peer runs as a **sink**: it subscribes to `data` at the point's QoS,
counts every PUBLISH, and records the latency from the payload stamp to
arrival.

| Point | QoS | Payload | Rate | Measures |
|---|---|---|---|---|
| `pub_qos0_max` | 0 | 256 B | capacity | messages handed to the transport per second, confirmed by broker and sink |
| `pub_qos1_max` | 1 | 256 B | capacity, 64 in flight | PUBACKs per second |
| `pub_qos1_fixed` (+ `_v5`) | 1 | 256 B | 2,000/s | CPU µs/msg, RSS, publish→delivery latency |
| `pub_16k_fixed` | 1 | 16 KiB | 1,000/s | the same at a large payload |

The extended suite adds more publish points; see [Extended suite](#extended-suite).

**Completion contract.** QoS 0 completes when the payload reaches the
transport; QoS 1 completes on PUBACK; QoS 2 completes on PUBCOMP.

**Capacity points** keep `window` publishes outstanding and stop at `t_end`.
The reported rate is what the sink received in the window.

**Fixed points** tick every millisecond and catch up at most 4 ticks late, so
a stalled client shows as skipped sends instead of a burst.

**Schedule lag** (fixed publish and round-trip points). Message `n` of the
offer is due at `t_start + ceil((n + 1) / rate)`. The stamp is the actual
publish time, so latency is transit only. The worker also keeps each send
time, and after the run it reports send minus due over the messages due in
the window, plus how many of them were never published. The awaited shape
hands workers credits for specific message indices, so a publish that waited
for a free worker counts its wait. Up to 1 ms of lag is the pacing tick, and
that part is the same for every client.

**Checks**

- `broker_confirms_client_publishes`: `$SYS` received lies between the
  client's completions and its sends.
- `broker_confirms_deliveries`: `$SYS` sent equals what the sink received.
- `no_loss` (QoS 1 and 2): the sink received at least every acknowledged
  publish.
- `payloads_intact`: every payload the sink received has one of the lengths
  the client published.
- `offered_rate_held` (fixed rate): at least 98 % of the offer was sent in the
  window; a failure here means `not_sustained`.
- `broker_headroom` (fixed rate): the broker stayed below 85 % of its core.

## Receive points (`sub_*`)

The peer runs as a **source**: it publishes stamped payloads to `data` at the
given rate. The client subscribes, counts deliveries and records latency in
its own callback.

| Point | QoS | Rate | Measures |
|---|---|---|---|
| `sub_qos0_max` | 0 | receive offer | messages received per second |
| `sub_qos1_fixed` (+ `_v5`) | 1 | 2,000/s | receive cost, delivery latency |

**Receive offer.** At session start the orchestrator measures the broker's
C→C ceiling (peer source → peer sink, no Python). It offers 90 % of that
ceiling, so the client, not Mosquitto, is the first limit.

**Checks**

- `broker_confirms_peer_publishes`: `$SYS` received equals what the source
  wrote.
- `broker_confirms_deliveries`, which depends on the point:
  - fixed rate: `$SYS` sent equals what the client received;
  - capacity: the client received no more than was sent. A client slower than
    the offer still has data in its socket buffers at `t_stop`, and the gap is
    reported as `undelivered_at_stop`.
- `no_loss`, on fixed-rate QoS 1 and 2 points: the client received every publish the
  broker acknowledged. When the broker dropped messages from its queue, the
  run is `not_sustained`; without drops it is `invalid`.
- `offered_rate_held`: the source wrote its offer.
- `client_kept_up`: the client received at least 98 % of it in the window.
  A failure here means `not_sustained`.

**Flags**

- `offer_bound`: the client received the whole capacity offer, so its real
  capacity is higher.
- `broker_bound`: Mosquitto was at its core limit.
- `broker_queue_overflow`: a QoS 1 capacity point exceeded Mosquitto's queue
  limit. That is the measurement, not a failure.

## Round-trip points (`rtt_*`)

The peer runs as an **echo**: it subscribes to `data` and republishes every
payload unchanged to `reply`. The client publishes stamped requests at the
fixed rate, subscribes to `reply`, and records `now − stamp` for each reply.
That is the whole application round trip through two broker hops and the C
echo. Requests carry a schedule lag, as on publish points.

| Point | QoS | Rate |
|---|---|---|
| `rtt_qos1_fixed` (+ `_v5`) | 1 both ways | 1,000 req/s |

**Checks**

- `broker_confirms_client_publishes`: `$SYS` received minus the echo's
  republishes matches the client's sends.
- `broker_confirms_deliveries`: `$SYS` sent equals the replies the client
  received plus the requests the echo received.
- `no_loss` (QoS 1): the client received every reply the echo sent.
- `offered_rate_held` and `responses_kept_up`: at least 98 % of requests were
  sent and answered in the window. A failure of any of these three means
  `not_sustained`.

## `idle_connect`

There is no peer. The worker connects, then sits idle through the schedule
with only keepalive traffic.

The report shows:

- `connect_ms`: time from the connect call to CONNACK;
- the idle CPU share;
- RSS and thread count.

The broker headroom check applies.

## Extended suite

Every point below reuses the wiring above; only what differs is described.
Fixed offers are the same absolute rates for every client, and a library
that lacks a feature is refused through its capabilities (`unsupported`,
`not_implemented:<feature>`), never approximated.

| Point | Kind | QoS | Payload | Rate | What differs |
|---|---|---|---|---|---|
| `sub_qos1_max`, `sub_qos1_max_v5` | sub | 1 | 256 B | receive offer | QoS 1 receive capacity, MQTT 3.1.1 and 5 |
| `pub_qos1_max_v5`, `pub_qos1_max_16k` | pub | 1 | 256 B, 16 KiB | capacity | protocol and payload variants |
| `pub_qos1_fixed_500`, `pub_qos1_fixed_5k` | pub | 1 | 256 B | 500/s, 5,000/s | load curve around the core's 2,000/s |
| `sub_qos1_fixed_500`, `sub_qos1_fixed_5k` | sub | 1 | 256 B | 500/s, 5,000/s | the same when receiving |
| `rtt_qos0_fixed` | rtt | 0 | 256 B | 1,000 req/s | round trip without acknowledgements |
| `pub_qos2_max`, `pub_qos2_fixed` | pub | 2 | 256 B | capacity, 2,000/s | exactly-once publish |
| `sub_qos2_fixed` | sub | 2 | 256 B | 2,000/s | exactly-once receive |
| `pub_fanout_fixed` | pub | 1 | 256 B | 2,000/s | 1,000 topics round-robin |
| `sub_fanin_fixed` | sub | 1 | 256 B | 2,000/s | 1,000 topics, one wildcard subscription |
| `sub_filters_fixed` | sub | 1 | 256 B | 2,000/s | the same, dispatched to 100 `message_callback_add` filters |
| `duplex_qos1_fixed` | duplex | 1 | 256 B | 1,000/s each way | one client publishing and receiving |
| `pub_qos1_fixed_tls`, `sub_qos1_fixed_tls` | pub, sub | 1 | 256 B | 2,000/s | the client on the TLS listener |
| `idle_connect_tls` | idle | | | | connect including the TLS handshake |
| `pub_qos1_fixed_v5_props`, `sub_qos1_fixed_v5_props` | pub, sub | 1 | 256 B | 2,000/s | four PUBLISH properties on every message |
| `pub_qos1_fixed_v5_alias` | pub | 1 | 256 B | 2,000/s | a 200-byte topic sent once, then as topic alias 1 |
| `sub_qos1_max_v5_rm16` | sub | 1 | 256 B | receive offer | the client sets Receive Maximum 16 |
| `pub_64k_fixed`, `pub_1m_fixed` | pub | 1 | 64 KiB, 1 MiB | 500/s, 50/s | large payloads |
| `pub_rl_boundaries` | pub | 1 | see below | 60/s | packets on each side of a remaining-length step |

**QoS 2.** The C peer completes every exchange: as a sink it answers PUBLISH
with PUBREC and PUBREL with PUBCOMP; as a source its reader thread answers
PUBREC with PUBREL and counts PUBCOMP as the completion. gmqtt and aiomqtt3
complete a QoS 2 publish at PUBREC and awscrt's QoS 2 fails against
Mosquitto, so all three are refused (`not_implemented:qos2`).

**Many topics.** With `topics = N` the publisher spreads its messages
round-robin over `data/<i/10>/<i%10>` for `i < N`, and the receiver subscribes
to `data/#`. On the client side, the drive loop's publishes go through one
extra call that picks the topic; every client at the point pays it. On
`sub_filters_fixed` the client registers a `message_callback_add` on
`data/<d>/+` for each `d < 100`, and its `on_message` only counts strays.
Only libraries that match filters natively (paho, mqttium) run it.

- `callbacks_matched`: every message reached a filter callback and none the
  catch-all.

**Duplex.** Two C peers: a sink on `data`, started before the worker, and a
source on `reply`, started after it, each at the point's rate. The client
publishes to `data` and subscribes to `reply`. CPU per message divides by the
messages handled in both directions. The report shows the publish-path
latency (to the sink) and the receive-path latency (to the client's callback)
side by side.

- `broker_confirms_client_publishes`: `$SYS` received minus what the source
  wrote matches the client's sends.
- `broker_confirms_deliveries`: `$SYS` sent equals what the sink and the
  client received.
- `no_loss` for the publish direction, `no_loss_inbound` for the receive
  direction.
- `offered_rate_held` for the client, `source_rate_held` for the C source,
  `client_kept_up` for the client's receive side.

**TLS.** Mosquitto has a second listener on 127.0.0.1:11884 with a throwaway
CA and server certificate that `broker.ensure_certs()` generates into
`build/certs` on first use. Only the client connects over TLS; the C peer and
the `$SYS` probe stay on the plaintext listener. The listener accepts nothing
else, so a completed run is a TLS run.

**MQTT 5 properties.** The client attaches message expiry, content type
(`application/octet-stream`) and two user properties to every PUBLISH. There
is no payload format indicator, because the stamped payload is not UTF-8. The C sink counts the
messages that still carry a user property.

- `properties_delivered`: every message the sink received carried them.

On the receive point the C source attaches the same set, so the client's
library parses them on every delivery.

**Topic alias.** The data topic is padded to 200 bytes. The client's first
PUBLISH carries the topic and alias 1; every later one carries an empty topic
and the alias. Only libraries whose API accepts that are run.

- `topic_alias_used`: the broker's `$SYS/broker/bytes/received`, minus the
  sink's acknowledgements, divided by the client's publishes, is below the
  payload plus half the topic. Without the alias each publish would carry the
  whole topic.

**Receive Maximum.** The client puts Receive Maximum 16 in its CONNECT, so
the broker keeps at most 16 QoS 1 deliveries unacknowledged towards it. No
third party can read that value back, so it has the same standing as the
capacity window: an adapter setting that the adapter tests pin, while the
delivered count is still broker-confirmed. gmqtt also sizes its outbound
packet-id pool from this setting, so the catalogue only uses it on receive
points.

**Remaining-length boundaries.** The client's PUBLISH packets have remaining
lengths 127, 128, 16,383, 16,384, 2,097,151 and 2,097,152 in turn: each pair
straddles a step of the remaining-length varint (1→2, 2→3 and 3→4 bytes). The
payload sizes are computed from the topic length, and the sink checks every
length with `payloads_intact`.

## Suites and profiles

| Suite | Points |
|---|---|
| `core` | the eight points above |
| `v5` | `pub_qos1_fixed_v5`, `sub_qos1_fixed_v5`, `rtt_qos1_fixed_v5` |
| `extended` | the 26 points of the [extended suite](#extended-suite) |

A campaign runs `core,v5` by default (about 55 minutes on the standard
profile). `--suites core,v5,extended` runs everything in about 2 h 50 min, and
a test keeps that estimate under 95 % of 3 hours.

| Profile | Warm-up | Window | Drain | Runs | Published |
|---|---|---|---|---|---|
| `standard` | 2 s | 8 s | 2 s | 3 | yes |
| `smoke` | 0.5 s | 2 s | 1 s | 1 | never (`non_comparable`) |

Only `valid` runs enter a median. `not_sustained` runs appear as such in their
cell, and `invalid` or `unsupported` pairs appear in the report's coverage
section.
