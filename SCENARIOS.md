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
| `pub_qos0_fixed` (extended) | 0 | 256 B | 2,000/s | QoS 0 cost |
| `pub_qos0_max_1k`, `pub_qos1_max_16k`, `pub_qos1_max_v5` (extended) | | | capacity | payload / protocol variants |

**Completion contract.** QoS 0 completes when the payload reaches the
transport; QoS 1 completes on PUBACK.

**Capacity points** keep `window` publishes outstanding and stop at `t_end`.
The reported rate is what the sink received in the window.

**Fixed points** tick every millisecond and catch up at most 4 ticks late, so
a stalled client shows as skipped sends instead of a burst.

**Checks**

- `broker_confirms_client_publishes`: `$SYS` received lies between the
  client's completions and its sends.
- `broker_confirms_deliveries`: `$SYS` sent equals what the sink received.
- `no_loss` (QoS 1): the sink received at least every acknowledged publish.
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
| `sub_qos0_fixed`, `sub_qos1_max`, `sub_qos0_max_v5` (extended) | | | variants |

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
- `no_loss`, on `sub_qos1_fixed`: the client received every publish the
  broker acknowledged. When the broker dropped messages from its queue, the
  run is `not_sustained`; without drops it is `invalid`.
- `offered_rate_held`: the source wrote its offer.
- `client_kept_up`: the client received at least 98 % of it in the window.
  A failure here means `not_sustained`.

**Flags**

- `offer_bound`: the client received the whole capacity offer, so its real
  capacity is higher.
- `broker_bound`: Mosquitto was at its core limit.
- `broker_queue_overflow`: `sub_qos1_max` exceeded Mosquitto's queue limit.
  That is the measurement, not a failure.

## Round-trip points (`rtt_*`)

The peer runs as an **echo**: it subscribes to `data` and republishes every
payload unchanged to `reply`. The client publishes stamped requests at the
fixed rate, subscribes to `reply`, and records `now − stamp` for each reply.
That is the whole application round trip through two broker hops and the C
echo.

| Point | QoS | Rate |
|---|---|---|
| `rtt_qos1_fixed` (+ `_v5`) | 1 both ways | 1,000 req/s |
| `rtt_qos0_fixed` (extended) | 0 | 1,000 req/s |

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

## Suites and profiles

| Suite | Points |
|---|---|
| `core` | the eight points above |
| `v5` | `pub_qos1_fixed_v5`, `sub_qos1_fixed_v5`, `rtt_qos1_fixed_v5` |
| `extended` | the eight variants listed in the tables |

A campaign runs `core,v5` by default.

| Profile | Warm-up | Window | Drain | Runs | Published |
|---|---|---|---|---|---|
| `standard` | 2 s | 8 s | 2 s | 3 | yes |
| `smoke` | 0.5 s | 2 s | 1 s | 1 | never (`non_comparable`) |

Only `valid` runs enter a median. `not_sustained` runs appear as such in their
cell, and `invalid` or `unsupported` pairs appear in the report's coverage
section.
