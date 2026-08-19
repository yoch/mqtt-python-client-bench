# Mosquitto ingress profiling

Why core `sub_*` campaigns used to plateau at ~30k msgs/s, what actually
limits the broker, and why the bench now defaults to upstream Mosquitto 2.1.

## The 30k number was the offer, not a Mosquitto cap

`sub_exact_telemetry` used to run emqtt-bench at `-c 32 -I 1`. That is a
**32,000 msgs/s offer** (`clients × 1000 / interval_ms`). Fast clients
(gmqtt, mqttium, awscrt) all delivered ~30.2k with `bottleneck=broker_limited`
because broker CPU crossed the 70 % headroom gate on the **reference host**.
They were matching the offer; the gate fired anyway.

On a 4-core Xeon with the broker pinned to **one core**, Mosquitto 2.0.20
(upstream Alpine image, MQTT v5, 256-byte QoS0, one emqtt-bench subscriber)
does:

| emqtt-bench `-c` | Offer (real) | Delivered | Broker CPU (1 core) | Loadgen CPU (1 core) |
|---|---:|---:|---:|---:|
| 32 | 32k | 32k | 20 % | 35 % |
| 64 | 64k | 64k | 39 % | 60 % |
| 100 | 100k | 100k | 60 % | 92 % |
| 128 | 128k | ~97k | 60 % | 93 % + `pub_overrun` |

So on this class of machine the 50–100k band is reachable. At 128 clients the
**loadgen** cpuset saturates first. Do not widen the broker cpuset: Mosquitto
2.1 is still single-threaded.

A closed-loop C hammer (no 1 ms pacing) pushed the same Docker broker to
~150–190k delivered. Pacing at 1 msg/ms/connection is a different shape:
one small PUBLISH per socket per wakeup, which is exactly what
`packet__read()` is slow at.

## Hot path

Every incoming PUBLISH does roughly:

1. `packet__read()` — **one `read(2)` per MQTT header byte**, then one read
   for the remaining payload. A 256-byte PUBLISH is ~4 syscalls before any
   MQTT work.
2. `handle__publish()` — `calloc` a `mosquitto_msg_store`, malloc topic,
   malloc+copy payload, `strdup` the publisher client id, ACL check (cheap
   when no ACL file), `plugin__handle_message` (NULL-check exit).
3. `sub__messages_queue` → `db__message_insert` → `send__real_publish` —
   another packet alloc, copy topic+payload, `write(2)` to the subscriber.

`mux_epoll` then calls `packet__read` once per `EPOLLIN` unless
`SSL_pending()` is set. Level-triggered epoll hides the one-packet-per-event
limit for the unbuffered path (the kernel still has data). A userspace
read-ahead buffer **must** drain remaining buffered bytes in that loop, or
coalesced packets stall until the next TCP segment.

## Upstream: do not PR this patch to Mosquitto develop

Mosquitto **2.1** already replaced the per-byte header `read(2)` with a
proper parse-from-buffer path (`packet_buffer_size`, default 4096, see
`read_header()` in `lib/packet_mosq.c` on `master`). That is the same idea,
done in the parser rather than as a `net__read` shim, and it drains leftover
buffered bytes in `packet__read`'s outer loop.

- **Do not open a PR against `develop` / 2.1** with
  `0001-socket-read-ahead-buffer.patch` — it would duplicate a better fix.
- **2.0.22 still has the per-byte loop.** A backport of *their* 2.1 buffer
  (not this shim) could be offered to the 2.0.x branch if that line is still
  maintained; our shim is a bench-local workaround for the 2.0.20 image.
- Remaining 2.0 hot-path cost after the syscall tax is gone:
  `handle__publish` (`calloc` msg_store, malloc topic, malloc+copy payload,
  `strdup` source_id) and `send__real_publish` (another packet alloc + copy +
  `write(2)`). Those are architectural; compile flags (jemalloc, `-O3`) moved
  the needle by a few CPU points here. TLS still goes through `SSL_read`
  before the shim. Rebasing the bench image on 2.1 would pick up the official
  buffer and is the cleaner long-term path, but it changes more than ingress
  syscalls (defaults such as `max_packet_size`).

`/proc/<pid>/io` at 100k msgs/s, same flags, one core:

| Binary | `syscr` / s | Broker CPU | Delivered |
|---|---:|---:|---:|
| 2.0.20 `-O2`, no buffer | 400k | 65 % | 100k |
| 2.0.20 `-O2` + 8 KiB read-ahead | 99k | 61 % | ~99k |
| 2.0.20 `-O3 -flto` + jemalloc + read-ahead | 99k | 63 % | ~99k |

The patch removes the 4× header-syscall tax. On a fast Xeon that is only a
few CPU points; on a host that was already at 70 % for 32k, it is the
difference between the 70 % gate firing or not as the offer rises.

Compile-flag wins (jemalloc, `-O3`, no `WITH_MEMORY_TRACKING`, no
libwebsockets) are smaller than the syscall change on this path.

## What the bench now ships

- Default broker: **eclipse-mosquitto:2.1.2-alpine** (upstream `packet_buffer_size`).
  `mosquitto/Dockerfile` + the 2.0.20 read-ahead patch remain as an optional A/B
  (`MQTT_BENCH_MOSQUITTO_IMAGE=mqtt-bench-mosquitto:2.0.20-fast` after a local
  `docker build`). Mixed images are not comparable.
- Core `sub_*` QoS0 exact-topic capacity uses **paced mqtt_hammer at 150k**
  (`--rate 150000`, two publisher threads). emqtt-bench `-I` is milliseconds, so
  150×`I=1` overruns on one loadgen core (~95–120k observed here). Templated
  topics and QoS>0 stay on emqtt-bench. `MQTT_BENCH_LOADGEN=emqtt` forces it;
  `MQTT_BENCH_LOADGEN=hammer` is a **paced 200k** diagnostic (never a firehose).
- `broker_ceiling_ingress` / `client_ceiling_ingress` still sweep 32k / 64k /
  128k.

JSON already committed under `results/` was measured at the 32k offer against
upstream `eclipse-mosquitto:2.0.20`. Re-run the campaign before publishing a
new ranking.

## Reproducing the ceiling probe

```bash
make -C scripts/mosquitto_profile
# Pulls eclipse-mosquitto:2.1.2-alpine (override MQTT_BENCH_MOSQUITTO_IMAGE to A/B)
python -m mqtt_client_bench.run broker up

# Optional 2.0.20-fast A/B: docker build -t mqtt-bench-mosquitto:2.0.20-fast mosquitto/
# Native A/B: clone v2.0.20, apply the patch, see
# scripts/mosquitto_profile/build_broker.sh
```

The C hammer in `scripts/mosquitto_profile/mqtt_hammer.c` is MQTT 3.1.1 QoS0
only. It **is** the ranking loadgen for QoS0 exact-topic ingress, paced with
`--rate` (busy-wait slot; `nanosleep` cannot hold a 5 µs period). Default
ranking rate is 150k; `MQTT_BENCH_LOADGEN=hammer` caps at 200k. Omit `--rate`
only for an explicit firehose probe. Templated topics (`%i`) and QoS>0 stay
on emqtt-bench.

The optional `mqtt-bench-mosquitto:2.0.20-fast` image does not understand
`packet_buffer_size`; drop that line from `mosquitto/mosquitto.conf` before
an A/B against 2.0.

Diagnostic subscribe smoke at 200k (not ranking — `non_comparable`, do not commit):

```bash
export PYTHONPATH=src MQTT_BENCH_LOADGEN=hammer
python -m mqtt_client_bench.run run --scenario sub_exact_telemetry \
  --client mqttium --profile smoke \
  --output logs/smoke/mqttium-sub_exact_telemetry-hammer-smoke.json
```

`--rate 200000` is an aggregate cap. Read `loadgen.observed_pub_rate` (must
stay ≤ 200k) and `primary_msgs_per_s`.
