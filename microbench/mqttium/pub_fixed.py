"""Fixed-rate QoS 1 publishing: where mqttium's extra CPU and latency come from.

Campaign points: ``pub_qos1_fixed`` (2,000/s) and ``pub_qos1_fixed_5k``.
Comparison: the two cheapest clients there, awscrt and gmqtt.

Each tick (1 ms) publishes exactly ``rate / 1000`` messages back to back, the
burst the campaign's 1 ms pacing produces, through the campaign adapter. A C
sink on its own core subscribes, so the broker forwards like in the campaign
and the sink's latency is end-to-end. Reported per message, over the window:

- ``cpu_us``: on-CPU time of every thread of the client process;
- ``loop_it``: asyncio loop iterations, ``nb_it`` those that polled with a zero
  timeout (they exist only because a callback was already queued: one extra
  ``epoll_wait`` and one pass of loop bookkeeping), ``call_soon``: callbacks
  scheduled;
- ``e2e_p50``: sink-side latency; with ``--trace-writes``, ``wire[k]`` is the
  median delay from the publish call to the moment the PUBLISH bytes reach
  ``transport.write``/``writelines`` for the k-th message of a burst
  (asyncio clients only; the hook costs the same for both).

Variants isolate one mechanism each in mqttium (diagnostics, not fixes):

- ``noobserve``: ``publish_nowait`` without the per-publish completion future
  the adapter must register since 1.0.0rc15 removed ``on_publish``;
- ``eager``: the writer's eager-write permit is re-armed immediately instead
  of once per loop turn, so every PUBLISH goes straight to the transport.

    PYTHONPATH=src .venvs/_all/bin/python microbench/mqttium/pub_fixed.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import threading
import time
from array import array
from asyncio import selector_events

import common
from mqttium.api._writer import WritePump
from mqtt_client_bench.adapters.registry import create_adapter, create_async_adapter
from mqtt_client_bench.bench import histogram

MARK = b"MBIX"
TICK_NS = 1_000_000
ASYNC_CLIENTS = ("mqttium", "gmqtt")


class WriteTracer:
    """Delay from publish call to transport write, by position in the burst."""

    def __init__(self, pub_ns: array, burst: int) -> None:
        self.pub_ns = pub_ns
        self.burst = burst
        self.delays = [[] for _ in range(burst)]
        self.calls = 0
        self.active = False
        cls = selector_events._SelectorSocketTransport
        orig_write, orig_writelines = cls.write, cls.writelines
        tracer = self

        def write(transport, data):
            tracer.see((data,))
            return orig_write(transport, data)

        def writelines(transport, list_of_data):
            list_of_data = list(list_of_data)
            tracer.see(list_of_data)
            return orig_writelines(transport, list_of_data)

        cls.write = write
        cls.writelines = writelines

    def see(self, chunks) -> None:
        if not self.active:
            return
        now = time.monotonic_ns()
        self.calls += 1
        for chunk in chunks:
            data = bytes(chunk)
            at = data.find(MARK)
            while at >= 0:
                index = int.from_bytes(data[at + 4:at + 8], "little")
                self.delays[index % self.burst].append(now - self.pub_ns[index])
                at = data.find(MARK, at + 8)

    def summary(self) -> dict:
        return {f"wire{k}": common.percentile_us(v, 0.5) for k, v in enumerate(self.delays)}


def make_publish(adapter, client: str, variant: str):
    if client != "mqttium" or variant == "default":
        return adapter.publish_nowait if client in ASYNC_CLIENTS else adapter.publish
    if variant == "noobserve":
        native = adapter._client

        def publish(topic, payload, qos):
            native.publish_nowait(topic, payload, qos=qos)
            return 1

        return publish
    if variant == "eager":

        def rearm_now(pump):
            pump._eager_armed = True
            pump._ack_eager_armed = True

        WritePump._schedule_eager_rearm = rearm_now
        return adapter.publish_nowait
    raise SystemExit(f"unknown variant {variant!r}")


class Window:
    def __init__(self, counters, profiler=None) -> None:
        self.counters = counters
        self.profiler = profiler
        self.marks = {}

    def mark(self, name: str, sent: int) -> None:
        if self.profiler and name == "end":
            self.profiler.stop()
        self.marks[name] = {
            "cpu": common.process_cpu_ns(),
            "ctx": common.ctx_switches(),
            "ru": common.rusage(),
            "sent": sent,
            "loop": self.counters.snapshot() if self.counters else None,
        }
        if self.profiler and name == "measure":
            self.profiler.start()

    def result(self) -> dict:
        a, b = self.marks["measure"], self.marks["end"]
        n = b["sent"] - a["sent"]
        out = {"msgs": n, "cpu_us": (b["cpu"] - a["cpu"]) / 1000 / n, "ctx_per_msg": (b["ctx"] - a["ctx"]) / n}
        out.update(common.rusage_per_msg(a["ru"], b["ru"], n))
        if a["loop"]:
            d = common.delta(a["loop"], b["loop"])
            out.update(loop_it=d["selects"] / n, nb_it=d["nonblocking"] / n, call_soon=d["call_soon"] / n)
        if self.profiler:
            out.update(self.profiler.report(n))
        return out


def payload_for(index: int, now: int, tail: bytes) -> bytes:
    return now.to_bytes(8, "little") + MARK + index.to_bytes(4, "little") + tail


async def drive_async(args, sched) -> dict:
    loop = asyncio.get_running_loop()
    adapter = create_async_adapter(args.client, client_id=f"mb-{os.getpid()}", protocol="MQTTv311",
                                   max_inflight=256, max_queued=10_000, tls_ca_certs=None)
    done = [0]

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        done[0] += 1

    adapter.on_publish = on_publish
    await adapter.connect(common.HOST, common.PORT, keepalive=60)
    publish = make_publish(adapter, args.client, args.variant)
    burst = args.rate // 1000
    pub_ns = array("q", [0]) * (args.rate * (args.warmup + args.seconds + 2))
    tracer = WriteTracer(pub_ns, burst) if args.trace_writes else None
    window = Window(common.LoopCounters(loop), common.WindowProfiler() if args.profile else None)
    tail = b"A" * (args.payload - 16)
    t_start, t_measure, t_end = sched["t_start"], sched["t_measure"], sched["t_end"]
    index = 0
    next_tick = t_start
    await asyncio.sleep(max(0, t_start - common.monotonic_ns()) / 1e9)
    while True:
        now = common.monotonic_ns()
        if now >= t_end:
            break
        if "measure" not in window.marks and now >= t_measure:
            window.mark("measure", index)
            if tracer:
                tracer.active = True
        for _ in range(burst):
            t = common.monotonic_ns()
            pub_ns[index] = t
            publish(args.topic, payload_for(index, t, tail), 1)
            index += 1
        next_tick += TICK_NS
        if next_tick < now:
            next_tick = now + TICK_NS
        delay = next_tick - common.monotonic_ns()
        if delay > 0:
            await asyncio.sleep(delay / 1e9)
    window.mark("end", index)
    if tracer:
        tracer.active = False
    await asyncio.sleep(1.0)
    out = window.result()
    out["completed"] = done[0]
    out["published"] = index
    if tracer:
        out.update(tracer.summary())
        out["writes_per_msg"] = tracer.calls / out["msgs"]
    await adapter.disconnect()
    return out


def drive_sync(args, sched) -> dict:
    adapter = create_adapter(args.client, client_id=f"mb-{os.getpid()}", protocol="MQTTv311",
                             max_inflight=256, max_queued=10_000, tls_ca_certs=None)
    connected = threading.Event()
    done = [0]
    adapter.on_connect = lambda *a, **k: connected.set()

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        done[0] += 1

    adapter.on_publish = on_publish
    adapter.connect(common.HOST, common.PORT, keepalive=60)
    adapter.loop_start()
    if not connected.wait(10):
        raise SystemExit("connect timeout")
    burst = args.rate // 1000
    window = Window(None)
    tail = b"A" * (args.payload - 16)
    t_start, t_measure, t_end = sched["t_start"], sched["t_measure"], sched["t_end"]
    index = 0
    next_tick = t_start
    time.sleep(max(0, t_start - common.monotonic_ns()) / 1e9)
    while True:
        now = common.monotonic_ns()
        if now >= t_end:
            break
        if "measure" not in window.marks and now >= t_measure:
            window.mark("measure", index)
        for _ in range(burst):
            adapter.publish(args.topic, payload_for(index, common.monotonic_ns(), tail), 1)
            index += 1
        next_tick += TICK_NS
        if next_tick < now:
            next_tick = now + TICK_NS
        delay = next_tick - common.monotonic_ns()
        if delay > 0:
            time.sleep(delay / 1e9)
    window.mark("end", index)
    time.sleep(1.0)
    out = window.result()
    out["completed"] = done[0]
    out["published"] = index
    adapter.disconnect()
    return out


def one(args) -> None:
    common.pin(args.cpu)
    args.topic = common.unique_topic("pub")
    sink = common.Peer("sink", topic=args.topic, qos=1)
    now = common.monotonic_ns()
    t_start = now + 1_500_000_000
    sched = {
        "t_start": t_start,
        "t_measure": t_start + args.warmup * 1_000_000_000,
        "t_end": t_start + (args.warmup + args.seconds) * 1_000_000_000,
    }
    sink.go(sched["t_start"], sched["t_measure"], sched["t_end"], sched["t_end"] + 1_500_000_000)
    if args.client in ASYNC_CLIENTS:
        out = asyncio.run(drive_async(args, sched))
    else:
        out = drive_sync(args, sched)
    peer = sink.result()
    lat = peer.get("latency")
    if lat:
        s = histogram.summary(lat)
        out["e2e_p50"], out["e2e_p99"] = s.get("p50_us"), s.get("p99_us")
    out.update(client=args.client, variant=args.variant, rate=args.rate)
    common.emit(out)


COLUMNS = [("client", "%-8s"), ("variant", "%-9s"), ("rate", "%5.0f"), ("cpu_us", "%7.1f"), ("user_us", "%7.1f"), ("sys_us", "%6.1f"),
           ("minflt", "%6.2f"), ("ctx_per_msg", "%5.2f"),
           ("loop_it", "%7.2f"), ("nb_it", "%6.2f"), ("call_soon", "%9.2f"), ("e2e_p50", "%7.0f"), ("e2e_p99", "%7.0f")]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--one", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--client", default="mqttium")
    parser.add_argument("--variant", default="default")
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--rates", default="2000,5000")
    parser.add_argument("--cases", default="awscrt,gmqtt,mqttium,mqttium:noobserve,mqttium:eager")
    parser.add_argument("--payload", type=int, default=256)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--seconds", type=int, default=6)
    parser.add_argument("--cpu", type=int, default=common.CLIENT_CPU)
    parser.add_argument("--trace-writes", action="store_true")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--profile", action="store_true",
                        help="asyncio clients only: Python calls per message instead of the timing table")
    args = parser.parse_args()
    if args.one:
        one(args)
        return
    if args.profile:
        shared_profile = ["--warmup", str(args.warmup), "--seconds", str(args.seconds), "--cpu", str(args.cpu),
                          "--payload", str(args.payload), "--profile"]
        cases = [["--client", c, "--variant", v or "default", "--rate", rate]
                 for rate in args.rates.split(",")
                 for c, _, v in (case.partition(":") for case in args.cases.split(","))
                 if c in ASYNC_CLIENTS]
        for r in common.run_children(__file__, cases, shared_profile):
            print(f"\n== rate {r['rate']:.0f}/s", end="")
            common.print_profile(r)
        return

    shared = ["--warmup", str(args.warmup), "--seconds", str(args.seconds), "--cpu", str(args.cpu),
              "--payload", str(args.payload)]
    cases = []
    for rate in args.rates.split(","):
        for case in args.cases.split(","):
            client, _, variant = case.partition(":")
            cases.append(["--client", client, "--variant", variant or "default", "--rate", rate])
    print(" ".join(name for name, _ in COLUMNS), "  (per message; latencies in us)")
    results = common.run_children(__file__, cases, shared, args.repeat)
    for r in results:
        print(" ".join((fmt % r[name]) if r.get(name) is not None else "-" for name, fmt in COLUMNS))
    if args.trace_writes:
        print()
        traced = common.run_children(
            __file__, [c + ["--trace-writes"] for c in cases if c[1] in ASYNC_CLIENTS], shared, args.repeat)
        for r in traced:
            wires = " ".join(f"{k}={v:.0f}" for k, v in r.items() if k.startswith("wire") and v is not None)
            print(f"{r['client']:<8} {r['variant']:<9} {r['rate']:>5.0f}  writes/msg={r['writes_per_msg']:.2f}  "
                  f"publish->transport.write p50 by burst position (us): {wires}")


if __name__ == "__main__":
    main()
