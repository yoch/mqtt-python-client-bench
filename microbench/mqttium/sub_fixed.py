"""Fixed-rate QoS 1 receiving: plain TCP, TLS, rate and filter dispatch.

Campaign points: ``sub_qos1_fixed`` (2,000/s), ``sub_qos1_fixed_5k``,
``sub_qos1_fixed_tls`` and ``sub_filters_fixed``. Comparison: awscrt and gmqtt,
the cheapest receivers there (paho for filters, the only other native
dispatcher). The C peer publishes on its own core; the client subscribes
through the campaign adapter. Reported per received message, over the window:
``cpu_us``, ``ctx`` (context switches), ``loop_it`` / ``nb_it`` / ``call_soon``
(asyncio clients, see ``common.LoopCounters``) and ``lat_p50`` (one-way, from
the peer's stamp to the client callback).

A case is ``transport:rate:client[:variant]``:

- transport ``plain`` or ``tls`` (the peer always publishes in clear);
- variant ``pull`` (mqttium only): plain TCP forced onto the
  ``asyncio.open_connection`` + ``StreamReader.read()`` + ``decoder.feed()``
  receive path that mqttium otherwise uses only for TLS, which separates the
  cost of that path from the cost of the encryption;
- variant ``fanin``: 1,000 topics, one ``on_message``; ``f<N>``: the same with
  N ``message_callback_add`` filters ``<base>/<d>/+``, as ``sub_filters_fixed``.

    PYTHONPATH=src .venvs/_all/bin/python microbench/mqttium/sub_fixed.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import threading
import time
from array import array

import common
import mqttium.transport.tcp as mqttium_tcp
from mqtt_client_bench.adapters.registry import create_adapter, create_async_adapter

ASYNC_CLIENTS = ("mqttium", "gmqtt")
DEFAULT_CASES = (
    "plain:2000:awscrt,plain:2000:gmqtt,plain:2000:mqttium,plain:2000:mqttium:pull,"
    "tls:2000:awscrt,tls:2000:gmqtt,tls:2000:mqttium,"
    "plain:5000:awscrt,plain:5000:gmqtt,plain:5000:mqttium"
)


class Receiver:
    """Counts and one-way latencies, written by the client's message callback."""

    def __init__(self, capacity: int) -> None:
        self.received = 0
        self.unmatched = 0
        self.latencies = array("q", bytes(8 * capacity))

    def on_message(self, client, userdata, msg) -> None:
        now = time.monotonic_ns()
        try:
            self.latencies[self.received] = now - int.from_bytes(msg.payload[:8], "little")
        except IndexError:
            pass
        self.received += 1

    def on_unmatched(self, client, userdata, msg) -> None:
        self.unmatched += 1


def wire(adapter, receiver: Receiver, base: str, filters: int) -> None:
    if not filters:
        adapter.on_message = receiver.on_message
        return
    adapter.on_message = receiver.on_unmatched
    for d in range(filters):
        adapter.message_callback_add(f"{base}/{d}/+", receiver.on_message)


def options(args) -> dict:
    return {
        "client_id": f"mb-{os.getpid()}",
        "protocol": "MQTTv311",
        "max_inflight": 256,
        "max_queued": 10_000,
        "tls_ca_certs": str(common.CA_CERT) if args.transport == "tls" else None,
    }


def port(args) -> int:
    return common.TLS_PORT if args.transport == "tls" else common.PORT


def mark(receiver: Receiver, counters) -> dict:
    return {
        "cpu": common.process_cpu_ns(),
        "ctx": common.ctx_switches(),
        "received": receiver.received,
        "loop": counters.snapshot() if counters else None,
    }


def result(a: dict, b: dict, receiver: Receiver, profiler=None) -> dict:
    n = b["received"] - a["received"]
    out = {"msgs": n, "cpu_us": (b["cpu"] - a["cpu"]) / 1000 / n, "ctx": (b["ctx"] - a["ctx"]) / n,
           "unmatched": receiver.unmatched}
    if profiler:
        out.update(profiler.report(n))
    if a["loop"]:
        d = common.delta(a["loop"], b["loop"])
        out.update(loop_it=d["selects"] / n, nb_it=d["nonblocking"] / n, call_soon=d["call_soon"] / n)
    out["lat_p50"] = common.percentile_us(list(receiver.latencies[a["received"]:b["received"]]), 0.5)
    return out


async def run_async(args, receiver: Receiver, source_go) -> dict:
    if args.variant == "pull":
        mqttium_tcp._is_stdlib_selector_loop = lambda loop: False
    adapter = create_async_adapter(args.client, **options(args))
    wire(adapter, receiver, args.base, args.filters)
    await adapter.connect(common.HOST, port(args), keepalive=60)
    await adapter.subscribe(args.listen, qos=1)
    transport = type(getattr(adapter._client, "_transport", None)).__name__
    counters = common.LoopCounters(asyncio.get_running_loop())
    profiler = common.WindowProfiler() if args.profile else None
    sched = source_go()
    await asyncio.sleep(max(0, sched["t_measure"] - common.monotonic_ns()) / 1e9)
    a = mark(receiver, counters)
    if profiler:
        profiler.start()
    await asyncio.sleep(max(0, sched["t_end"] - common.monotonic_ns()) / 1e9)
    if profiler:
        profiler.stop()
    b = mark(receiver, counters)
    await asyncio.sleep(max(0, sched["t_stop"] - common.monotonic_ns()) / 1e9)
    await adapter.disconnect()
    out = result(a, b, receiver, profiler)
    out["transport"] = transport
    return out


def run_sync(args, receiver: Receiver, source_go) -> dict:
    adapter = create_adapter(args.client, **options(args))
    connected, subscribed = threading.Event(), threading.Event()
    adapter.on_connect = lambda *a, **k: connected.set()
    adapter.on_subscribe = lambda *a, **k: subscribed.set()
    wire(adapter, receiver, args.base, args.filters)
    adapter.connect(common.HOST, port(args), keepalive=60)
    adapter.loop_start()
    if not connected.wait(10):
        raise SystemExit("connect timeout")
    adapter.subscribe(args.listen, qos=1)
    if not subscribed.wait(10):
        raise SystemExit("subscribe timeout")
    sched = source_go()
    time.sleep(max(0, sched["t_measure"] - common.monotonic_ns()) / 1e9)
    a = mark(receiver, None)
    time.sleep(max(0, sched["t_end"] - common.monotonic_ns()) / 1e9)
    b = mark(receiver, None)
    time.sleep(max(0, sched["t_stop"] - common.monotonic_ns()) / 1e9)
    adapter.disconnect()
    adapter.loop_stop()
    return result(a, b, receiver)


def one(args) -> None:
    common.pin(args.cpu)
    is_filters = args.variant[:1] == "f" and args.variant[1:].isdigit()
    args.filters = int(args.variant[1:]) if is_filters else 0
    topics = 1000 if is_filters or args.variant == "fanin" else 1
    args.base = common.unique_topic("sub")
    args.listen = f"{args.base}/#" if topics > 1 else args.base
    source = common.Peer("source", topic=args.base, qos=1, rate=args.rate, topics=topics)
    receiver = Receiver(args.rate * (args.warmup + args.seconds + 2))

    def source_go() -> dict:
        t_start = common.monotonic_ns() + 300_000_000
        sched = {
            "t_start": t_start,
            "t_measure": t_start + args.warmup * 1_000_000_000,
            "t_end": t_start + (args.warmup + args.seconds) * 1_000_000_000,
        }
        sched["t_stop"] = sched["t_end"] + 1_000_000_000
        source.go(sched["t_start"], sched["t_measure"], sched["t_end"], sched["t_stop"])
        return sched

    if args.client in ASYNC_CLIENTS:
        out = asyncio.run(run_async(args, receiver, source_go))
    else:
        out = run_sync(args, receiver, source_go)
    peer = source.result()
    out.update(case=f"{args.transport}:{args.rate}:{args.client}:{args.variant}", peer_sent=peer.get("sent_window"))
    common.emit(out)


COLUMNS = [("case", "%-28s"), ("cpu_us", "%7.1f"), ("ctx", "%5.2f"), ("loop_it", "%7.2f"), ("nb_it", "%6.2f"),
           ("call_soon", "%9.2f"), ("lat_p50", "%7.0f"), ("transport", "%-20s")]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--one", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--client", default="mqttium")
    parser.add_argument("--variant", default="default")
    parser.add_argument("--transport", default="plain", choices=("plain", "tls"))
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--cases", default=DEFAULT_CASES)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--seconds", type=int, default=6)
    parser.add_argument("--cpu", type=int, default=common.CLIENT_CPU)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--profile", action="store_true",
                        help="asyncio clients only: Python calls per message instead of the timing table")
    args = parser.parse_args()
    if args.one:
        one(args)
        return

    shared = ["--warmup", str(args.warmup), "--seconds", str(args.seconds), "--cpu", str(args.cpu)]
    if args.profile:
        cases = []
        for case in args.cases.split(","):
            transport, rate, client, *variant = case.split(":")
            if client in ASYNC_CLIENTS:
                cases.append(["--transport", transport, "--rate", rate, "--client", client,
                              "--variant", variant[0] if variant else "default", "--profile"])
        for r in common.run_children(__file__, cases, shared):
            common.print_profile(r)
        return
    cases = []
    for case in args.cases.split(","):
        transport, rate, client, *variant = case.split(":")
        cases.append(["--transport", transport, "--rate", rate, "--client", client,
                      "--variant", variant[0] if variant else "default"])
    print(" ".join(name for name, _ in COLUMNS), "  (per received message; latency in us)")
    for r in common.run_children(__file__, cases, shared, args.repeat):
        print(" ".join((fmt % r[name]) if r.get(name) is not None else "-" for name, fmt in COLUMNS))


if __name__ == "__main__":
    main()
