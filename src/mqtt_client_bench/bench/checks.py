"""What a run's counts may be used for.

Every check compares counts produced by two parties that do not share code:
the client worker, the C peer and the broker's ``$SYS`` counters. A run is

- ``valid`` when every check passes;
- ``not_sustained`` when the only failures are the client not keeping up with
  a fixed offer — a real finding about that client, reported as such and kept
  out of latency/cost tables, since a backlog's latency is queueing time;
- ``invalid`` when the harness, the peer, the broker or the host failed: the
  run says nothing about the client and is re-run.

Tolerance for count reconciliation is ``5 + 0.05 %``: ``$SYS`` is published
once a second and read after a fresh update, so it is exact at rest, and the
slack only absorbs a handful of in-flight packets at the boundaries.
"""

from __future__ import annotations

from typing import List, Optional

from mqtt_client_bench.bench import histogram

RATE_HELD = 0.98
BROKER_CPU_LIMIT = 0.85
HOST_NOISE_CORES = 0.5

INVALID = "invalid"
NOT_SUSTAINED = "not_sustained"
VALID = "valid"


STATUS_DOCS = {
    VALID: "Every check passed. Only valid runs enter a median.",
    NOT_SUSTAINED: (
        "The client could not hold the fixed offer. A real finding about the client, kept out of "
        "cost and latency medians because a backlog's latency is queueing time."
    ),
    INVALID: (
        "The harness, the peer, the broker or the host failed. The run says nothing about the "
        "client; the campaign retries it once."
    ),
}

# The report's methodology page is generated from these; a test fails when a
# check or flag emitted by evaluate() is missing here.
CHECK_DOCS = {
    "worker_completed": "The client worker connected, followed the schedule and reported its counters.",
    "peer_completed": "The C peer connected, followed the schedule and reported its counters.",
    "broker_counters_read": "A fresh $SYS reading was taken before and after the run.",
    "broker_confirms_client_publishes": (
        "Publishes the broker received from the client lie between the client's completions and "
        "its sends (round trips subtract the echo's republishes)."
    ),
    "broker_confirms_peer_publishes": "Publishes the broker received equal what the C source wrote.",
    "broker_confirms_deliveries": (
        "Messages the broker sent equal what the receiving side counted. At receive capacity, "
        "only receiving more than was sent fails: a slow client still has data in its socket "
        "buffers when the run stops."
    ),
    "no_loss": (
        "QoS 1: every acknowledged publish reached its subscriber. A client that fell behind "
        "until the broker's queue overflowed is not_sustained; a loss without drops is invalid."
    ),
    "offered_rate_held": f"At least {RATE_HELD:.0%} of the fixed offer was produced in the window.",
    "responses_kept_up": f"Round trips: at least {RATE_HELD:.0%} of the requests were answered in the window.",
    "client_kept_up": f"Receive: the client took at least {RATE_HELD:.0%} of the offer in the window.",
    "broker_headroom": f"Fixed-rate and idle points: the broker used less than {BROKER_CPU_LIMIT:.0%} of its core.",
    "host_quiet": (
        f"At most {HOST_NOISE_CORES} cores were busy outside the client, the peer and the broker. "
        "Enforced on comparable profiles only."
    ),
}

FLAG_DOCS = {
    "broker_bound": (
        f"Capacity point where the broker used at least {BROKER_CPU_LIMIT:.0%} of its core: the "
        "rate is partly the broker's."
    ),
    "offer_bound": "The client received the whole receive offer; its capacity is at least this rate.",
    "broker_queue_overflow": "The broker discarded QoS 1 messages a slower client could not drain.",
    "host_noisy": "The rest of the host was busy (only tolerated on non-comparable profiles).",
    "non_comparable": "Development profile: never published or compared.",
}


def tolerance(n: int) -> int:
    return 5 + int(0.0005 * max(0, n))


def _within(value: int, low: int, high: int) -> bool:
    return low <= value <= high


class _Checks:
    def __init__(self) -> None:
        self.items: List[dict] = []

    def add(self, name: str, passed: bool, detail: str, severity: str = INVALID) -> None:
        self.items.append({"name": name, "passed": bool(passed), "detail": detail, "severity": severity})

    def status(self) -> str:
        failed = [c for c in self.items if not c["passed"]]
        if any(c["severity"] == INVALID for c in failed):
            return INVALID
        if failed:
            return NOT_SUSTAINED
        return VALID


def _window_delta(worker: dict, key: str) -> int:
    return int(worker["end"][key]) - int(worker["measure"][key])


def rate_offer_reached(record: dict, window_s: float) -> bool:
    """The client received everything offered: its capacity is above the offer."""
    worker, peer = record["worker"], record.get("peer") or {}
    offered = int(peer.get("sent_window", 0))
    return offered > 0 and _window_delta(worker, "received") >= RATE_HELD * offered


def evaluate(record: dict, *, strict: bool = True) -> dict:
    """Return ``{"status", "checks", "metrics", "flags"}`` for one raw run.

    ``strict=False`` (non-comparable profiles) turns a noisy host into a flag
    so development runs on a busy machine still exercise every other check.
    """
    point = record["point"]
    kind, qos, rate = point["kind"], int(point["qos"]), int(point["rate"])
    worker = record.get("worker") or {}
    peer = record.get("peer")
    sysd = (record.get("broker") or {}).get("sys")
    res = record.get("resources") or {}
    checks = _Checks()
    flags: List[str] = []

    checks.add(
        "worker_completed",
        bool(worker.get("ok")) and worker.get("measure") is not None and worker.get("end") is not None,
        worker.get("error") or "worker reported its counts",
    )
    if kind != "idle":
        checks.add(
            "peer_completed",
            peer is not None and peer.get("error") is None,
            "peer reported its counts" if peer and peer.get("error") is None else f"peer error: {(peer or {}).get('error', 'no result')}",
        )
    checks.add("broker_counters_read", sysd is not None, "fresh $SYS reading before and after the run")
    if checks.status() == INVALID:
        return {"status": INVALID, "checks": checks.items, "metrics": {}, "flags": flags}

    final = worker["final"]
    window_s = (worker.get("window_ns") or 0) / 1e9 or record["schedule"]["measure_s"]

    # -- publish side confirmed by the broker
    if kind in ("pub", "rtt"):
        published_by_others = int(peer.get("echoed_total", 0)) if kind == "rtt" else 0
        got = int(sysd["received"]) - published_by_others
        low = int(final["done"]) - tolerance(final["done"])
        high = int(final["sent"]) + tolerance(final["sent"])
        checks.add(
            "broker_confirms_client_publishes",
            _within(got, low, high),
            f"broker received {got} from the client; client sent {final['sent']}, completed {final['done']}",
        )
    elif kind == "sub":
        got = int(sysd["received"])
        sent = int(peer["sent_total"])
        checks.add(
            "broker_confirms_peer_publishes",
            abs(got - sent) <= tolerance(sent),
            f"broker received {got}; peer wrote {sent}",
        )

    # -- deliveries confirmed by the broker
    if kind == "pub":
        delivered = int(peer["received_total"])
        checks.add(
            "broker_confirms_deliveries",
            abs(int(sysd["sent"]) - delivered) <= tolerance(delivered),
            f"broker sent {sysd['sent']}; peer received {delivered}",
        )
    elif kind == "sub" and rate:
        delivered = int(final["received"])
        checks.add(
            "broker_confirms_deliveries",
            abs(int(sysd["sent"]) - delivered) <= tolerance(delivered),
            f"broker sent {sysd['sent']}; client received {delivered}",
        )
    elif kind == "sub":
        # A client slower than the offer still has megabytes in its socket
        # buffers at t_stop; the broker counted them as sent. Only receiving
        # more than was sent would be wrong.
        delivered = int(final["received"])
        checks.add(
            "broker_confirms_deliveries",
            delivered <= int(sysd["sent"]) + tolerance(delivered),
            f"broker sent {sysd['sent']}; client received {delivered} by t_stop",
        )
    elif kind == "rtt":
        delivered = int(final["received"]) + int(peer["received_total"])
        checks.add(
            "broker_confirms_deliveries",
            abs(int(sysd["sent"]) - delivered) <= tolerance(delivered),
            f"broker sent {sysd['sent']}; client received {final['received']}, echo received {peer['received_total']}",
        )

    # -- QoS 1: every acknowledged publish reached its subscriber
    if qos == 1 and kind == "pub":
        delivered = int(peer["received_total"])
        acked = int(final["done"])
        checks.add(
            "no_loss",
            delivered >= acked - tolerance(acked),
            f"client saw {acked} PUBACKs; peer received {delivered}",
        )
    elif qos == 1 and kind == "sub" and not rate:
        # A capacity offer exceeds what a slow client drains; the broker's
        # queue limit then discards the excess, which is the measurement.
        if int(sysd.get("dropped", 0)):
            flags.append("broker_queue_overflow")
    elif qos == 1 and kind == "sub":
        acked = int(peer["acked_total"])
        delivered = int(final["received"])
        dropped = int(sysd.get("dropped", 0))
        checks.add(
            "no_loss",
            delivered >= acked - tolerance(acked),
            f"broker acknowledged {acked} from the peer; client received {delivered}; broker dropped {dropped}",
            severity=NOT_SUSTAINED if dropped else INVALID,
        )
    elif qos == 1 and kind == "rtt":
        replies = int(final["received"])
        echoed = int(peer["echoed_total"])
        checks.add(
            "no_loss",
            replies >= echoed - tolerance(echoed),
            f"echo sent {echoed} replies; client received {replies}",
            severity=NOT_SUSTAINED,
        )

    # -- the fixed offer was actually offered and absorbed
    if rate and kind in ("pub", "rtt"):
        offered = rate * window_s
        sent = _window_delta(worker, "sent")
        checks.add(
            "offered_rate_held",
            sent >= RATE_HELD * offered,
            f"client published {sent} of {offered:.0f} offered in the window",
            severity=NOT_SUSTAINED,
        )
        if kind == "rtt":
            replies = _window_delta(worker, "received")
            checks.add(
                "responses_kept_up",
                replies >= RATE_HELD * sent,
                f"client received {replies} replies to {sent} requests in the window",
                severity=NOT_SUSTAINED,
            )
    if rate and kind == "sub":
        offered = rate * window_s
        peer_sent = int(peer["sent_window"])
        checks.add(
            "offered_rate_held",
            peer_sent >= RATE_HELD * offered,
            f"peer wrote {peer_sent} of {offered:.0f} offered in the window",
        )
        got = _window_delta(worker, "received")
        checks.add(
            "client_kept_up",
            got >= RATE_HELD * peer_sent,
            f"client received {got} of {peer_sent} in the window",
            severity=NOT_SUSTAINED,
        )

    # -- the broker and the host were not the constraint
    broker_cpu = (record.get("broker") or {}).get("cpu_cores")
    if broker_cpu is not None:
        if point["rate"] or kind == "idle":
            checks.add(
                "broker_headroom",
                broker_cpu < BROKER_CPU_LIMIT,
                f"broker used {broker_cpu:.2f} of its core",
            )
        elif broker_cpu >= BROKER_CPU_LIMIT:
            flags.append("broker_bound")
    noise = (res.get("host") or {}).get("other_cores")
    if noise is not None:
        quiet = noise <= HOST_NOISE_CORES
        if strict:
            checks.add("host_quiet", quiet, f"{noise:.2f} cores busy outside the client, peer and broker")
        elif not quiet:
            flags.append("host_noisy")
    if kind == "sub" and not rate and rate_offer_reached(record, window_s):
        flags.append("offer_bound")

    metrics = _metrics(record, window_s)
    return {"status": checks.status(), "checks": checks.items, "metrics": metrics, "flags": flags}


def _per_msg_us(value_s: Optional[float], msgs: int) -> Optional[float]:
    if value_s is None or msgs <= 0:
        return None
    return value_s * 1e6 / msgs


def _metrics(record: dict, window_s: float) -> dict:
    point = record["point"]
    kind, rate = point["kind"], int(point["rate"])
    worker = record["worker"]
    peer = record.get("peer") or {}
    client = (record.get("resources") or {}).get("client") or {}
    m: dict = {"window_s": window_s}

    if kind == "pub":
        m["client_sent"] = _window_delta(worker, "sent")
        m["client_completed"] = _window_delta(worker, "done")
        m["delivered"] = int(peer.get("received_window", 0))
        m["msgs_per_s"] = m["delivered"] / window_s
        msgs = m["client_sent"]
    elif kind == "sub":
        m["offered"] = int(peer.get("sent_window", 0))
        m["offered_per_s"] = m["offered"] / window_s
        m["delivered"] = _window_delta(worker, "received")
        m["msgs_per_s"] = m["delivered"] / window_s
        sysd = (record.get("broker") or {}).get("sys") or {}
        m["broker_dropped"] = int(sysd.get("dropped", 0))
        if sysd.get("sent") is not None:
            m["undelivered_at_stop"] = max(0, int(sysd["sent"]) - int(worker["final"]["received"]))
        msgs = m["delivered"]
    elif kind == "rtt":
        m["requests"] = _window_delta(worker, "sent")
        m["replies"] = _window_delta(worker, "received")
        m["msgs_per_s"] = m["replies"] / window_s
        msgs = m["requests"]
    else:
        msgs = 0
        m["connect_ms"] = worker.get("connect_ns", 0) / 1e6

    cpu_ns = client.get("cpu_ns")
    if cpu_ns is not None:
        m["cpu_cores"] = cpu_ns / 1e9 / window_s
        m["cpu_us_per_msg"] = _per_msg_us(cpu_ns / 1e9, msgs)
        m["cpu_user_us_per_msg"] = _per_msg_us(client.get("cpu_user_s"), msgs)
        m["cpu_sys_us_per_msg"] = _per_msg_us(client.get("cpu_sys_s"), msgs)
    for key in ("rss_start_kb", "rss_end_kb", "rss_peak_kb", "threads"):
        if client.get(key) is not None:
            m[key] = client[key]
    if client.get("ctx_involuntary") is not None and msgs:
        m["ctx_switches_per_1k_msgs"] = (client["ctx_voluntary"] + client["ctx_involuntary"]) * 1000 / msgs

    if rate and kind == "pub" and peer.get("latency"):
        m["latency"] = peer["latency"]
    elif rate and kind in ("sub", "rtt") and worker.get("latency"):
        m["latency"] = worker["latency"]
    if m.get("latency"):
        m["latency_summary"] = histogram.summary(m["latency"])
    return m
