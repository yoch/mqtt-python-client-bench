"""Host calibration: measure what the harness used to assume.

Four numbers decided what this bench measures, and none of them was measured:
the ingress offer, the hammer pacing cap, the emqtt-bench cap, and the harness's
own per-message cost. All four are properties of one machine. Frozen as
constants they made the bench unrunnable anywhere else — not loudly, which is
the problem: a campaign taken on a smaller host came back `valid` at an offer
that host could not sustain.

This module measures them on whatever machine it is run on and writes a host
profile. The profile is fingerprinted, committed under ``hosts/``, and carried
into every result, so a number can be read back against the ceilings that
produced it.

It does not make hosts comparable. A 4-vCPU Xeon and an i7-3770 move the harness
cost, the broker ceiling and every client capacity by different factors; any
normalisation across them would be inventing a comparability that does not
exist. What it buys is that each host produces a ranking that is honest about
itself, and that mixing two of them becomes impossible by construction.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import shutil
import subprocess
import time
from typing import Any, Dict, List, Optional

from mqtt_client_bench.adapters.base import AdapterCapabilities
from mqtt_client_bench.roles import publisher
from mqtt_client_bench.telemetry import (
    environment_metadata,
    physical_cpu_groups,
    temporarily_pinned,
)

# Passes for the harness-cost probe. The statistic is the *minimum*, not the
# mean or a single shot: the quantity is a fixed per-message cost, so every
# sample is that cost plus some amount of interference, and the smallest sample
# is the one with the least of it. A single wall-clock shot is what made the
# old unit-test bound flip on ordinary scheduling noise while the code itself
# had not moved at all.
HARNESS_COST_PASSES = 15

# Per-pass window. Long enough that loop setup does not show up in the ratio,
# short enough that fifteen passes stay under ten seconds.
HARNESS_COST_WINDOW_S = 0.5

# The window the probe drives, matched to a capacity point: the harness cost is
# a per-message tax on the throughput scenarios, so it must be priced in the
# shape those scenarios run.
HARNESS_COST_OUTSTANDING = 64
HARNESS_COST_PAYLOAD = b"x" * 256
HARNESS_COST_RUN_ID = b"hostcal1"


class _NullAdapter:
    """A client that admits a publish and acknowledges it immediately.

    Stands in for a library so the loop can be timed with nothing else in it:
    no socket, no protocol, no broker. What is left on the clock is the
    harness — the header stamp, the publish call, the clock reads and the
    interpreter.

    It takes the ``publish_sync_on_loop`` shape because that is the cheaper of
    the two publish paths; pricing the harness on the awaited shape would fold
    a coroutine allocation per message into a number that is meant to be the
    floor.
    """

    def __init__(self) -> None:
        self._mid = 0
        self.on_connect = None
        self.on_publish = None
        self.on_message = None

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return AdapterCapabilities(
            name="null", native_async=True, publish_sync_on_loop=True
        )

    def publish_nowait(self, topic, payload=None, qos=0, retain=False, properties=None):
        self._mid = 1 if self._mid >= 65535 else self._mid + 1
        mid = self._mid
        if self.on_publish is not None:
            self.on_publish(self, None, mid, 0)
        return mid


def _one_harness_pass() -> Optional[float]:
    """Drive the publish loop once against the null client; ns per message."""
    adapter = _NullAdapter()
    state = publisher.new_publisher_state()
    adapter.on_publish = publisher._make_on_publish(state, 0, lock=None)
    state["phase"] = "measure"

    async def drive() -> float:
        started = time.perf_counter()
        await publisher._run_publish_loop_async(
            adapter,
            state,
            topic="hostcal/probe",
            qos=0,
            body=HARNESS_COST_PAYLOAD,
            corpus=[],
            run_id=HARNESS_COST_RUN_ID,
            outstanding=HARNESS_COST_OUTSTANDING,
            cadence="capacity",
            until=started + HARNESS_COST_WINDOW_S,
            target_rate=None,
            properties_builder=lambda: None,
            track_sequences=False,
        )
        return time.perf_counter() - started

    loop = asyncio.new_event_loop()
    try:
        elapsed = loop.run_until_complete(drive())
    finally:
        loop.close()
    offered = int(state["offered"])
    if offered <= 0:
        return None
    return (elapsed * 1e9) / offered


def measure_harness_cost_ns(
    *, passes: int = HARNESS_COST_PASSES, cpuset: Optional[str] = None
) -> Dict[str, Any]:
    """Price the harness's own per-message cost on this machine.

    Returns the minimum over ``passes``, plus the median and the spread so a
    reader can tell a quiet machine from a busy one. A wide spread does not
    invalidate the minimum — it says the host was contended while the probe
    ran, which is worth recording next to the number.
    """
    samples: List[float] = []
    with temporarily_pinned(cpuset):
        for _ in range(max(1, int(passes))):
            value = _one_harness_pass()
            if value is not None:
                samples.append(value)
    if not samples:
        raise RuntimeError("harness cost probe offered no messages")
    samples.sort()
    n = len(samples)
    median = samples[n // 2] if n % 2 else 0.5 * (samples[n // 2 - 1] + samples[n // 2])
    return {
        "ns_per_message": round(samples[0], 1),
        "median_ns_per_message": round(median, 1),
        "max_ns_per_message": round(samples[-1], 1),
        "passes": n,
        # Contention while probing, as a fraction of the floor. Small means the
        # machine was quiet and the floor is trustworthy.
        "spread_pct": round(100.0 * (samples[-1] - samples[0]) / samples[0], 1),
    }


def frequency_policy(env: Dict[str, Any]) -> str:
    """How this host's clock is pinned, as a declared value rather than a guess.

    ``performance`` is the reference posture. ``unpinned`` is a host that has no
    cpufreq to read at all — a container or a VM. That is not disqualifying on
    its own, but it has to be written down and fingerprinted: the run-to-run
    variance it admits belongs next to the numbers it produced, and a host that
    merely *happens* to expose no sysfs must never inherit the reference host's
    posture by silence.
    """
    governor = env.get("scaling_governor")
    if governor is None:
        return "unpinned"
    if governor == "performance":
        return "performance"
    return f"governed:{governor}"


def host_identity(env: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The machine facts a ceiling depends on.

    ``threads_per_group`` is here because two hosts can both satisfy the
    standard profile's four disjoint core groups and still be different
    machines: this workstation gives each role an SMT pair, the 4-vCPU Xeon
    gives each role a single thread. `allocate_cpuset` accepts both; the result
    must not pretend they are the same.
    """
    env = env or environment_metadata()
    groups = env.get("physical_cpu_groups") or physical_cpu_groups()
    sizes = sorted({len(g) for g in groups}) if groups else []
    return {
        "hostname": env.get("hostname"),
        "cpu_model": env.get("cpu_model"),
        "cpu_count": env.get("cpu_count"),
        "physical_groups": len(groups),
        "threads_per_group": sizes[0] if len(sizes) == 1 else sizes,
        "frequency_policy": frequency_policy(env),
        "scaling_governor": env.get("scaling_governor"),
        "kernel": env.get("platform") or platform.platform(),
        "python": env.get("python"),
    }


# Fields that change what a measured ceiling means. Anything outside this set
# (loadavg at probe time, the hostname's letter case, a kernel point release)
# must not move the fingerprint, or every profile would need regenerating for
# reasons that do not touch a single number.
_FINGERPRINT_IDENTITY = (
    "cpu_model",
    "cpu_count",
    "physical_groups",
    "threads_per_group",
    "frequency_policy",
)
_FINGERPRINT_CEILINGS = (
    "harness_cost_ns_per_message",
    "loadgen_ceiling_msgs_per_s",
    "broker_fanout_msgs_per_s",
)


def host_fingerprint(profile: Dict[str, Any]) -> str:
    """Short digest over the identity and the ceilings, in the shape of
    ``provenance.harness_fingerprint``."""
    identity = profile.get("host") or {}
    ceilings = profile.get("ceilings") or {}
    digest = hashlib.sha256()
    for key in _FINGERPRINT_IDENTITY:
        digest.update(f"{key}={identity.get(key)!r}\0".encode("utf-8"))
    for key in _FINGERPRINT_CEILINGS:
        value = ceilings.get(key)
        # Quantise so re-probing the same machine does not churn the digest on
        # a percent of measurement noise. Two hosts that differ by less than
        # this are not distinguishable by these probes anyway.
        if isinstance(value, (int, float)):
            value = round(float(value), -2) if value >= 1000 else round(float(value), 1)
        digest.update(f"{key}={value!r}\0".encode("utf-8"))
    return digest.hexdigest()[:16]


def profile_path_name(profile: Dict[str, Any]) -> str:
    """``<hostname>-<fingerprint>.json``, the committed file name."""
    host = (profile.get("host") or {}).get("hostname") or "unknown"
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in str(host))
    return f"{safe}-{profile.get('host_fingerprint')}.json"


def load_host_profile(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# --- Ceiling probes ---------------------------------------------------------
#
# The three ceilings are found by escalation, not asserted: raise the offer
# until what comes out stops tracking what went in. The knee is the ceiling.
# `broker_ceiling_ingress` already provides the topology — emqtt/hammer publish,
# emqtt subscribe, no Python SUT — so the probe reuses it rather than building a
# second ingress path that could drift from the one campaigns run.

# Escalation grid, in msgs/s. Starts below the current constants so a smaller
# host finds its knee early and stops, and reaches past them so this host can
# show whether 200k was the ceiling or merely the last value anyone tried.
CEILING_GRID = (32_000, 64_000, 100_000, 150_000, 200_000, 260_000, 320_000)

# A step counts as sustained when delivery holds this share of the offer.
# Below it, the offer is no longer what is being measured.
CEILING_HOLD_RATIO = 0.95


def _ceiling_from_steps(
    steps: List[Dict[str, Any]], field: str = "delivered_msgs_per_s"
) -> Optional[float]:
    """Highest sustained value of ``field`` before the knee.

    Not the highest offer that was *attempted*: an offer the host cannot hold is
    something falling behind, and recording it as a ceiling is exactly the
    mistake the 200k constant made on smaller machines.

    ``field`` decides *which* ceiling: the loadgen's is what it emitted, the
    broker's is what it decoded, and the subscriber's delivery is neither. The
    first version of this probe read delivery for all three and reported a
    loadgen ceiling of 64k on a host whose loadgen holds 200k — it had measured
    an emqtt-bench subscriber sharing a core group with the publisher.
    """
    best: Optional[float] = None
    # `run_scenario` does not promise to return points in the order they were
    # given, and this walk stops at the first step the host cannot hold: read
    # out of order it would stop at the wrong one. Sort by what was offered.
    steps = sorted(
        steps, key=lambda s: float(s.get("effective_offer_msgs_per_s") or 0.0)
    )
    for step in steps:
        offer = step.get("effective_offer_msgs_per_s")
        value = step.get(field)
        if not offer or not value:
            continue
        if value >= CEILING_HOLD_RATIO * offer:
            best = max(best or 0.0, float(value))
        else:
            # First step the host cannot hold: everything above it is noise.
            break
    return best


# --- Idleness: the precondition, not a footnote --------------------------------
#
# A contended *run* comes back `inconclusive` and is re-run. A contended
# *calibration* is committed, fingerprinted, and then governs every campaign
# that follows: it lowers the offer, lowers the ceilings, and raises the priced
# harness cost, permanently and invisibly. So the bar here is deliberately
# stricter than the one `host_state_reasons()` applies to a run, and failing it
# refuses to write a profile rather than annotating one.
#
# 0.2 per CPU, against 1.0 for a run: the probes saturate a core group by
# construction, so anything already queued competes with the measurement
# directly rather than merely sharing the machine.
IDLE_LOADAVG_PER_CPU_MAX = 0.2

# Seconds of observation before probing. A machine that has just finished
# something reports a decaying 1-minute average that looks worse than it is, and
# one that is about to start something looks better; sampling both ends catches
# the second case, which is the dangerous one.
IDLE_SETTLE_S = 3.0

# Busy share of all CPUs over the settle window, from /proc/stat.
#
# This is the check that matters, and loadavg alone does not do it: measured on
# this workstation with an editor, a browser and a chat client running — better
# than 50% of eight CPUs between them — loadavg read 1.38 against a 1.60 gate
# and sailed through. Load average counts tasks in the run queue; interactive
# applications burn CPU in short bursts that are rarely queued at the instant
# it is sampled. Utilisation sees them.
#
# The median matters as much as the signal. On this machine with everything
# closed, five consecutive windows read 2.7 / 11.7 / 2.8 / 2.5 / 2.4 %: a
# desktop session bursts even at rest, so a single window refuses a free
# machine about one time in five. Taking the middle sample keeps the check
# honest about the burst without mistaking it for the state of the host - the
# same reason the harness-cost probe takes a floor over N passes.
IDLE_BUSY_SAMPLES = 5
IDLE_MAX_BUSY_PCT = 15.0

# Spread of the harness-cost passes above which the machine was evidently not
# quiet *during* the probe, whatever loadavg claimed before it.
IDLE_MAX_PROBE_SPREAD_PCT = 35.0

# Pause between the last probe and the closing idleness check, so the check sees
# the machine rather than the probes' own teardown.
IDLE_AFTER_SETTLE_S = 10.0


def _cpu_totals() -> Optional[tuple]:
    """(idle_jiffies, total_jiffies) from /proc/stat, or None off Linux."""
    try:
        with open("/proc/stat", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("cpu "):
                    fields = [int(x) for x in line.split()[1:]]
                    break
            else:
                return None
    except OSError:
        return None
    if len(fields) < 5:
        return None
    # user nice system idle iowait irq softirq steal ...
    idle = fields[3] + fields[4]
    return idle, sum(fields)


def _busy_pct_window(window_s: float) -> Optional[float]:
    """Share of all CPUs busy over one window."""
    first = _cpu_totals()
    if first is None:
        time.sleep(window_s)
        return None
    time.sleep(window_s)
    second = _cpu_totals()
    if second is None:
        return None
    d_idle = second[0] - first[0]
    d_total = second[1] - first[1]
    if d_total <= 0:
        return None
    return 100.0 * (1.0 - d_idle / d_total)


def busy_pct_over(total_s: float, *, samples: int = IDLE_BUSY_SAMPLES) -> Optional[float]:
    """Median busy share across ``samples`` windows spanning ``total_s``."""
    n = max(1, int(samples))
    window = max(0.2, float(total_s) / n)
    values = [v for v in (_busy_pct_window(window) for _ in range(n)) if v is not None]
    if not values:
        return None
    values.sort()
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return 0.5 * (values[mid - 1] + values[mid])


def check_host_idle(*, strict: bool = True, use_loadavg: bool = True) -> Dict[str, Any]:
    """Is this machine quiet enough to be measured?

    Two signals, because neither is sufficient alone: CPU utilisation over the
    settle window catches the bursty interactive load that loadavg misses, and
    loadavg catches a long run queue of tasks that are waiting rather than
    burning cycles.

    Returns the observation and a list of reasons it is not idle. The caller
    decides what to do with them; nothing here writes anything.
    """
    import os as _os

    cpus = _os.cpu_count() or 1
    load_threshold = IDLE_LOADAVG_PER_CPU_MAX if strict else 1.0
    busy_threshold = IDLE_MAX_BUSY_PCT if strict else 60.0
    before = _os.getloadavg()[0]
    busy = busy_pct_over(IDLE_SETTLE_S)
    after = _os.getloadavg()[0]
    observed = max(before, after)

    reasons: List[str] = []
    if busy is not None and busy > busy_threshold:
        reasons.append(f"host_busy:cpu={busy:.1f}% over {busy_threshold:.0f}%")
    if use_loadavg and observed > load_threshold * cpus:
        reasons.append(
            f"host_busy:loadavg={observed:.2f} over {load_threshold * cpus:.2f} "
            f"({load_threshold} x {cpus} cpus)"
        )
    return {
        "loadavg_before": round(before, 2),
        "loadavg_after": round(after, 2),
        "busy_pct": round(busy, 1) if busy is not None else None,
        "cpu_count": cpus,
        "loadavg_threshold": round(load_threshold * cpus, 2),
        "busy_pct_threshold": busy_threshold,
        "idle": not reasons,
        "reasons": reasons,
    }


class HostNotIdle(RuntimeError):
    """Raised when a calibration is asked for on a machine that is working.

    Deliberately an exception and not a warning: the whole value of a host
    profile is that the numbers in it are the host's, not the host's minus
    whatever else was running when someone happened to run the probe.
    """


HAMMER_THREAD_GRID = (1, 2, 4, 8)
CEILING_PROBE_S = 5


def _run_hammer(mode: str, *, cpuset: Optional[str], clients: int, seconds: int,
                topic: str, rate: int = 0, host: str = "127.0.0.1", port: int = 11883):
    """One direct hammer run; returns its own JSON line."""
    from mqtt_client_bench.loadgen import ensure_mqtt_hammer

    cmd: List[str] = []
    if cpuset and shutil.which("taskset"):
        cmd += ["taskset", "-c", cpuset]
    cmd += [
        str(ensure_mqtt_hammer()), mode, "--host", host, "--port", str(port),
        "--topic", topic, "--clients", str(clients), "--duration", str(seconds),
    ]
    if mode == "pub":
        cmd += ["--payload", "256", "--rate", str(rate)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    for line in reversed((proc.stdout or "").strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except ValueError:
                continue
    return None


def measure_loadgen_ceiling(*, cpuset: Optional[str], seconds: int = CEILING_PROBE_S) -> Dict[str, Any]:
    """What this host's loadgen can put on the wire, with nothing consuming it.

    No subscriber. That is the whole point of this probe: attach one and
    Mosquitto has to fan out as well as decode, which roughly triples its
    per-message cost and back-pressures the publisher to the broker's rate.
    Measured here, the same hammer went from 198,842 msgs/s to 73,120 the
    moment a single subscriber connected. That number is real and worth having,
    but it is the broker's, not the loadgen's, and reading it as the loadgen's
    is what made the first version of this probe report 64k on a host that
    emits ten times that.

    The thread count is swept rather than assumed: `HAMMER_PUB_CLIENTS = 2` is a
    hand-tuned constant, and on this machine 4 threads emit 637k against 326k
    for 2 and 217k for 8 -- the peak is a property of the core group, so it has
    to be found on each host, not inherited from this one.
    """
    steps = []
    for clients in HAMMER_THREAD_GRID:
        parsed = _run_hammer(
            "pub", cpuset=cpuset, clients=clients, seconds=seconds,
            topic="hostcal/loadgen", rate=0,
        )
        if parsed:
            steps.append({"clients": clients, "msgs_per_s": parsed.get("msgs_per_s")})
    best = max(steps, key=lambda s: s.get("msgs_per_s") or 0.0, default=None)
    return {
        "msgs_per_s": (best or {}).get("msgs_per_s"),
        "best_thread_count": (best or {}).get("clients"),
        "steps": steps,
    }


def measure_broker_fanout_ceiling(
    *, pub_cpuset: Optional[str], sub_cpuset: Optional[str], seconds: int = CEILING_PROBE_S
) -> Dict[str, Any]:
    """What the broker forwards with one subscriber attached.

    Diagnostic, deliberately: this is *not* where the ingress offer comes from.
    The core `sub_*` offer is an over-offer whose job is to make the SUT client
    the bottleneck, and deriving it from a sustainable rate would collapse it to
    this number and make the fastest clients neighbours of the constraint
    instead of its subject.
    """
    import threading

    result: Dict[str, Any] = {}

    def _sub():
        result["sub"] = _run_hammer(
            "sub", cpuset=sub_cpuset, clients=1, seconds=seconds + 3,
            topic="hostcal/fanout",
        )

    thread = threading.Thread(target=_sub, daemon=True)
    thread.start()
    time.sleep(1.0)
    pub = _run_hammer(
        "pub", cpuset=pub_cpuset, clients=2, seconds=seconds,
        topic="hostcal/fanout", rate=0,
    )
    thread.join(timeout=seconds + 15)
    return {
        "msgs_per_s": (pub or {}).get("msgs_per_s"),
        "subscriber_msgs_per_s": (result.get("sub") or {}).get("msgs_per_s"),
    }


def calibrate_host(
    *,
    profile: str = "standard",
    role: str = "runner",
    passes: int = HARNESS_COST_PASSES,
    skip_ceilings: bool = False,
    allow_busy: bool = False,
) -> Dict[str, Any]:
    """Measure this machine and return a host profile.

    Idleness is checked before the probes and again after them. The second check
    is not redundant: a machine that stayed quiet for the loadavg sample and
    then had a backup kick off halfway through would otherwise produce a profile
    that looks clean and is not.
    """
    idle_before = check_host_idle(strict=not allow_busy)
    if not idle_before["idle"] and not allow_busy:
        raise HostNotIdle(
            "refusing to calibrate a busy machine: "
            + "; ".join(idle_before["reasons"])
            + ". A calibration taken under load is committed and then governs "
            "every campaign that follows. Quiet the machine, or pass "
            "--allow-busy to write a profile that can never be the reference."
        )

    env = environment_metadata()
    identity = host_identity(env)
    harness = measure_harness_cost_ns(passes=passes)

    ceilings: Dict[str, Any] = {"harness_cost_ns_per_message": harness["ns_per_message"]}
    probes: Dict[str, Any] = {"harness_cost": harness}
    if not skip_ceilings:
        from mqtt_client_bench.telemetry import allocate_cpuset

        try:
            cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
        except RuntimeError:
            cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")
        loadgen = measure_loadgen_ceiling(cpuset=cpusets.get("loadgen"))
        fanout = measure_broker_fanout_ceiling(
            pub_cpuset=cpusets.get("loadgen"), sub_cpuset=cpusets.get("orch")
        )
        probes["loadgen"] = loadgen
        probes["broker_fanout"] = fanout
        ceilings["loadgen_ceiling_msgs_per_s"] = loadgen["msgs_per_s"]
        ceilings["loadgen_best_thread_count"] = loadgen["best_thread_count"]
        # Diagnostic. Named for what it is so nobody derives an offer from it.
        ceilings["broker_fanout_msgs_per_s"] = fanout["msgs_per_s"]

    # The probes just saturated a core group by design. Checking idleness the
    # instant they stop measures their own tail — the loadgen winding down, the
    # broker draining, the container runtime reaping — and calls the machine
    # busy for the one reason that is not a problem. Let it settle first.
    time.sleep(IDLE_AFTER_SETTLE_S)
    # Utilisation only. Load average is a trailing one-minute window that the
    # probes just saturated by design, so it reports the measurement rather
    # than the machine: it read 2.77 against a 1.60 gate straight after a run
    # whose harness spread was 1.1%, which is as quiet as this host gets.
    # A fresh utilisation window answers the question actually being asked --
    # is something *else* running now.
    idle_after = check_host_idle(strict=not allow_busy, use_loadavg=False)

    contention: List[str] = []
    if not idle_before["idle"]:
        contention += [f"before:{r}" for r in idle_before["reasons"]]
    if not idle_after["idle"]:
        contention += [f"after:{r}" for r in idle_after["reasons"]]
    if harness["spread_pct"] > IDLE_MAX_PROBE_SPREAD_PCT:
        contention.append(
            f"probe_spread={harness['spread_pct']}% over {IDLE_MAX_PROBE_SPREAD_PCT}%"
        )

    profile_doc: Dict[str, Any] = {
        "schema_version": 1,
        "role": role,
        "host": identity,
        "ceilings": ceilings,
        "probes": probes,
        "idle": {
            "before": idle_before,
            "after": idle_after,
            "probe_spread_pct": harness["spread_pct"],
            # False whenever anything suggested the machine was working. A
            # profile that is not idle-verified must never become the reference.
            "verified": not contention,
            "contention": contention,
        },
        "environment": env,
        "measured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if role == "reference" and contention:
        # Demote rather than raise. The probes are minutes of work and they are
        # still a valid picture of this machine under whatever conditions
        # actually held; throwing them away teaches an operator to reach for
        # --allow-busy, which is worse. What must not happen is the reference
        # label ending up on a measurement nobody verified.
        profile_doc["role"] = "runner"
        profile_doc["reference_refused"] = contention
    profile_doc["host_fingerprint"] = host_fingerprint(profile_doc)
    return profile_doc
