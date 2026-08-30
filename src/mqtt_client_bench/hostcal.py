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
import platform
import shutil
import subprocess
import time
from pathlib import Path
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


def broker_identity() -> Dict[str, Any]:
    """The broker these ceilings were measured against.

    The fan-out rate is a property of *this* Mosquitto: a different version, a
    different build of the image, or a changed config makes the number mean
    something else. Recording the image without its digest would not be enough
    - a tag moves - so both travel, alongside the config hash the harness
    already computes for its own broker checks.
    """
    from mqtt_client_bench.broker import MOSQUITTO_IMAGE, config_hash, image_digest

    return {
        "image": MOSQUITTO_IMAGE,
        "image_digest": image_digest(MOSQUITTO_IMAGE),
        "config_hash": config_hash(),
    }


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
    # The harness cost is an interpreter cost before it is anything else, so a
    # different Python makes it a different number. The kernel is deliberately
    # left out: a point release moves it without moving any measurement, and a
    # fingerprint that churns for reasons nobody can act on gets ignored.
    "python",
)

# The broker is measured, not merely present: the fan-out ceiling is a property
# of this Mosquitto build and this config, and both belong in the digest for the
# same reason the CPU does.
_FINGERPRINT_BROKER = ("image", "image_digest", "config_hash")
_FINGERPRINT_CEILINGS = (
    "harness_cost_ns_per_message",
    "broker_paced_ceiling_msgs_per_s",
    "recommended_offer_msgs_per_s",
    "broker_fanout_msgs_per_s",
)


def host_fingerprint(profile: Dict[str, Any]) -> str:
    """Short digest over the identity and the ceilings, in the shape of
    ``provenance.harness_fingerprint``."""
    identity = profile.get("host") or {}
    ceilings = profile.get("ceilings") or {}
    broker = profile.get("broker") or {}
    digest = hashlib.sha256()
    for key in _FINGERPRINT_IDENTITY:
        digest.update(f"{key}={identity.get(key)!r}\0".encode("utf-8"))
    for key in _FINGERPRINT_BROKER:
        digest.update(f"broker.{key}={broker.get(key)!r}\0".encode("utf-8"))
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


class CalibrationFailed(RuntimeError):
    """A probe could not measure what it was asked to.

    Distinct from a low number. Zero msgs/s is not a slow host, it is a probe
    that reached nothing — measured here when the loadgen ran with no broker
    listening and every thread step returned 0.0. The profile was still written,
    marked idle-verified and signed, and would have become the committed
    reference whose ceilings drive every campaign offer. A probe that reports a
    number where it should refuse is the failure mode this whole module exists
    to remove; it does not get an exemption for being the module itself.
    """


class HostNotIdle(RuntimeError):
    """Raised when a calibration is asked for on a machine that is working.

    Deliberately an exception and not a warning: the whole value of a host
    profile is that the numbers in it are the host's, not the host's minus
    whatever else was running when someone happened to run the probe.
    """



# Total wall-clock budget for one calibration, in seconds.
#
# Generous on purpose. A host is calibrated once and the profile is then
# committed and read by every campaign that follows, so a few minutes spent
# here is bought back many times over — and short probes were visibly not
# enough. At five seconds per step the thread sweep read 131k / 326k / 637k /
# 217k on one run and 174k / 232k / 222k / 226k on the next: the ceiling was in
# the right band both times, but the *best thread count* moved from 4 to 2,
# which is not a number worth recording if it changes between two consecutive
# measurements of the same machine.
CALIBRATION_BUDGET_S = 300.0

# How the budget is split. The thread sweep gets the bulk because it is the
# noisiest measurement and the one whose answer is a choice, not a magnitude.
_BUDGET_HARNESS_SHARE = 0.10
_BUDGET_FANOUT_SHARE = 0.10
_BUDGET_SWEEP_SHARE = 0.80


def probe_durations(budget_s: float = CALIBRATION_BUDGET_S) -> Dict[str, Any]:
    """Split a calibration budget across the probes."""
    budget = max(30.0, float(budget_s))
    harness_total = budget * _BUDGET_HARNESS_SHARE
    passes = max(5, int(harness_total / HARNESS_COST_WINDOW_S))
    # The paced sweep is the bulk: one run per grid point per repeat.
    runs = max(1, len(PACED_GRID) * PACED_REPEATS)
    per_step = budget * _BUDGET_SWEEP_SHARE / runs
    return {
        "budget_s": budget,
        "harness_passes": passes,
        "sweep_step_s": max(2, int(per_step)),
        "fanout_s": max(2, int(budget * _BUDGET_FANOUT_SHARE)),
    }


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


# Paced offers to walk, in msgs/s. Below the knee a paced hammer delivers what
# it was asked for to within a fraction of a percent; above it, delivery flattens
# at the broker's rate.
PACED_GRID = (50_000, 100_000, 150_000, 200_000, 250_000, 300_000, 400_000)

# Repeats per step. Two would do below the knee, where the spread is 0.3%; the
# plateau is the noisier part and three keeps it honest without tripling the
# whole sweep's cost.
PACED_REPEATS = 3

# A step counts as delivered when it holds this share of what was asked.
PACED_HOLD_RATIO = 0.97

# Publisher threads. Two hold well past this broker's ceiling, and sweeping the
# count is what the unpaced probe used to do — pointlessly, since the number it
# was reading back was the broker's and not the loadgen's.
PACED_PUB_THREADS = 2

# How far below the measured knee a campaign offer should sit.
#
# Not caution for its own sake: an offer *at* the knee lands above it on about
# half the runs, and above the knee the offer stops being an offer — delivery
# flattens at whatever the broker does, which is the degenerate regime this
# probe exists to stay out of. A margin keeps every run in the part of the curve
# where what was asked for is what arrives.
#
# 0.85 of the 231k measured here gives ~196k, which is where the hand-set
# constant of 200k already sat. That the two agree is the useful part: the
# constant was about right, and now there is a measurement behind it and a rule
# that travels to another machine.
OFFER_SAFETY_MARGIN = 0.85


def measure_broker_paced_ceiling(*, cpuset: Optional[str], seconds: int) -> Dict[str, Any]:
    """The rate this broker sustains when arrivals are paced.

    Named for what it measures. The probe this replaces was called a *loadgen*
    ceiling and never measured one: it wrote into Mosquitto, which was the
    limiting element in every configuration it could be run in. There is no
    setting in which it would have measured the generator alone, because that
    needs a sink that never limits and Mosquitto is not one.

    It also measured in a regime the bench never operates in. Unpaced, this is a
    closed loop — the writer blocks, so its rate is whatever the broker drains —
    and the loop is *degenerate*: throughput is ``reads/s x packets-per-read``,
    while packets-per-read is itself ``throughput x cycle-time``. Substituting
    gives R = R. Every rate is a fixed point, so the system sits wherever it
    landed and stays there. Measured across eight identical runs: 233k, 235k,
    333k, 559k, 595k, 702k, 734k, with the broker at 98-100% CPU throughout and
    its read rate varying by only 25%.

    Pacing breaks the degeneracy by fixing arrivals, and campaigns pace, so this
    is also the regime they run in. Measured on the reference host:

        --rate 100000 ->  99985  99984  99991   (0.007%)
        --rate 200000 -> 199099 199728 199161   (0.3%)
        --rate 400000 -> 230023 232483 231876   (plateau, 1%)

    Saturating the receive queue instead - the other candidate fix - makes it
    worse rather than better: at a 4096-byte payload the unpaced rate splits
    48391 / 152124 / 49048 / 144648. Non-blocking writes would not have helped
    either; the hammer already retries on EAGAIN, so it would spin instead of
    sleeping on the same code path.

    Returns the highest *delivered* rate that still tracked its offer. Steps past
    the knee are kept for the record but never become the ceiling.
    """
    steps: List[Dict[str, Any]] = []
    ceiling: Optional[float] = None
    for target in PACED_GRID:
        samples: List[float] = []
        for _ in range(PACED_REPEATS):
            parsed = _run_hammer(
                "pub", cpuset=cpuset, clients=PACED_PUB_THREADS, seconds=seconds,
                topic="hostcal/paced", rate=target,
            )
            value = (parsed or {}).get("msgs_per_s")
            if value:
                samples.append(float(value))
        if not samples:
            continue
        samples.sort()
        mid = len(samples) // 2
        median = samples[mid] if len(samples) % 2 else 0.5 * (samples[mid - 1] + samples[mid])
        held = median >= PACED_HOLD_RATIO * target
        steps.append(
            {
                "offer_msgs_per_s": target,
                "delivered_msgs_per_s": round(median, 1),
                "held": held,
                "samples": [round(v, 1) for v in samples],
            }
        )
        if held:
            ceiling = max(ceiling or 0.0, median)
        else:
            # First offer the broker cannot hold. Everything above it is the
            # plateau, and one of those steps drifting high is the degenerate
            # regime returning, not a higher ceiling.
            break
    if not ceiling:
        raise CalibrationFailed(
            "no paced offer was delivered: the probe reached nothing. Check that "
            "the broker is reachable and that mqtt_hammer builds."
        )
    return {
        "msgs_per_s": round(ceiling, 1),
        # What a campaign should actually offer: the knee is where delivery
        # stops tracking, not where it is safe to sit.
        "recommended_offer_msgs_per_s": round(ceiling * OFFER_SAFETY_MARGIN, 1),
        "safety_margin": OFFER_SAFETY_MARGIN,
        "repeats": PACED_REPEATS,
        "threads": PACED_PUB_THREADS,
        "steps": steps,
    }


def measure_broker_fanout_ceiling(
    *, pub_cpuset: Optional[str], sub_cpuset: Optional[str], seconds: int
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
    budget_s: float = CALIBRATION_BUDGET_S,
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

    plan = probe_durations(budget_s)
    env = environment_metadata()
    identity = host_identity(env)
    harness = measure_harness_cost_ns(passes=plan["harness_passes"])

    ceilings: Dict[str, Any] = {"harness_cost_ns_per_message": harness["ns_per_message"]}
    probes: Dict[str, Any] = {"harness_cost": harness, "budget": plan}
    if not skip_ceilings:
        from mqtt_client_bench.broker import broker_down, broker_up, broker_running
        from mqtt_client_bench.telemetry import allocate_cpuset

        try:
            cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
        except RuntimeError:
            cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")

        # Establish the broker rather than hope for one. Every ceiling here is
        # measured *through* it, and the probes have no way to tell "this host
        # is slow" from "nothing was listening" — they both come back as a
        # number. Leave the machine as it was found.
        was_running = broker_running()
        if not was_running:
            broker_up(wait=True, cpuset=cpusets.get("broker"))
        try:
            paced = measure_broker_paced_ceiling(
                cpuset=cpusets.get("loadgen"), seconds=plan["sweep_step_s"]
            )
            fanout = measure_broker_fanout_ceiling(
                pub_cpuset=cpusets.get("loadgen"),
                sub_cpuset=cpusets.get("orch"),
                seconds=plan["fanout_s"],
            )
        finally:
            if not was_running:
                broker_down()

        probes["paced_sweep"] = paced
        probes["broker_fanout"] = fanout
        for label, value in (
            ("broker_paced_ceiling_msgs_per_s", paced["msgs_per_s"]),
            ("broker_fanout_msgs_per_s", fanout["msgs_per_s"]),
        ):
            if not value:
                raise CalibrationFailed(
                    f"{label} came back {value!r}: the probe reached nothing. "
                    "Check that the broker is reachable and that mqtt_hammer "
                    "builds; a zero ceiling is not a slow host."
                )
        ceilings["broker_paced_ceiling_msgs_per_s"] = paced["msgs_per_s"]
        # The number an offer is derived from. Recorded next to the ceiling so
        # a reader sees both the measurement and the margin taken off it.
        ceilings["recommended_offer_msgs_per_s"] = paced["recommended_offer_msgs_per_s"]
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
        # Absent when --skip-ceilings ran: no broker was measured, so claiming
        # one would put a digest in the fingerprint that nothing stood behind.
        "broker": None if skip_ceilings else broker_identity(),
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


# --- Reading profiles back --------------------------------------------------

HOSTS_DIR = Path(__file__).resolve().parents[2] / "hosts"


def list_host_profiles(hosts_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Every committed host profile, newest name order."""
    directory = Path(hosts_dir) if hosts_dir else HOSTS_DIR
    if not directory.is_dir():
        return []
    out = []
    for path in sorted(directory.glob("*.json")):
        try:
            out.append(load_host_profile(str(path)))
        except (OSError, ValueError):
            continue
    return out


def reference_profile(hosts_dir: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """The one profile marked ``reference``.

    More than one is a repository error rather than a runtime choice: the site
    publishes exactly one host, so two candidates means nobody can say which
    numbers are the published ones.
    """
    candidates = [p for p in list_host_profiles(hosts_dir) if p.get("role") == "reference"]
    if len(candidates) > 1:
        names = ", ".join(str(p.get("host_fingerprint")) for p in candidates)
        raise ValueError(f"more than one reference host profile: {names}")
    return candidates[0] if candidates else None


def result_host_key(document: Dict[str, Any]) -> Dict[str, Any]:
    """Which machine produced this result document.

    A fingerprinted run says so itself. Everything measured before host
    profiles existed - the whole committed corpus - has to be identified from
    its ``environment`` block instead, and is marked as such: those runs are
    comparable with each other, because they came off one machine in one
    posture, but they carry no evidence of the ceilings they ran against and
    must never be pooled with runs that do.
    """
    profile = document.get("host_profile") or {}
    fingerprint = profile.get("fingerprint")
    if fingerprint:
        return {
            "fingerprint": str(fingerprint),
            "role": profile.get("role"),
            "hostname": profile.get("hostname"),
            "legacy": False,
        }
    env = document.get("environment") or {}
    return {
        "fingerprint": None,
        "role": None,
        "hostname": env.get("hostname"),
        "cpu_model": env.get("cpu_model"),
        "scaling_governor": env.get("scaling_governor"),
        "legacy": True,
    }


def matches_reference(host_key: Dict[str, Any], profile: Optional[Dict[str, Any]]) -> bool:
    """Does this result belong to the published host?

    With no reference profile in the repository nothing is filtered - that is
    the state the corpus is in today, and a report that silently emptied itself
    the moment the mechanism landed would be worse than one that publishes what
    it always did.
    """
    if profile is None:
        return True
    if host_key.get("fingerprint"):
        return host_key["fingerprint"] == profile.get("host_fingerprint")
    host = profile.get("host") or {}
    if host_key.get("hostname") is None and host_key.get("cpu_model") is None:
        # No evidence either way. Only *positive* evidence of a different
        # machine excludes a document: a filter that drops whatever it cannot
        # attribute empties the site for anyone whose results predate the
        # `environment` block, which is a worse failure than publishing a
        # document whose provenance is merely unrecorded.
        return True
    return (
        host_key.get("hostname") == host.get("hostname")
        and host_key.get("cpu_model") == host.get("cpu_model")
    )


def results_dir_for(
    profile: Optional[Dict[str, Any]], base: str = "results"
) -> str:
    """Where a campaign on this host should write.

    The reference host owns `results/` because that is what the site publishes.
    A runner writes under `results/<hostname>-<fingerprint>/`, and the reason is
    not tidiness: campaign output is named `<client>-<scenario>.json`, so a
    runner writing to the same directory would silently overwrite the published
    corpus file by file. `report build` only globs the top level, so a runner's
    directory is invisible to the default build and readable on its own with
    `--reference none`.

    An uncalibrated host also gets a subdirectory. It has no business writing
    into the published corpus either, and it cannot even say which machine it
    is.
    """
    if profile and profile.get("role") == "reference":
        return base
    if not profile:
        return f"{base}/uncalibrated"
    host = (profile.get("host") or {}).get("hostname") or "unknown"
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in str(host))
    return f"{base}/{safe}-{profile.get('host_fingerprint')}"


def resolve_host_profile(
    path: Optional[str] = None, *, hosts_dir: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """The profile to stamp onto this run.

    An explicit path wins. Otherwise the committed profiles are searched for one
    whose machine facts match this host, so a run on a calibrated machine is
    stamped without anyone having to remember a flag — and a run on a machine
    nobody calibrated is stamped with nothing, which is the honest answer.
    """
    if path:
        return load_host_profile(path)
    mine = host_identity()
    for profile in list_host_profiles(hosts_dir):
        host = profile.get("host") or {}
        if all(
            host.get(key) == mine.get(key)
            for key in ("cpu_model", "cpu_count", "physical_groups", "threads_per_group", "frequency_policy")
        ):
            return profile
    return None


def host_profile_summary(profile: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """What travels inside a result: the fingerprint, the role, the ceilings.

    Not the probe traces. A result carries what is needed to read its numbers
    back and to refuse to pool it with another machine's; the raw walks stay in
    ``hosts/``, where they are stored once instead of a few hundred times.
    """
    if not profile:
        return None
    host = profile.get("host") or {}
    return {
        "fingerprint": profile.get("host_fingerprint"),
        "role": profile.get("role"),
        "hostname": host.get("hostname"),
        "frequency_policy": host.get("frequency_policy"),
        "idle_verified": (profile.get("idle") or {}).get("verified"),
        "ceilings": profile.get("ceilings"),
        "measured_at": profile.get("measured_at"),
    }
