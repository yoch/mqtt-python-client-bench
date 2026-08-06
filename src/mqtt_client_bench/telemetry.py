"""Host / process / container telemetry sampling."""

from __future__ import annotations

import contextlib
import os
import platform
import subprocess
import threading
import time
from typing import Dict, List, Optional


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return None


def cpu_model() -> Optional[str]:
    text = _read_text("/proc/cpuinfo")
    if not text:
        return platform.processor() or None
    for line in text.splitlines():
        if line.lower().startswith("model name"):
            return line.split(":", 1)[1].strip()
    return None


def scaling_governor() -> Optional[str]:
    return (_read_text("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") or "").strip() or None


def loadavg() -> List[float]:
    try:
        return list(os.getloadavg())
    except OSError:
        return []


def process_stats(pid: int) -> dict:
    status = _read_text(f"/proc/{pid}/status") or ""
    rss_kb = None
    voluntary = None
    nonvoluntary = None
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            rss_kb = int(line.split()[1])
        elif line.startswith("voluntary_ctxt_switches:"):
            voluntary = int(line.split()[1])
        elif line.startswith("nonvoluntary_ctxt_switches:"):
            nonvoluntary = int(line.split()[1])
    utime = None
    stime = None
    stat = _read_text(f"/proc/{pid}/stat") or ""
    if stat:
        # After comm (parentheses), fields: ... utime stime are 14th/15th overall.
        try:
            close = stat.rfind(")")
            fields = stat[close + 2 :].split()
            utime = int(fields[11])
            stime = int(fields[12])
        except (IndexError, ValueError):
            pass
    return {
        "pid": pid,
        "rss_kb": rss_kb,
        "voluntary_ctxt_switches": voluntary,
        "nonvoluntary_ctxt_switches": nonvoluntary,
        "utime_ticks": utime,
        "stime_ticks": stime,
        "cpu_ticks": (utime + stime) if utime is not None and stime is not None else None,
    }


def self_rss_kb() -> Optional[int]:
    """Resident set size of the calling process, in KiB."""
    text = _read_text("/proc/self/status")
    if not text:
        return None
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            try:
                return int(line.split()[1])
            except (IndexError, ValueError):
                return None
    return None


class MemoryGuard:
    """Abort a role worker before it can take the machine down.

    A fire-and-forget QoS0 path whose completion fires at an in-process queue
    (rather than at the socket) leaves the harness with nothing to throttle: the
    outstanding window never engages, so the publish loop fills an unbounded
    transport buffer as fast as it can. Observed at 11.5 GB RSS in a single
    worker on 1 MiB payloads, enough to push the host into swap thrash.

    The guard is a safety net, not a fix for that semantic gap: it stops the run
    and lets it be reported inconclusive instead of letting the host die.
    """

    # Memory that may be queued between two RSS samples. Bounds the overshoot
    # past the limit, which is otherwise check_every x payload_size.
    OVERSHOOT_BUDGET_BYTES = 64 * 1024 * 1024

    def __init__(
        self,
        limit_mb: float,
        check_every: Optional[int] = None,
        payload_bytes: int = 0,
    ) -> None:
        self.limit_kb = int(limit_mb * 1024)
        if check_every is None:
            # Sample often enough that big payloads cannot run far past the
            # limit, rarely enough that small ones pay almost nothing: reading
            # /proc/self/status every message would itself be a harness tax.
            if payload_bytes > 0:
                check_every = self.OVERSHOOT_BUDGET_BYTES // max(payload_bytes, 1)
            else:
                check_every = 4096
            check_every = max(1, min(4096, int(check_every)))
        self.check_every = max(1, int(check_every))
        self._counter = 0
        self.tripped_at_kb: Optional[int] = None

    def exceeded(self) -> bool:
        """True once RSS passes the limit. Cheap: samples 1 call in N."""
        if self.tripped_at_kb is not None:
            return True
        self._counter += 1
        if self._counter % self.check_every:
            return False
        rss = self_rss_kb()
        if rss is not None and rss > self.limit_kb:
            self.tripped_at_kb = rss
            return True
        return False


def container_cgroup_path(container_name: str) -> Optional[str]:
    """Resolve a container's cgroup v2 directory, or None.

    Runs ``docker inspect`` **once** (at probe setup, outside the measure window)
    to get the container PID, then reads ``/proc/<pid>/cgroup`` — which yields the
    right path regardless of the cgroup driver (systemd vs cgroupfs).
    """
    try:
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Pid}}", container_name],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0:
        return None
    try:
        pid = int(proc.stdout.strip())
    except ValueError:
        return None
    if pid <= 0:
        return None
    text = _read_text(f"/proc/{pid}/cgroup") or ""
    for line in text.splitlines():
        # cgroup v2 has a single "0::<path>" entry.
        if line.startswith("0::"):
            rel = line[3:].strip()
            path = os.path.join("/sys/fs/cgroup", rel.lstrip("/"))
            if os.path.exists(os.path.join(path, "cpu.stat")):
                return path
            return None
    return None


def cgroup_cpu_usec(cgroup_path: str) -> Optional[int]:
    """Cumulative CPU time of a cgroup in microseconds."""
    text = _read_text(os.path.join(cgroup_path, "cpu.stat"))
    if not text:
        return None
    for line in text.splitlines():
        if line.startswith("usage_usec"):
            try:
                return int(line.split()[1])
            except (IndexError, ValueError):
                return None
    return None


def cgroup_memory_bytes(cgroup_path: str) -> Optional[int]:
    text = _read_text(os.path.join(cgroup_path, "memory.current"))
    if not text:
        return None
    try:
        return int(text.strip())
    except ValueError:
        return None


class ContainerSampler:
    """Sample a container's CPU/memory from cgroup counters.

    ``docker stats --no-stream`` spawns a docker CLI process and round-trips the
    daemon on every sample; at one sample per second inside a 12 s measure window
    that is a large, unpinned perturbation of the run being measured. Reading the
    cgroup files costs two ``open()`` calls. ``docker stats`` stays as a fallback
    when the cgroup path cannot be resolved (cgroup v1, rootless, remote daemon).
    """

    def __init__(self, container_name: str) -> None:
        self.name = container_name
        self.cgroup_path = container_cgroup_path(container_name)
        self._last: Optional[tuple] = None

    def sample(self) -> Optional[dict]:
        if self.cgroup_path is None:
            return docker_stats(self.name)
        usage = cgroup_cpu_usec(self.cgroup_path)
        now = time.monotonic()
        if usage is None:
            return docker_stats(self.name)
        cpu_pct = None
        if self._last is not None:
            last_ts, last_usage = self._last
            elapsed_us = (now - last_ts) * 1e6
            if elapsed_us > 0:
                # Same convention as docker stats: 100% == one full core.
                cpu_pct = 100.0 * (usage - last_usage) / elapsed_us
        self._last = (now, usage)
        mem_bytes = cgroup_memory_bytes(self.cgroup_path)
        return {
            "name": self.name,
            "cpu_pct": cpu_pct,
            "mem": None if mem_bytes is None else f"{mem_bytes / (1024 * 1024):.1f}MiB",
            "mem_bytes": mem_bytes,
            "cpu_usage_usec": usage,
            "source": "cgroup",
        }


def docker_stats(container_name: str) -> Optional[dict]:
    try:
        proc = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.CPUPerc}};{{.MemUsage}};{{.Name}}",
                container_name,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    line = proc.stdout.strip().splitlines()[0]
    parts = line.split(";")
    cpu = parts[0].replace("%", "").strip() if parts else None
    try:
        cpu_pct = float(cpu) if cpu else None
    except ValueError:
        cpu_pct = None
    return {"name": container_name, "cpu_pct": cpu_pct, "mem": parts[1] if len(parts) > 1 else None}


def physical_cpu_groups() -> List[List[int]]:
    """Return groups of logical CPUs sharing a physical core (SMT siblings)."""
    path = "/sys/devices/system/cpu"
    if not os.path.isdir(path):
        count = os.cpu_count() or 1
        return [[i] for i in range(count)]
    groups = {}
    for entry in sorted(os.listdir(path)):
        if not entry.startswith("cpu") or not entry[3:].isdigit():
            continue
        cpu = int(entry[3:])
        topo = os.path.join(path, entry, "topology", "core_cpus_list")
        text = _read_text(topo)
        if not text:
            groups[cpu] = [cpu]
            continue
        siblings = []
        for part in text.strip().split(","):
            if "-" in part:
                a, b = part.split("-", 1)
                siblings.extend(range(int(a), int(b) + 1))
            else:
                siblings.append(int(part))
        key = tuple(sorted(siblings))
        groups[key] = list(key)
    # Deduplicate by frozenset of siblings.
    unique = {}
    for siblings in groups.values():
        unique[frozenset(siblings)] = sorted(siblings)
    return sorted(unique.values(), key=lambda g: g[0])


def allocate_cpuset(roles: List[str], profile: str = "standard") -> Dict[str, str]:
    """Assign disjoint physical-core groups to roles.

    Rejects standard profile when fewer than len(roles) physical groups exist.
    """
    groups = physical_cpu_groups()
    if profile == "standard" and len(groups) < len(roles):
        raise RuntimeError(
            f"need {len(roles)} physical CPU groups for standard profile, found {len(groups)}"
        )
    mapping = {}
    for i, role in enumerate(roles):
        if i < len(groups):
            mapping[role] = ",".join(str(c) for c in groups[i])
        else:
            # smoke fallback: share remaining cores
            mapping[role] = ",".join(str(c) for c in groups[i % len(groups)])
    return mapping


def pin_current_process(cpuset: Optional[str]) -> Optional[str]:
    """Pin the orchestrator (and its in-process probes) to ``cpuset``.

    Role workers, the broker and the loadgen are pinned explicitly (preexec
    affinity / ``--cpuset-cpus``), but the orchestrator was not: its telemetry
    sampler and the ``$SYS`` probe could be scheduled onto the SUT cores and
    perturb the run. Returns the applied cpuset, or None when unavailable.
    """
    if not cpuset or not hasattr(os, "sched_setaffinity"):
        return None
    try:
        cpus = {int(x) for x in cpuset.split(",") if x.strip() != ""}
    except ValueError:
        return None
    if not cpus:
        return None
    try:
        os.sched_setaffinity(0, cpus)
    except OSError:
        return None
    return cpuset


@contextlib.contextmanager
def temporarily_pinned(cpuset: Optional[str]):
    """Run an in-orchestrator SUT probe on ``cpuset``, then restore affinity.

    ``connect`` and ``fleet`` scenarios drive the client library inside the
    orchestrator process. They must be measured on the SUT cores like every other
    SUT workload, not on the orchestrator's own cores.
    """
    previous = None
    if hasattr(os, "sched_getaffinity"):
        try:
            previous = os.sched_getaffinity(0)
        except OSError:
            previous = None
    pin_current_process(cpuset)
    try:
        yield
    finally:
        if previous:
            try:
                os.sched_setaffinity(0, previous)
            except OSError:
                pass


class TelemetrySampler:
    def __init__(self, pids: Optional[Dict[str, int]] = None, containers: Optional[List[str]] = None):
        self.pids = pids or {}
        self.containers = containers or []
        self.samples: List[dict] = []
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Resolve cgroup paths up front: the one docker inspect per container
        # happens before the measure window, not inside it.
        self._container_samplers = [ContainerSampler(name) for name in self.containers]

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="bench-telemetry", daemon=True)
        self._thread.start()

    def stop(self) -> List[dict]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        return list(self.samples)

    def _loop(self) -> None:
        while not self._stop.is_set():
            sample = {
                "ts": time.time(),
                "loadavg": loadavg(),
                "processes": {name: process_stats(pid) for name, pid in self.pids.items()},
                "containers": {s.name: s.sample() for s in self._container_samplers},
            }
            self.samples.append(sample)
            self._stop.wait(1.0)


def environment_metadata() -> dict:
    import socket

    installed: dict = {}
    for pkg in ("paho", "gmqtt", "aiomqtt", "amqtt", "awscrt", "zmqtt", "mqttium"):
        try:
            mod = __import__(pkg if pkg != "paho" else "paho.mqtt.client")
            if pkg == "paho":
                import paho.mqtt

                ver = getattr(paho.mqtt, "__version__", None)
                if ver is None:
                    try:
                        from importlib.metadata import version as pkg_version

                        ver = pkg_version("paho-mqtt")
                    except Exception:  # noqa: BLE001
                        ver = None
                installed[pkg] = ver
            else:
                ver = getattr(mod, "__version__", None)
                if ver is None:
                    try:
                        from importlib.metadata import version as pkg_version

                        ver = pkg_version(pkg)
                    except Exception:  # noqa: BLE001
                        ver = None
                installed[pkg] = ver
        except Exception:  # noqa: BLE001
            installed[pkg] = None
    # aiomqtt3 shares the import name with aiomqtt v2; record only when major >= 3.
    installed["aiomqtt3"] = None
    if installed.get("aiomqtt"):
        try:
            major = int(str(installed["aiomqtt"]).split(".")[0].split("a")[0].split("b")[0])
            if major >= 3:
                installed["aiomqtt3"] = installed["aiomqtt"]
                # Avoid presenting a v3 install as the stable aiomqtt v2 slot.
                installed["aiomqtt"] = None
        except ValueError:
            pass
    return {
        "hostname": socket.gethostname(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count(),
        "physical_cpu_groups": physical_cpu_groups(),
        "scaling_governor": scaling_governor(),
        "loadavg": loadavg(),
        "client_versions": installed,
    }
