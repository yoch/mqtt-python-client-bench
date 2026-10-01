"""Resource readings taken from outside the measured process.

Nothing here runs inside the client's process: the orchestrator reads
``/proc/<pid>`` at the window boundaries, so measuring costs the client
nothing and cannot be skewed by it.

- CPU: ``utime``/``stime`` from ``/proc/<pid>/stat`` split user from kernel
  time for the whole process, including threads that already exited, at
  ``CLK_TCK`` resolution (10 ms). The sum of ``/proc/<pid>/task/*/schedstat``
  gives the same total at nanosecond resolution for the threads alive at the
  reading, which is what the per-message figures use.
- Memory: ``VmRSS`` at each boundary, and ``VmHWM`` after it has been reset
  at the window start (``clear_refs`` = 5), so the peak is the window's own
  and not import time's.
- The rest of the host: ``/proc/stat`` gives the busy time by state and by
  CPU, and one pass over ``/proc/<pid>/stat`` at each boundary says which
  other processes used it. This explains a ``host_quiet`` failure after the
  fact; it never changes a count or a cost.
"""

from __future__ import annotations

import os
from typing import Dict, Iterable, List, Optional, Tuple

CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def _read(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="ascii", errors="replace") as fh:
            return fh.read()
    except OSError:
        return None


def process(pid: int) -> Optional[dict]:
    stat = _read(f"/proc/{pid}/stat")
    if stat is None:
        return None
    fields = stat[stat.rfind(")") + 2 :].split()
    out: Dict[str, int] = {
        "utime_ticks": int(fields[11]),
        "stime_ticks": int(fields[12]),
        "threads": int(fields[17]),
    }
    for line in (_read(f"/proc/{pid}/status") or "").splitlines():
        key, _, rest = line.partition(":")
        if key == "VmRSS":
            out["rss_kb"] = int(rest.split()[0])
        elif key == "VmHWM":
            out["hwm_kb"] = int(rest.split()[0])
        elif key == "voluntary_ctxt_switches":
            out["ctx_voluntary"] = int(rest)
        elif key == "nonvoluntary_ctxt_switches":
            out["ctx_involuntary"] = int(rest)
    total = 0
    try:
        tasks = os.listdir(f"/proc/{pid}/task")
    except OSError:
        tasks = []
    for tid in tasks:
        text = _read(f"/proc/{pid}/task/{tid}/schedstat")
        if text:
            total += int(text.split()[0])
    out["cpu_ns"] = total
    return out


def reset_peak_rss(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/clear_refs", "w", encoding="ascii") as fh:
            fh.write("5")
        return True
    except OSError:
        return False


STATES = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
_IDLE_STATES = ("idle", "iowait")


def _cpu_line(parts: List[str]) -> Dict[str, int]:
    vals = [int(x) for x in parts[1:9]]
    return dict(zip(STATES, vals + [0] * (len(STATES) - len(vals))))


def parse_proc_stat(text: str) -> dict:
    """The aggregate and per-CPU jiffies of ``/proc/stat``.

    ``busy_ticks`` is everything but idle and iowait, softirq and irq included:
    kernel time that no process is charged for still counts as the host's.
    """
    lines = [line.split() for line in text.splitlines() if line.startswith("cpu")]
    total = _cpu_line(lines[0])
    busy = sum(v for k, v in total.items() if k not in _IDLE_STATES)
    per_cpu = []
    for parts in lines[1:]:
        cpu = _cpu_line(parts)
        per_cpu.append(sum(v for k, v in cpu.items() if k not in _IDLE_STATES))
    return {
        "busy_ticks": busy,
        "total_ticks": busy + total["idle"] + total["iowait"],
        "states": total,
        "per_cpu_busy_ticks": per_cpu,
    }


def host_cpu() -> Optional[dict]:
    text = _read("/proc/stat")
    return parse_proc_stat(text) if text else None


def host_breakdown(a: dict, b: dict, wall_s: float) -> dict:
    """Where the host's busy time went between two ``host_cpu`` readings, in cores."""
    states = {
        k: round((b["states"][k] - a["states"][k]) / CLK_TCK / wall_s, 4) for k in STATES if k not in _IDLE_STATES
    }
    per_cpu = [round((y - x) / CLK_TCK / wall_s, 3) for x, y in zip(a["per_cpu_busy_ticks"], b["per_cpu_busy_ticks"])]
    return {"states": states, "per_cpu": per_cpu}


def parse_process_stat(text: str) -> Tuple[str, int]:
    """(command name, utime + stime ticks) of one ``/proc/<pid>/stat``.

    The name sits between the first ``(`` and the last ``)`` and may contain
    spaces and parentheses, so fields are counted from the last ``)``.
    """
    comm = text[text.find("(") + 1 : text.rfind(")")]
    fields = text[text.rfind(")") + 2 :].split()
    return comm, int(fields[11]) + int(fields[12])


def host_processes() -> Dict[int, Tuple[str, int]]:
    """CPU ticks of every process alive now, threads included."""
    out: Dict[int, Tuple[str, int]] = {}
    try:
        entries = os.listdir("/proc")
    except OSError:
        return out
    for entry in entries:
        if not entry.isdigit():
            continue
        text = _read(f"/proc/{entry}/stat")
        if text:
            try:
                out[int(entry)] = parse_process_stat(text)
            except (ValueError, IndexError):
                continue
    return out


def cgroup_pids(cgroup_path: Optional[str]) -> List[int]:
    if not cgroup_path:
        return []
    return [int(x) for x in (_read(os.path.join(cgroup_path, "cgroup.procs")) or "").split() if x.isdigit()]


def attribute(
    a: Dict[int, Tuple[str, int]],
    b: Dict[int, Tuple[str, int]],
    exclude: Iterable[int],
    wall_s: float,
    *,
    labels: Optional[Dict[int, str]] = None,
    limit: int = 5,
    floor: float = 0.01,
) -> dict:
    """Who used the CPU between two ``host_processes`` readings, apart from ``exclude``.

    Processes are grouped by command name. Only processes alive at both
    readings are seen, so a process that started and ended inside the window
    is part of the host's busy time but not of this list.
    """
    skip = set(exclude)
    labels = labels or {}
    by_name: Dict[str, float] = {}
    for pid, (comm, ticks) in b.items():
        if pid in skip or pid not in a:
            continue
        used = (ticks - a[pid][1]) / CLK_TCK / wall_s
        if used > 0:
            name = labels.get(pid, comm)
            by_name[name] = by_name.get(name, 0.0) + used
    ranked = sorted(by_name.items(), key=lambda kv: -kv[1])
    return {
        "top": [{"comm": name, "cores": round(cores, 3)} for name, cores in ranked[:limit] if cores >= floor],
        "processes_cores": sum(by_name.values()),
    }


def cgroup_cpu_ns(cgroup_path: Optional[str]) -> Optional[int]:
    if not cgroup_path:
        return None
    for line in (_read(os.path.join(cgroup_path, "cpu.stat")) or "").splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1]) * 1000
    return None


def process_delta(a: Optional[dict], b: Optional[dict]) -> Optional[dict]:
    """What a process consumed between two readings."""
    if not a or not b:
        return None
    user_s = (b["utime_ticks"] - a["utime_ticks"]) / CLK_TCK
    sys_s = (b["stime_ticks"] - a["stime_ticks"]) / CLK_TCK
    cpu_ns = b["cpu_ns"] - a["cpu_ns"]
    source = "schedstat"
    if cpu_ns < 0 or a.get("threads") != b.get("threads"):
        # A thread that exited took its schedstat with it; the process-wide
        # tick counters still hold it.
        cpu_ns = int((user_s + sys_s) * 1e9)
        source = "ticks"
    return {
        "cpu_user_s": user_s,
        "cpu_sys_s": sys_s,
        "cpu_ns": cpu_ns,
        "cpu_source": source,
        "rss_start_kb": a.get("rss_kb"),
        "rss_end_kb": b.get("rss_kb"),
        "rss_peak_kb": b.get("hwm_kb"),
        "threads": b.get("threads"),
        "ctx_voluntary": b.get("ctx_voluntary", 0) - a.get("ctx_voluntary", 0),
        "ctx_involuntary": b.get("ctx_involuntary", 0) - a.get("ctx_involuntary", 0),
    }


def scaling_governor() -> Optional[str]:
    text = _read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    return text.strip() if text else None
