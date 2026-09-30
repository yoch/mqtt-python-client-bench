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
"""

from __future__ import annotations

import os
from typing import Dict, Optional

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


def host_cpu() -> Optional[dict]:
    """Aggregate busy and total jiffies over every CPU."""
    text = _read("/proc/stat")
    if not text:
        return None
    parts = text.splitlines()[0].split()[1:]
    vals = [int(x) for x in parts]
    idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
    total = sum(vals[:8])
    return {"busy_ticks": total - idle, "total_ticks": total}


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
