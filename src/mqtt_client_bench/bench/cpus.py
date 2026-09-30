"""Physical-core allocation, orchestrator pinning, and the broker's cgroup."""

from __future__ import annotations

import os
import subprocess
from typing import Dict, List, Optional


def _read(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="ascii", errors="replace") as fh:
            return fh.read()
    except OSError:
        return None


def physical_cpu_groups() -> List[List[int]]:
    """Logical CPUs grouped by the physical core they share (SMT siblings)."""
    root = "/sys/devices/system/cpu"
    if not os.path.isdir(root):
        return [[i] for i in range(os.cpu_count() or 1)]
    unique: Dict[frozenset, List[int]] = {}
    for entry in sorted(os.listdir(root)):
        if not entry.startswith("cpu") or not entry[3:].isdigit():
            continue
        cpu = int(entry[3:])
        text = _read(os.path.join(root, entry, "topology", "core_cpus_list"))
        siblings: List[int] = []
        for part in (text or str(cpu)).strip().split(","):
            if "-" in part:
                a, b = part.split("-", 1)
                siblings.extend(range(int(a), int(b) + 1))
            else:
                siblings.append(int(part))
        unique[frozenset(siblings)] = sorted(siblings)
    return sorted(unique.values(), key=lambda g: g[0])


def allocate_cpuset(roles: List[str], *, strict: bool) -> Dict[str, str]:
    """One physical core per role; ``strict`` refuses to share cores."""
    groups = physical_cpu_groups()
    if strict and len(groups) < len(roles):
        raise RuntimeError(f"need {len(roles)} physical cores (one per role), found {len(groups)}")
    return {role: ",".join(str(c) for c in groups[i % len(groups)]) for i, role in enumerate(roles)}


def pin_current_process(cpuset: Optional[str]) -> Optional[str]:
    if not cpuset or not hasattr(os, "sched_setaffinity"):
        return None
    try:
        os.sched_setaffinity(0, {int(x) for x in cpuset.split(",") if x.strip()})
    except (OSError, ValueError):
        return None
    return cpuset


def container_cgroup_path(container_name: str) -> Optional[str]:
    """A container's cgroup v2 directory, resolved once through its PID."""
    try:
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Pid}}", container_name],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    try:
        pid = int(proc.stdout.strip()) if proc.returncode == 0 else 0
    except ValueError:
        return None
    if pid <= 0:
        return None
    for line in (_read(f"/proc/{pid}/cgroup") or "").splitlines():
        if line.startswith("0::"):
            path = os.path.join("/sys/fs/cgroup", line[3:].strip().lstrip("/"))
            return path if os.path.exists(os.path.join(path, "cpu.stat")) else None
    return None
