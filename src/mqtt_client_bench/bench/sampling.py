"""Explicit, non-comparable Tachyon captures of a client worker only."""

from __future__ import annotations

import ctypes
import os
import subprocess
import sys
import time
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

MODES = ("cpu", "wall", "gil", "exception", "async-running", "async-all")


@dataclass(frozen=True)
class Sampling:
    mode: str
    rate: int
    output: Path

    def __post_init__(self) -> None:
        if self.mode not in MODES or self.rate <= 0:
            raise ValueError(
                "sampling requires a supported mode and a positive frequency"
            )

    def as_dict(self) -> dict:
        return {"tool": "profiling.sampling", "mode": self.mode, "rate_hz": self.rate}

    def flags(self) -> list:
        if self.mode.startswith("async-"):
            return ["--async-aware", "--async-mode", self.mode.split("-", 1)[1]]
        return ["--mode", self.mode, "--all-threads"]


def authorize_parent() -> None:
    """Allow this worker's parent and descendants to inspect it under Linux Yama.

    Only called for an explicitly requested capture. This changes the worker's
    permission, not the machine's ptrace policy, and needs no root capability.
    """
    if sys.platform != "linux":
        return
    scope = Path("/proc/sys/kernel/yama/ptrace_scope")
    if not scope.exists() or scope.read_text(encoding="ascii").strip() == "0":
        return  # kernels without Yama do not implement PR_SET_PTRACER
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    libc.prctl.restype = ctypes.c_int
    if libc.prctl(0x59616D61, os.getppid(), 0, 0, 0) != 0:  # PR_SET_PTRACER
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))


def preflight(python: Path) -> None:
    subprocess.run(
        [str(python), "-I", "-c", "import profiling.sampling"],
        capture_output=True,
        text=True,
        check=True,
    )


class Capture:
    def __init__(
        self,
        python: Path,
        pid: int,
        settings: Sampling,
        run_id: str,
        duration: float,
        cpuset: Optional[str] = None,
    ):
        self.python = python
        self.settings = settings
        self.root = settings.output / run_id
        self.root.mkdir(parents=True, exist_ok=False)
        self.async_aware = settings.mode.startswith("async-")
        self.profile = self.root / (
            "flamegraph.html" if self.async_aware else "profile.bin"
        )
        self.log = (self.root / "capture.log").open("w", encoding="utf-8")
        argv = [
            str(python),
            "-I",
            "-m",
            "profiling.sampling",
            "attach",
            *settings.flags(),
            "--sampling-rate",
            str(settings.rate),
            "--duration",
            str(math.ceil(duration)),
            *(
                ["--flamegraph"]
                if self.async_aware
                else ["--binary", "--compression", "none"]
            ),
            "--output",
            str(self.profile),
            str(pid),
        ]
        if cpuset:
            argv = ["taskset", "-c", cpuset, *argv]
        self.info = dict(
            settings.as_dict(),
            target_pid=pid,
            requested_duration_s=duration,
            started_ns=time.monotonic_ns(),
            cpuset=cpuset,
            argv=argv,
        )
        try:
            self.proc = subprocess.Popen(
                argv, stdout=self.log, stderr=subprocess.STDOUT
            )
        except BaseException:
            self.log.close()
            raise

    def finish(self, timeout: float = 15.0) -> dict:
        try:
            rc = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.close()
            raise RuntimeError(
                f"Tachyon exceeded its capture deadline; see {self.root / 'capture.log'}"
            )
        self.log.close()
        if rc != 0 or not self.profile.is_file() or not self.profile.stat().st_size:
            raise RuntimeError(
                f"Tachyon failed (exit {rc}); see {self.root / 'capture.log'}"
            )
        html = self.root / "flamegraph.html"
        # Conversion follows the measured window and never consumes the worker's core.
        if not self.async_aware:
            with (self.root / "replay.log").open("w", encoding="utf-8") as log:
                subprocess.run(
                    [
                        str(self.python),
                        "-I",
                        "-m",
                        "profiling.sampling",
                        "replay",
                        str(self.profile),
                        "--flamegraph",
                        "--output",
                        str(html),
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=60,
                    check=True,
                )
        # CPython 3.15 embeds the complete tree as JSON in its self-contained HTML.
        # Decode data only; never execute the embedded JavaScript. Detect format drift.
        html_text = html.read_text(encoding="utf-8")
        marker = "const EMBEDDED_DATA = "
        if marker not in html_text:
            raise RuntimeError(
                "Tachyon flamegraph data format changed; raw capture retained"
            )
        tree, _ = json.JSONDecoder().raw_decode(html_text.split(marker, 1)[1])
        if not tree.get("value"):
            raise RuntimeError(
                "Tachyon captured no stacks; extend the capture or select another mode"
            )
        data_path = self.root / "profile.json"
        data_path.write_text(json.dumps(tree) + "\n", encoding="utf-8")
        info = dict(
            self.info,
            raw_profile=str(self.profile),
            flamegraph=str(html),
            bytes=self.profile.stat().st_size,
            profile_format="flamegraph" if self.async_aware else "binary",
            profile_data=str(data_path),
            stack_weight=tree["value"],
            statistics=tree.get("stats"),
        )
        (self.root / "metadata.json").write_text(
            json.dumps(info, indent=2) + "\n", encoding="utf-8"
        )
        return info

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()
