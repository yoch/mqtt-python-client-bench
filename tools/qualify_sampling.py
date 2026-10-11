"""Exercise Tachyon against known CPU, threaded and suspended-async workloads.

Run with Python 3.15 and PYTHONPATH=src. No broker is needed. Captures and
qualification.json go to a new external directory; failures retain evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

from mqtt_client_bench.bench import runtime, sampling


def busy_work(seconds=0.005):
    stop = time.perf_counter() + seconds
    value = 1
    while time.perf_counter() < stop:
        value = (value * 17 + 3) % 1000003
    return value


async def suspended_task():
    await asyncio.Event().wait()


async def computing_task():
    while True:
        busy_work()
        await asyncio.sleep(0.005)


def thread_work(stop):
    while not stop.is_set():
        busy_work()
        stop.wait(0.005)


async def worker():
    sampling.authorize_parent()
    stop = threading.Event()
    thread = threading.Thread(target=thread_work, args=(stop,), name="known-cpu-thread")
    thread.start()
    computing = asyncio.create_task(computing_task(), name="known-computing-task")
    waiting = asyncio.create_task(suspended_task(), name="known-suspended-task")
    try:
        await asyncio.sleep(0.05)
        print("READY", flush=True)
        await asyncio.Event().wait()
    finally:
        stop.set()
        thread.join()
        for task in (computing, waiting):
            task.cancel()
        await asyncio.gather(computing, waiting, return_exceptions=True)


def qualify(root: Path, seconds: int):
    root.mkdir(parents=True, exist_ok=False)
    outcomes = []
    for mode in ("cpu", "async-running", "async-all"):
        capture = None
        with (root / f"{mode}-worker.log").open("w", encoding="utf-8") as log:
            proc = subprocess.Popen(
                [sys.executable, __file__, "--worker"],
                stdout=subprocess.PIPE,
                stderr=log,
                text=True,
            )
            try:
                pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                ready = pool.submit(proc.stdout.readline)
                try:
                    if ready.result(timeout=15).strip() != "READY":
                        raise RuntimeError(
                            f"fixture failed before READY; see {log.name}"
                        )
                finally:
                    pool.shutdown(wait=False)
                capture = sampling.Capture(
                    Path(sys.executable),
                    proc.pid,
                    sampling.Sampling(mode, 1000, root),
                    mode,
                    seconds,
                )
                info = capture.finish(timeout=seconds + 15)
                tree = json.loads(
                    Path(info["profile_data"]).read_text(encoding="utf-8")
                )
                strings = json.dumps(tree.get("strings", []))
                expected = (
                    "busy_work"
                    if mode == "cpu"
                    else "known-computing-task"
                    if mode == "async-running"
                    else "known-suspended-task"
                )
                if expected not in strings:
                    raise RuntimeError(f"{mode} did not capture expected {expected}")
                outcomes.append(
                    {"mode": mode, "expected_frame": expected, "passed": True, **info}
                )
            finally:
                if capture is not None:
                    capture.close()
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                proc.stdout.close()
    (root / "qualification.json").write_text(
        json.dumps(
            {"runtime": runtime.local_identity(), "outcomes": outcomes}, indent=2
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"qualified {len(outcomes)} Tachyon recipes; {root / 'qualification.json'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--seconds", type=int, default=2)
    args = parser.parse_args()
    if args.worker:
        asyncio.run(worker())
        return
    if sys.version_info[:2] != (3, 15) or args.seconds <= 0 or args.output is None:
        parser.error("Python 3.15, a positive duration and --output are required")
    qualify(args.output.absolute(), args.seconds)


if __name__ == "__main__":
    main()
