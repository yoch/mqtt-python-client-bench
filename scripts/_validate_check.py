"""Decide whether a validation smoke exposed a code fault.

Not "is the run valid" — a smoke run shares cores and saturates the broker, so
inconclusive is the norm and says nothing about the code. The fault that matters
is a worker that produced no result, or reported an error: that is what a
regression in a role, an adapter or the config plumbing looks like, and it is
what the unit suite has repeatedly failed to catch.
"""

import json
import sys
from pathlib import Path

ALLOWED_PREFIXES = ("not_implemented", "load_profile", "broker_", "host_", "sys_",
                    "delivery_", "loadgen_", "offer_", "container_", "cpu_",
                    "open_loop_rate_out_of_tolerance", "open_loop_backpressure_misses",
                    "integrity_mismatch", "memory_guard")


def main() -> int:
    tmp, scenario = Path(sys.argv[1]), sys.argv[2]
    broken = []
    for path in sorted(tmp.glob(f"*-{scenario}.json")):
        doc = json.loads(path.read_text())
        for block in doc.get("results") or []:
            for run in block.get("runs") or []:
                reasons = run.get("reasons") or []
                if any(r.startswith("worker_error") for r in reasons):
                    broken.append(f"{path.stem}: {reasons[:2]}")
                elif run.get("status") != "valid" and not any(
                    r.startswith(ALLOWED_PREFIXES) for r in reasons
                ):
                    broken.append(f"{path.stem}: unexplained {reasons[:2]}")
    print("; ".join(sorted(set(broken))[:4]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
