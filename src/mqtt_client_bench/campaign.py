"""Whether a scenario's committed results still count as finished work.

There is one rule and it lives here. It used to live twice - once in the
campaign loop, which decides what to re-measure, and once in the control
script's status display - and the two drifted: the display reported "11/11
scenarios complete" for a set of results the gate was about to re-measure in
full, because the display never looked at the harness fingerprint it claimed to
be filtering on. A status line that contradicts the thing it reports on is worse
than no status line.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional

from mqtt_client_bench.adapters.registry import adapter_identity
from mqtt_client_bench.harness import HARNESS_FINGERPRINT
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario

DONE = "done"
PARTIAL = "partial"
STALE = "stale"
MISSING = "missing"


def client_state(
    scenario: str,
    client: str,
    results_dir: Path = Path("results"),
    expected_points: Optional[int] = None,
) -> tuple[str, str, int]:
    """Return (state, why, points) for one client's file.

    ``stale`` means the file is complete but was produced by something other
    than what is installed now - a different harness or a different library
    version - so its numbers cannot be mixed with fresh ones.
    """
    if expected_points is None:
        expected_points = len(expand_scenario(SCENARIO_BY_NAME[scenario], "standard"))
    path = results_dir / f"{client}-{scenario}.json"
    if not path.exists():
        return MISSING, "no result file", 0
    try:
        data = json.loads(path.read_text())
    except Exception:  # noqa: BLE001
        return MISSING, "unreadable", 0

    blocks = data.get("results") or []
    runs = [r for b in blocks for r in (b.get("runs") or [])]
    if not runs:
        return MISSING, "no runs", 0
    # `started_at` only exists on runs produced after the fairness fixes, so
    # older results count as missing rather than as finished work.
    if not all("started_at" in r for r in runs):
        return STALE, "predates the current result contract", len(blocks)
    if len(blocks) < expected_points:
        return PARTIAL, f"{len(blocks)}/{expected_points} points", len(blocks)

    # A library upgrade invalidates that client's numbers just as surely as a
    # missing file, and a campaign that skipped it would publish a matrix mixing
    # two versions of the same client.
    recorded = (data.get("client_identity") or {}).get("client_version")
    try:
        installed = adapter_identity(client).get("client_version")
    except Exception:  # noqa: BLE001
        installed = None
    if recorded and installed and recorded != installed:
        return STALE, f"measured on {recorded}, installed {installed}", len(blocks)

    # The client can sit still while the harness moves under it. A measurement
    # path that changed makes the old numbers incomparable with the new ones,
    # and the version check above cannot see that.
    stamps = {r.get("harness_fingerprint") for r in runs}
    if stamps != {HARNESS_FINGERPRINT}:
        seen = ", ".join(sorted(str(x) for x in stamps)) or "unstamped"
        return STALE, f"harness {seen}, now {HARNESS_FINGERPRINT}", len(blocks)

    return DONE, "", len(blocks)


def scenario_state(
    scenario: str,
    clients: Iterable[str],
    results_dir: Path = Path("results"),
) -> dict:
    """Aggregate one scenario over the clients a campaign is actually running."""
    expected = len(expand_scenario(SCENARIO_BY_NAME[scenario], "standard"))
    per_client = {
        client: client_state(scenario, client, results_dir, expected)
        for client in clients
    }
    states = [s for s, _, _ in per_client.values()]
    if states and all(s == DONE for s in states):
        overall = DONE
    elif any(s in (PARTIAL, DONE) for s in states):
        overall = PARTIAL
    else:
        overall = states[0] if len(set(states)) == 1 else MISSING
    return {
        "scenario": scenario,
        "state": overall,
        "expected_points": expected,
        "done": sum(1 for s in states if s == DONE),
        "total": len(states),
        "clients": per_client,
    }


def scenario_complete(scenario: str, clients: Iterable[str], results_dir: Path = Path("results")) -> bool:
    """The campaign's skip gate: every client complete, fresh, and current."""
    return scenario_state(scenario, clients, results_dir)["state"] == DONE
