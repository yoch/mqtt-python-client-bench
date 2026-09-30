"""Load one campaign and fold its runs into one cell per point x client.

A cell's value comes from its ``valid`` runs only: scalar metrics are the
median over runs, latency percentiles are read from the runs' merged
histogram (percentiles of every sample, not a median of medians). Every other
run stays attached to the cell so the page can show why it is missing.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from mqtt_client_bench.bench import histogram
from mqtt_client_bench.bench.checks import INVALID, NOT_SUSTAINED, VALID

LATENCY_QUANTILES = {"p50": 0.5, "p90": 0.9, "p99": 0.99, "p999": 0.999, "max": 1.0}
LAG_QUANTILES = {"lag_p50": 0.5, "lag_p99": 0.99, "lag_max": 1.0}
# Duplex: the client's own receive path, next to "latency" on its publish path.
RX_QUANTILES = {"rx_p50": 0.5, "rx_p99": 0.99}
# Only a development profile tolerates these, and it says so on every page.
CAMPAIGN_FLAGS = {"non_comparable", "host_noisy"}


@dataclass
class Cell:
    point: dict
    client: str
    runs: List[dict] = field(default_factory=list)  # final attempt of each run index
    retried: List[dict] = field(default_factory=list)  # superseded invalid attempts
    unsupported: Optional[List[str]] = None

    @property
    def valid(self) -> List[dict]:
        return [r for r in self.runs if r["status"] == VALID]

    def count(self, status: str) -> int:
        return sum(1 for r in self.runs if r["status"] == status)

    @property
    def state(self) -> str:
        """What the cell can say: a value, or why it has none."""
        if self.unsupported is not None:
            return "unsupported"
        if self.valid:
            return VALID
        if self.count(NOT_SUSTAINED):
            return NOT_SUSTAINED
        if self.runs:
            return INVALID
        return "missing"

    def median(self, key: str, runs: Optional[List[dict]] = None) -> Optional[float]:
        values = [r["metrics"][key] for r in (self.valid if runs is None else runs) if r["metrics"].get(key) is not None]
        return statistics.median(values) if values else None

    def _merged(self, key: str, runs: Optional[List[dict]]) -> Optional[dict]:
        hists = [r[key] for r in (self.valid if runs is None else runs) if r.get(key)]
        merged = histogram.merge(hists) if hists else None
        return merged if merged and merged["count"] else None

    def latency(self, runs: Optional[List[dict]] = None) -> Optional[dict]:
        return self._merged("latency", runs)

    def lag(self, runs: Optional[List[dict]] = None) -> Optional[dict]:
        """Schedule lag: how late the client published against the offer."""
        return self._merged("lag", runs)

    def latency_rx(self, runs: Optional[List[dict]] = None) -> Optional[dict]:
        return self._merged("latency_rx", runs)

    def value(self, key: str) -> Optional[float]:
        """A metric of the valid runs; latency and lag keys are in microseconds."""
        for quantiles, h in (
            (LATENCY_QUANTILES, self.latency),
            (LAG_QUANTILES, self.lag),
            (RX_QUANTILES, self.latency_rx),
        ):
            if key in quantiles:
                hist = h()
                ns = histogram.percentile(hist, quantiles[key]) if hist else None
                return ns / 1e3 if ns is not None else None
        return self.median(key)

    def flags(self) -> List[str]:
        """Flags that qualify the value; campaign-wide ones are on the banner."""
        return sorted({f for r in self.valid for f in r.get("flags", [])} - CAMPAIGN_FLAGS)

    def bounded(self, metric: str) -> Optional[str]:
        """The limit a capacity value hit instead of the client's own, if any.

        ``offer``: the client took the whole offer, identical for every client,
        so bounded clients tie. ``broker``: the broker saturated first, so the
        value is only a lower bound on the client's capacity.
        """
        if metric != "msgs_per_s" or not self.valid:
            return None
        flags = {f for r in self.valid for f in r.get("flags", [])}
        if "offer_bound" in flags:
            return "offer"
        if "broker_bound" in flags:
            return "broker"
        return None


@dataclass
class Campaign:
    path: Path
    manifest: dict
    docs: Dict[str, dict]
    cells: Dict[Tuple[str, str], Cell]
    points: List[dict]
    clients: List[str]

    @property
    def name(self) -> str:
        return self.manifest.get("campaign", self.path.name)

    @property
    def comparable(self) -> bool:
        return bool((self.manifest.get("profile") or {}).get("comparable"))

    @property
    def sessions(self) -> List[dict]:
        return self.manifest.get("sessions") or []

    def cell(self, point: str, client: str) -> Cell:
        return self.cells[(point, client)]

    def expected_runs(self) -> int:
        n = 0
        for p in self.points:
            n += sum(1 for c in self.clients if self.cells[(p["name"], c)].unsupported is None)
        return n * int(self.manifest.get("runs_per_point") or 1)

    def done_runs(self) -> int:
        return sum(len(c.runs) for c in self.cells.values())

    def hosts(self) -> List[str]:
        """Distinct machines the sessions ran on; a published campaign has one."""
        seen = []
        for s in self.sessions:
            h = s.get("host") or {}
            key = f"{h.get('hostname', '?')} ({h.get('cpu_model', '?')})"
            if key not in seen:
                seen.append(key)
        return seen


def _final_attempts(runs: List[dict]) -> Tuple[List[dict], List[dict]]:
    by_index: Dict[int, List[dict]] = {}
    for r in runs:
        by_index.setdefault(int(r.get("run_index", 0)), []).append(r)
    final, retried = [], []
    for index in sorted(by_index):
        attempts = sorted(by_index[index], key=lambda r: int(r.get("attempt", 0)))
        final.append(attempts[-1])
        retried.extend(attempts[:-1])
    return final, retried


def load_campaign(path: Path) -> Campaign:
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    docs: Dict[str, dict] = {}
    for f in sorted(path.glob("*.json")):
        if f.name != "manifest.json":
            doc = json.loads(f.read_text(encoding="utf-8"))
            docs[doc["client"]] = doc
    points = list(manifest.get("points") or [])
    clients = list(manifest.get("clients") or [])
    # Runs the manifest does not announce are still shown, never dropped.
    clients += [c for c in docs if c not in clients]
    known = {p["name"] for p in points}
    for doc in docs.values():
        for r in doc.get("runs", []):
            if r["point"]["name"] not in known:
                points.append(r["point"])
                known.add(r["point"]["name"])
    cells: Dict[Tuple[str, str], Cell] = {}
    for p in points:
        for client in clients:
            doc = docs.get(client) or {}
            runs = [r for r in doc.get("runs", []) if r["point"]["name"] == p["name"]]
            final, retried = _final_attempts(runs)
            refused = next((u["reasons"] for u in doc.get("unsupported", []) if u["point"] == p["name"]), None)
            cells[(p["name"], client)] = Cell(p, client, final, retried, refused)
    return Campaign(path, manifest, docs, cells, points, clients)


def find_campaigns(root: Path) -> List[Path]:
    """Campaign directories under ``root`` (or ``root`` itself), oldest first."""
    if (root / "manifest.json").exists():
        return [root]
    if not root.is_dir():
        return []
    return sorted(p for p in root.iterdir() if (p / "manifest.json").exists())


def select_campaign(root: Path, name: Optional[str] = None) -> Tuple[Optional[Campaign], List[str]]:
    """The campaign to publish and the names of the ones passed over.

    Without ``name``, the newest comparable campaign; a smoke campaign is only
    ever built when asked for by name, and says so on every page.
    """
    skipped: List[str] = []
    chosen: Optional[Campaign] = None
    for path in reversed(find_campaigns(root)):
        c = load_campaign(path)
        if name is not None:
            if c.name == name or path.name == name:
                chosen = c
            continue
        if chosen is None and c.comparable:
            chosen = c
        else:
            skipped.append(c.name)
    if name is not None and chosen is None:
        raise FileNotFoundError(f"no campaign named {name!r} under {root}")
    return chosen, skipped
