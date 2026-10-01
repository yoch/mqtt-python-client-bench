"""A campaign: every point x client x run, interleaved, resumable.

Clients rotate within each point — run ``i`` starts at client ``i`` — so slow
drift of the host over the hour spreads across every client instead of
landing on whoever ran last. Each client's file is rewritten after every run,
so an interrupted campaign resumes where it stopped, and a run that came back
``invalid`` is retried once at the end.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from mqtt_client_bench.bench import envs
from mqtt_client_bench.bench.catalog import ALL_POINTS, PROFILES, RUN_OVERHEAD_S, Point, Profile
from mqtt_client_bench.bench.checks import KERNEL_STATE_FLOOR, KERNEL_STATES
from mqtt_client_bench.bench.runner import refusals, run_once, unsupported_record
from mqtt_client_bench.bench.session import open_session

RESULTS_DIR = Path("results") / "v2"
SCHEMA = "mqtt-client-bench/2"
MAX_ATTEMPTS = 2

Key = Tuple[str, str, int]


def campaign_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def plan(points: List[Point], clients: List[str], runs: int) -> List[Key]:
    """(point, client, run) in execution order, supported pairs only."""
    order: List[Key] = []
    for point in points:
        able = [c for c in clients if not refusals(c, point)]
        for i in range(runs):
            k = i % len(able) if able else 0
            for client in able[k:] + able[:k]:
                order.append((point.name, client, i))
    return order


def estimate_s(order: List[Key], profile: Profile) -> float:
    return len(order) * (profile.warmup_s + profile.measure_s + profile.drain_s + RUN_OVERHEAD_S)


class Store:
    """``<dir>/manifest.json`` plus one ``<client>.json`` holding its runs."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.docs: Dict[str, dict] = {}
        for path in sorted(self.root.glob("*.json")):
            if path.name == "manifest.json":
                continue
            doc = json.loads(path.read_text(encoding="utf-8"))
            self.docs[doc["client"]] = doc

    def manifest(self) -> Optional[dict]:
        path = self.root / "manifest.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

    def write_manifest(self, manifest: dict) -> None:
        (self.root / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n", encoding="utf-8")

    def doc(self, client: str) -> dict:
        if client not in self.docs:
            self.docs[client] = {
                "schema": SCHEMA,
                "client": client,
                "version": envs.installed_version(client),
                "identity": None,
                "runs": [],
                "unsupported": [],
            }
        return self.docs[client]

    def attempts(self, key: Key) -> List[dict]:
        point, client, i = key
        doc = self.docs.get(client)
        if not doc:
            return []
        return [r for r in doc["runs"] if r["point"]["name"] == point and r["run_index"] == i]

    def done(self, key: Key) -> bool:
        attempts = self.attempts(key)
        return any(r["status"] != "invalid" for r in attempts) or len(attempts) >= MAX_ATTEMPTS

    def add(self, record: dict) -> None:
        doc = self.doc(record["client"])
        worker = record.get("worker") or {}
        identity = worker.pop("identity", None)
        if identity and not doc.get("identity"):
            doc["identity"] = identity
        doc["runs"].append(record)
        self._save(doc)

    def add_unsupported(self, record: dict) -> None:
        doc = self.doc(record["client"])
        name = record["point"]["name"]
        if all(u["point"] != name for u in doc["unsupported"]):
            doc["unsupported"].append({"point": name, "reasons": record["reasons"]})
            self._save(doc)

    def _save(self, doc: dict) -> None:
        path = self.root / f"{doc['client']}.json"
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, separators=(",", ":")) + "\n", encoding="utf-8")
        tmp.replace(path)


def resume_settings(root: Path) -> Tuple[Profile, List[Point], List[str], int]:
    """What a campaign was started with, so resuming it cannot mix settings."""
    path = root / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"{root} has no manifest.json: nothing to resume")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    name = manifest["profile"]["name"]
    if name not in PROFILES:
        raise ValueError(f"{root}: unknown profile {name!r}")
    points = []
    for recorded in manifest["points"]:
        current = ALL_POINTS.get(recorded["name"])
        if current is None or current.as_dict() != recorded:
            raise ValueError(f"{root}: point {recorded['name']!r} changed in the catalogue since the campaign started")
        points.append(current)
    return PROFILES[name], points, list(manifest["clients"]), int(manifest["runs_per_point"])


def _mismatches(manifest: dict, profile: Profile, points: List[Point], clients: List[str], runs: int) -> List[str]:
    out = []
    if manifest["profile"] != profile.__dict__:
        out.append(f"profile {manifest['profile'].get('name')} != {profile.name}")
    if manifest["points"] != [p.as_dict() for p in points]:
        out.append("points differ")
    if list(manifest["clients"]) != list(clients):
        out.append(f"clients {', '.join(manifest['clients'])} != {', '.join(clients)}")
    if int(manifest["runs_per_point"]) != runs:
        out.append(f"runs per point {manifest['runs_per_point']} != {runs}")
    return out


def noise_summary(docs: Dict[str, dict]) -> List[str]:
    """Where the host noise came from in the runs that failed ``host_quiet``.

    One line per consumer: its mean cores over the failed runs that recorded
    it, and how many of them. Empty when no run failed the check.
    """
    runs = [r for doc in docs.values() for r in doc["runs"]]
    noisy = [r for r in runs if any(c["name"] == "host_quiet" and not c["passed"] for c in r.get("checks", []))]
    if not noisy:
        return []
    seen: Dict[str, List[float]] = {}
    for r in noisy:
        host = (r.get("resources") or {}).get("host") or {}
        for top in host.get("top") or []:
            seen.setdefault(top["comm"], []).append(top["cores"])
        for state, cores in (host.get("states") or {}).items():
            if state in KERNEL_STATES and cores >= KERNEL_STATE_FLOOR:
                seen.setdefault(f"kernel {state}", []).append(cores)
        if (host.get("unattributed_cores") or 0) >= KERNEL_STATE_FLOOR:
            seen.setdefault("unattributed", []).append(host["unattributed_cores"])
    lines = [f"host_quiet failed in {len(noisy)} of {len(runs)} runs; the other consumers in those runs:"]
    if not seen:
        return lines + ["  no attribution recorded"]
    for name, cores in sorted(seen.items(), key=lambda kv: -sum(kv[1]))[:8]:
        lines.append(f"  {name:24s} {sum(cores) / len(cores):.2f} cores on average, in {len(cores)} runs")
    return lines


def run_campaign(
    points: List[Point],
    clients: List[str],
    profile: Profile,
    root: Path,
    *,
    runs: Optional[int] = None,
    log: Callable[[str], None] = print,
    describe: Callable[[dict], str] = lambda r: r["status"],
) -> Store:
    runs = runs or profile.runs
    store = Store(root)
    existing = store.manifest()
    if existing is not None:
        mismatches = _mismatches(existing, profile, points, clients, runs)
        if mismatches:
            raise ValueError(f"{root} was started with other settings: {'; '.join(mismatches)}")
    by_name = {p.name: p for p in points}
    for point in points:
        for client in clients:
            reasons = refusals(client, point)
            if reasons:
                store.add_unsupported(unsupported_record(client, point, profile, reasons))

    order = plan(points, clients, runs)
    if all(store.done(k) for k in order):
        return store
    with open_session(profile) as (ctx, info):
        manifest = store.manifest() or {
            "schema": SCHEMA,
            "campaign": root.name,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "profile": profile.__dict__,
            "points": [p.as_dict() for p in points],
            "clients": {c: envs.installed_version(c) for c in clients},
            "runs_per_point": runs,
            "sessions": [],
        }
        manifest["sessions"].append(info)
        store.write_manifest(manifest)
        c = info.get("ceiling") or {}
        log(f"broker ceiling (C->C): {c.get('msgs_per_s', 0):.0f} msg/s; receive offer {ctx.sub_offer}/s")

        for attempt in range(MAX_ATTEMPTS):
            todo = [k for k in order if not store.done(k) and len(store.attempts(k)) == attempt]
            if not todo:
                continue
            if attempt:
                log(f"retrying {len(todo)} invalid run(s)")
            started = time.monotonic()
            for n, key in enumerate(todo):
                point, client, i = key
                record = run_once(client, by_name[point], profile, ctx, run_index=i)
                record["attempt"] = attempt
                # Index into manifest["sessions"]: host, ceiling, harness floor.
                record["session"] = len(manifest["sessions"]) - 1
                store.add(record)
                elapsed = time.monotonic() - started
                per_run = elapsed / (n + 1)
                left = (len(todo) - n - 1) * per_run
                log(f"[{n + 1}/{len(todo)} eta {left / 60:4.1f} min] {describe(record)}")

        manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        store.write_manifest(manifest)
    return store
