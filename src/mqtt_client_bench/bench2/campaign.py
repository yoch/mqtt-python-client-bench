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

from mqtt_client_bench.bench2 import envs
from mqtt_client_bench.bench2.catalog import RUN_OVERHEAD_S, Point, Profile
from mqtt_client_bench.bench2.runner import refusals, run_once, unsupported_record
from mqtt_client_bench.bench2.session import open_session

RESULTS_DIR = Path("results") / "v2"
SCHEMA = "bench2/1"
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
                store.add(record)
                elapsed = time.monotonic() - started
                per_run = elapsed / (n + 1)
                left = (len(todo) - n - 1) * per_run
                log(f"[{n + 1}/{len(todo)} eta {left / 60:4.1f} min] {describe(record)}")

        manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        store.write_manifest(manifest)
    return store
