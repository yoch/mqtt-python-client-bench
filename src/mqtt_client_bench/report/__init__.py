"""Static report: ``build(results_dir, site_dir)``. Standard library only."""

from __future__ import annotations

import json
import shutil
import re
from pathlib import Path
from typing import List, Optional

from mqtt_client_bench.paths import portable_path
from mqtt_client_bench.report import pages
from mqtt_client_bench.report.data import select_campaign
from mqtt_client_bench.report.html import STYLE, e, page_url

# Written into every site; only a directory carrying it may be replaced.
MARKER = ".mqtt-client-bench-site"


def check_output(results: Path, site: Path) -> None:
    """Refuse an output directory whose removal could destroy anything else."""
    r, s = results.resolve(), site.resolve()
    if s == r or s in r.parents or r in s.parents:
        raise ValueError(f"output {site} overlaps the results {results}")
    if s.exists() and (not s.is_dir() or (any(s.iterdir()) and not (s / MARKER).exists())):
        raise ValueError(f"output {site} exists and is not a site this command built; remove it yourself")


def _publishable(doc: dict, profiles: Optional[dict] = None) -> dict:
    identity = doc.get("identity") or {}
    for key in ("client_module", "client_path"):
        if identity.get(key):
            identity[key] = portable_path(str(identity[key]))
    for run in doc.get("runs", []):
        capture = run.get("sampling")
        if capture:
            capture.pop("argv", None)
            for key in ("raw_profile", "flamegraph", "profile_data"):
                if key in capture:
                    capture[key] = (profiles or {}).get((run.get("run_id"), key))
    return doc


def _copy_profiles(chosen, site: Path) -> dict:
    copied = {}
    for doc in chosen.docs.values():
        for run in doc.get("runs", []):
            run_id = run.get("run_id", "")
            if not re.fullmatch(r"[a-f0-9]{12}", run_id):
                continue
            capture = run.get("sampling") or {}
            for key in ("raw_profile", "flamegraph", "profile_data"):
                source = Path(capture.get(key) or "")
                if (
                    source.name
                    not in {"profile.bin", "flamegraph.html", "profile.json"}
                    or source.parent.name != run_id
                    or source.is_symlink()
                    or not source.is_file()
                ):
                    continue
                relative = Path("profiles") / run_id / source.name
                target = site / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                copied[(run_id, key)] = str(relative)
    return copied


def write_sitemap(site: Path, pages_written: List[str]) -> None:
    """``sitemap.xml`` for the generated pages, at their canonical URLs."""
    urls = "".join(f"  <url><loc>{e(page_url(p))}</loc></url>\n" for p in pages_written)
    (site / "sitemap.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + urls + "</urlset>\n",
        encoding="utf-8",
    )


def build(
    results: Path, site: Path, *, campaign: Optional[str] = None
) -> Optional[str]:
    """Write the site; return the published campaign's name, or None."""
    check_output(results, site)
    chosen, skipped = select_campaign(results, campaign)
    if site.exists():
        shutil.rmtree(site)
    site.mkdir(parents=True)
    (site / MARKER).write_text("", encoding="utf-8")
    (site / ".nojekyll").write_text("", encoding="utf-8")
    shutil.copyfile(STYLE, site / "style.css")
    if chosen is None:
        (site / "index.html").write_text(pages.empty_page(skipped), encoding="utf-8")
        write_sitemap(site, ["index.html"])
        return None

    data = site / "data"
    data.mkdir()
    files = []
    profiles = _copy_profiles(chosen, site)
    for f in sorted(chosen.path.glob("*.json")):
        doc = json.loads(f.read_text(encoding="utf-8"))
        if f.name != "manifest.json":
            doc = _publishable(doc, profiles)
        (data / f.name).write_text(
            json.dumps(doc, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        files.append(f.name)

    (site / "point").mkdir()
    (site / "client").mkdir()
    (site / "index.html").write_text(
        pages.index_page(chosen, skipped), encoding="utf-8"
    )
    (site / "coverage.html").write_text(pages.coverage_page(chosen), encoding="utf-8")
    (site / "methodology.html").write_text(
        pages.methodology_page(chosen, files), encoding="utf-8"
    )
    written = ["index.html", "coverage.html", "methodology.html"]
    if profiles:
        links = []
        labels = {r.get("run_id"): f"{r.get('client', '?')} · {r.get('point', {}).get('name', '?')} · {r.get('run_id', '?')}"
                  for doc in chosen.docs.values() for r in doc.get("runs", [])}
        kinds = {"raw_profile": "Raw capture", "flamegraph": "Flamegraph", "profile_data": "Profile data"}
        for (run_id, key), target in sorted(profiles.items()):
            links.append(f'<li><a href="{target}">{e(labels[run_id])}: {kinds[key]}</a></li>')
        (site / "profiles.html").write_text(
            '<!doctype html><html lang="en"><meta charset="utf-8"><title>Tachyon diagnostics</title>'
            "<h1>Tachyon diagnostics</h1><p>Instrumented runs; excluded from performance rankings.</p><ul>"
            + "".join(links)
            + '</ul><a href="index.html">Campaign</a></html>',
            encoding="utf-8",
        )
        index = site / "index.html"
        content = index.read_text(encoding="utf-8").replace(
            "</main>",
            '<p><a href="profiles.html">Tachyon profiles by run</a></p></main>',
        )
        index.write_text(content, encoding="utf-8")
        written.append("profiles.html")
    for point in chosen.points:
        (site / "point" / f"{point['name']}.html").write_text(
            pages.point_page(chosen, point), encoding="utf-8"
        )
        written.append(f"point/{point['name']}.html")
    for client in chosen.clients:
        (site / "client" / f"{client}.html").write_text(
            pages.client_page(chosen, client), encoding="utf-8"
        )
        written.append(f"client/{client}.html")
    write_sitemap(site, written)
    return chosen.name
