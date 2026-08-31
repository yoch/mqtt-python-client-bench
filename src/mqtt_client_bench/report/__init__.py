"""Build a static HTML report site from committed benchmark JSON results.

The package is laid out by responsibility: :mod:`model` decides what a number is
allowed to mean, :mod:`catalog` says which way is better, :mod:`aggregate` turns
documents into series, :mod:`charts` draws them, and the modules under
:mod:`pages` compose the six kinds of page. The import path stays
``mqtt_client_bench.report`` so nothing outside has to know it grew.

Nothing here reaches the network. Charts are inline SVG rendered at build time;
``app.js`` adds hover, sorting and the theme switch on top, and every value it
touches is also readable without it.
"""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .model import (  # re-exported: the package's public and test-facing surface
    ClientMeta,
    EMPTY_GLYPHS,
    PointRow,
    ResultDoc,
    _AUTO_REFERENCE,
    _CHART_EXCLUDED_SCENARIOS,
    _client_meta,
    _collect_integrity,
    _collect_latency,
    _reason_kind,
    _short_reason,
    classify_payload,
    load_results,
    load_results_with_skips,
)
from .theme import client_color_css
from .pages import client as client_page
from .pages import corpus as corpus_page
from .pages import detail as detail_page
from .pages import index as index_page
from .pages import methodology as methodology_page
from .pages import scenario as scenario_page

ASSETS_DIR = Path(__file__).resolve().parents[1] / "report_assets"

# The leading-underscore entries are imported by the test suite, which pins the
# comparability gates directly rather than through a rendered page. They are part
# of this package's contract even though they are not public API.
__all__ = [
    "ASSETS_DIR",
    "EMPTY_GLYPHS",
    "_CHART_EXCLUDED_SCENARIOS",
    "_client_meta",
    "_collect_integrity",
    "_collect_latency",
    "_reason_kind",
    "_short_reason",
    "ClientMeta",
    "PointRow",
    "ResultDoc",
    "build_site",
    "classify_payload",
    "load_results",
    "load_results_with_skips",
    "render_detail",
    "render_index",
    "render_methodology",
]


def render_index(docs, generated_at: str) -> str:
    return index_page.render(docs, generated_at)


def render_detail(doc, generated_at: str, related=None) -> str:
    return detail_page.render(doc, generated_at, related=related)


def render_methodology(docs, generated_at: str) -> str:
    return methodology_page.render(docs, generated_at)


def render_corpus(docs, generated_at: str, *, skipped_by_host=None) -> str:
    return corpus_page.render(docs, generated_at, skipped_by_host=skipped_by_host)


def build_site(
    input_dir: Path | str, output_dir: Path | str, *, reference: Any = _AUTO_REFERENCE
) -> Dict[str, Any]:
    """Generate the static site under output_dir from JSON files in input_dir.

    ``reference=None`` publishes every document regardless of which machine it
    came from. That is for callers that are exercising rendering rather than
    provenance; a published site always passes the default.
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    docs, skipped_hosts = load_results_with_skips(input_path, reference=reference)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if output_path.exists():
        shutil.rmtree(output_path)
    runs_dir = output_path / "runs"
    scenarios_dir = output_path / "scenarios"
    clients_dir = output_path / "clients"
    assets_out = output_path / "assets"
    for directory in (runs_dir, scenarios_dir, clients_dir, assets_out):
        directory.mkdir(parents=True, exist_ok=True)

    for asset in ("style.css", "app.js"):
        source = ASSETS_DIR / asset
        if source.exists():
            shutil.copy2(source, assets_out / asset)
    # Series colours are generated rather than shipped: they bind only the
    # clients this corpus actually contains, and every page links the result.
    (assets_out / "series.css").write_text(
        client_color_css([d.client for d in docs if d.kind == "scenario" and d.client]),
        encoding="utf-8",
    )

    (output_path / "index.html").write_text(render_index(docs, generated_at), encoding="utf-8")
    (output_path / "methodology.html").write_text(
        render_methodology(docs, generated_at), encoding="utf-8"
    )
    (output_path / "corpus.html").write_text(
        render_corpus(docs, generated_at, skipped_by_host=skipped_hosts), encoding="utf-8"
    )

    # Suite documents link to the run pages that sit beside them, so the map is
    # keyed both ways: a suite that names a scenario without a client still finds
    # a page, and one that names both finds the right client's page.
    related: Dict[str, str] = {}
    for doc in docs:
        if doc.kind == "scenario" and doc.scenario:
            related[doc.scenario] = f"{doc.slug}.html"
            if doc.client:
                related[f"{doc.client}:{doc.scenario}"] = f"{doc.slug}.html"
    for doc in docs:
        (runs_dir / f"{doc.slug}.html").write_text(
            render_detail(doc, generated_at, related=related), encoding="utf-8"
        )

    for slug, html in scenario_page.render_all(docs, generated_at).items():
        (scenarios_dir / f"{slug}.html").write_text(html, encoding="utf-8")
    for slug, html in client_page.render_all(docs, generated_at).items():
        (clients_dir / f"{slug}.html").write_text(html, encoding="utf-8")

    # No raw JSON copied into site/.
    return {
        "input": str(input_path),
        "output": str(output_path),
        "results": len(docs),
        # Named, not just counted: a reader who expected a document and does not
        # find it needs to know which machine it came from.
        "skipped_by_host": skipped_hosts,
        "generated_at": generated_at,
    }
