"""Fingerprint of the code that produces a measurement.

A result is only comparable with another that came out of the same measurement
path. Client versions are already recorded and checked, but that only catches a
client moving: when the *harness* changed — a per-message tax added by a merge,
a hot path rewritten — every completed scenario still looked finished, was
skipped on resume, and the published matrix mixed two generations whose
per-message cost differed by 40%. That is invisible in the numbers and fatal to
the ranking, since the tax is paid per message and so compresses the fast
clients more than the slow ones.

The fingerprint is derived from the code rather than from a constant someone has
to remember to bump. Python modules are hashed by *structure* — the AST with
docstrings removed — so reformatting, comments and prose do not invalidate a
campaign, while any change to what the code actually does invalidates it
immediately. Non-Python files (the in-tree C loadgen) are hashed as bytes:
there is no AST to strip, and a pacing or counting change must invalidate
``sub_*`` numbers.

Per-adapter modules are deliberately excluded: a change to one client's adapter
has no bearing on another client's numbers, and `client_identity` already
carries that client's library version. `adapters/base.py` and
`adapters/async_bridge.py` are included, being shared by every client.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from typing import Iterable

_ROOT = Path(__file__).resolve().parent
_REPO_ROOT = _ROOT.parents[1]

# The shared measurement path: anything here changes what a number means.
# Package-relative unless the entry starts with ``scripts/`` (repo root).
MEASUREMENT_PATH = (
    "harness.py",
    "control.py",
    "metrics.py",
    "sampling.py",
    "telemetry.py",
    "workloads.py",
    "loadgen.py",
    # Joins the measurement path here and not earlier: until the ceilings
    # capped offers, nothing this module produced could change a number, and
    # moving the fingerprint would have signalled a change of meaning that had
    # not happened.
    "hostcal.py",
    "broker.py",
    "adapters/base.py",
    "adapters/async_bridge.py",
    "roles/publisher.py",
    "roles/subscriber.py",
    "roles/rtt_drive.py",
    "roles/rtt_initiator.py",
    "roles/responder.py",
    "scripts/mqtt_hammer.c",
)


def _strip_docstrings(tree: ast.AST) -> ast.AST:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            node.body = body[1:]
    return tree


def _measurement_file(rel: str) -> Path:
    if rel.startswith("scripts/"):
        return _REPO_ROOT / rel
    return _ROOT / rel


def _structural_digest(path: Path) -> str:
    tree = _strip_docstrings(ast.parse(path.read_text(encoding="utf-8")))
    return hashlib.sha256(ast.dump(tree).encode("utf-8")).hexdigest()


def _file_digest(path: Path) -> str:
    if path.suffix == ".py":
        return _structural_digest(path)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def harness_fingerprint(files: Iterable[str] = MEASUREMENT_PATH) -> str:
    """Short digest of the measurement path, stable across prose-only edits."""
    digest = hashlib.sha256()
    for rel in sorted(files):
        path = _measurement_file(rel)
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        digest.update((_file_digest(path) if path.exists() else "missing").encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()[:16]
