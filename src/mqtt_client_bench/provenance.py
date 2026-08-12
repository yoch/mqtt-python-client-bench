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
to remember to bump. It is taken over the *structure* — the AST with docstrings
removed — so reformatting, comments and prose do not invalidate a campaign,
while any change to what the code actually does invalidates it immediately.

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

# The shared measurement path: anything here changes what a number means.
MEASUREMENT_PATH = (
    "harness.py",
    "control.py",
    "metrics.py",
    "sampling.py",
    "telemetry.py",
    "workloads.py",
    "broker.py",
    "adapters/base.py",
    "adapters/async_bridge.py",
    "roles/publisher.py",
    "roles/subscriber.py",
    "roles/rtt_initiator.py",
    "roles/responder.py",
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


def _structural_digest(path: Path) -> str:
    tree = _strip_docstrings(ast.parse(path.read_text(encoding="utf-8")))
    return hashlib.sha256(ast.dump(tree).encode("utf-8")).hexdigest()


def harness_fingerprint(files: Iterable[str] = MEASUREMENT_PATH) -> str:
    """Short digest of the measurement path, stable across prose-only edits."""
    digest = hashlib.sha256()
    for rel in sorted(files):
        path = _ROOT / rel
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        digest.update((_structural_digest(path) if path.exists() else "missing").encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()[:16]
