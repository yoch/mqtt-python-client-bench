"""Project-root paths for infra assets (compose, mosquitto, the C peer)."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
# src/mqtt_client_bench -> src -> project root
PROJECT_ROOT = PACKAGE_DIR.parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
MOSQUITTO_CONF = PROJECT_ROOT / "mosquitto" / "mosquitto.conf"


def portable_path(path: str) -> str:
    """A recorded path without the machine it was recorded on.

    Results are published; an absolute path would carry the maintainer's
    home directory and say nothing a reader can use.
    """
    marker = "site-packages/"
    if marker in path:
        return path[path.index(marker) + len(marker):]
    try:
        return str(Path(path).resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return Path(path).name
