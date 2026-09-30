"""Project-root paths for infra assets (compose, mosquitto, the C peer)."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
# src/mqtt_client_bench -> src -> project root
PROJECT_ROOT = PACKAGE_DIR.parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
MOSQUITTO_CONF = PROJECT_ROOT / "mosquitto" / "mosquitto.conf"
