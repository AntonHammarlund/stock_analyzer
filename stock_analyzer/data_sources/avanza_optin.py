from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

from ..utils import read_json
from ..paths import CONFIG_DIR

CONFIG_FILE = CONFIG_DIR / "avanza_optin.json"


def _parse_date(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def avanza_opt_in_status() -> Tuple[bool, str, Dict]:
    config = read_json(CONFIG_FILE)
    if not config:
        return False, "Avanza opt-in config missing.", {}

    if not config.get("enabled", False):
        return False, "Avanza opt-in is disabled.", config

    last_release = _parse_date(config.get("library_last_release_date", ""))
    if not last_release:
        return False, "Library last release date not set.", config

    freshness_days = int(config.get("library_freshness_days", 90))
    age_days = (datetime.now(timezone.utc) - last_release).days
    if age_days > freshness_days:
        return False, f"Library release is stale ({age_days} days old).", config

    return True, "Avanza opt-in enabled.", config


def is_data_fresh(last_quote_date: datetime) -> bool:
    config = read_json(CONFIG_FILE)
    max_age_days = int(config.get("data_freshness_days", 1))
    age_days = (datetime.now(timezone.utc) - last_quote_date).days
    return age_days <= max_age_days
