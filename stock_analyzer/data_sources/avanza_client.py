from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import importlib.metadata as metadata
import os

import requests

from ..paths import CONFIG_DIR
from ..utils import read_json


@dataclass
class AvanzaConfig:
    enabled: bool
    username_env: str
    password_env: str
    totp_env: str
    max_release_age_days: int
    enforce_package_freshness: bool
    chart_time_period: str
    chart_resolution: str
    search_limit: int


def load_avanza_config() -> AvanzaConfig:
    cfg = read_json(CONFIG_DIR / "avanza.json")
    return AvanzaConfig(
        enabled=bool(cfg.get("enabled", False)),
        username_env=str(cfg.get("username_env", "AVANZA_USERNAME")),
        password_env=str(cfg.get("password_env", "AVANZA_PASSWORD")),
        totp_env=str(cfg.get("totp_env", "AVANZA_TOTP_SECRET")),
        max_release_age_days=int(cfg.get("max_release_age_days", 90)),
        enforce_package_freshness=bool(cfg.get("enforce_package_freshness", True)),
        chart_time_period=str(cfg.get("chart_time_period", "ONE_YEAR")),
        chart_resolution=str(cfg.get("chart_resolution", "DAY")),
        search_limit=int(cfg.get("search_limit", 25)),
    )


def _package_release_age_days(package: str, version: str) -> Optional[int]:
    try:
        response = requests.get(f"https://pypi.org/pypi/{package}/json", timeout=15)
        response.raise_for_status()
        payload = response.json()
        releases = payload.get("releases", {}).get(version, [])
        if not releases:
            return None
        uploaded = releases[0].get("upload_time_iso_8601")
        if not uploaded:
            return None
        ts = datetime.fromisoformat(uploaded.replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - ts
        return age.days
    except Exception:
        return None


def _is_package_fresh(config: AvanzaConfig) -> bool:
    if not config.enforce_package_freshness:
        return True
    try:
        version = metadata.version("avanza-api")
    except Exception:
        return False
    age_days = _package_release_age_days("avanza-api", version)
    if age_days is None:
        return False
    return age_days <= config.max_release_age_days


def get_avanza_client() -> tuple[object | None, str | None]:
    cfg = load_avanza_config()
    if not cfg.enabled:
        return None, "disabled"

    if not _is_package_fresh(cfg):
        return None, "package-too-old"

    username = os.getenv(cfg.username_env, "")
    password = os.getenv(cfg.password_env, "")
    totp_secret = os.getenv(cfg.totp_env, "")
    if not username or not password or not totp_secret:
        return None, "missing-credentials"

    try:
        from avanza import Avanza
    except Exception:
        return None, "package-missing"

    try:
        client = Avanza(
            {
                "username": username,
                "password": password,
                "totpSecret": totp_secret,
            }
        )
        return client, None
    except Exception as exc:
        return None, f"login-failed: {exc}"


def get_avanza_constants():
    try:
        from avanza.constants import TimePeriod, Resolution, InstrumentType
    except Exception:
        return None
    return TimePeriod, Resolution, InstrumentType
