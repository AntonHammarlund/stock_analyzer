import json
from pathlib import Path
from typing import Dict

import requests
import pandas as pd

from ..paths import DATA_DIR, CONFIG_DIR
from ..utils import read_json, write_json

ML_CACHE = DATA_DIR / "ml_scores.json"
HOSTS_FILE = CONFIG_DIR / "hosts.json"


def _get_remote_endpoint() -> str:
    hosts = read_json(HOSTS_FILE)
    for host in hosts.get("hosts", []):
        if host.get("type") == "remote_ml" and host.get("enabled"):
            return host.get("endpoint", "")
    return ""


def fetch_remote_ml_scores() -> Dict:
    endpoint = _get_remote_endpoint()
    if not endpoint:
        return {}
    try:
        response = requests.get(endpoint, timeout=15)
        if response.status_code != 200:
            return {}
        payload = response.json()
        write_json(ML_CACHE, payload)
        return payload
    except Exception:
        return {}


def load_ml_scores() -> pd.DataFrame:
    if ML_CACHE.exists():
        payload = read_json(ML_CACHE)
        return _payload_to_df(payload)

    payload = fetch_remote_ml_scores()
    return _payload_to_df(payload)


def _payload_to_df(payload: Dict) -> pd.DataFrame:
    if not payload:
        return pd.DataFrame(columns=["instrument_id", "ml_score", "ml_confidence"])
    return pd.DataFrame(payload.get("scores", []))
