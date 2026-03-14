import pandas as pd
from pathlib import Path

from ..paths import CONFIG_DIR

AVANZA_MAP = CONFIG_DIR / "avanza_availability.csv"


def load_avanza_map() -> pd.DataFrame:
    if not AVANZA_MAP.exists():
        return pd.DataFrame(columns=["instrument_id", "isin", "name", "avanza_available", "last_verified_date"])
    return pd.read_csv(AVANZA_MAP)
