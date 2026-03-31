from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from ..paths import DATA_DIR

WATCHLIST_PRICES = DATA_DIR / "prices_watchlist.csv"


def load_watchlist_prices(instrument_ids: Optional[Iterable[str]] = None) -> pd.DataFrame:
    if not WATCHLIST_PRICES.exists() or WATCHLIST_PRICES.stat().st_size == 0:
        return pd.DataFrame(columns=["instrument_id", "date", "close", "source"])
    df = pd.read_csv(WATCHLIST_PRICES)
    df = _ensure_columns(df)
    if instrument_ids is None:
        return df
    ids = {str(value) for value in instrument_ids}
    return df[df["instrument_id"].astype(str).isin(ids)]


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["instrument_id", "date", "close", "source"])
    for column in ("instrument_id", "date", "close"):
        if column not in df.columns:
            df[column] = pd.NA
    if "source" not in df.columns:
        df["source"] = pd.NA
    return df
