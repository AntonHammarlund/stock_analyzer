import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

from ..paths import DATA_DIR
from ..config import load_config

SAMPLE_PRICES = DATA_DIR / "sample_prices.csv"
IMPORT_PRICES = DATA_DIR / "prices_import.csv"


def load_prices(universe: pd.DataFrame, instrument_ids: Optional[Iterable[str]] = None) -> pd.DataFrame:
    if IMPORT_PRICES.exists() and IMPORT_PRICES.stat().st_size > 0:
        df = pd.read_csv(IMPORT_PRICES)
        df = _ensure_columns(df)
        return _filter_instruments(df, instrument_ids)

    config = load_config()
    require_import = bool(config.get("require_imported_universe", True))
    if require_import:
        return _ensure_columns(pd.DataFrame())

    if SAMPLE_PRICES.exists():
        df = pd.read_csv(SAMPLE_PRICES)
        df = _ensure_columns(df)
        return _filter_instruments(df, instrument_ids)
    return _generate_sample_prices(universe, instrument_ids)


def _filter_instruments(df: pd.DataFrame, instrument_ids: Optional[Iterable[str]]) -> pd.DataFrame:
    if instrument_ids is None:
        return df
    ids = {str(value) for value in instrument_ids}
    return df[df["instrument_id"].astype(str).isin(ids)]


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["instrument_id", "date", "close"])
    for column in ("instrument_id", "date", "close"):
        if column not in df.columns:
            df[column] = pd.NA
    return df


def _generate_sample_prices(
    universe: pd.DataFrame, instrument_ids: Optional[Iterable[str]] = None
) -> pd.DataFrame:
    config = load_config()
    history_days = int(config.get("price_history_days", 365))
    max_sample = int(config.get("max_sample_instruments", 500))
    np.random.seed(42)
    rows = []
    end = datetime.utcnow().date()
    start = end - timedelta(days=history_days)
    dates = pd.date_range(start=start, end=end, freq="B")

    if instrument_ids is not None:
        instruments = universe[universe["instrument_id"].isin(list(instrument_ids))]
    else:
        instruments = universe

    if instrument_ids is None and len(instruments) > max_sample:
        return pd.DataFrame(columns=["instrument_id", "date", "close"])

    for _, row in instruments.iterrows():
        price = 100 + np.random.rand() * 20
        for date in dates:
            price *= 1 + np.random.normal(0, 0.002)
            rows.append(
                {
                    "instrument_id": row["instrument_id"],
                    "date": date.date().isoformat(),
                    "close": round(price, 2),
                }
            )

    df = pd.DataFrame(rows)
    if instrument_ids is None and not df.empty:
        df.to_csv(SAMPLE_PRICES, index=False)
    return df
