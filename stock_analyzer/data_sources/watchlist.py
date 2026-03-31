from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from ..paths import DATA_DIR
from ..config import load_config

WATCHLIST_COLUMNS = [
    "instrument_id",
    "symbol",
    "provider",
    "name",
    "asset_type",
    "ticker",
    "currency",
    "market",
    "country",
    "notes",
]

STRING_COLUMNS = [
    "instrument_id",
    "symbol",
    "provider",
    "name",
    "asset_type",
    "ticker",
    "currency",
    "market",
    "country",
    "notes",
]


def _ensure_file(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=WATCHLIST_COLUMNS).to_csv(path, index=False)


def _clean_strings(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = df[column].fillna("").astype(str).str.strip()
        df.loc[df[column] == "", column] = pd.NA


def _watchlist_path() -> Path:
    config = load_config()
    path = config.get("watchlist_file", "data/watchlist.csv")
    return Path(path) if isinstance(path, str) else DATA_DIR / "watchlist.csv"


def load_watchlist() -> pd.DataFrame:
    path = _watchlist_path()
    _ensure_file(path)
    df = pd.read_csv(path, dtype=str)
    if df.empty:
        return pd.DataFrame(columns=WATCHLIST_COLUMNS)

    for column in WATCHLIST_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    _clean_strings(df, STRING_COLUMNS)

    if "instrument_id" in df.columns:
        df["instrument_id"] = df["instrument_id"].fillna(df.get("symbol"))
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].fillna(df.get("instrument_id"))

    df["provider"] = df["provider"].fillna("alpha_vantage")
    df = df.dropna(subset=["instrument_id"]).drop_duplicates(subset=["instrument_id"], keep="last")
    return df[WATCHLIST_COLUMNS]


def build_watchlist_universe(base_universe: pd.DataFrame | None = None) -> pd.DataFrame:
    watchlist = load_watchlist()
    if watchlist.empty:
        return watchlist

    if base_universe is None or base_universe.empty:
        return watchlist

    merged = watchlist.merge(
        base_universe,
        on="instrument_id",
        how="left",
        suffixes=("", "_base"),
    )

    for column in ("name", "asset_type", "ticker", "currency", "market", "country"):
        base_col = f"{column}_base"
        if base_col in merged.columns:
            merged[column] = merged[column].fillna(merged[base_col])

    keep_cols = [col for col in WATCHLIST_COLUMNS if col in merged.columns]
    return merged[keep_cols]
