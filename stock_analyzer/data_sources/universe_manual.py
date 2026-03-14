from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from ..paths import CONFIG_DIR

MANUAL_UNIVERSE = CONFIG_DIR / "universe_manual.csv"

OUTPUT_COLUMNS = [
    "instrument_id",
    "isin",
    "name",
    "asset_type",
    "ticker",
    "currency",
    "market",
    "country",
    "sector",
    "industry",
    "manual_source",
    "notes",
]

STRING_COLUMNS = [
    "instrument_id",
    "isin",
    "name",
    "asset_type",
    "ticker",
    "currency",
    "market",
    "country",
    "sector",
    "industry",
    "manual_source",
    "notes",
]

RENAME_COLUMNS = {"source": "manual_source"}


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _clean_strings(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = df[column].fillna("").astype(str).str.strip()
        df.loc[df[column] == "", column] = pd.NA


class ManualUniverseSource:
    name = "manual_universe"

    def __init__(self, path: Path = MANUAL_UNIVERSE) -> None:
        self.path = path

    def is_available(self) -> bool:
        return self.path.exists() and self.path.stat().st_size > 0

    def fetch(self) -> pd.DataFrame:
        if not self.path.exists():
            return _empty()

        df = pd.read_csv(self.path, dtype=str)
        if df.empty:
            return _empty()

        df = df.rename(columns=RENAME_COLUMNS)
        for column in OUTPUT_COLUMNS:
            if column not in df.columns:
                df[column] = pd.NA

        _clean_strings(df, STRING_COLUMNS)

        df["instrument_id"] = df["instrument_id"].fillna(df["isin"])
        df = df.dropna(subset=["instrument_id"]).drop_duplicates(subset=["instrument_id"], keep="last")
        return df[OUTPUT_COLUMNS]
