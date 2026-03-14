from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from ..paths import CONFIG_DIR

AVANZA_MAP = CONFIG_DIR / "avanza_availability.csv"

OUTPUT_COLUMNS = [
    "instrument_id",
    "isin",
    "name",
    "asset_type",
    "ticker",
    "currency",
    "market",
    "country",
    "avanza_available",
    "last_verified_date",
    "availability_source",
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
    "last_verified_date",
    "availability_source",
    "notes",
]

TRUE_VALUES = {"1", "true", "yes", "y", "t"}
FALSE_VALUES = {"0", "false", "no", "n", "f"}


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _clean_strings(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = df[column].fillna("").astype(str).str.strip()
        df.loc[df[column] == "", column] = pd.NA


def _parse_bool(value) -> bool | pd.NA:
    if pd.isna(value):
        return pd.NA
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return pd.NA


def _normalize_date(value) -> str | pd.NA:
    if pd.isna(value):
        return pd.NA
    text = str(value).strip()
    if not text:
        return pd.NA
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return pd.NA
    return parsed.date().isoformat()


class AvanzaAvailabilitySource:
    name = "avanza_availability"

    def __init__(self, path: Path = AVANZA_MAP) -> None:
        self.path = path

    def is_available(self) -> bool:
        return self.path.exists() and self.path.stat().st_size > 0

    def fetch(self) -> pd.DataFrame:
        if not self.path.exists():
            return _empty()

        df = pd.read_csv(self.path, dtype=str)
        if df.empty:
            return _empty()

        df = df.rename(columns={"availability_notes": "notes"})
        for column in OUTPUT_COLUMNS:
            if column not in df.columns:
                df[column] = pd.NA

        _clean_strings(df, STRING_COLUMNS)

        df["avanza_available"] = df["avanza_available"].apply(_parse_bool)
        df["last_verified_date"] = df["last_verified_date"].apply(_normalize_date)

        df["instrument_id"] = df["instrument_id"].fillna(df["isin"])
        df = df.dropna(subset=["instrument_id"]).drop_duplicates(subset=["instrument_id"], keep="last")
        return df[OUTPUT_COLUMNS]
