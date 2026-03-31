from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd


@dataclass
class NasdaqNordicConfig:
    reference_file: Path
    eod_file: Path | None
    delimiter: str = ";"
    reference_mapping: Dict[str, str] | None = None
    eod_mapping: Dict[str, str] | None = None


def _read_table(path: Path | None, delimiter: str) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, delimiter=delimiter, dtype=str, low_memory=False)


def _apply_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if not mapping:
        return df
    return df.rename(columns=mapping)


def normalize_reference(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    required = [
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
    ]

    for column in required:
        if column not in df.columns:
            df[column] = pd.NA

    df["manual_source"] = "nasdaq_nordic"
    df["notes"] = df.get("notes", "")
    df["instrument_id"] = df["instrument_id"].fillna(df.get("isin"))
    df = df.dropna(subset=["instrument_id"]).drop_duplicates(subset=["instrument_id"], keep="last")
    keep = required + ["manual_source", "notes"]
    return df[keep]


def normalize_eod(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    required = ["instrument_id", "date", "close"]
    for column in required:
        if column not in df.columns:
            df[column] = pd.NA

    df = df.dropna(subset=required)
    df = df.drop_duplicates(subset=["instrument_id", "date"], keep="last")
    return df[required]


def build_reference_universe(config: NasdaqNordicConfig) -> pd.DataFrame:
    df = _read_table(config.reference_file, config.delimiter)
    df = _apply_mapping(df, config.reference_mapping or {})
    return normalize_reference(df)


def build_eod_prices(config: NasdaqNordicConfig) -> pd.DataFrame:
    df = _read_table(config.eod_file, config.delimiter)
    df = _apply_mapping(df, config.eod_mapping or {})
    return normalize_eod(df)
