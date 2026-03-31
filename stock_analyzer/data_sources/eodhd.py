from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests


@dataclass
class EODHDConfig:
    api_token: str
    exchanges: List[Dict]
    timeout_sec: int = 30


def _request_json(url: str, timeout: int) -> list | dict:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_exchange_symbols(exchange_code: str, api_token: str, timeout: int = 30) -> pd.DataFrame:
    url = (
        f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}"
        f"?api_token={api_token}&fmt=json"
    )
    payload = _request_json(url, timeout)
    if not isinstance(payload, list):
        return pd.DataFrame()
    df = pd.DataFrame(payload)
    return df


def fetch_bulk_last_day(exchange_code: str, api_token: str, timeout: int = 30) -> pd.DataFrame:
    url = f"https://eodhd.com/api/eod-bulk-last-day/{exchange_code}?api_token={api_token}&fmt=json"
    payload = _request_json(url, timeout)
    if not isinstance(payload, list):
        return pd.DataFrame()
    return pd.DataFrame(payload)


def normalize_symbols(
    symbols_df: pd.DataFrame, exchange_code: str, asset_type: str
) -> pd.DataFrame:
    if symbols_df.empty:
        return pd.DataFrame()

    df = symbols_df.copy()
    rename = {
        "Code": "instrument_id",
        "Symbol": "instrument_id",
        "Name": "name",
        "Company Name": "name",
        "Country": "country",
        "Exchange": "market",
        "Currency": "currency",
        "ISIN": "isin",
    }
    df = df.rename(columns=rename)

    if "instrument_id" not in df.columns and "symbol" in df.columns:
        df["instrument_id"] = df["symbol"]

    df["asset_type"] = asset_type
    df["market"] = df.get("market", exchange_code)
    df["manual_source"] = f"eodhd:{exchange_code}"
    df["notes"] = df.get("notes", "")
    df["ticker"] = df.get("instrument_id")

    keep = [
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
    for column in keep:
        if column not in df.columns:
            df[column] = pd.NA

    df = df.dropna(subset=["instrument_id"]).drop_duplicates(subset=["instrument_id"], keep="last")
    return df[keep]


def normalize_prices(price_df: pd.DataFrame, exchange_code: str) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame()

    df = price_df.copy()
    rename = {
        "code": "instrument_id",
        "Code": "instrument_id",
        "symbol": "instrument_id",
        "Symbol": "instrument_id",
        "date": "date",
        "Date": "date",
        "close": "close",
        "Close": "close",
    }
    df = df.rename(columns=rename)

    for column in ("instrument_id", "date", "close"):
        if column not in df.columns:
            df[column] = pd.NA

    df["exchange"] = exchange_code
    df = df.dropna(subset=["instrument_id", "date", "close"])
    df = df.drop_duplicates(subset=["instrument_id", "date"], keep="last")
    return df[["instrument_id", "date", "close", "exchange"]]


def build_eodhd_universe(config: EODHDConfig) -> pd.DataFrame:
    frames = []
    for entry in config.exchanges:
        exchange_code = entry.get("code")
        asset_type = entry.get("asset_type", "stock")
        if not exchange_code:
            continue
        symbols_df = fetch_exchange_symbols(exchange_code, config.api_token, config.timeout_sec)
        normalized = normalize_symbols(symbols_df, exchange_code, asset_type)
        if not normalized.empty:
            frames.append(normalized)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset=["instrument_id"], keep="last")


def build_eodhd_prices(config: EODHDConfig) -> pd.DataFrame:
    frames = []
    for entry in config.exchanges:
        exchange_code = entry.get("code")
        if not exchange_code:
            continue
        prices_df = fetch_bulk_last_day(exchange_code, config.api_token, config.timeout_sec)
        normalized = normalize_prices(prices_df, exchange_code)
        if not normalized.empty:
            frames.append(normalized)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset=["instrument_id", "date"], keep="last")
