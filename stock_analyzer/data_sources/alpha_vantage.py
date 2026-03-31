from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
import os
import time

import pandas as pd
import requests

from ..paths import DATA_DIR, CONFIG_DIR
from ..utils import read_json, write_json

LISTING_STATUS_URL = "https://www.alphavantage.co/query?function=LISTING_STATUS"
TIME_SERIES_DAILY_URL = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY"
PROGRESS_FILE = DATA_DIR / "alpha_vantage_progress.json"


@dataclass
class AlphaVantageConfig:
    enabled: bool
    api_key_env: str
    include_delisted: bool
    exchange_allowlist: List[str]
    exclude_asset_types: List[str]
    max_symbols: int
    batch_size: int
    rate_limit_sleep_sec: float
    outputsize: str
    history_days: int


def load_alpha_vantage_config() -> AlphaVantageConfig:
    cfg = read_json(CONFIG_DIR / "alpha_vantage.json")
    return AlphaVantageConfig(
        enabled=bool(cfg.get("enabled", False)),
        api_key_env=str(cfg.get("api_key_env", "ALPHAVANTAGE_API_KEY")),
        include_delisted=bool(cfg.get("include_delisted", False)),
        exchange_allowlist=[str(x) for x in (cfg.get("exchange_allowlist") or [])],
        exclude_asset_types=[str(x) for x in (cfg.get("exclude_asset_types") or [])],
        max_symbols=int(cfg.get("max_symbols", 5000)),
        batch_size=int(cfg.get("batch_size", 25)),
        rate_limit_sleep_sec=float(cfg.get("rate_limit_sleep_sec", 12)),
        outputsize=str(cfg.get("outputsize", "compact")),
        history_days=int(cfg.get("history_days", 365)),
    )


def _resolve_api_key(env_name: str) -> str | None:
    return os.getenv(env_name) or os.getenv(env_name.lower()) or os.getenv(env_name.upper())


def _fetch_listing_status(api_key: str, state: str = "active") -> pd.DataFrame:
    url = f"{LISTING_STATUS_URL}&state={state}&apikey={api_key}"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    text = response.text
    df = pd.read_csv(StringIO(text))
    return df


def build_alpha_vantage_universe(config: AlphaVantageConfig) -> Tuple[pd.DataFrame, str | None]:
    if not config.enabled:
        return pd.DataFrame(), "disabled"

    api_key = _resolve_api_key(config.api_key_env)
    if not api_key:
        return pd.DataFrame(), "missing-api-key"

    df = _fetch_listing_status(api_key, state="active")
    if df.empty:
        return pd.DataFrame(), "empty"

    df = df.rename(
        columns={
            "symbol": "instrument_id",
            "name": "name",
            "exchange": "market",
            "assetType": "asset_type",
            "status": "status",
        }
    )
    df["instrument_id"] = df["instrument_id"].astype(str)
    df["ticker"] = df["instrument_id"]
    df["asset_type"] = df["asset_type"].astype(str).str.lower()
    df["market"] = df["market"].astype(str)

    if config.exchange_allowlist:
        allow = {x.upper() for x in config.exchange_allowlist}
        df = df[df["market"].str.upper().isin(allow)]

    exclude = {x.lower() for x in config.exclude_asset_types}
    if exclude:
        df = df[~df["asset_type"].str.lower().isin(exclude)]

    df["currency"] = "USD"
    df["country"] = "US"
    df["isin"] = pd.NA
    df["sector"] = pd.NA
    df["industry"] = pd.NA
    df["manual_source"] = "alpha_vantage:listing_status"
    df["notes"] = "Alpha Vantage listing status"

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
    df = df.dropna(subset=["instrument_id"]).drop_duplicates(subset=["instrument_id"], keep="last")
    if config.max_symbols > 0:
        df = df.head(config.max_symbols)
    return df[keep], None


def _load_progress() -> dict:
    if not PROGRESS_FILE.exists():
        return {"cursor": 0}
    return read_json(PROGRESS_FILE)


def _save_progress(payload: dict) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_json(PROGRESS_FILE, payload)


def _select_batch(symbols: List[str], batch_size: int) -> Tuple[List[str], int]:
    progress = _load_progress()
    cursor = int(progress.get("cursor", 0))
    if cursor >= len(symbols):
        cursor = 0
    batch = symbols[cursor : cursor + batch_size]
    next_cursor = cursor + len(batch)
    _save_progress(
        {
            "cursor": next_cursor,
            "symbols_count": len(symbols),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "last_symbol": batch[-1] if batch else None,
        }
    )
    return batch, next_cursor


def _fetch_time_series(symbol: str, api_key: str, outputsize: str) -> Optional[dict]:
    url = f"{TIME_SERIES_DAILY_URL}&symbol={symbol}&outputsize={outputsize}&apikey={api_key}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and ("Note" in payload or "Error Message" in payload):
        return None
    return payload


def build_alpha_vantage_prices(
    config: AlphaVantageConfig, symbols: Iterable[str]
) -> Tuple[pd.DataFrame, str | None]:
    if not config.enabled:
        return pd.DataFrame(), "disabled"

    api_key = _resolve_api_key(config.api_key_env)
    if not api_key:
        return pd.DataFrame(), "missing-api-key"

    symbol_list = sorted({str(s).strip() for s in symbols if str(s).strip()})
    if not symbol_list:
        return pd.DataFrame(), "no-symbols"

    batch_size = max(1, min(config.batch_size, len(symbol_list)))
    batch, _ = _select_batch(symbol_list, batch_size)

    rows: List[dict] = []
    for symbol in batch:
        payload = _fetch_time_series(symbol, api_key, config.outputsize)
        if payload is None:
            print(f"Alpha Vantage rate limit or error for {symbol}; stopping batch.")
            break
        series = payload.get("Time Series (Daily)")
        if not isinstance(series, dict):
            continue
        for date_str, entry in series.items():
            if not isinstance(entry, dict):
                continue
            close_value = entry.get("4. close") or entry.get("5. adjusted close")
            if close_value is None:
                continue
            rows.append(
                {
                    "instrument_id": symbol,
                    "date": date_str,
                    "close": float(close_value),
                }
            )
        if config.rate_limit_sleep_sec:
            time.sleep(config.rate_limit_sleep_sec)

    if not rows:
        return pd.DataFrame(), "no-rows"

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].notna().any():
        max_date = df["date"].max()
        cutoff = max_date - pd.Timedelta(days=config.history_days)
        df = df[df["date"] >= cutoff]
    df["date"] = df["date"].dt.date.astype(str)
    df = df.dropna(subset=["instrument_id", "date", "close"])
    df = df.drop_duplicates(subset=["instrument_id", "date"], keep="last")
    return df, None
