import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analyzer.config import load_config
from stock_analyzer.data_sources.avanza_client import (
    get_avanza_client,
    get_avanza_constants,
    load_avanza_config,
)
from stock_analyzer.data_sources.watchlist import load_watchlist
from stock_analyzer.watchlist_builder import build_watchlist_if_needed
from stock_analyzer.paths import DATA_DIR, CONFIG_DIR
from stock_analyzer.utils import read_json

WATCHLIST_PRICES = DATA_DIR / "prices_watchlist.csv"


def _load_config() -> Dict:
    cfg_path = CONFIG_DIR / "free_sources.json"
    return read_json(cfg_path)


def _fetch_alpha_vantage(
    symbol: str, api_key: str, function: str, outputsize: str
) -> List[Dict[str, float]] | None:
    url = (
        "https://www.alphavantage.co/query"
        f"?function={function}&symbol={symbol}&outputsize={outputsize}&apikey={api_key}"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and ("Note" in payload or "Error Message" in payload):
        return None
    series = payload.get("Time Series (Daily)") or payload.get("Time Series (Daily)".lower())
    if not isinstance(series, dict) or not series:
        return None
    rows: List[Dict[str, float]] = []
    for date_str, entry in series.items():
        if not isinstance(entry, dict):
            continue
        close_value = entry.get("4. close") or entry.get("5. adjusted close")
        if close_value is None:
            continue
        rows.append({"date": date_str, "close": float(close_value)})
    return rows or None


def _fetch_marketstack(symbol: str, api_key: str, endpoint: str) -> List[Dict[str, float]] | None:
    url = f"{endpoint}?access_key={api_key}&symbols={symbol}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("error"):
        return None
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        return None
    latest = data[0]
    close_value = latest.get("close")
    date_value = latest.get("date")
    if close_value is None or date_value is None:
        return None
    date_str = str(date_value).split("T")[0]
    return [{"date": date_str, "close": float(close_value)}]


def _fetch_avanza_prices(orderbook_id: str) -> List[Dict[str, float]] | None:
    cfg = load_avanza_config()
    client, reason = get_avanza_client()
    if client is None:
        return None

    constants = get_avanza_constants()
    if constants is None:
        return None
    TimePeriod, Resolution, _ = constants

    time_period = getattr(TimePeriod, cfg.chart_time_period, TimePeriod.ONE_YEAR)
    resolution = getattr(Resolution, cfg.chart_resolution, Resolution.DAY)

    try:
        payload = client.get_chart_data(orderbook_id, time_period, resolution)
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None
    ohlc = payload.get("ohlc") or []
    rows: List[Dict[str, float]] = []
    for entry in ohlc:
        if not isinstance(entry, dict):
            continue
        timestamp = entry.get("timestamp")
        close_value = entry.get("close")
        if timestamp is None or close_value is None:
            continue
        try:
            dt = pd.to_datetime(timestamp, unit="ms", utc=True)
        except Exception:
            continue
        rows.append({"date": dt.date().isoformat(), "close": float(close_value)})
    return rows or None


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync daily prices for the watchlist (free sources).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing watchlist prices.")
    parser.add_argument(
        "--keep-days",
        type=int,
        default=None,
        help="Keep only the last N days of watchlist history (defaults to price_history_days).",
    )
    args = parser.parse_args()

    config = load_config()
    build_watchlist_if_needed()
    watchlist = load_watchlist()
    if watchlist.empty:
        print("Watchlist is empty; nothing to sync.")
        return

    free_cfg = _load_config()
    watch_cfg = free_cfg.get("watchlist", {})
    if watch_cfg.get("enabled") is False:
        print("Watchlist sync disabled in config/free_sources.json.")
        return
    providers = watch_cfg.get("providers", {})
    default_provider = watch_cfg.get("default_provider", "alpha_vantage")

    alpha_cfg = providers.get("alpha_vantage", {})
    market_cfg = providers.get("marketstack", {})

    alpha_enabled = bool(alpha_cfg.get("enabled", True))
    market_enabled = bool(market_cfg.get("enabled", False))

    alpha_key = os.getenv(alpha_cfg.get("api_key_env", "ALPHAVANTAGE_API_KEY"))
    market_key = os.getenv(market_cfg.get("api_key_env", "MARKETSTACK_API_KEY"))
    if alpha_enabled and not alpha_key:
        print("Alpha Vantage enabled but API key missing (ALPHAVANTAGE_API_KEY).")
    if market_enabled and not market_key:
        print("Marketstack enabled but API key missing (MARKETSTACK_API_KEY).")

    alpha_function = alpha_cfg.get("function", "TIME_SERIES_DAILY")
    alpha_outputsize = alpha_cfg.get("outputsize", "compact")
    alpha_sleep = float(alpha_cfg.get("rate_limit_sleep_sec", 12))
    market_endpoint = market_cfg.get("endpoint", "https://api.marketstack.com/v1/eod/latest")
    market_sleep = float(market_cfg.get("rate_limit_sleep_sec", 1))

    rows: List[Dict] = []
    failed_symbols: List[str] = []
    max_size = int(watch_cfg.get("max_size", 25))
    if max_size > 0 and len(watchlist) > max_size:
        watchlist = watchlist.head(max_size)

    for _, row in watchlist.iterrows():
        instrument_id = str(row.get("instrument_id"))
        symbol = str(row.get("symbol") or row.get("ticker") or instrument_id)
        provider = str(row.get("provider") or default_provider).lower()

        result = None
        source = provider
        try:
            if provider == "marketstack" and market_enabled and market_key:
                result = _fetch_marketstack(symbol, market_key, market_endpoint)
                source = "marketstack"
                if market_sleep:
                    time.sleep(market_sleep)
            elif provider == "alpha_vantage" and alpha_enabled and alpha_key:
                result = _fetch_alpha_vantage(symbol, alpha_key, alpha_function, alpha_outputsize)
                source = "alpha_vantage"
                if alpha_sleep:
                    time.sleep(alpha_sleep)
            elif provider == "avanza":
                result = _fetch_avanza_prices(symbol)
                source = "avanza"
        except Exception as exc:
            print(f"Fetch failed for {symbol} via {provider}: {exc}")
            failed_symbols.append(symbol)
            continue

        if not result:
            failed_symbols.append(symbol)
            continue

        for entry in result:
            if not isinstance(entry, dict):
                continue
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "date": entry["date"],
                    "close": entry["close"],
                    "source": source,
                }
            )

    if not rows:
        print("No watchlist prices fetched. Check API keys and provider config.")
        return

    new_df = pd.DataFrame(rows)
    if WATCHLIST_PRICES.exists() and not args.overwrite:
        existing = pd.read_csv(WATCHLIST_PRICES)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["instrument_id", "date"], keep="last")
    else:
        combined = new_df

    keep_days = args.keep_days or int(config.get("price_history_days", 365))
    if keep_days:
        combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
        max_date = combined["date"].max()
        if pd.notna(max_date):
            cutoff = max_date - pd.Timedelta(days=keep_days)
            combined = combined[combined["date"] >= cutoff]
        combined["date"] = combined["date"].dt.date.astype(str)

    WATCHLIST_PRICES.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(WATCHLIST_PRICES, index=False)
    print(f"Fetched {len(new_df)} watchlist rows.")
    if failed_symbols:
        unique_failed = sorted(set(failed_symbols))
        print(f"Failed symbols: {', '.join(unique_failed)}")


if __name__ == "__main__":
    main()
