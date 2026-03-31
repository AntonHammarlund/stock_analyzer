import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analyzer.config import load_config
from stock_analyzer.data_sources.eodhd import EODHDConfig, build_eodhd_prices, build_eodhd_universe
from stock_analyzer.data_sources.nasdaq_nordic import (
    NasdaqNordicConfig,
    build_eod_prices,
    build_reference_universe,
)
from stock_analyzer.data_sources.stooq import StooqConfig, build_stooq_prices, build_stooq_universe
from stock_analyzer.data_sources.alpha_vantage import (
    load_alpha_vantage_config,
    build_alpha_vantage_universe,
    build_alpha_vantage_prices,
)
from stock_analyzer.data_sources.universe_import import IMPORT_FILE, OUTPUT_COLUMNS
from stock_analyzer.paths import CONFIG_DIR, DATA_DIR
from stock_analyzer.utils import read_json

PRICES_IMPORT = DATA_DIR / "prices_import.csv"


def _load_json(path: Path) -> Dict:
    return read_json(path)


def _merge_universe(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if existing is not None and not existing.empty:
        frames.append(existing)
    if incoming is not None and not incoming.empty:
        frames.append(incoming)
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    if "instrument_id" not in combined.columns:
        combined["instrument_id"] = pd.NA
    combined = combined.dropna(subset=["instrument_id"]).drop_duplicates(
        subset=["instrument_id"], keep="last"
    )
    for column in OUTPUT_COLUMNS:
        if column not in combined.columns:
            combined[column] = pd.NA
    return combined[OUTPUT_COLUMNS]


def _merge_prices(existing: pd.DataFrame, incoming: pd.DataFrame, keep_days: int | None) -> pd.DataFrame:
    frames = []
    if existing is not None and not existing.empty:
        frames.append(existing)
    if incoming is not None and not incoming.empty:
        frames.append(incoming)
    if not frames:
        return pd.DataFrame(columns=["instrument_id", "date", "close"])
    combined = pd.concat(frames, ignore_index=True)
    for column in ("instrument_id", "date", "close"):
        if column not in combined.columns:
            combined[column] = pd.NA
    combined = combined.dropna(subset=["instrument_id", "date", "close"])
    combined = combined.drop_duplicates(subset=["instrument_id", "date"], keep="last")
    if keep_days:
        combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
        max_date = combined["date"].max()
        if pd.notna(max_date):
            cutoff = max_date - pd.Timedelta(days=keep_days)
            combined = combined[combined["date"] >= cutoff]
        combined["date"] = combined["date"].dt.date.astype(str)
    return combined


def _load_eodhd_config() -> EODHDConfig | None:
    cfg = _load_json(CONFIG_DIR / "eodhd.json")
    if not cfg:
        return None
    if cfg.get("enabled") is False:
        return None
    token_env = cfg.get("api_token_env", "EODHD_API_TOKEN")
    api_token = cfg.get("api_token") or os.getenv(token_env)
    exchanges = cfg.get("exchanges") or []
    timeout_sec = int(cfg.get("timeout_sec", 30))
    if not api_token or not exchanges:
        return None
    return EODHDConfig(api_token=api_token, exchanges=exchanges, timeout_sec=timeout_sec)


def _load_nasdaq_config() -> NasdaqNordicConfig | None:
    cfg = _load_json(CONFIG_DIR / "nasdaq_nordic.json")
    if not cfg:
        return None
    if cfg.get("enabled") is False:
        return None
    reference_file = cfg.get("reference_file")
    eod_file = cfg.get("eod_file")
    delimiter = cfg.get("delimiter", ";")
    if not reference_file:
        return None
    reference_path = Path(reference_file)
    if not reference_path.is_absolute():
        reference_path = (ROOT / reference_path).resolve()
    eod_path = Path(eod_file) if eod_file else None
    if eod_path and not eod_path.is_absolute():
        eod_path = (ROOT / eod_path).resolve()
    reference_mapping = cfg.get("reference_mapping") or {}
    eod_mapping = cfg.get("eod_mapping") or {}
    return NasdaqNordicConfig(
        reference_file=reference_path,
        eod_file=eod_path,
        delimiter=delimiter,
        reference_mapping=reference_mapping,
        eod_mapping=eod_mapping,
    )


def _load_stooq_config(keep_days: int) -> StooqConfig | None:
    cfg = _load_json(CONFIG_DIR / "stooq.json")
    if not cfg:
        return None
    if cfg.get("enabled") is False:
        return None
    markets = cfg.get("markets") or ["us", "world"]
    timeout_sec = int(cfg.get("timeout_sec", 60))
    history_days = int(cfg.get("history_days", keep_days))
    max_instruments = int(cfg.get("max_instruments", 5000))
    exclude_asset_types = cfg.get("exclude_asset_types") or ["etf"]
    cache_ttl_days = int(cfg.get("cache_ttl_days", 1))
    max_retries = int(cfg.get("max_retries", 3))
    backoff_sec = float(cfg.get("backoff_sec", 2.0))
    retry_statuses = cfg.get("retry_statuses") or [408, 429, 500, 502, 503, 504]
    download_enabled = bool(cfg.get("download_enabled", True))
    allow_stale_local = bool(cfg.get("allow_stale_local", False))
    base_urls = cfg.get("base_urls")
    stooq_cfg = StooqConfig(
        enabled=True,
        markets=markets,
        timeout_sec=timeout_sec,
        history_days=history_days,
        max_instruments=max_instruments,
        exclude_asset_types=exclude_asset_types,
        cache_ttl_days=cache_ttl_days,
        max_retries=max_retries,
        backoff_sec=backoff_sec,
        retry_statuses=retry_statuses,
        download_enabled=download_enabled,
        allow_stale_local=allow_stale_local,
    )
    if base_urls:
        stooq_cfg.base_urls = base_urls
    return stooq_cfg


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync daily universe + prices from external providers.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing import files.")
    parser.add_argument("--no-eodhd", action="store_true", help="Skip EODHD provider.")
    parser.add_argument("--no-nasdaq", action="store_true", help="Skip Nasdaq Nordic provider.")
    parser.add_argument(
        "--keep-days",
        type=int,
        default=None,
        help="Keep only the last N days of price history (defaults to config price_history_days).",
    )
    args = parser.parse_args()

    config = load_config()
    keep_days = args.keep_days or int(config.get("price_history_days", 365))

    universe_frames: List[pd.DataFrame] = []
    price_frames: List[pd.DataFrame] = []
    sources: List[str] = []

    alpha_cfg = load_alpha_vantage_config()
    if alpha_cfg.enabled:
        av_universe, reason = build_alpha_vantage_universe(alpha_cfg)
        if not av_universe.empty:
            universe_frames.append(av_universe)
            sources.append("alpha_vantage_universe")
            av_prices, price_reason = build_alpha_vantage_prices(
                alpha_cfg, av_universe["instrument_id"]
            )
            if not av_prices.empty:
                price_frames.append(av_prices)
                sources.append("alpha_vantage_prices")
            elif price_reason:
                print(f"Alpha Vantage prices skipped: {price_reason}")
        else:
            print(f"Alpha Vantage universe skipped: {reason}")

    stooq_cfg = _load_stooq_config(keep_days)
    if stooq_cfg:
        stooq_universe = build_stooq_universe(stooq_cfg)
        if not stooq_universe.empty:
            universe_frames.append(stooq_universe)
            sources.append("stooq_universe")
            stooq_prices = build_stooq_prices(stooq_cfg, stooq_universe["instrument_id"])
            if not stooq_prices.empty:
                price_frames.append(stooq_prices)
                sources.append("stooq_prices")
        else:
            print("Stooq enabled but no universe rows were built.")

    if not args.no_nasdaq:
        nasdaq_cfg = _load_nasdaq_config()
        if nasdaq_cfg:
            ref_df = build_reference_universe(nasdaq_cfg)
            if not ref_df.empty:
                universe_frames.append(ref_df)
                sources.append("nasdaq_reference")
            if nasdaq_cfg.eod_file and nasdaq_cfg.eod_file.exists():
                eod_df = build_eod_prices(nasdaq_cfg)
                if not eod_df.empty:
                    price_frames.append(eod_df)
                    sources.append("nasdaq_eod")
        else:
            print("Nasdaq Nordic config missing/disabled or reference file not found.")

    if not args.no_eodhd:
        eodhd_cfg = _load_eodhd_config()
        if eodhd_cfg:
            eod_universe = build_eodhd_universe(eodhd_cfg)
            if not eod_universe.empty:
                universe_frames.append(eod_universe)
                sources.append("eodhd_symbols")
            eod_prices = build_eodhd_prices(eodhd_cfg)
            if not eod_prices.empty:
                price_frames.append(eod_prices)
                sources.append("eodhd_prices")
        else:
            print("EODHD config missing/disabled or token/exchanges not set.")

    if not universe_frames and not price_frames:
        print("No data sources configured. Check config/eodhd.json and config/nasdaq_nordic.json.")
        return

    if IMPORT_FILE.exists() and not args.overwrite:
        existing_universe = pd.read_csv(IMPORT_FILE, dtype=str)
    else:
        existing_universe = pd.DataFrame(columns=OUTPUT_COLUMNS)

    incoming_universe = (
        pd.concat(universe_frames, ignore_index=True) if universe_frames else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )
    merged_universe = _merge_universe(existing_universe, incoming_universe)
    IMPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged_universe.to_csv(IMPORT_FILE, index=False)

    if PRICES_IMPORT.exists() and not args.overwrite:
        existing_prices = pd.read_csv(PRICES_IMPORT)
    else:
        existing_prices = pd.DataFrame(columns=["instrument_id", "date", "close"])

    incoming_prices = (
        pd.concat(price_frames, ignore_index=True)
        if price_frames
        else pd.DataFrame(columns=["instrument_id", "date", "close"])
    )
    merged_prices = _merge_prices(existing_prices, incoming_prices, keep_days)
    PRICES_IMPORT.parent.mkdir(parents=True, exist_ok=True)
    merged_prices.to_csv(PRICES_IMPORT, index=False)

    print(f"Universe rows: {len(merged_universe)}")
    print(f"Price rows: {len(merged_prices)}")
    print(f"Sources used: {', '.join(sorted(set(sources))) if sources else 'none'}")


if __name__ == "__main__":
    main()
