from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional
import time
import zipfile

import pandas as pd
import requests

from ..paths import DATA_DIR

STOOQ_STATIC_BASE = "https://static.stooq.com/db/h/"
STOOQ_DYNAMIC_BASE = "https://stooq.com/db/h/db/d/?b="

MARKET_META = {
    "us": {"country": "US", "currency": "USD"},
    "uk": {"country": "GB", "currency": "GBP"},
    "jp": {"country": "JP", "currency": "JPY"},
    "hk": {"country": "HK", "currency": "HKD"},
    "pl": {"country": "PL", "currency": "PLN"},
    "hu": {"country": "HU", "currency": "HUF"},
    "world": {"country": "ZZ", "currency": "USD"},
    "macro": {"country": "ZZ", "currency": "USD"},
}


@dataclass
class StooqConfig:
    enabled: bool = True
    markets: List[str] = field(default_factory=lambda: ["us", "world"])
    timeout_sec: int = 60
    history_days: int = 365
    max_instruments: int = 5000
    exclude_asset_types: List[str] = field(default_factory=lambda: ["etf"])
    cache_ttl_days: int = 1
    max_retries: int = 3
    backoff_sec: float = 2.0
    retry_statuses: List[int] = field(default_factory=lambda: [408, 429, 500, 502, 503, 504])
    download_enabled: bool = True
    allow_stale_local: bool = False
    base_urls: List[str] = field(
        default_factory=lambda: [STOOQ_STATIC_BASE, STOOQ_DYNAMIC_BASE]
    )


def _stooq_filename(market: str) -> str:
    return f"d_{market}_txt.zip"


def _build_download_urls(market: str, base_urls: Iterable[str]) -> List[str]:
    urls: List[str] = []
    for base in base_urls:
        if base.endswith("/"):
            urls.append(f"{base}{_stooq_filename(market)}")
        else:
            urls.append(f"{base}{_stooq_filename(market)}")
    return urls


def _should_retry_status(status_code: Optional[int], retry_statuses: Iterable[int]) -> bool:
    if status_code is None:
        return True
    return int(status_code) in {int(code) for code in retry_statuses}


def _download_zip(market: str, config: StooqConfig) -> Optional[Path]:
    cache_dir = DATA_DIR / "stooq"
    cache_dir.mkdir(parents=True, exist_ok=True)
    dest = cache_dir / _stooq_filename(market)
    fallback = DATA_DIR / _stooq_filename(market)

    if dest.exists() and dest.stat().st_size > 0:
        age_days = (datetime.utcnow() - datetime.utcfromtimestamp(dest.stat().st_mtime)).days
        if age_days <= config.cache_ttl_days:
            return dest
        if config.allow_stale_local:
            print(
                f"Using stale local Stooq archive for {market} ({age_days} days old)."
            )
            return dest
        print(
            f"Local Stooq archive for {market} is stale ({age_days} days). "
            "Download a fresh copy to continue."
        )
        if not config.download_enabled:
            return None

    if fallback.exists() and fallback.stat().st_size > 0:
        age_days = (datetime.utcnow() - datetime.utcfromtimestamp(fallback.stat().st_mtime)).days
        if age_days <= config.cache_ttl_days:
            print(f"Using Stooq archive from data/ for {market}.")
            return fallback
        if config.allow_stale_local:
            print(
                f"Using stale Stooq archive from data/ for {market} ({age_days} days old)."
            )
            return fallback
        print(
            f"Local Stooq archive in data/ for {market} is stale ({age_days} days). "
            "Download a fresh copy to continue."
        )
        if not config.download_enabled:
            return None

    if not config.download_enabled:
        print(
            f"Stooq download disabled and no local archive available for {market}. "
            "Place d_{market}_txt.zip into data/stooq/."
        )
        return None

    urls = _build_download_urls(market, config.base_urls)
    headers = {"User-Agent": "Mozilla/5.0"}
    last_error = None

    for url in urls:
        for attempt in range(1, max(1, config.max_retries) + 1):
            try:
                with requests.get(url, stream=True, timeout=config.timeout_sec, headers=headers) as response:
                    if response.status_code >= 400:
                        if _should_retry_status(response.status_code, config.retry_statuses):
                            raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
                        response.raise_for_status()
                    with dest.open("wb") as file:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                file.write(chunk)
                if dest.stat().st_size > 1_000_000 and zipfile.is_zipfile(dest):
                    return dest
                raise ValueError("Downloaded file is too small or not a valid zip.")
            except Exception as exc:
                last_error = exc
                if dest.exists():
                    dest.unlink(missing_ok=True)
                if attempt < config.max_retries:
                    sleep_for = config.backoff_sec * attempt
                    time.sleep(sleep_for)
                else:
                    print(f"Download failed for {url}: {exc}")
                    break

    if last_error:
        print(f"Failed to download Stooq {market} data: {last_error}")
    return None


def _infer_asset_type(path: str) -> str:
    lower = path.lower()
    if "etf" in lower:
        return "etf"
    if "bond" in lower:
        return "bond"
    if "fund" in lower:
        return "fund"
    if "index" in lower:
        return "index"
    if "stock" in lower:
        return "stock"
    return "unknown"


def _infer_exchange(path: str) -> Optional[str]:
    parts = Path(path).parts
    for part in parts:
        token = part.lower()
        if "stock" in token or "etf" in token or "bond" in token or "index" in token:
            return part.split(" ")[0].upper()
    return None


def _market_defaults(market: str) -> dict:
    return MARKET_META.get(market.lower(), {"country": pd.NA, "currency": pd.NA})


def _iter_symbol_files(zip_file: zipfile.ZipFile) -> Iterable[str]:
    for name in zip_file.namelist():
        if not name.lower().endswith(".txt"):
            continue
        yield name


def build_stooq_universe(config: StooqConfig) -> pd.DataFrame:
    rows: List[dict] = []
    seen: set[str] = set()
    exclude = {item.lower() for item in config.exclude_asset_types}

    for market in config.markets:
        zip_path = _download_zip(market, config)
        if zip_path is None:
            continue
        defaults = _market_defaults(market)

        with zipfile.ZipFile(zip_path) as zip_file:
            for name in _iter_symbol_files(zip_file):
                symbol = Path(name).stem
                if symbol in seen:
                    continue
                asset_type = _infer_asset_type(name)
                if asset_type.lower() in exclude:
                    continue
                exchange = _infer_exchange(name) or market.upper()
                seen.add(symbol)
                rows.append(
                    {
                        "instrument_id": symbol,
                        "isin": pd.NA,
                        "name": symbol.upper(),
                        "asset_type": asset_type,
                        "ticker": symbol.split(".")[0].upper(),
                        "currency": defaults.get("currency", pd.NA),
                        "market": exchange,
                        "country": defaults.get("country", pd.NA),
                        "sector": pd.NA,
                        "industry": pd.NA,
                        "manual_source": f"stooq:{market}",
                        "notes": "Stooq bulk data",
                    }
                )
                if config.max_instruments and len(rows) >= config.max_instruments:
                    break

        if config.max_instruments and len(rows) >= config.max_instruments:
            break

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["instrument_id"], keep="last")


def _last_lines(file_obj, keep: int) -> List[str]:
    buffer = deque(maxlen=keep)
    for raw in file_obj:
        try:
            line = raw.decode("utf-8").strip()
        except UnicodeDecodeError:
            line = raw.decode("latin-1", errors="ignore").strip()
        if not line:
            continue
        buffer.append(line)
    return list(buffer)


def build_stooq_prices(
    config: StooqConfig,
    instrument_ids: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    if instrument_ids is None:
        allowed = None
    else:
        try:
            allowed = {str(value) for value in instrument_ids}
        except TypeError:
            allowed = None
    exclude = {item.lower() for item in config.exclude_asset_types}
    rows: List[dict] = []

    for market in config.markets:
        zip_path = _download_zip(market, config)
        if zip_path is None:
            continue

        with zipfile.ZipFile(zip_path) as zip_file:
            for name in _iter_symbol_files(zip_file):
                symbol = Path(name).stem
                if allowed is not None and symbol not in allowed:
                    continue
                asset_type = _infer_asset_type(name)
                if asset_type.lower() in exclude:
                    continue

                try:
                    with zip_file.open(name) as file_obj:
                        lines = _last_lines(file_obj, config.history_days + 1)
                except Exception:
                    continue

                for line in lines:
                    if line.lower().startswith("date"):
                        continue
                    if ";" in line and "," not in line:
                        parts = line.split(";")
                    else:
                        parts = line.split(",")
                    if len(parts) < 5:
                        continue
                    date_str = parts[0].strip()
                    close_str = parts[4].strip()
                    try:
                        close_val = float(close_str)
                    except ValueError:
                        continue
                    rows.append(
                        {
                            "instrument_id": symbol,
                            "date": date_str,
                            "close": close_val,
                            "source": f"stooq:{market}",
                        }
                    )

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["instrument_id", "date", "close"])
    return df.drop_duplicates(subset=["instrument_id", "date"], keep="last")
