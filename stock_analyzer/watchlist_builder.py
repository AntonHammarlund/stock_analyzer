from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from .config import load_config
from .paths import CONFIG_DIR, DATA_DIR
from .utils import read_json
from .users import get_active_user_id
from .portfolio import load_portfolio
from .reports import load_latest_report
from .universe import build_universe

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


def _watchlist_path() -> Path:
    cfg = load_config()
    path = cfg.get("watchlist_file", "data/watchlist.csv")
    return Path(path) if isinstance(path, str) else DATA_DIR / "watchlist.csv"


def _looks_like_isin(value: str) -> bool:
    value = value.strip()
    return len(value) == 12 and value[:2].isalpha() and value[2:].isalnum()


def _guess_symbol(instrument_id: str, ticker: str | None) -> str | None:
    if ticker:
        return ticker
    if not instrument_id:
        return None
    if _looks_like_isin(instrument_id):
        return None
    return instrument_id


def _load_auto_config() -> Dict:
    cfg = read_json(CONFIG_DIR / "free_sources.json")
    return cfg.get("watchlist", {})


def _ensure_file(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=WATCHLIST_COLUMNS).to_csv(path, index=False)


def build_watchlist_if_needed(force: bool = False) -> pd.DataFrame:
    watch_cfg = _load_auto_config()
    auto_cfg = watch_cfg.get("auto_seed", {})
    if auto_cfg.get("enabled") is False:
        return pd.DataFrame(columns=WATCHLIST_COLUMNS)

    path = _watchlist_path()
    _ensure_file(path)
    if path.exists() and not force:
        existing = pd.read_csv(path, dtype=str)
        if not existing.empty:
            return existing

    max_size = int(watch_cfg.get("max_size", 25))
    provider_default = watch_cfg.get("default_provider", "alpha_vantage")

    universe = build_universe()
    universe_map = {}
    if not universe.empty:
        universe_map = (
            universe.set_index("instrument_id")[
                ["ticker", "name", "asset_type", "currency", "market", "country"]
            ]
            .fillna("")
            .to_dict("index")
        )

    entries: List[Dict] = []

    def add_entry(
        instrument_id: str,
        name: str | None = None,
        asset_type: str | None = None,
        ticker: str | None = None,
        provider: str | None = None,
        notes: str | None = None,
    ) -> None:
        instrument_id = str(instrument_id).strip()
        if not instrument_id:
            return
        meta = universe_map.get(instrument_id, {})
        ticker_val = ticker or meta.get("ticker") or ""
        symbol = _guess_symbol(instrument_id, ticker_val)
        if not symbol:
            return
        entries.append(
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "provider": provider or provider_default,
                "name": name or meta.get("name") or "",
                "asset_type": asset_type or meta.get("asset_type") or "",
                "ticker": ticker_val,
                "currency": meta.get("currency") or "",
                "market": meta.get("market") or "",
                "country": meta.get("country") or "",
                "notes": notes or "",
            }
        )

    if auto_cfg.get("from_portfolio", True):
        active_user = get_active_user_id()
        for holding in load_portfolio(active_user):
            add_entry(
                holding.get("instrument_id", ""),
                name=holding.get("name"),
                notes="portfolio",
            )

    if auto_cfg.get("from_top_picks", True):
        report = load_latest_report() or {}
        for pick in report.get("top_picks_combined", []):
            add_entry(
                pick.get("instrument_id", ""),
                name=pick.get("name"),
                asset_type=pick.get("asset_type"),
                notes="top_pick",
            )

    fallback = auto_cfg.get("fallback_symbols", []) or []
    for symbol in fallback:
        add_entry(str(symbol), name=str(symbol), notes="fallback")

    if not entries:
        df = pd.DataFrame(columns=WATCHLIST_COLUMNS)
        df.to_csv(path, index=False)
        return df

    df = pd.DataFrame(entries)
    df = df.drop_duplicates(subset=["instrument_id"], keep="first")
    if max_size > 0 and len(df) > max_size:
        df = df.head(max_size)

    for column in WATCHLIST_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    df.to_csv(path, index=False)
    return df
