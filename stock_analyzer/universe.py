from datetime import date
from typing import Dict, Iterable

import pandas as pd

from .data_sources.avanza_availability import AvanzaAvailabilitySource
from .data_sources.universe_manual import ManualUniverseSource
from .paths import CONFIG_DIR
from .utils import read_json

SETTINGS_FILE = CONFIG_DIR / "universe_settings.json"

DEFAULT_SETTINGS = {
    "availability_ttl_days": 45,
    "default_asset_type": "stock",
    "default_currency": "SEK",
    "default_market": "XSTO",
    "default_country": "SE",
    "default_manual_source": "seed",
}

PROFILE_FIELDS = [
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

AVAILABILITY_FIELDS = [
    "avanza_available",
    "last_verified_date",
    "availability_source",
]

UNIVERSE_COLUMNS = [
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
    "avanza_available",
    "availability_status",
    "last_verified_date",
    "availability_source",
    "notes",
    "source",
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
    "last_verified_date",
    "availability_source",
    "notes",
    "source",
]


def _load_settings() -> Dict[str, object]:
    settings = DEFAULT_SETTINGS.copy()
    settings.update(read_json(SETTINGS_FILE))
    settings["availability_ttl_days"] = int(
        settings.get("availability_ttl_days", DEFAULT_SETTINGS["availability_ttl_days"])
    )
    return settings


def _clean_strings(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = df[column].fillna("").astype(str).str.strip()
        df.loc[df[column] == "", column] = pd.NA


def _merge_notes(manual_notes: pd.Series, avanza_notes: pd.Series) -> pd.Series:
    manual_clean = manual_notes.fillna("").astype(str)
    avanza_clean = avanza_notes.fillna("").astype(str)
    both = (manual_clean != "") & (avanza_clean != "")
    combined = manual_clean.mask(manual_clean == "", avanza_clean)
    combined = combined.mask(both, manual_clean + "; " + avanza_clean)
    combined = combined.replace("", pd.NA)
    return combined


def _merge_sources(manual: pd.DataFrame, avanza: pd.DataFrame, settings: Dict[str, object]) -> pd.DataFrame:
    if manual.empty and avanza.empty:
        return pd.DataFrame()

    manual = manual.copy()
    avanza = avanza.copy()

    if not manual.empty:
        manual["_manual_present"] = True
    if not avanza.empty:
        avanza["_avanza_present"] = True

    combined = manual.merge(avanza, on="instrument_id", how="outer", suffixes=("", "_avanza"))

    if "_manual_present" not in combined.columns:
        combined["_manual_present"] = False
    else:
        combined["_manual_present"] = combined["_manual_present"].fillna(False)

    if "_avanza_present" not in combined.columns:
        combined["_avanza_present"] = False
    else:
        combined["_avanza_present"] = combined["_avanza_present"].fillna(False)

    for field in PROFILE_FIELDS:
        avanza_field = f"{field}_avanza"
        if avanza_field in combined.columns:
            combined[field] = combined[field].fillna(combined[avanza_field])

    for field in AVAILABILITY_FIELDS:
        avanza_field = f"{field}_avanza"
        if avanza_field in combined.columns:
            combined[field] = combined[field].fillna(combined[avanza_field])

    if "notes_avanza" in combined.columns:
        combined["notes"] = _merge_notes(
            combined.get("notes", pd.Series(index=combined.index, dtype="object")),
            combined.get("notes_avanza", pd.Series(index=combined.index, dtype="object")),
        )

    combined["source"] = "manual"
    combined.loc[(~combined["_manual_present"]) & combined["_avanza_present"], "source"] = "avanza"
    combined.loc[combined["_manual_present"] & combined["_avanza_present"], "source"] = "manual+avanza"
    combined.loc[(~combined["_manual_present"]) & (~combined["_avanza_present"]), "source"] = "unknown"

    manual_mask = combined["source"].isin(["manual", "manual+avanza"])
    combined.loc[manual_mask, "manual_source"] = combined.loc[manual_mask, "manual_source"].fillna(
        settings["default_manual_source"]
    )
    combined.loc[~manual_mask, "manual_source"] = pd.NA

    drop_columns = [
        column
        for column in combined.columns
        if column.endswith("_avanza") or column in ("_manual_present", "_avanza_present")
    ]
    return combined.drop(columns=drop_columns, errors="ignore")


def _apply_defaults(df: pd.DataFrame, settings: Dict[str, object]) -> pd.DataFrame:
    defaults = {
        "asset_type": settings.get("default_asset_type"),
        "currency": settings.get("default_currency"),
        "market": settings.get("default_market"),
        "country": settings.get("default_country"),
    }

    _clean_strings(df, STRING_COLUMNS)

    for column, value in defaults.items():
        if column in df.columns and value is not None:
            df[column] = df[column].fillna(value)

    if "name" in df.columns:
        df["name"] = df["name"].fillna(df["instrument_id"])

    if "asset_type" in df.columns:
        df["asset_type"] = df["asset_type"].fillna("").astype(str).str.lower()
        df.loc[df["asset_type"] == "", "asset_type"] = pd.NA

    return df


def _evaluate_availability(df: pd.DataFrame, settings: Dict[str, object]) -> pd.DataFrame:
    df = df.copy()
    ttl_days = int(settings.get("availability_ttl_days", DEFAULT_SETTINGS["availability_ttl_days"]))

    if "avanza_available" not in df.columns:
        df["avanza_available"] = pd.NA

    df["availability_status"] = "unknown"
    available = df["avanza_available"] == True
    unavailable = df["avanza_available"] == False

    df.loc[unavailable, "availability_status"] = "not_available"
    df.loc[available, "availability_status"] = "unverified"

    if "last_verified_date" in df.columns:
        verified = pd.to_datetime(df["last_verified_date"], errors="coerce")
        today = pd.Timestamp.utcnow().normalize()
        if getattr(today, "tzinfo", None) is not None:
            today = today.tz_localize(None)
        if getattr(verified.dt, "tz", None) is not None:
            verified = verified.dt.tz_localize(None)
        age_days = (today - verified).dt.days
        fresh = verified.notna() & (age_days <= ttl_days)
        stale = verified.notna() & (age_days > ttl_days)
        df.loc[available & fresh, "availability_status"] = "confirmed"
        df.loc[available & stale, "availability_status"] = "stale"

    return df


def _fallback_universe() -> pd.DataFrame:
    today = date.today().isoformat()
    rows = [
        {
            "instrument_id": "SE0000000000",
            "isin": "SE0000000000",
            "name": "Example Fund",
            "asset_type": "fund",
            "ticker": "EXFUND",
            "currency": "SEK",
            "market": "XSTO",
            "country": "SE",
            "sector": "Multi-asset",
            "industry": "Balanced",
            "manual_source": "sample",
            "avanza_available": True,
            "availability_status": "confirmed",
            "last_verified_date": today,
            "availability_source": "sample",
            "notes": "Fallback seed",
            "source": "fallback",
        },
        {
            "instrument_id": "SE0000000001",
            "isin": "SE0000000001",
            "name": "Example Stock",
            "asset_type": "stock",
            "ticker": "EXSTK",
            "currency": "SEK",
            "market": "XSTO",
            "country": "SE",
            "sector": "Industrials",
            "industry": "Diversified",
            "manual_source": "sample",
            "avanza_available": True,
            "availability_status": "confirmed",
            "last_verified_date": today,
            "availability_source": "sample",
            "notes": "Fallback seed",
            "source": "fallback",
        },
        {
            "instrument_id": "US0000000001",
            "isin": "US0000000001",
            "name": "Example Global",
            "asset_type": "stock",
            "ticker": "EXGLB",
            "currency": "USD",
            "market": "XNYS",
            "country": "US",
            "sector": "Technology",
            "industry": "Platforms",
            "manual_source": "sample",
            "avanza_available": False,
            "availability_status": "not_available",
            "last_verified_date": today,
            "availability_source": "sample",
            "notes": "Fallback seed",
            "source": "fallback",
        },
    ]
    df = pd.DataFrame(rows)
    for column in UNIVERSE_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
    return df[UNIVERSE_COLUMNS]


def build_universe() -> pd.DataFrame:
    """Builds the base universe from manual + availability sources."""
    settings = _load_settings()

    manual = ManualUniverseSource().fetch()
    avanza = AvanzaAvailabilitySource().fetch()

    if manual.empty and avanza.empty:
        return _fallback_universe()

    universe = _merge_sources(manual, avanza, settings)
    if universe.empty:
        return _fallback_universe()

    universe["instrument_id"] = universe["instrument_id"].fillna(universe.get("isin"))
    universe = universe.dropna(subset=["instrument_id"])

    universe = _apply_defaults(universe, settings)
    universe = _evaluate_availability(universe, settings)

    for column in UNIVERSE_COLUMNS:
        if column not in universe.columns:
            universe[column] = pd.NA

    return universe[UNIVERSE_COLUMNS]


def attach_universe_metadata(universe: pd.DataFrame) -> pd.DataFrame:
    universe = universe.copy()
    universe["as_of"] = date.today().isoformat()
    return universe
