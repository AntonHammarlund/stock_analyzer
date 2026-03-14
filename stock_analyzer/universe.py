import pandas as pd
from datetime import date

from .data_sources.local_manual import load_avanza_map


def build_universe() -> pd.DataFrame:
    """Builds a placeholder universe. Replace with real sources."""
    avanza_map = load_avanza_map()
    if avanza_map.empty:
        return pd.DataFrame(
            [
                {"instrument_id": "SE0000000000", "isin": "SE0000000000", "name": "Example Fund", "asset_type": "fund", "avanza_available": True},
                {"instrument_id": "SE0000000001", "isin": "SE0000000001", "name": "Example Stock", "asset_type": "stock", "avanza_available": True},
                {"instrument_id": "US0000000001", "isin": "US0000000001", "name": "Example Global", "asset_type": "stock", "avanza_available": False},
            ]
        )

    avanza_map = avanza_map.copy()
    avanza_map["asset_type"] = avanza_map.get("asset_type", "fund")
    return avanza_map[["instrument_id", "isin", "name", "asset_type", "avanza_available"]]


def attach_universe_metadata(universe: pd.DataFrame) -> pd.DataFrame:
    universe = universe.copy()
    universe["as_of"] = date.today().isoformat()
    return universe
