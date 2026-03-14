import pandas as pd

from .avanza_availability import AvanzaAvailabilitySource
from .universe_manual import ManualUniverseSource


def load_avanza_map() -> pd.DataFrame:
    return AvanzaAvailabilitySource().fetch()


def load_manual_universe() -> pd.DataFrame:
    return ManualUniverseSource().fetch()
