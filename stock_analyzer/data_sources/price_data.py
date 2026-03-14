import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from ..paths import DATA_DIR

SAMPLE_PRICES = DATA_DIR / "sample_prices.csv"


def load_prices(universe: pd.DataFrame) -> pd.DataFrame:
    if SAMPLE_PRICES.exists():
        return pd.read_csv(SAMPLE_PRICES)
    return _generate_sample_prices(universe)


def _generate_sample_prices(universe: pd.DataFrame) -> pd.DataFrame:
    np.random.seed(42)
    rows = []
    end = datetime.utcnow().date()
    start = end - timedelta(days=120)
    dates = pd.date_range(start=start, end=end, freq="B")

    for _, row in universe.iterrows():
        price = 100 + np.random.rand() * 20
        for date in dates:
            price *= 1 + np.random.normal(0, 0.002)
            rows.append(
                {
                    "instrument_id": row["instrument_id"],
                    "date": date.date().isoformat(),
                    "close": round(price, 2),
                }
            )
    return pd.DataFrame(rows)
