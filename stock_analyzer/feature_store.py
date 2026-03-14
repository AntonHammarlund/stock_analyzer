import pandas as pd
import numpy as np


def compute_features(prices: pd.DataFrame) -> pd.DataFrame:
    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"])
    prices.sort_values(["instrument_id", "date"], inplace=True)

    features = []
    for instrument_id, group in prices.groupby("instrument_id"):
        group = group.copy()
        group["return_1d"] = group["close"].pct_change()
        group["return_20d"] = group["close"].pct_change(20)
        group["vol_20d"] = group["return_1d"].rolling(20).std() * np.sqrt(252)
        latest = group.iloc[-1]
        features.append(
            {
                "instrument_id": instrument_id,
                "price": float(latest["close"]),
                "momentum_20d": float(latest["return_20d"] if pd.notna(latest["return_20d"]) else 0.0),
                "vol_20d": float(latest["vol_20d"] if pd.notna(latest["vol_20d"]) else 0.0),
            }
        )

    return pd.DataFrame(features)
