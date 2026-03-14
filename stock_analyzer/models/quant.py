import pandas as pd


def score_quant(features: pd.DataFrame) -> pd.DataFrame:
    df = features.copy()
    if df.empty:
        return df

    df["momentum_score"] = df["momentum_20d"].rank(pct=True)
    df["risk_score"] = 1 - df["vol_20d"].rank(pct=True)
    df["quant_score"] = 0.6 * df["momentum_score"] + 0.4 * df["risk_score"]
    return df[["instrument_id", "momentum_score", "risk_score", "quant_score"]]
