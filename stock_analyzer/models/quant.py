import pandas as pd


def _winsorize(series: pd.Series, lower: float = 0.05, upper: float = 0.95) -> pd.Series:
    if series.empty:
        return series
    valid = series.dropna()
    if valid.size < 3:
        return series
    lower_q = valid.quantile(lower)
    upper_q = valid.quantile(upper)
    return series.clip(lower_q, upper_q)


def _safe_rank(series: pd.Series) -> pd.Series:
    if series.dropna().empty or series.nunique(dropna=True) <= 1:
        return pd.Series(0.5, index=series.index)
    return series.rank(pct=True, method="average").fillna(0.5)


def score_quant(features: pd.DataFrame) -> pd.DataFrame:
    df = features.copy()
    if df.empty:
        return df

    if "momentum_20d" not in df.columns:
        df["momentum_20d"] = 0.0
    if "vol_20d" not in df.columns:
        df["vol_20d"] = 0.0

    momentum_raw = pd.to_numeric(df["momentum_20d"], errors="coerce")
    volatility_raw = pd.to_numeric(df["vol_20d"], errors="coerce").abs()

    momentum_adj = _winsorize(momentum_raw)
    volatility_adj = _winsorize(volatility_raw)

    df["momentum_score"] = _safe_rank(momentum_adj)
    df["risk_score"] = 1 - _safe_rank(volatility_adj)

    momentum_weight = (0.55 + 0.15 * df["risk_score"]).clip(0.5, 0.7)
    risk_weight = 1 - momentum_weight
    df["quant_score"] = momentum_weight * df["momentum_score"] + risk_weight * df["risk_score"]

    return df[["instrument_id", "momentum_score", "risk_score", "quant_score"]]
