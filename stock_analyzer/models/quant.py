import pandas as pd

from ..config import load_config


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


def _resolve_quant_weights(config: dict | None = None, weights: dict | None = None) -> dict:
    cfg = dict(config or load_config())
    if weights:
        cfg.update(weights)
    return {
        "momentum_weight_base": float(cfg.get("momentum_weight_base", 0.55)),
        "momentum_weight_risk_scale": float(cfg.get("momentum_weight_risk_scale", 0.15)),
        "momentum_weight_min": float(cfg.get("momentum_weight_min", 0.5)),
        "momentum_weight_max": float(cfg.get("momentum_weight_max", 0.7)),
    }


def _score_quant_group(features: pd.DataFrame, weights: dict) -> pd.DataFrame:
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

    min_weight = min(weights["momentum_weight_min"], weights["momentum_weight_max"])
    max_weight = max(weights["momentum_weight_min"], weights["momentum_weight_max"])
    momentum_weight = (
        weights["momentum_weight_base"] + weights["momentum_weight_risk_scale"] * df["risk_score"]
    ).clip(min_weight, max_weight)
    risk_weight = 1 - momentum_weight
    df["quant_score"] = momentum_weight * df["momentum_score"] + risk_weight * df["risk_score"]

    return df[["instrument_id", "momentum_score", "risk_score", "quant_score"]]


def score_quant(
    features: pd.DataFrame,
    config: dict | None = None,
    weights: dict | None = None,
    group_by: str | None = None,
) -> pd.DataFrame:
    if features.empty:
        return features
    resolved = _resolve_quant_weights(config, weights)
    if group_by and group_by in features.columns:
        groups = []
        for value, group in features.groupby(group_by):
            scored = _score_quant_group(group, resolved)
            scored[group_by] = value
            groups.append(scored)
        if not groups:
            return pd.DataFrame(columns=["instrument_id", "momentum_score", "risk_score", "quant_score", group_by])
        return pd.concat(groups, ignore_index=True)
    return _score_quant_group(features, resolved)
