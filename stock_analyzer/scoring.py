import pandas as pd

from .config import load_config


DEFAULT_SCORE = 0.5


def _clip_series(series: pd.Series, default: float = DEFAULT_SCORE) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default).clip(0, 1)


def _ensure_column(df: pd.DataFrame, column: str, default: float = DEFAULT_SCORE) -> None:
    if column not in df.columns:
        df[column] = default
    df[column] = _clip_series(df[column], default)


def _resolve_score_weights(config: dict | None = None, weights: dict | None = None) -> dict:
    cfg = dict(config or load_config())
    if weights:
        cfg.update(weights)
    return {
        "ml_weight_base": float(cfg.get("ml_weight_base", 0.1)),
        "ml_weight_scale": float(cfg.get("ml_weight_scale", 0.4)),
        "confidence_weight_quant": float(cfg.get("confidence_weight_quant", 0.5)),
        "confidence_weight_ml_conf": float(cfg.get("confidence_weight_ml_conf", 0.3)),
        "confidence_weight_risk": float(cfg.get("confidence_weight_risk", 0.2)),
    }


def combine_scores(
    quant: pd.DataFrame,
    ml: pd.DataFrame,
    config: dict | None = None,
    weights: dict | None = None,
    keep_columns: list[str] | None = None,
) -> pd.DataFrame:
    output_columns = [
        "instrument_id",
        "quant_score",
        "ml_score",
        "final_score",
        "confidence",
        "risk_score",
        "momentum_score",
    ]
    extras = []
    if keep_columns:
        extras = [col for col in keep_columns if col not in output_columns]
        output_columns = extras + output_columns
    if quant.empty:
        return pd.DataFrame(columns=output_columns)

    weights = _resolve_score_weights(config, weights)

    df = quant.copy()
    _ensure_column(df, "quant_score")
    _ensure_column(df, "risk_score")
    _ensure_column(df, "momentum_score")
    for column in extras:
        if column not in df.columns:
            df[column] = pd.NA

    df = df.merge(ml, on="instrument_id", how="left")
    if "ml_score" in df.columns:
        ml_available = df["ml_score"].notna()
    else:
        ml_available = pd.Series(False, index=df.index)

    if "ml_score" not in df.columns:
        df["ml_score"] = DEFAULT_SCORE
    df["ml_score"] = _clip_series(df["ml_score"], DEFAULT_SCORE)

    if "ml_confidence" not in df.columns:
        df["ml_confidence"] = DEFAULT_SCORE
    df["ml_confidence"] = _clip_series(df["ml_confidence"], DEFAULT_SCORE)
    df.loc[~ml_available, "ml_confidence"] = 0.0

    ml_weight = (weights["ml_weight_base"] + weights["ml_weight_scale"] * df["ml_confidence"]).clip(0, 1)
    df["final_score"] = (1 - ml_weight) * df["quant_score"] + ml_weight * df["ml_score"]
    df["confidence"] = (
        weights["confidence_weight_quant"] * df["quant_score"]
        + weights["confidence_weight_ml_conf"] * df["ml_confidence"]
        + weights["confidence_weight_risk"] * df["risk_score"]
    ).clip(0, 1)

    return df[output_columns]
