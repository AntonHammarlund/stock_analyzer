import pandas as pd


DEFAULT_SCORE = 0.5


def _clip_series(series: pd.Series, default: float = DEFAULT_SCORE) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default).clip(0, 1)


def _ensure_column(df: pd.DataFrame, column: str, default: float = DEFAULT_SCORE) -> None:
    if column not in df.columns:
        df[column] = default
    df[column] = _clip_series(df[column], default)


def combine_scores(quant: pd.DataFrame, ml: pd.DataFrame) -> pd.DataFrame:
    output_columns = [
        "instrument_id",
        "quant_score",
        "ml_score",
        "final_score",
        "confidence",
        "risk_score",
        "momentum_score",
    ]
    if quant.empty:
        return pd.DataFrame(columns=output_columns)

    df = quant.copy()
    _ensure_column(df, "quant_score")
    _ensure_column(df, "risk_score")
    _ensure_column(df, "momentum_score")

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

    ml_weight = 0.1 + 0.4 * df["ml_confidence"]
    df["final_score"] = (1 - ml_weight) * df["quant_score"] + ml_weight * df["ml_score"]
    df["confidence"] = (
        0.5 * df["quant_score"] + 0.3 * df["ml_confidence"] + 0.2 * df["risk_score"]
    ).clip(0, 1)

    return df[output_columns]
