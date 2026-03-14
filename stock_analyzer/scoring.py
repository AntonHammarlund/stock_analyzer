import pandas as pd


def combine_scores(quant: pd.DataFrame, ml: pd.DataFrame) -> pd.DataFrame:
    if quant.empty:
        return pd.DataFrame()

    df = quant.merge(ml, on="instrument_id", how="left")
    df["ml_score"] = df["ml_score"].fillna(0.5)
    df["ml_confidence"] = df.get("ml_confidence", 0.5)
    df["ml_confidence"] = df["ml_confidence"].fillna(0.5)

    df["final_score"] = 0.7 * df["quant_score"] + 0.3 * df["ml_score"]
    df["confidence"] = (df["quant_score"] + df["ml_confidence"]) / 2

    return df[["instrument_id", "quant_score", "ml_score", "final_score", "confidence", "risk_score"]]
