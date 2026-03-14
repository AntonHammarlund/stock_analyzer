from typing import Dict, List

import pandas as pd

from .paths import ensure_dirs
from .config import load_config
from .universe import build_universe, attach_universe_metadata
from .data_sources.price_data import load_prices
from .feature_store import compute_features
from .models.quant import score_quant
from .models.ml_proxy import load_ml_scores
from .scoring import combine_scores
from .portfolio import load_portfolio, portfolio_summary
from .outlook import daily_summary, deep_summary
from .reports import build_report, write_latest_report


def _estimate_horizon(risk_score: float, momentum_score: float) -> str:
    if risk_score >= 0.7 and momentum_score >= 0.6:
        return "6-12 months"
    if risk_score >= 0.7:
        return "3-6 months"
    if risk_score >= 0.5:
        return "1-3 years"
    return "3-10+ years"


def run_daily() -> Dict:
    ensure_dirs()
    config = load_config()
    notes: List[str] = []

    universe = attach_universe_metadata(build_universe())
    prices = load_prices(universe)
    features = compute_features(prices)
    quant_scores = score_quant(features)
    ml_scores = load_ml_scores()

    if ml_scores.empty:
        notes.append("ML scores unavailable; using quant-only blend with neutral ML.")

    combined = combine_scores(quant_scores, ml_scores)
    combined = combined.merge(universe, on="instrument_id", how="left")

    confidence_gate = float(config.get("confidence_gate", 0.55))
    combined = combined[combined["confidence"] >= confidence_gate]

    combined = combined.sort_values("final_score", ascending=False)
    top_n = int(config.get("top_picks_count", 10))
    top = combined.head(top_n)

    top_picks = []
    for _, row in top.iterrows():
        top_picks.append(
            {
                "instrument_id": row["instrument_id"],
                "name": row.get("name", "Unknown"),
                "asset_type": row.get("asset_type", "unknown"),
                "score": round(float(row["final_score"]), 4),
                "risk_score": round(float(row["risk_score"]), 4),
                "confidence": round(float(row["confidence"]), 4),
                "horizon": _estimate_horizon(float(row["risk_score"]), float(row["momentum_score"])),
                "rationale": "Balanced momentum and risk profile.",
            }
        )

    outlook = {
        "daily": daily_summary(),
        "deep": deep_summary(),
    }

    portfolio = portfolio_summary(load_portfolio())

    report = build_report(top_picks, outlook, portfolio, notes)
    write_latest_report(report)
    return report
