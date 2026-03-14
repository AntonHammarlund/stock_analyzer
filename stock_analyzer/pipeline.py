from time import perf_counter
from typing import Dict, List
from uuid import uuid4

import pandas as pd
from datetime import datetime, timezone

from .paths import ensure_dirs
from .config import load_config
from .universe import build_universe, attach_universe_metadata
from .data_sources.price_data import load_prices
from .data_sources.universe_import import ImportedUniverseSource
from .feature_store import compute_features
from .models.quant import score_quant
from .models.ml_proxy import load_ml_scores_with_meta
from .scoring import combine_scores
from .portfolio import load_portfolio, portfolio_summary
from .users import get_active_user_id
from .outlook import daily_summary, deep_summary
from .reports import build_report, write_latest_report
from .host_manager import select_host, summarize_host
from .notifications import notify_report
from .utils import utc_now_iso


def _estimate_horizon(risk_score: float, momentum_score: float) -> str:
    if risk_score >= 0.7 and momentum_score >= 0.6:
        return "6-12 months"
    if risk_score >= 0.7:
        return "3-6 months"
    if risk_score >= 0.5:
        return "1-3 years"
    return "3-10+ years"


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _filter_assets(df: pd.DataFrame, asset_types: list[str]) -> pd.DataFrame:
    if df.empty or "asset_type" not in df.columns:
        return df
    return df[df["asset_type"].isin(asset_types)]


def run_daily(user_id: str | None = None) -> Dict:
    ensure_dirs()
    config = load_config()
    notes: List[str] = []
    warnings: List[str] = []
    started_at = utc_now_iso()
    timer = perf_counter()
    run_id = uuid4().hex

    execution_host = summarize_host(select_host())
    ml_host = summarize_host(select_host("remote_ml", require_endpoint=False))

    imported_universe = ImportedUniverseSource().fetch()
    universe = attach_universe_metadata(build_universe())
    if universe.empty:
        warnings.append("Universe is empty; no instruments available.")
    prices = load_prices(universe)
    if prices.empty:
        warnings.append("Price data is empty; cannot compute features.")
    features = compute_features(prices)
    if features.empty:
        warnings.append("Feature set is empty; check price inputs.")
    quant_scores = score_quant(features)
    if quant_scores.empty:
        warnings.append("Quant scores are empty; pipeline will be sparse.")
    ml_scores, ml_meta = load_ml_scores_with_meta()

    if ml_scores.empty:
        status = ml_meta.get("status")
        if status == "no-endpoint":
            notes.append("Remote ML endpoint not configured; using quant-only blend.")
        elif status and str(status).startswith("http"):
            warnings.append("Remote ML fetch returned an error; using quant-only blend.")
        elif status == "error":
            warnings.append("Remote ML fetch failed; using quant-only blend.")
        else:
            notes.append("ML scores unavailable; using quant-only blend with neutral ML.")
    elif ml_meta.get("source") == "cache" and ml_meta.get("status") == "stale":
        notes.append("Using stale ML cache; remote refresh unavailable.")

    combined = combine_scores(quant_scores, ml_scores)
    combined = combined.merge(universe, on="instrument_id", how="left")
    if combined.empty:
        warnings.append("No combined scores produced; report will be empty.")

    if "asset_type" in combined.columns:
        combined["asset_type"] = combined["asset_type"].fillna("").astype(str).str.lower()

    combined = combined[combined["asset_type"] != "etf"]
    candidate_rows = int(len(combined))

    require_import = bool(config.get("require_imported_universe", True))
    min_imported = int(config.get("min_imported_universe_count", 0))
    max_price_age = int(config.get("max_price_age_days", 1))
    imported_count = int(len(imported_universe))
    data_ready = True
    if require_import and imported_count < min_imported:
        warnings.append(
            f"Imported universe too small ({imported_count}); expected at least {min_imported} instruments."
        )
        combined = combined.head(0)
        candidate_rows = 0
        data_ready = False

    confidence_gate = float(config.get("confidence_gate", 0.55))
    confidence_gate = min(max(confidence_gate, 0.0), 1.0)
    if "confidence" in combined.columns:
        eligible = combined[combined["confidence"] >= confidence_gate]
    else:
        eligible = combined
    filtered_out = int(candidate_rows - len(eligible))

    combined = eligible.sort_values("final_score", ascending=False) if not eligible.empty else eligible
    top_n = max(0, int(config.get("top_picks_count", 10)))
    top_stocks_n = max(0, int(config.get("top_stocks_count", top_n)))
    top_bonds_n = max(0, int(config.get("top_bonds_count", top_n)))

    stocks_df = _filter_assets(combined, ["stock", "equity"])
    bonds_df = _filter_assets(combined, ["bond", "fixed_income", "bond_fund"])

    top = combined.head(top_n)
    top_stocks = stocks_df.head(top_stocks_n)
    top_bonds = bonds_df.head(top_bonds_n)
    if candidate_rows > 0 and combined.empty and top_n > 0:
        warnings.append("No candidates passed the confidence gate.")

    def _build_picks(df: pd.DataFrame) -> list[dict]:
        picks: list[dict] = []
        for _, row in df.iterrows():
            risk_score = _safe_float(row.get("risk_score"), 0.0)
            momentum_score = _safe_float(row.get("momentum_score"), 0.0)
            quant_score = _safe_float(row.get("quant_score"), 0.0)
            ml_score = _safe_float(row.get("ml_score"), 0.5)
            confidence = _safe_float(row.get("confidence"), 0.0)
            final_score = _safe_float(row.get("final_score"), 0.0)
            picks.append(
                {
                    "instrument_id": row["instrument_id"],
                    "name": row.get("name", "Unknown"),
                    "asset_type": row.get("asset_type", "unknown"),
                    "score": round(final_score, 4),
                    "quant_score": round(quant_score, 4),
                    "ml_score": round(ml_score, 4),
                    "risk_score": round(risk_score, 4),
                    "momentum_score": round(momentum_score, 4),
                    "confidence": round(confidence, 4),
                    "horizon": _estimate_horizon(risk_score, momentum_score),
                    "rationale": "Balanced momentum and risk profile.",
                }
            )
        return picks

    top_picks = _build_picks(top)
    top_picks_stocks = _build_picks(top_stocks)
    top_picks_bonds = _build_picks(top_bonds)

    active_user_id = user_id or get_active_user_id()
    portfolio = portfolio_summary(load_portfolio(active_user_id))

    status = "success" if not warnings else "degraded"
    finished_at = utc_now_iso()
    run = {
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_sec": round(perf_counter() - timer, 2),
        "execution_host": execution_host,
        "ml_host": ml_host,
    }

    universe_as_of = None
    if not universe.empty and "as_of" in universe.columns:
        universe_as_of = universe["as_of"].max()

    prices_as_of = None
    if not prices.empty and "date" in prices.columns:
        prices_as_of = prices["date"].max()

    price_age_days = None
    if prices_as_of:
        try:
            prices_date = pd.to_datetime(prices_as_of).date()
            price_age_days = (datetime.now(timezone.utc).date() - prices_date).days
        except Exception:
            price_age_days = None

    if require_import and price_age_days is not None and price_age_days > max_price_age:
        warnings.append(
            f"Price data is stale ({price_age_days} days old); expected <= {max_price_age}."
        )
        combined = combined.head(0)
        candidate_rows = 0
        data_ready = False

    if require_import and not data_ready:
        if imported_count < min_imported:
            outlook = {
                "daily": "Market summary withheld until the full imported universe is available.",
                "deep": "Load the larger universe to enable the 1-3 week and 1-5 year trend summaries.",
            }
        else:
            outlook = {
                "daily": "Market summary withheld because price data is stale.",
                "deep": "Refresh daily prices from your data provider to enable trend summaries.",
            }
    else:
        outlook = {
            "daily": daily_summary(),
            "deep": deep_summary(),
        }

    inputs = {
        "universe_as_of": universe_as_of,
        "prices_as_of": prices_as_of,
        "price_age_days": price_age_days,
        "ml": ml_meta,
    }

    summary = {
        "universe_count": int(len(universe)),
        "imported_universe_count": imported_count,
        "data_ready": data_ready,
        "price_rows": int(len(prices)),
        "price_age_days": price_age_days,
        "feature_rows": int(len(features)),
        "quant_rows": int(len(quant_scores)),
        "ml_rows": int(len(ml_scores)),
        "candidate_rows": candidate_rows,
        "filtered_out": filtered_out,
        "top_picks_target": top_n,
        "top_picks_count": int(len(top_picks)),
        "top_stocks_count": int(len(top_picks_stocks)),
        "top_bonds_count": int(len(top_picks_bonds)),
        "confidence_gate": confidence_gate,
    }

    report = build_report(
        top_picks,
        outlook,
        portfolio,
        notes,
        run=run,
        summary=summary,
        inputs=inputs,
        warnings=warnings,
    )
    report["top_picks_combined"] = top_picks
    report["top_picks_stocks"] = top_picks_stocks
    report["top_picks_bonds"] = top_picks_bonds
    report["active_user_id"] = active_user_id
    notification = notify_report(report)
    if notification.get("attempted"):
        reason = notification.get("reason", "Notification attempted.")
        report["notes"].append(f"Email notification: {reason}")
    write_latest_report(report)
    return report
