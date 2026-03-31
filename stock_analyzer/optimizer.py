import itertools
import random
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

from .config import load_config, write_optimized_config
from .data_sources.price_data import load_prices
from .models.ml_proxy import load_ml_scores_with_meta
from .models.quant import score_quant
from .paths import REPORTS_DIR
from .reasoning import build_reasoning_summary
from .scoring import combine_scores
from .universe import attach_universe_metadata, build_universe
from .utils import utc_now_iso, write_json


def _candidate_values(base: float, deltas: list[float], min_value: float, max_value: float) -> list[float]:
    values = {min(max_value, max(min_value, base + delta)) for delta in deltas}
    return sorted(values)


def _extract_tunable_params(config: Dict[str, Any]) -> Dict[str, float]:
    return {
        "momentum_weight_base": float(config.get("momentum_weight_base", 0.55)),
        "momentum_weight_risk_scale": float(config.get("momentum_weight_risk_scale", 0.15)),
        "ml_weight_base": float(config.get("ml_weight_base", 0.1)),
        "ml_weight_scale": float(config.get("ml_weight_scale", 0.4)),
        "expected_return_scale": float(config.get("expected_return_scale", 0.2)),
    }


def _build_candidate_grid(params: Dict[str, float]) -> list[Dict[str, float]]:
    grid = {
        "momentum_weight_base": _candidate_values(params["momentum_weight_base"], [-0.1, 0.0, 0.1], 0.1, 0.9),
        "momentum_weight_risk_scale": _candidate_values(
            params["momentum_weight_risk_scale"], [-0.05, 0.0, 0.05], 0.0, 0.6
        ),
        "ml_weight_base": _candidate_values(params["ml_weight_base"], [-0.05, 0.0, 0.05], 0.0, 0.5),
        "ml_weight_scale": _candidate_values(params["ml_weight_scale"], [-0.1, 0.0, 0.1], 0.0, 0.9),
        "expected_return_scale": _candidate_values(params["expected_return_scale"], [-0.05, 0.0, 0.05], 0.05, 0.6),
    }
    keys = list(grid.keys())
    values = [grid[key] for key in keys]
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def _prepare_prices(prices: pd.DataFrame, max_instruments: int) -> pd.DataFrame:
    if prices.empty:
        return prices
    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["instrument_id", "date", "close"])
    df.sort_values(["instrument_id", "date"], inplace=True)

    instrument_ids = df["instrument_id"].astype(str).unique().tolist()
    if max_instruments > 0 and len(instrument_ids) > max_instruments:
        random.seed(42)
        chosen = set(random.sample(instrument_ids, max_instruments))
        df = df[df["instrument_id"].astype(str).isin(chosen)]
    return df


def build_training_frame(prices: pd.DataFrame, ml_scores: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()

    horizon_days = int(config.get("optimization_horizon_days", config.get("projection_days", 90)))
    lookback_days = int(config.get("optimization_lookback_days", 365))
    max_instruments = int(config.get("optimization_max_instruments", 250))

    df = _prepare_prices(prices, max_instruments)
    if df.empty:
        return pd.DataFrame()

    def _compute(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        group["return_1d"] = group["close"].pct_change()
        group["momentum_20d"] = group["close"].pct_change(20)
        group["vol_20d"] = group["return_1d"].rolling(20).std() * np.sqrt(252)
        group["forward_return"] = group["close"].shift(-horizon_days) / group["close"] - 1
        return group

    df = df.groupby("instrument_id", group_keys=False).apply(_compute)

    if lookback_days:
        max_date = df["date"].max()
        if pd.notna(max_date):
            cutoff = max_date - pd.Timedelta(days=lookback_days)
            df = df[df["date"] >= cutoff]

    df = df.dropna(subset=["momentum_20d", "vol_20d", "forward_return"])
    if df.empty:
        return pd.DataFrame()

    ml = ml_scores.copy()
    if not ml.empty:
        if "ml_score" not in ml.columns:
            ml["ml_score"] = 0.5
        if "ml_confidence" not in ml.columns:
            ml["ml_confidence"] = 0.0
        ml = ml[["instrument_id", "ml_score", "ml_confidence"]].drop_duplicates(subset=["instrument_id"])
    else:
        ml = pd.DataFrame(columns=["instrument_id", "ml_score", "ml_confidence"])

    df = df.merge(ml, on="instrument_id", how="left")
    df["ml_score"] = pd.to_numeric(df.get("ml_score"), errors="coerce").fillna(0.5).clip(0, 1)
    df["ml_confidence"] = pd.to_numeric(df.get("ml_confidence"), errors="coerce").fillna(0.0).clip(0, 1)

    return df[
        ["date", "instrument_id", "momentum_20d", "vol_20d", "forward_return", "ml_score", "ml_confidence"]
    ]


def _safe_corr(series_a: pd.Series, series_b: pd.Series) -> float | None:
    if series_a.nunique(dropna=True) <= 1 or series_b.nunique(dropna=True) <= 1:
        return None
    value = series_a.corr(series_b, method="spearman")
    return float(value) if pd.notna(value) else None


def evaluate_params(frame: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    if frame.empty:
        return {"status": "no-data"}

    ml = frame[["instrument_id", "ml_score", "ml_confidence"]].drop_duplicates(subset=["instrument_id"])
    quant = score_quant(frame, config=config, group_by="date")
    combined = combine_scores(quant, ml, config=config, keep_columns=["date"])
    combined = combined.merge(
        frame[["instrument_id", "date", "forward_return", "ml_confidence"]],
        on=["instrument_id", "date"],
        how="left",
    )
    combined = combined.dropna(subset=["forward_return"])
    if combined.empty:
        return {"status": "no-data"}

    expected_scale = float(config.get("expected_return_scale", 0.2))
    combined["expected_change"] = (combined["final_score"] - 0.5) * 2 * expected_scale
    combined["error"] = combined["expected_change"] - combined["forward_return"]

    mse = float((combined["error"] ** 2).mean())
    mae = float(combined["error"].abs().mean())
    spearman = _safe_corr(combined["final_score"], combined["forward_return"])

    feature_corr = {
        "momentum_score": _safe_corr(combined["momentum_score"], combined["forward_return"]),
        "risk_score": _safe_corr(combined["risk_score"], combined["forward_return"]),
        "ml_score": _safe_corr(combined["ml_score"], combined["forward_return"]),
        "ml_confidence": _safe_corr(combined["ml_confidence"], combined["forward_return"]),
    }

    return {
        "status": "ok",
        "row_count": int(len(combined)),
        "mse": mse,
        "mae": mae,
        "spearman": spearman,
        "expected_return_scale": expected_scale,
        "feature_corr": feature_corr,
    }


def optimize_parameters(frame: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    baseline_params = _extract_tunable_params(config)
    baseline = evaluate_params(frame, config)
    if baseline.get("status") != "ok":
        return {
            "status": baseline.get("status", "no-data"),
            "baseline": baseline,
            "best": baseline,
            "params_before": baseline_params,
            "params_after": baseline_params,
            "applied": False,
            "grid_size": 0,
        }

    candidates = _build_candidate_grid(baseline_params)
    best_params = dict(baseline_params)
    best_metrics = baseline

    for candidate in candidates:
        candidate_config = dict(config)
        candidate_config.update(candidate)
        metrics = evaluate_params(frame, candidate_config)
        if metrics.get("status") != "ok":
            continue
        if metrics["mse"] < best_metrics["mse"]:
            best_metrics = metrics
            best_params = candidate
        elif metrics["mse"] == best_metrics["mse"]:
            if (metrics.get("spearman") or 0) > (best_metrics.get("spearman") or 0):
                best_metrics = metrics
                best_params = candidate

    min_improvement = float(config.get("optimization_min_improvement", 0.01))
    baseline_mse = baseline["mse"]
    best_mse = best_metrics["mse"]
    improvement = (baseline_mse - best_mse) / baseline_mse if baseline_mse else 0.0
    applied = improvement >= min_improvement

    return {
        "status": "ok",
        "baseline": baseline,
        "best": best_metrics,
        "params_before": baseline_params,
        "params_after": best_params if applied else baseline_params,
        "improvement_ratio": improvement,
        "applied": applied,
        "grid_size": len(candidates),
    }


def run_quarterly_rework() -> Dict[str, Any]:
    config = load_config()
    universe = attach_universe_metadata(build_universe())
    prices = load_prices(universe)
    ml_scores, ml_meta = load_ml_scores_with_meta()

    frame = build_training_frame(prices, ml_scores, config)
    data_meta = {
        "horizon_days": int(config.get("optimization_horizon_days", config.get("projection_days", 90))),
        "lookback_days": int(config.get("optimization_lookback_days", 365)),
        "max_instruments": int(config.get("optimization_max_instruments", 250)),
        "frame_rows": int(len(frame)),
    }
    optimization = optimize_parameters(frame, config)

    reasoning_summary = build_reasoning_summary(
        optimization.get("baseline", {}),
        optimization.get("best", {}),
        optimization.get("params_before", {}),
        optimization.get("params_after", {}),
        optimization.get("applied", False),
        config=config,
    )

    if optimization.get("applied"):
        write_optimized_config(optimization.get("params_after", {}))

    report = {
        "generated_at": utc_now_iso(),
        "status": optimization.get("status", "no-data"),
        "data": data_meta,
        "optimization": optimization,
        "ml_meta": ml_meta,
        "reasoning": {"summary": reasoning_summary},
    }

    report_path = REPORTS_DIR / "quarterly_rework.json"
    write_json(report_path, report)
    report["report_path"] = str(report_path)
    return report
