from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from ..paths import DATA_DIR
from ..utils import read_json, write_json, utc_now_iso
from ..host_manager import select_host, summarize_host

ML_CACHE = DATA_DIR / "ml_scores.json"
CACHE_TTL_HOURS = 24
DEFAULT_ML_SCORE = 0.5
DEFAULT_ML_CONFIDENCE = 0.5
REQUEST_TIMEOUT = 15
SCHEMA_VERSION = 1


def _parse_iso(timestamp: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except Exception:
        return None


def _is_cache_fresh(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    fetched_at = payload.get("fetched_at")
    if not isinstance(fetched_at, str):
        return False
    parsed = _parse_iso(fetched_at)
    if not parsed:
        return False
    return datetime.now(timezone.utc) - parsed <= timedelta(hours=CACHE_TTL_HOURS)


def _parse_scores(payload: Any) -> List[Dict[str, Any]]:
    if not payload:
        return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        scores = payload.get("scores")
        if isinstance(scores, list):
            return [row for row in scores if isinstance(row, dict)]
    return []


def _coerce_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clip_unit(value: float, default: float) -> float:
    if value != value:
        return default
    return min(1.0, max(0.0, value))


def _resolve_ml_host() -> Dict[str, Any]:
    return select_host("remote_ml", require_endpoint=False)


def _get_remote_endpoint(host: Dict[str, Any] | None = None) -> str:
    host = host or _resolve_ml_host()
    if host.get("type") != "remote_ml":
        return ""
    return host.get("endpoint", "")


def _normalize_payload(raw: Any, endpoint: str) -> Dict[str, Any]:
    normalized_scores: List[Dict[str, Any]] = []
    for row in _parse_scores(raw):
        instrument_id = row.get("instrument_id")
        if not instrument_id:
            continue
        ml_score = _clip_unit(
            _coerce_float(row.get("ml_score", row.get("score")), DEFAULT_ML_SCORE),
            DEFAULT_ML_SCORE,
        )
        ml_confidence = _clip_unit(
            _coerce_float(row.get("ml_confidence", row.get("confidence")), DEFAULT_ML_CONFIDENCE),
            DEFAULT_ML_CONFIDENCE,
        )
        normalized_scores.append(
            {"instrument_id": instrument_id, "ml_score": ml_score, "ml_confidence": ml_confidence}
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "fetched_at": utc_now_iso(),
        "source": endpoint,
        "scores": normalized_scores,
    }


def _fetch_remote_ml_payload() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    host = _resolve_ml_host()
    meta: Dict[str, Any] = {
        "source": "remote",
        "status": "ok",
        "host": summarize_host(host),
    }
    endpoint = _get_remote_endpoint(host)
    if not endpoint:
        meta["status"] = "no-endpoint"
        return {}, meta
    try:
        response = requests.get(endpoint, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            meta["status"] = f"http-{response.status_code}"
            return {}, meta
        raw = response.json()
    except Exception:
        meta["status"] = "error"
        return {}, meta

    payload = _normalize_payload(raw, endpoint)
    write_json(ML_CACHE, payload)
    return payload, meta


def fetch_remote_ml_scores() -> Dict[str, Any]:
    payload, _ = _fetch_remote_ml_payload()
    return payload


def load_ml_scores_with_meta() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cached_payload: Any = read_json(ML_CACHE) if ML_CACHE.exists() else {}
    if _is_cache_fresh(cached_payload):
        df = _payload_to_df(cached_payload)
        meta = {"source": "cache", "status": "fresh", "count": len(df)}
        return df, meta

    remote_payload, remote_meta = _fetch_remote_ml_payload()
    if remote_payload:
        df = _payload_to_df(remote_payload)
        remote_meta = dict(remote_meta)
        remote_meta["count"] = len(df)
        return df, remote_meta

    if cached_payload:
        df = _payload_to_df(cached_payload)
        meta = {"source": "cache", "status": "stale", "count": len(df)}
        return df, meta

    meta = dict(remote_meta)
    meta["count"] = 0
    return _payload_to_df({}), meta


def load_ml_scores() -> pd.DataFrame:
    df, _ = load_ml_scores_with_meta()
    return df


def _payload_to_df(payload: Any) -> pd.DataFrame:
    scores = _parse_scores(payload)
    if not scores:
        return pd.DataFrame(columns=["instrument_id", "ml_score", "ml_confidence"])

    df = pd.DataFrame(scores)
    if "instrument_id" not in df.columns:
        return pd.DataFrame(columns=["instrument_id", "ml_score", "ml_confidence"])

    df = df[df["instrument_id"].notna()].copy()
    if "ml_score" not in df.columns:
        df["ml_score"] = DEFAULT_ML_SCORE
    df["ml_score"] = pd.to_numeric(df["ml_score"], errors="coerce").fillna(DEFAULT_ML_SCORE).clip(0, 1)

    if "ml_confidence" not in df.columns:
        df["ml_confidence"] = DEFAULT_ML_CONFIDENCE
    df["ml_confidence"] = (
        pd.to_numeric(df["ml_confidence"], errors="coerce").fillna(DEFAULT_ML_CONFIDENCE).clip(0, 1)
    )

    return df[["instrument_id", "ml_score", "ml_confidence"]]
