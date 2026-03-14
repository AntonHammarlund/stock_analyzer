from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .paths import DATA_DIR
from .utils import read_json, write_json
from .users import get_active_user_id

PORTFOLIO_DIR = DATA_DIR / "portfolios"


def _portfolio_path(user_id: str) -> Path:
    PORTFOLIO_DIR.mkdir(parents=True, exist_ok=True)
    return PORTFOLIO_DIR / f"{user_id}.json"


def load_portfolio(user_id: Optional[str] = None) -> List[Dict]:
    user_id = user_id or get_active_user_id()
    payload = read_json(_portfolio_path(user_id))
    return payload.get("holdings", [])


def save_portfolio(holdings: List[Dict], user_id: Optional[str] = None) -> None:
    user_id = user_id or get_active_user_id()
    write_json(_portfolio_path(user_id), {"holdings": holdings})


def portfolio_summary(holdings: List[Dict]) -> Dict:
    if not holdings:
        return {
            "risk": "No holdings added yet.",
            "positives": "Add holdings to see diversification and positives.",
            "concentration": 0.0,
        }

    df = pd.DataFrame(holdings)
    total = df.get("weight", pd.Series([1] * len(df))).sum()
    if total == 0:
        total = 1
    weights = df.get("weight", pd.Series([1] * len(df))) / total
    concentration = float((weights**2).sum())

    risk_text = "Moderate concentration" if concentration > 0.25 else "Diversified"
    positives_text = "Diversified exposure" if concentration < 0.25 else "Focused bets can outperform if thesis holds"

    return {
        "risk": risk_text,
        "positives": positives_text,
        "concentration": round(concentration, 3),
    }
