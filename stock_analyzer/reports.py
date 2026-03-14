from pathlib import Path
from typing import Dict, List

from .paths import REPORTS_DIR
from .utils import read_json, write_json, utc_now_iso

LATEST_REPORT = REPORTS_DIR / "latest_report.json"
REPORT_VERSION = 2


def write_latest_report(payload: Dict) -> None:
    write_json(LATEST_REPORT, payload)


def load_latest_report() -> Dict:
    if not LATEST_REPORT.exists():
        return {}
    return read_json(LATEST_REPORT)


def build_report(
    top_picks: List[Dict],
    outlook: Dict,
    portfolio: Dict,
    notes: List[str],
    *,
    run: Dict | None = None,
    summary: Dict | None = None,
    inputs: Dict | None = None,
    warnings: List[str] | None = None,
) -> Dict:
    payload = {
        "report_version": REPORT_VERSION,
        "generated_at": utc_now_iso(),
        "top_picks": top_picks,
        "outlook": outlook,
        "portfolio": portfolio,
        "notes": notes,
        "run": run or {},
        "summary": summary or {},
        "inputs": inputs or {},
        "warnings": warnings or [],
    }
    return payload
