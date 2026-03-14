import argparse
import json
import random
import sys
from pathlib import Path
from typing import List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analyzer.paths import DATA_DIR, CONFIG_DIR
from stock_analyzer.utils import utc_now_iso


def _load_instruments() -> List[str]:
    manual_file = CONFIG_DIR / "universe_manual.csv"
    if manual_file.exists():
        df = pd.read_csv(manual_file, dtype=str)
        ids = df.get("instrument_id")
        if ids is not None:
            return [value for value in ids.dropna().astype(str).tolist() if value]
    availability_file = CONFIG_DIR / "avanza_availability.csv"
    if availability_file.exists():
        df = pd.read_csv(availability_file, dtype=str)
        ids = df.get("instrument_id")
        if ids is not None:
            return [value for value in ids.dropna().astype(str).tolist() if value]
    return ["SE0000000000", "SE0000000001", "US0000000001"]


def build_stub_scores(instrument_ids: List[str]) -> dict:
    random.seed(42)
    scores = []
    for instrument_id in instrument_ids:
        ml_score = round(random.uniform(0.35, 0.85), 4)
        ml_confidence = round(random.uniform(0.4, 0.9), 4)
        scores.append(
            {
                "instrument_id": instrument_id,
                "ml_score": ml_score,
                "ml_confidence": ml_confidence,
            }
        )
    return {
        "schema_version": 1,
        "fetched_at": utc_now_iso(),
        "source": "local_stub",
        "scores": scores,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate stub ML scores.")
    parser.add_argument(
        "--output",
        type=str,
        default=str(DATA_DIR / "ml_scores.json"),
        help="Output path for ML scores JSON.",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    instrument_ids = _load_instruments()
    payload = build_stub_scores(instrument_ids)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)

    print(f"Wrote {len(payload['scores'])} ML scores to {output_path}")


if __name__ == "__main__":
    main()
