import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analyzer.data_sources.universe_import import OUTPUT_COLUMNS, IMPORT_FILE


def main() -> None:
    parser = argparse.ArgumentParser(description="Import a larger universe CSV.")
    parser.add_argument("--source", required=True, help="Path to the source CSV file.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite the existing import file.")
    args = parser.parse_args()

    source_path = Path(args.source)
    if not source_path.exists():
        raise SystemExit(f"Source file not found: {source_path}")

    df = pd.read_csv(source_path, dtype=str)
    if df.empty:
        raise SystemExit("Source file is empty.")

    for column in OUTPUT_COLUMNS:
        if column not in df.columns:
            df[column] = None

    if "instrument_id" not in df.columns and "isin" not in df.columns:
        raise SystemExit("CSV must include instrument_id or isin.")

    df["instrument_id"] = df["instrument_id"].fillna(df.get("isin"))
    df = df.dropna(subset=["instrument_id"]).drop_duplicates(subset=["instrument_id"], keep="last")

    IMPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if IMPORT_FILE.exists() and not args.overwrite:
        existing = pd.read_csv(IMPORT_FILE, dtype=str)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(
            subset=["instrument_id"], keep="last"
        )

    df.to_csv(IMPORT_FILE, index=False)
    print(f"Imported {len(df)} instruments into {IMPORT_FILE}")


if __name__ == "__main__":
    main()
