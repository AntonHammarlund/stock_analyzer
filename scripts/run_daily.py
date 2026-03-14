import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analyzer.config import load_config
from stock_analyzer.pipeline import run_daily
from stock_analyzer.scheduler import (
    load_run_state,
    parse_iso_datetime,
    save_run_state,
    should_run_daily,
)
from stock_analyzer.utils import utc_now_iso


def _report_status(report: dict) -> str:
    run_info = report.get("run", {}) if isinstance(report, dict) else {}
    return run_info.get("status", "success")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the daily stock analyzer pipeline.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if the daily schedule is not due.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print the last recorded run status and exit.",
    )
    args = parser.parse_args()

    config = load_config()
    run_state = load_run_state()
    last_run = parse_iso_datetime(run_state.get("last_daily_run_at"))
    daily_hour = int(config.get("daily_run_hour", 6))
    timezone = config.get("timezone")

    if args.status:
        print(f"Last run at: {run_state.get('last_daily_run_at', 'n/a')}")
        print(f"Last status: {run_state.get('last_status', 'n/a')}")
        print(f"Last run id: {run_state.get('last_run_id', 'n/a')}")
        return

    if not args.force and not should_run_daily(last_run, daily_hour, timezone):
        print("Daily run skipped; schedule not due yet.")
        return

    try:
        report = run_daily()
    except Exception as exc:
        run_state["last_daily_run_at"] = utc_now_iso()
        run_state["last_status"] = "failed"
        run_state["last_error"] = str(exc)
        save_run_state(run_state)
        raise

    run_state["last_daily_run_at"] = report.get("generated_at", utc_now_iso())
    run_state["last_status"] = _report_status(report)
    run_state["last_run_id"] = report.get("run", {}).get("run_id")
    run_state.pop("last_error", None)
    save_run_state(run_state)

    print("Daily run completed.")
    print(f"Generated at: {report.get('generated_at')}")
    print(f"Status: {_report_status(report)}")
    print(f"Top picks: {len(report.get('top_picks', []))}")


if __name__ == "__main__":
    main()
