import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analyzer.config import load_config
from stock_analyzer.host_manager import select_host
from stock_analyzer.optimizer import run_quarterly_rework as run_quarterly_optimizer
from stock_analyzer.scheduler import load_run_state, save_run_state
from stock_analyzer.utils import utc_now_iso

QUARTER_START_MONTHS = (1, 4, 7, 10)


def _now_in_timezone(timezone: str | None) -> datetime:
    if timezone:
        try:
            return datetime.now(ZoneInfo(timezone))
        except Exception:
            return datetime.now()
    return datetime.now()


def _first_sunday(year: int, month: int) -> date:
    first = date(year, month, 1)
    offset = (6 - first.weekday()) % 7
    return first + timedelta(days=offset)


def _is_first_sunday_of_quarter(today: date) -> bool:
    if today.month not in QUARTER_START_MONTHS:
        return False
    return today == _first_sunday(today.year, today.month)


def _run_step(args: list[str], label: str) -> None:
    print(f"== {label} ==")
    subprocess.run([sys.executable, *args], cwd=ROOT, check=True)


def _refresh_ml_scores() -> str:
    host = select_host("remote_ml", require_endpoint=False)
    endpoint = host.get("endpoint") if host.get("enabled") else ""
    if endpoint:
        from stock_analyzer.models.ml_proxy import fetch_remote_ml_scores

        payload = fetch_remote_ml_scores()
        score_count = len(payload.get("scores", [])) if isinstance(payload, dict) else 0
        if score_count:
            print(f"Refreshed remote ML scores: {score_count} rows.")
        else:
            print("Remote ML refresh returned no scores; keeping existing cache.")
        return "remote"

    _run_step([str(Path("scripts") / "run_ml_stub.py")], "Generate ML stub scores")
    return "stub"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the quarterly rework tasks.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if today is not the first Sunday of the quarter.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print the last recorded quarterly run status and exit.",
    )
    args = parser.parse_args()

    config = load_config()
    timezone = config.get("timezone")
    run_state = load_run_state()

    if args.status:
        print(f"Last quarterly run at: {run_state.get('last_quarterly_run_at', 'n/a')}")
        print(f"Last quarterly status: {run_state.get('last_quarterly_status', 'n/a')}")
        print(f"Last quarterly run id: {run_state.get('last_quarterly_run_id', 'n/a')}")
        print(f"Last ML source: {run_state.get('last_quarterly_ml_source', 'n/a')}")
        print(f"Last quarterly report: {run_state.get('last_quarterly_report_path', 'n/a')}")
        return

    now = _now_in_timezone(timezone)
    if not args.force and not _is_first_sunday_of_quarter(now.date()):
        print("Quarterly rework skipped; schedule not due yet.")
        print(f"Today: {now.date().isoformat()} ({timezone or 'local'})")
        return

    run_id = uuid4().hex
    report: dict = {}
    try:
        _run_step([str(Path("scripts") / "sync_data.py")], "Sync data")
        ml_source = _refresh_ml_scores()
        report = run_quarterly_optimizer()
        print(f"Optimization status: {report.get('status', 'n/a')}")
        applied = report.get("optimization", {}).get("applied")
        if applied is not None:
            print(f"Optimization applied: {applied}")
        reasoning_summary = report.get("reasoning", {}).get("summary")
        if reasoning_summary:
            print(f"Reasoning: {reasoning_summary}")
        if report.get("report_path"):
            print(f"Quarterly report: {report.get('report_path')}")
        _run_step([str(Path("scripts") / "run_daily.py"), "--force"], "Run daily pipeline")
    except Exception as exc:
        run_state["last_quarterly_run_at"] = utc_now_iso()
        run_state["last_quarterly_status"] = "failed"
        run_state["last_quarterly_error"] = str(exc)
        run_state["last_quarterly_run_id"] = run_id
        save_run_state(run_state)
        raise

    run_state["last_quarterly_run_at"] = utc_now_iso()
    run_state["last_quarterly_status"] = report.get("status", "success")
    run_state["last_quarterly_run_id"] = run_id
    run_state["last_quarterly_ml_source"] = ml_source
    run_state["last_quarterly_report_path"] = report.get("report_path")
    run_state.pop("last_quarterly_error", None)
    save_run_state(run_state)

    print("Quarterly rework completed.")
    print(f"Run id: {run_id}")
    print(f"ML source: {ml_source}")


if __name__ == "__main__":
    main()
