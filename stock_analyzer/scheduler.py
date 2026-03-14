from datetime import datetime
from zoneinfo import ZoneInfo

from .paths import DATA_DIR
from .utils import read_json, write_json

RUN_STATE_FILE = DATA_DIR / "run_state.json"


def load_run_state() -> dict:
    return read_json(RUN_STATE_FILE) or {}


def save_run_state(state: dict) -> None:
    write_json(RUN_STATE_FILE, state)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _now_in_timezone(timezone: str | None) -> datetime:
    if timezone:
        try:
            return datetime.now(ZoneInfo(timezone))
        except Exception:
            return datetime.now()
    return datetime.now()


def should_run_daily(last_run: datetime | None, hour: int, timezone: str | None = None) -> bool:
    now = _now_in_timezone(timezone)
    if last_run is None:
        return now.hour >= hour

    if last_run.tzinfo is None and now.tzinfo is not None:
        last_run = last_run.replace(tzinfo=now.tzinfo)
    elif last_run.tzinfo is not None and now.tzinfo is not None:
        last_run = last_run.astimezone(now.tzinfo)

    if now.date() > last_run.date() and now.hour >= hour:
        return True
    return False
