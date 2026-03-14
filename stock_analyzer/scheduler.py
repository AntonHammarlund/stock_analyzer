from datetime import datetime


def should_run_daily(last_run: datetime | None, hour: int) -> bool:
    if last_run is None:
        return True
    now = datetime.now()
    if now.date() > last_run.date() and now.hour >= hour:
        return True
    return False
