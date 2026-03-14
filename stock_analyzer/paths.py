from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
CONFIG_DIR = ROOT / "config"
CACHE_DB = DATA_DIR / "cache.sqlite"


def ensure_dirs() -> None:
    for path in (DATA_DIR, REPORTS_DIR, CONFIG_DIR):
        path.mkdir(parents=True, exist_ok=True)
