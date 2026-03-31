import json
from pathlib import Path
from typing import Any, Dict

from .paths import CONFIG_DIR
from .utils import write_json

DEFAULTS_FILE = CONFIG_DIR / "defaults.json"
OPTIMIZED_FILE = CONFIG_DIR / "optimized.json"


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_config() -> Dict[str, Any]:
    config = load_json(DEFAULTS_FILE)
    overrides = load_json(OPTIMIZED_FILE)
    if overrides:
        config.update(overrides)
    return config


def write_optimized_config(overrides: Dict[str, Any]) -> None:
    write_json(OPTIMIZED_FILE, overrides)
