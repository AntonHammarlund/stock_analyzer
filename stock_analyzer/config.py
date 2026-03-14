import json
from pathlib import Path
from typing import Any, Dict

from .paths import CONFIG_DIR

DEFAULTS_FILE = CONFIG_DIR / "defaults.json"


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_config() -> Dict[str, Any]:
    config = load_json(DEFAULTS_FILE)
    return config
