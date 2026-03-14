from typing import Dict

from .utils import read_json
from .paths import CONFIG_DIR

HOSTS_FILE = CONFIG_DIR / "hosts.json"


def select_host() -> Dict:
    config = read_json(HOSTS_FILE)
    hosts = sorted(config.get("hosts", []), key=lambda h: h.get("priority", 999))
    for host in hosts:
        if host.get("enabled"):
            return host
    return {"id": "none", "type": "none", "enabled": False}
