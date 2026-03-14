from typing import Dict, Iterable

from .utils import read_json
from .paths import CONFIG_DIR

HOSTS_FILE = CONFIG_DIR / "hosts.json"


def load_hosts_config() -> Dict:
    config = read_json(HOSTS_FILE)
    if not config:
        config = {}
    config.setdefault("fallback_mode", "automatic")
    config.setdefault("hosts", [])
    return config


def _normalize_host(host: Dict) -> Dict:
    normalized = dict(host or {})
    normalized.setdefault("id", "unknown")
    normalized.setdefault("type", "unknown")
    normalized.setdefault("enabled", False)
    normalized.setdefault("priority", 999)
    return normalized


def _sorted_hosts(hosts: Iterable[Dict]) -> list[Dict]:
    return sorted((_normalize_host(host) for host in hosts), key=lambda h: h.get("priority", 999))


def select_host(host_type: str | None = None, require_endpoint: bool = False) -> Dict:
    config = load_hosts_config()
    fallback_mode = config.get("fallback_mode", "automatic")
    hosts = _sorted_hosts(config.get("hosts", []))
    enabled_hosts = [host for host in hosts if host.get("enabled")]

    selected = None
    if host_type:
        for host in enabled_hosts:
            if host.get("type") == host_type:
                if require_endpoint and not host.get("endpoint"):
                    continue
                selected = host
                break
    else:
        if enabled_hosts:
            selected = enabled_hosts[0]

    if selected:
        selected = dict(selected)
        selected["selected_for"] = host_type or "any"
        selected["fallback_used"] = False
        return selected

    reason = "No enabled hosts."
    if host_type:
        reason = f"No enabled host of type '{host_type}'."
    if host_type and require_endpoint:
        reason = f"No enabled host of type '{host_type}' with endpoint configured."

    if fallback_mode == "automatic" and enabled_hosts:
        fallback = dict(enabled_hosts[0])
        fallback["selected_for"] = host_type or "any"
        fallback["fallback_used"] = True
        fallback["fallback_reason"] = reason
        return fallback

    return {
        "id": "none",
        "type": host_type or "none",
        "enabled": False,
        "selected_for": host_type or "any",
        "fallback_used": False,
        "fallback_reason": reason,
    }


def summarize_host(host: Dict) -> Dict:
    return {
        "id": host.get("id", "none"),
        "type": host.get("type", "none"),
        "enabled": bool(host.get("enabled", False)),
        "selected_for": host.get("selected_for", "any"),
        "fallback_used": bool(host.get("fallback_used", False)),
        "fallback_reason": host.get("fallback_reason"),
        "endpoint_configured": bool(host.get("endpoint")),
    }
