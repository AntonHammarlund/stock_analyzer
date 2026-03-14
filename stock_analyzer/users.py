from __future__ import annotations

from pathlib import Path
from typing import Dict, List
from uuid import uuid4

from .paths import DATA_DIR
from .utils import read_json, write_json, utc_now_iso

USERS_FILE = DATA_DIR / "users.json"


def _ensure_users_file() -> Dict:
    if USERS_FILE.exists():
        payload = read_json(USERS_FILE)
        if payload:
            return payload

    default_user = {
        "id": "default",
        "name": "Default",
        "created_at": utc_now_iso(),
    }
    payload = {"active_user_id": "default", "users": [default_user]}
    write_json(USERS_FILE, payload)
    return payload


def list_users() -> List[Dict]:
    payload = _ensure_users_file()
    users = payload.get("users", [])
    return [user for user in users if isinstance(user, dict)]


def get_active_user_id() -> str:
    payload = _ensure_users_file()
    active = payload.get("active_user_id") or "default"
    return str(active)


def set_active_user_id(user_id: str) -> None:
    payload = _ensure_users_file()
    users = payload.get("users", [])
    if not any(user.get("id") == user_id for user in users):
        return
    payload["active_user_id"] = user_id
    write_json(USERS_FILE, payload)


def add_user(name: str) -> Dict:
    payload = _ensure_users_file()
    users = payload.get("users", [])

    clean_name = name.strip()
    if not clean_name:
        return {}

    for user in users:
        if user.get("name", "").strip().lower() == clean_name.lower():
            return user

    user_id = clean_name.lower().replace(" ", "_")
    if any(user.get("id") == user_id for user in users):
        user_id = f"{user_id}_{uuid4().hex[:6]}"

    new_user = {"id": user_id, "name": clean_name, "created_at": utc_now_iso()}
    users.append(new_user)
    payload["users"] = users
    payload["active_user_id"] = user_id
    write_json(USERS_FILE, payload)
    return new_user
