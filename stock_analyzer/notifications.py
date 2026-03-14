from typing import Dict

from .paths import CONFIG_DIR
from .utils import read_json

EMAIL_FILE = CONFIG_DIR / "email.json"


def send_email(subject: str, body: str) -> Dict:
    config = read_json(EMAIL_FILE)
    if not config.get("enabled", False):
        return {"sent": False, "reason": "Email disabled"}

    # Placeholder: real SMTP integration will be added when credentials are provided.
    return {"sent": False, "reason": "Email configured but SMTP not implemented"}
