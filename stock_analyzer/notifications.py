from typing import Dict, List, Optional
import os
import smtplib
from email.message import EmailMessage

from .paths import CONFIG_DIR
from .utils import read_json

EMAIL_FILE = CONFIG_DIR / "email.json"

DEFAULT_EMAIL_CONFIG = {
    "enabled": False,
    "send_on_run": True,
    "subject_prefix": "[Stock Analyzer]",
    "from_name": "Stock Analyzer",
    "from_email": "",
    "to_emails": [],
    "max_top_picks": 5,
    "include_outlook": True,
    "include_portfolio": True,
    "include_notes": True,
    "provider": "smtp",
    "smtp_host": "",
    "smtp_port": 587,
    "smtp_starttls": True,
    "smtp_username": "",
    "smtp_password_env": "STOCK_ANALYZER_SMTP_PASSWORD",
}


def _normalize_recipients(raw: object) -> List[str]:
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [str(value).strip() for value in raw if str(value).strip()]


def load_email_config() -> Dict:
    config = DEFAULT_EMAIL_CONFIG.copy()
    config.update(read_json(EMAIL_FILE))
    config["to_emails"] = _normalize_recipients(config.get("to_emails", []))
    config["max_top_picks"] = int(config.get("max_top_picks", 5) or 0)
    config["smtp_port"] = int(config.get("smtp_port", 587) or 0)
    config["smtp_starttls"] = bool(config.get("smtp_starttls", True))
    return config


def _format_top_picks(top_picks: List[Dict], max_items: int) -> List[str]:
    if not top_picks:
        return ["Top picks: none"]
    lines = [f"Top picks ({len(top_picks)}):"]
    for row in top_picks[:max_items]:
        name = row.get("name", "Unknown")
        score = row.get("score", "n/a")
        horizon = row.get("horizon", "n/a")
        lines.append(f"- {name} (score {score}, horizon {horizon})")
    return lines


def _format_outlook(outlook: Dict) -> List[str]:
    if not outlook:
        return ["Outlook: none"]
    return [
        "Outlook:",
        f"- Daily: {outlook.get('daily', 'n/a')}",
        f"- Deep: {outlook.get('deep', 'n/a')}",
    ]


def _format_portfolio(portfolio: Dict) -> List[str]:
    if not portfolio:
        return ["Portfolio: none"]
    risk = portfolio.get("risk", "n/a")
    positives = portfolio.get("positives", "n/a")
    concentration = portfolio.get("concentration", "n/a")
    return [
        "Portfolio:",
        f"- Risk: {risk}",
        f"- Positives: {positives}",
        f"- Concentration: {concentration}",
    ]


def build_report_email(report: Dict, config: Dict) -> Dict[str, str]:
    generated_at = report.get("generated_at", "unknown")
    date_stamp = generated_at.split("T")[0] if generated_at else "unknown date"
    subject_prefix = config.get("subject_prefix", "[Stock Analyzer]").strip()
    subject = f"{subject_prefix} Daily report ({date_stamp})".strip()

    lines = [
        "Stock Analyzer daily report",
        f"Generated at: {generated_at}",
        "",
    ]

    max_top_picks = max(0, int(config.get("max_top_picks", 5)))
    lines.extend(_format_top_picks(report.get("top_picks", []), max_top_picks))

    if config.get("include_outlook", True):
        lines.append("")
        lines.extend(_format_outlook(report.get("outlook", {})))

    if config.get("include_portfolio", True):
        lines.append("")
        lines.extend(_format_portfolio(report.get("portfolio", {})))

    if config.get("include_notes", True):
        notes = report.get("notes", [])
        if notes:
            lines.append("")
            lines.append("Notes:")
            lines.extend([f"- {note}" for note in notes])

    return {"subject": subject, "body": "\n".join(lines)}


def send_email(subject: str, body: str, config: Optional[Dict] = None) -> Dict:
    config = config or load_email_config()
    if not config.get("enabled", False):
        return {"attempted": False, "sent": False, "reason": "Email disabled"}
    if not config.get("to_emails"):
        return {"attempted": True, "sent": False, "reason": "No recipient emails configured"}
    if not config.get("from_email"):
        return {"attempted": True, "sent": False, "reason": "No sender email configured"}
    if config.get("provider") != "smtp":
        return {"attempted": True, "sent": False, "reason": "Unsupported provider"}

    password_env = config.get("smtp_password_env", "STOCK_ANALYZER_SMTP_PASSWORD")
    password = config.get("smtp_password") or os.getenv(password_env, "")
    smtp_host = config.get("smtp_host", "")
    smtp_port = int(config.get("smtp_port", 587) or 0)
    smtp_user = config.get("smtp_username") or config.get("from_email")

    if not smtp_host:
        return {"attempted": True, "sent": False, "reason": "SMTP host not configured"}
    if not password:
        return {
            "attempted": True,
            "sent": False,
            "reason": f"Missing SMTP password (env {password_env})",
        }

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = f"{config.get('from_name', '').strip()} <{config.get('from_email')}>".strip()
    message["To"] = ", ".join(config.get("to_emails", []))
    message.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            server.ehlo()
            if config.get("smtp_starttls", True):
                server.starttls()
                server.ehlo()
            server.login(smtp_user, password)
            server.send_message(message)
        return {
            "attempted": True,
            "sent": True,
            "reason": "Email sent",
            "subject": subject,
            "recipients": config.get("to_emails", []),
        }
    except Exception as exc:
        return {
            "attempted": True,
            "sent": False,
            "reason": f"SMTP error: {exc}",
            "subject": subject,
            "recipients": config.get("to_emails", []),
        }


def notify_report(report: Dict) -> Dict:
    config = load_email_config()
    if not config.get("send_on_run", True):
        return {"attempted": False, "sent": False, "reason": "Email notifications disabled for daily run"}
    payload = build_report_email(report, config)
    return send_email(payload["subject"], payload["body"], config=config)
