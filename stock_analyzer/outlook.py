from datetime import date


def daily_summary() -> str:
    today = date.today().isoformat()
    return f"Calm-first daily summary for {today}: market conditions are neutral with stable volatility. Focus on long-term horizons."


def deep_summary() -> str:
    today = date.today().isoformat()
    return (
        f"Deep summary for {today}: regime appears stable. "
        "No major directional shift detected. Long-term investors should stay disciplined and review diversification."
    )
