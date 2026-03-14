# Stock Analyzer MVP

Local-first, calm-first stock analyzer for the Swedish market and global top-100 by market cap. This MVP scaffolds the pipeline, UI, and daily workflow. It is designed to be free to run, low maintenance, and explainable.

## Key principles
- Calm-first: long-term orientation, minimal churn.
- Source quality gates: only reliable sources by default.
- Ensemble models: quant + ML/AI (ML runs off-host).
- Local-first with automatic fallback (optional).

## What is included in the MVP scaffold
- Streamlit UI with Top Picks, Market Outlook, and Portfolio tabs.
- Daily pipeline skeleton with caching and reports output.
- Host manager and notification stubs.
- Avanza opt-in gating with freshness checks.
- Clear places to plug in data sources and models.

## Run locally
1. Create a virtual environment
2. Install requirements
3. Run the app

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Daily pipeline
Run once to generate or refresh outputs:

```bash
python scripts/run_daily.py
```

## ML stub (local testing)
Generate local ML scores to simulate off-host output:

```bash
python scripts/run_ml_stub.py
```

This writes `data/ml_scores.json`, which the app reads automatically.

## Email notifications (optional)
Email notifications use SMTP credentials from environment variables. Update `config/email.json` and set the
password environment variable before running the daily job.

Example (PowerShell):

```powershell
$env:STOCK_ANALYZER_SMTP_PASSWORD="your-app-password"
```

## Launcher menu (desktop-friendly)
Run the launcher menu:

```powershell
.\launch_menu.cmd
```

You can create a desktop shortcut to `launch_menu.cmd` if you want a one-click menu.

## Configuration
- config/defaults.json: core settings
- config/hosts.json: host priority list
- config/email.json: email notification settings (stub)
- config/avanza_optin.json: Avanza API opt-in and freshness thresholds
- config/avanza_availability.csv: manual Avanza availability map

## Outputs
- reports/latest_report.json: latest report for UI
- data/cache.sqlite: cached data and features

## Notes
- ML/AI heavy compute is designed to run off-host (for example, GitHub Actions). The local app pulls the latest ML outputs when available.
- Avanza API is opt-in only. If the library is older than 90 days or data is stale (older than 1 trading day), it is disabled automatically.
