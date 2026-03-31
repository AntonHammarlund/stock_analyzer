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

To sync data before the daily run:

```bash
python scripts/run_daily.py --sync-data
```

## Automatic daily runs (Windows)
You can install a scheduled task that runs hourly and triggers the daily pipeline when it is due:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install_scheduler.ps1
```

Remove it with:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\remove_scheduler.ps1
```

## Cloud scheduling (free, wakes on visit)
If you host the app on Streamlit Community Cloud (free) it will sleep when idle, but you can still
run scheduled jobs with GitHub Actions:

1. Create a GitHub repo and push this project.
2. Add the secret `EODHD_API_TOKEN` (Settings → Secrets and variables → Actions) if you want EODHD data.
3. The workflow in `.github/workflows/daily_sync.yml` runs daily at 06:00 Stockholm time, syncing data
   and committing `data/universe_import.csv`, `data/prices_import.csv`, and `reports/latest_report.json`.

Streamlit will wake when you visit, and it will render the latest committed outputs.

For the free watchlist sync, add these GitHub Actions secrets if you want the workflow to fetch prices:
- `ALPHAVANTAGE_API_KEY`
- `MARKETSTACK_API_KEY` (optional)

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

## Accounts (local profiles)
This app supports lightweight local profiles (no passwords). You can add users from the sidebar and switch the active user.
Each user has a separate portfolio file under `data/portfolios/`.

## Import a larger universe
To load thousands of instruments, import a CSV into `data/universe_import.csv`:

```powershell
python scripts/import_universe_csv.py --source "C:\path\to\your_universe.csv" --overwrite
```

Your CSV can include these columns:
`instrument_id`, `isin`, `name`, `asset_type`, `ticker`, `currency`, `market`, `country`, `sector`, `industry`, `manual_source`, `notes`.

The pipeline will warn and pause summaries if the imported universe is below the required size
(default `min_imported_universe_count` in `config/defaults.json`).

Daily summaries also require fresh price data. If the latest price date is older than
`max_price_age_days`, summaries are withheld until the data is refreshed.

## Daily data sync (Nasdaq Nordic + EODHD)
This project can ingest both Nasdaq Nordic files and EODHD data into the import files the app uses:

1. Download the Nasdaq Nordic reference and EOD files daily.
2. Update `config/nasdaq_nordic.json` with those file paths (or place them in `data/`).
3. Set your EODHD token in an environment variable (default `EODHD_API_TOKEN`) and edit
   `config/eodhd.json` to list the exchanges you want.
4. Run the sync script:

```powershell
python scripts/sync_data.py
```

The script updates `data/universe_import.csv` and `data/prices_import.csv`, merging both sources and
keeping the most recent `price_history_days` of prices. The daily pipeline will only publish summaries
when the imported universe is large enough and price data is within the freshness window.

## Hybrid free mode (daily watchlist + periodic large universe)
If you want everything free, use the hybrid mode:
- **Daily watchlist** uses free APIs (small list, updated daily).
- **Large universe** uses periodic CSV imports (not necessarily daily).

### Watchlist setup
1. Add instruments to `data/watchlist.csv` with columns:
   `instrument_id`, `symbol`, `provider` (alpha_vantage or marketstack), `name`, `asset_type`, `ticker`, `currency`, `market`, `country`.
2. Set your API keys in environment variables:

```powershell
$env:ALPHAVANTAGE_API_KEY="your-key"
$env:MARKETSTACK_API_KEY="your-key"
```

3. Sync watchlist prices:

```powershell
python scripts/sync_watchlist.py
```

### Daily run with watchlist sync

```powershell
python scripts/run_daily.py --sync-watchlist --force
```

The GitHub Actions workflow also runs `scripts/sync_watchlist.py` daily and commits
`data/watchlist.csv` and `data/prices_watchlist.csv`.

## Data provider configuration
Set your data provider in `config/data_provider.json`. The project expects a licensed, daily-updated
source for Swedish stocks/bonds and a large global universe.

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
