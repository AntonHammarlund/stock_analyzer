# Stock Analyzer Launcher Menu
# Run with: powershell -ExecutionPolicy Bypass -File scripts\launcher_menu.ps1

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectRoot

function Resolve-Python {
    if ($env:STOCK_ANALYZER_PYTHON -and (Test-Path $env:STOCK_ANALYZER_PYTHON)) {
        return $env:STOCK_ANALYZER_PYTHON
    }

    $pyLauncher = "C:\Windows\py.exe"
    if (Test-Path $pyLauncher) {
        try {
            $paths = & $pyLauncher -0p 2>$null
            if ($paths) {
                $first = $paths -split "`n" | Select-Object -First 1
                if ($first -match "\*\s+(?<path>.+)$") {
                    return $Matches["path"].Trim()
                }
            }
        } catch {}
    }

    $candidates = @(
        "C:\Users\Anton\AppData\Local\Programs\Python\Python312\python.exe",
        "C:\Users\Anton\AppData\Local\Programs\Python\Python311\python.exe",
        "C:\Program Files\Python312\python.exe",
        "C:\Program Files\Python311\python.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) { return $candidate }
    }

    return $null
}

function Show-Menu {
    Clear-Host
    Write-Host "Stock Analyzer" -ForegroundColor Cyan
    Write-Host "================" -ForegroundColor Cyan
    Write-Host "1) Launch Web App (Streamlit)"
    Write-Host "2) Run Data Sync + Daily Pipeline"
    Write-Host "3) Install Daily Automation"
    Write-Host "4) Remove Daily Automation"
    Write-Host "5) Generate ML Stub Scores"
    Write-Host "6) Open Latest Report"
    Write-Host "7) Open Portfolio File"
    Write-Host "8) Exit"
    Write-Host ""
}

function Get-AlphaVantageKey {
    $key = $env:ALPHAVANTAGE_API_KEY
    if (-not $key) { $key = $env:alphavantage_api_key }
    return $key
}

function Get-ActiveUserId {
    $usersFile = Join-Path $projectRoot "data\users.json"
    if (-not (Test-Path $usersFile)) {
        return "default"
    }
    try {
        $payload = Get-Content -Path $usersFile -Raw | ConvertFrom-Json
        if ($payload.active_user_id) {
            return $payload.active_user_id
        }
    } catch {}
    return "default"
}

$python = Resolve-Python
if (-not $python) {
    Write-Host "Python not found. Set STOCK_ANALYZER_PYTHON to your python.exe path." -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

while ($true) {
    Show-Menu
    $choice = Read-Host "Choose an option"

    switch ($choice) {
        "1" {
            Write-Host "Launching Streamlit..." -ForegroundColor Green
            & $python -m streamlit run app.py
        }
        "2" {
            Write-Host "Running data sync + daily pipeline..." -ForegroundColor Green
            $alphaKey = Get-AlphaVantageKey
            if (-not $alphaKey) {
                Write-Host "Alpha Vantage API key missing. Set ALPHAVANTAGE_API_KEY to enable free universe + watchlist." -ForegroundColor Yellow
                Write-Host "Example: `$env:ALPHAVANTAGE_API_KEY=`"your-key`"" -ForegroundColor Yellow
            }
            & $python scripts\run_daily.py --sync-watchlist --sync-data --force
            Read-Host "Press Enter to continue"
        }
        "3" {
            Write-Host "Installing daily automation..." -ForegroundColor Green
            & powershell -ExecutionPolicy Bypass -File scripts\install_scheduler.ps1
            Read-Host "Press Enter to continue"
        }
        "4" {
            Write-Host "Removing daily automation..." -ForegroundColor Green
            & powershell -ExecutionPolicy Bypass -File scripts\remove_scheduler.ps1
            Read-Host "Press Enter to continue"
        }
        "5" {
            Write-Host "Generating ML stub scores..." -ForegroundColor Green
            & $python scripts\run_ml_stub.py
            Read-Host "Press Enter to continue"
        }
        "6" {
            $report = Join-Path $projectRoot "reports\latest_report.json"
            if (Test-Path $report) {
                Start-Process $report
            } else {
                Write-Host "No report found yet." -ForegroundColor Yellow
                Read-Host "Press Enter to continue"
            }
        }
        "7" {
            $userId = Get-ActiveUserId
            $portfolio = Join-Path $projectRoot ("data\portfolios\" + $userId + ".json")
            if (Test-Path $portfolio) {
                Start-Process $portfolio
            } else {
                Write-Host "No portfolio file for the active user yet. Add holdings in the app first." -ForegroundColor Yellow
                Read-Host "Press Enter to continue"
            }
        }
        "8" { break }
        Default {
            Write-Host "Invalid selection." -ForegroundColor Yellow
            Start-Sleep -Seconds 1
        }
    }
}
