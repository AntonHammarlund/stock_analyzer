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
    Write-Host "2) Run Daily Pipeline"
    Write-Host "3) Generate ML Stub Scores"
    Write-Host "4) Open Latest Report"
    Write-Host "5) Open Portfolio File"
    Write-Host "6) Exit"
    Write-Host ""
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
            Write-Host "Running daily pipeline..." -ForegroundColor Green
            & $python scripts\run_daily.py --force
            Read-Host "Press Enter to continue"
        }
        "3" {
            Write-Host "Generating ML stub scores..." -ForegroundColor Green
            & $python scripts\run_ml_stub.py
            Read-Host "Press Enter to continue"
        }
        "4" {
            $report = Join-Path $projectRoot "reports\latest_report.json"
            if (Test-Path $report) {
                Start-Process $report
            } else {
                Write-Host "No report found yet." -ForegroundColor Yellow
                Read-Host "Press Enter to continue"
            }
        }
        "5" {
            $portfolio = Join-Path $projectRoot "data\portfolio.json"
            if (Test-Path $portfolio) {
                Start-Process $portfolio
            } else {
                Write-Host "No portfolio file yet. Add holdings in the app first." -ForegroundColor Yellow
                Read-Host "Press Enter to continue"
            }
        }
        "6" { break }
        Default {
            Write-Host "Invalid selection." -ForegroundColor Yellow
            Start-Sleep -Seconds 1
        }
    }
}
