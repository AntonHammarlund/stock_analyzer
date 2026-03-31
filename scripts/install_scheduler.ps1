# Install a scheduled task to run the daily pipeline with data sync.
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

$python = Resolve-Python
if (-not $python) {
    Write-Host "Python not found. Set STOCK_ANALYZER_PYTHON to your python.exe path." -ForegroundColor Yellow
    exit 1
}

$taskName = "Stock Analyzer Daily"
$command = "cmd /c `"cd /d `"$projectRoot`" && `"$python`" scripts\run_daily.py --sync-data`""

schtasks /Create /F /SC HOURLY /MO 1 /TN $taskName /TR $command | Out-Null
Write-Host "Scheduled task '$taskName' created. It will run hourly and execute the daily pipeline when due." -ForegroundColor Green
