# Remove the scheduled task for the daily pipeline.
$ErrorActionPreference = "Stop"

$taskName = "Stock Analyzer Daily"
schtasks /Delete /F /TN $taskName | Out-Null
Write-Host "Scheduled task '$taskName' removed." -ForegroundColor Green
