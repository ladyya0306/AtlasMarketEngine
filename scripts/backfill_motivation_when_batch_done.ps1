param(
  [Parameter(Mandatory = $true)]
  [string]$BatchDir,
  [int]$PollSeconds = 60,
  [int]$TimeoutHours = 6
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$summary = Join-Path $BatchDir "batch_summary.json"
$deadline = (Get-Date).AddHours($TimeoutHours)

while ((Get-Date) -lt $deadline) {
  if (Test-Path $summary) { break }
  Start-Sleep -Seconds $PollSeconds
}

if (-not (Test-Path $summary)) {
  Write-Host "[timeout] batch_summary.json not found: $summary"
  exit 2
}

Write-Host "[start] backfill motivation report from $summary"
$data = Get-Content $summary -Raw | ConvertFrom-Json
foreach ($run in $data.runs) {
  $db = $run.db_path
  if (-not (Test-Path $db)) {
    Write-Host "[skip] missing db: $db"
    continue
  }
  Write-Host "[run] $db"
  python scripts/backfill_motivation_report.py --db-path "$db"
}
Write-Host "[done] motivation report backfill complete"

