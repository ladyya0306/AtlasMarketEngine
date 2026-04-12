param(
  [string]$BatchDir = "",
  [int]$PollSeconds = 180,
  [double]$TimeoutHours = 16,
  [switch]$AutoFixSafe = $true
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not $BatchDir -or $BatchDir.Trim() -eq "") {
  $candidates = Get-ChildItem "results/experiment_batches" -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like "threshold_tuning_*" } |
    Sort-Object LastWriteTime -Descending
  if (-not $candidates -or $candidates.Count -eq 0) {
    throw "No threshold_tuning batch found."
  }
  $BatchDir = $candidates[0].FullName
}

$watchOut = Join-Path $BatchDir "watch.out.log"
$watchErr = Join-Path $BatchDir "watch.err.log"

$fixArg = ""
if ($AutoFixSafe) {
  $fixArg = "--auto-fix-safe"
}

$cmd = "python scripts/watch_threshold_tuning_night.py --batch-dir `"$BatchDir`" --poll-seconds $PollSeconds --timeout-hours $TimeoutHours $fixArg"

$proc = Start-Process -FilePath "powershell" `
  -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $cmd) `
  -RedirectStandardOutput $watchOut `
  -RedirectStandardError $watchErr `
  -PassThru

Write-Host "Watchdog started."
Write-Host "PID: $($proc.Id)"
Write-Host "Batch: $BatchDir"
Write-Host "Out: $watchOut"
Write-Host "Err: $watchErr"

