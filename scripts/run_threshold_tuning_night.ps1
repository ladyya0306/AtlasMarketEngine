param(
  [string]$Timestamp = $(Get-Date -Format 'yyyyMMdd_HHmmss'),
  [int]$Repeats = 3
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$baseDir = "results/experiment_batches/threshold_tuning_$Timestamp"
$null = New-Item -ItemType Directory -Force -Path $baseDir

$t1Out = "$baseDir/T1"
$t2Out = "$baseDir/T2"
$null = New-Item -ItemType Directory -Force -Path $t1Out
$null = New-Item -ItemType Directory -Force -Path $t2Out

Write-Host "=== Threshold Tuning Night Run ==="
Write-Host "Base Dir: $baseDir"
Write-Host "Repeats: $Repeats"

Write-Host ""
Write-Host "[T1] buyer=0.45, negotiation=0.40 (R2A+R2B)"
python scripts/run_research_experiments.py `
  --groups R2A R2B `
  --repeats $Repeats `
  --routing-enabled true `
  --buyer-match-threshold 0.45 `
  --negotiation-threshold 0.40 `
  --out-dir $t1Out

Write-Host ""
Write-Host "[T2] buyer=0.40, negotiation=0.40 (R2A+R2B)"
python scripts/run_research_experiments.py `
  --groups R2A R2B `
  --repeats $Repeats `
  --routing-enabled true `
  --buyer-match-threshold 0.40 `
  --negotiation-threshold 0.40 `
  --out-dir $t2Out

Write-Host ""
Write-Host "Done. Results:"
Write-Host "  - $t1Out"
Write-Host "  - $t2Out"
