param(
  [string]$BaseConfig = "config/baseline.yaml",
  [string]$Plan = "config/night_run_example.yaml",
  [string]$DbPath = "",
  [switch]$Resume
)

$pythonCandidates = @(
  "C:\Users\wyl\AppData\Local\Microsoft\WindowsApps\python.exe",
  "python"
)

$python = $null
foreach ($candidate in $pythonCandidates) {
  try {
    $null = & $candidate --version 2>$null
    if ($LASTEXITCODE -eq 0) {
      $python = $candidate
      break
    }
  }
  catch {
  }
}

if (-not $python) {
  throw "No usable Python interpreter was found."
}

if (Test-Path Env:LLM_MOCK_MODE) {
  Remove-Item Env:LLM_MOCK_MODE
}

Write-Host "Night run mode: live LLM, non-interactive" -ForegroundColor Yellow
Write-Host "Python: $python" -ForegroundColor Cyan
Write-Host "Base config: $BaseConfig" -ForegroundColor Cyan
Write-Host "Plan: $Plan" -ForegroundColor Cyan

$argsList = @("scripts/night_run.py", "--base-config", $BaseConfig, "--plan", $Plan)
if ($DbPath) {
  $argsList += @("--db-path", $DbPath)
}
if ($Resume) {
  $argsList += "--resume"
}

& $python @argsList
