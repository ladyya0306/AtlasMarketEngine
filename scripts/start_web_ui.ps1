param(
  [int]$Port = 8000,
  [string]$BindHost = "127.0.0.1",
  [switch]$Mock
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

if ($Mock) {
  $env:LLM_MOCK_MODE = "true"
  Write-Host "LLM_MOCK_MODE=true (mock mode)" -ForegroundColor Yellow
}
else {
  if (Test-Path Env:LLM_MOCK_MODE) {
    Remove-Item Env:LLM_MOCK_MODE
  }
  Write-Host "LLM_MOCK_MODE is off (live LLM mode)" -ForegroundColor Yellow
}

Write-Host "Python: $python" -ForegroundColor Cyan
Write-Host "URL: http://$BindHost`:$Port/" -ForegroundColor Green
Write-Host "Stop: press Ctrl+C in this window" -ForegroundColor DarkGray

& $python -m uvicorn api_server:app --host $BindHost --port $Port
