param(
  [int]$Port = 8012,
  [string]$OutDir = "output/browser-smoke",
  [ValidateSet("manual", "night_run")]
  [string]$Mode = "manual",
  [switch]$Headed
)

$python = "C:\Users\wyl\AppData\Local\Microsoft\WindowsApps\python.exe"
$node = "node"
$url = "http://127.0.0.1:$Port/"
$serverProc = $null

try {
  $env:LLM_MOCK_MODE = "true"
  $serverProc = Start-Process -FilePath $python -ArgumentList "-m","uvicorn","api_server:app","--host","127.0.0.1","--port",$Port -WorkingDirectory $PWD.Path -WindowStyle Hidden -PassThru

  $ready = $false
  for ($i = 0; $i -lt 25; $i++) {
    Start-Sleep -Milliseconds 500
    try {
      $resp = Invoke-WebRequest -UseBasicParsing $url
      if ($resp.StatusCode -eq 200) {
        $ready = $true
        break
      }
    } catch {
    }
  }

  if (-not $ready) {
    throw "Smoke server did not become ready on $url"
  }

  $args = @("scripts/browser_smoke.js", "--url", $url, "--out-dir", $OutDir, "--mode", $Mode)
  if ($Headed) {
    $args += "--headed"
  }
  & $node @args
  if ($LASTEXITCODE -ne 0) {
    throw "Browser smoke script failed with exit code $LASTEXITCODE"
  }
}
finally {
  if ($serverProc -and -not $serverProc.HasExited) {
    Stop-Process -Id $serverProc.Id -Force
  }
}
