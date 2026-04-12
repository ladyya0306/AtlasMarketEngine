param(
  [string[]]$Groups = @("B", "C", "D"),
  [int]$Repeats = 3,
  [int]$RetryCount = 1,
  [string]$BatchRoot = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = "C:\Users\wyl\AppData\Local\Microsoft\WindowsApps\python.exe"
$runner = Join-Path $repoRoot "scripts\run_research_experiments.py"

if (-not (Test-Path $python)) {
  throw "Python not found at $python"
}

if (-not (Test-Path $runner)) {
  throw "Runner script not found at $runner"
}

if (-not $BatchRoot) {
  $BatchRoot = Join-Path $repoRoot ("results\experiment_batches\night_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
}

New-Item -ItemType Directory -Force -Path $BatchRoot | Out-Null
$masterLog = Join-Path $BatchRoot "night_batch.log"

function Write-Log {
  param([string]$Message)
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  $line | Tee-Object -FilePath $masterLog -Append
}

Write-Log "Night research batch started."
Write-Log "Groups: $($Groups -join ', ') | Repeats: $Repeats | RetryCount: $RetryCount"
Write-Log "BatchRoot: $BatchRoot"

Push-Location $repoRoot
try {
  foreach ($group in $Groups) {
    $groupOutDir = Join-Path $BatchRoot ("group_" + $group)
    $attempt = 0
    $success = $false

    while (-not $success -and $attempt -le $RetryCount) {
      $attempt += 1
      Write-Log "Starting group $group (attempt $attempt)."
      $args = @(
        $runner,
        "--group", $group,
        "--repeats", $Repeats,
        "--out-dir", $groupOutDir
      )

      & $python @args *>> $masterLog
      $exitCode = $LASTEXITCODE

      if ($exitCode -eq 0) {
        $success = $true
        Write-Log "Group $group completed successfully."
      } else {
        Write-Log "Group $group failed with exit code $exitCode."
        if ($attempt -le $RetryCount) {
          Write-Log "Retrying group $group after 10 seconds."
          Start-Sleep -Seconds 10
        }
      }
    }

    if (-not $success) {
      Write-Log "Group $group exhausted retries and remains failed."
    }
  }
} finally {
  Pop-Location
}

Write-Log "Night research batch finished."
