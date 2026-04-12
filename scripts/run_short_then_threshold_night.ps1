param(
  [string]$Timestamp = $(Get-Date -Format 'yyyyMMdd_HHmmss'),
  [int]$ShortSeed = 101,
  [int]$Repeats = 3
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = "C:\Users\wyl\AppData\Local\Programs\Python\Launcher\py.exe"

if (-not (Test-Path $python)) {
  throw "Python not found at $python"
}

Set-Location $repoRoot

$batchRoot = Join-Path $repoRoot ("results\experiment_batches\short_then_night_" + $Timestamp)
$shortOut = Join-Path $batchRoot "short_smoke"
$t1Out = Join-Path $batchRoot "T1"
$t2Out = Join-Path $batchRoot "T2"
$null = New-Item -ItemType Directory -Force -Path $batchRoot, $shortOut, $t1Out, $t2Out
$masterLog = Join-Path $batchRoot "pipeline.log"

function Write-Log {
  param([string]$Message)
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  $line | Tee-Object -FilePath $masterLog -Append
}

Write-Log "Pipeline start: short test + overnight threshold run"
Write-Log "BatchRoot: $batchRoot"

# Ensure live LLM mode unless caller explicitly sets env.
if (-not $env:LLM_MOCK_MODE) {
  $env:LLM_MOCK_MODE = "false"
}
Write-Log "LLM_MOCK_MODE=$env:LLM_MOCK_MODE"

function Invoke-ResearchRun {
  param(
    [string[]]$ScriptArgs
  )
  $oldEap = $ErrorActionPreference
  try {
    # Some dependencies print non-critical lines to stderr even on success.
    # Keep strict exit-code checks but avoid stderr text interrupting the pipeline.
    $ErrorActionPreference = "Continue"
    & $python -3 @ScriptArgs *>> $masterLog
    return $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $oldEap
  }
}

# Stage 1: one real short sanity run
Write-Log "Stage 1/3: short sanity run (R2A, repeats=1, seed=$ShortSeed)"
$shortArgs = @(
  "scripts/run_research_experiments.py",
  "--group", "R2A",
  "--repeats", "1",
  "--seed", "$ShortSeed",
  "--routing-enabled", "true",
  "--buyer-match-threshold", "0.40",
  "--negotiation-threshold", "0.40",
  "--out-dir", "$shortOut"
)
$exitCode = Invoke-ResearchRun -ScriptArgs $shortArgs

if ($exitCode -ne 0) {
  Write-Log "Stage 1 failed with exit code $exitCode. Pipeline aborted."
  exit $exitCode
}
Write-Log "Stage 1 done."

# Stage 2: threshold tuning T1
Write-Log "Stage 2/3: T1 threshold tuning (buyer=0.45, negotiation=0.40)"
$t1Args = @(
  "scripts/run_research_experiments.py",
  "--groups", "R2A", "R2B",
  "--repeats", "$Repeats",
  "--routing-enabled", "true",
  "--buyer-match-threshold", "0.45",
  "--negotiation-threshold", "0.40",
  "--out-dir", "$t1Out"
)
$exitCode = Invoke-ResearchRun -ScriptArgs $t1Args

if ($exitCode -ne 0) {
  Write-Log "Stage 2 failed with exit code $exitCode."
}
else {
  Write-Log "Stage 2 done."
}

# Stage 3: threshold tuning T2
Write-Log "Stage 3/3: T2 threshold tuning (buyer=0.40, negotiation=0.40)"
$t2Args = @(
  "scripts/run_research_experiments.py",
  "--groups", "R2A", "R2B",
  "--repeats", "$Repeats",
  "--routing-enabled", "true",
  "--buyer-match-threshold", "0.40",
  "--negotiation-threshold", "0.40",
  "--out-dir", "$t2Out"
)
$exitCode = Invoke-ResearchRun -ScriptArgs $t2Args

if ($exitCode -ne 0) {
  Write-Log "Stage 3 failed with exit code $exitCode."
  exit $exitCode
}

Write-Log "Pipeline finished successfully."
Write-Host "Done. Batch root: $batchRoot"
