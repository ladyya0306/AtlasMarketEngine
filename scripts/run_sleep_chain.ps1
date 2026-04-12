param(
  [string]$Timestamp = $(Get-Date -Format 'yyyyMMdd_HHmmss')
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$python = "python"
$batchRoot = Join-Path $repoRoot ("results\experiment_batches\sleep_chain_" + $Timestamp)
$logPath = Join-Path $batchRoot "pipeline.log"

New-Item -ItemType Directory -Force -Path $batchRoot | Out-Null

function Write-Log {
  param([string]$Message)
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  $line | Tee-Object -FilePath $logPath -Append
}

function Invoke-Stage {
  param(
    [string]$StageName,
    [string[]]$StageArgs
  )
  Write-Log "START $StageName"
  Write-Log "CMD: $python $($StageArgs -join ' ')"
  & $python @StageArgs *>> $logPath
  $code = $LASTEXITCODE
  if ($code -ne 0) {
    Write-Log "FAILED $StageName (exit=$code)"
    throw "Stage failed: $StageName"
  }
  Write-Log "DONE $StageName"
}

Write-Log "Sleep chain pipeline start"
Write-Log "BatchRoot: $batchRoot"

# Stage A: B区后段短门禁（快照轨，2 seeds=101/202）
[string]$aOut = [System.IO.Path]::GetFullPath((Join-Path $batchRoot "A_gate_snap_i1d_i2_s101202_m2"))
New-Item -ItemType Directory -Force -Path $aOut | Out-Null
Write-Log "A_OUT=$aOut"
Invoke-Stage -StageName "A_gate_snap_i1d_i2_s101202_m2" -StageArgs @(
  "scripts/run_research_experiments.py",
  "--groups", "I1D", "I2",
  "--repeats", "2",
  "--months-override", "2",
  "--use-init-snapshot",
  "--out-dir", $aOut
)
Invoke-Stage -StageName "A_analyze" -StageArgs @("scripts/analyze_experiment_batch.py", $aOut)

# Stage B: B区后段短门禁（Fresh轨，seed=303）
[string]$bOut = [System.IO.Path]::GetFullPath((Join-Path $batchRoot "B_gate_fresh_i1d_i2_s303_m2"))
New-Item -ItemType Directory -Force -Path $bOut | Out-Null
Write-Log "B_OUT=$bOut"
Invoke-Stage -StageName "B_gate_fresh_i1d_i2_s303_m2" -StageArgs @(
  "scripts/run_research_experiments.py",
  "--groups", "I1D", "I2",
  "--seed", "303",
  "--months-override", "2",
  "--out-dir", $bOut
)
Invoke-Stage -StageName "B_analyze" -StageArgs @("scripts/analyze_experiment_batch.py", $bOut)

# Stage C: P1 稳定性主批（快照轨，R2A/R2B，各3 seeds）
[string]$cOut = [System.IO.Path]::GetFullPath((Join-Path $batchRoot "C_p1_snap_r2ab_s101202303"))
New-Item -ItemType Directory -Force -Path $cOut | Out-Null
Write-Log "C_OUT=$cOut"
Invoke-Stage -StageName "C_p1_snap_r2ab_s101202303" -StageArgs @(
  "scripts/run_research_experiments.py",
  "--groups", "R2A", "R2B",
  "--repeats", "3",
  "--use-init-snapshot",
  "--out-dir", $cOut
)
Invoke-Stage -StageName "C_analyze" -StageArgs @("scripts/analyze_experiment_batch.py", $cOut)

# Stage D: P1 关键复核（Fresh轨，R2A/R2B，seed=303）
[string]$dOut = [System.IO.Path]::GetFullPath((Join-Path $batchRoot "D_p1_fresh_r2ab_s303"))
New-Item -ItemType Directory -Force -Path $dOut | Out-Null
Write-Log "D_OUT=$dOut"
Invoke-Stage -StageName "D_p1_fresh_r2ab_s303" -StageArgs @(
  "scripts/run_research_experiments.py",
  "--groups", "R2A", "R2B",
  "--seed", "303",
  "--out-dir", $dOut
)
Invoke-Stage -StageName "D_analyze" -StageArgs @("scripts/analyze_experiment_batch.py", $dOut)

Write-Log "Sleep chain pipeline finished successfully."
Write-Host "DONE. BatchRoot=$batchRoot"
