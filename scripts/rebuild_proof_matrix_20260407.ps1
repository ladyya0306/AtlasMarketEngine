$ErrorActionPreference = "Stop"

Set-Location "D:\GitProj\visual_real_estate"

$python = "C:\Users\wyl\anaconda3\envs\oasis\python.exe"
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$rootOut = "results\rebuild_proof_$ts"
New-Item -ItemType Directory -Force -Path $rootOut | Out-Null

# Stable runtime settings for formal matrix reruns.
$env:ROLE_ACTIVATION_SERIAL_MODE = "true"
$env:LLM_MAX_RETRIES = "3"
$env:LLM_TIMEOUT_SECONDS = "60"
$env:LLM_BREAKER_FAIL_THRESHOLD = "999"
$env:LLM_BREAKER_COOLDOWN_SECONDS = "20"
$env:LLM_MAX_CONCURRENCY_SMART = "1"
$env:LLM_MAX_CONCURRENCY_FAST = "1"

function Run-Stage {
    param(
        [string]$Name,
        [string[]]$Args,
        [string]$OutDir
    )
    Write-Host "[$(Get-Date -Format s)] START $Name"
    & $python scripts\run_market_state_matrix.py $Args --use-init-snapshot --out-dir $OutDir
    if ($LASTEXITCODE -ne 0) {
        throw "Stage failed: $Name (exit=$LASTEXITCODE)"
    }
    Write-Host "[$(Get-Date -Format s)] DONE  $Name"
}

Run-Stage -Name "baseline_state_m6" -Args @("--months","6","--seeds","606","607","608","--groups","V1","V2","V3") -OutDir "$rootOut\baseline_state_m6_606_607_608"
Run-Stage -Name "info_m12" -Args @("--months","12","--seeds","606","607","608","--groups","V2B","V2I","V3B","V3I") -OutDir "$rootOut\info_m12_606_607_608"
Run-Stage -Name "supply_m12" -Args @("--months","12","--seeds","606","607","608","--groups","V4","V5") -OutDir "$rootOut\supply_m12_606_607_608"
Run-Stage -Name "income_m12" -Args @("--months","12","--seeds","606","607","608","--groups","V6","V7") -OutDir "$rootOut\income_m12_606_607_608"

Write-Host "[$(Get-Date -Format s)] ALL_DONE root=$rootOut"
