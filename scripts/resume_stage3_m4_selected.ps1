$ErrorActionPreference = "Stop"

$root = (Resolve-Path ".").Path
$batchRoot = Join-Path $root "results/night_plan/night_gate_20260322_210527/03_stability1_resume_m4_selected"
$planPath = Join-Path $root "config/night_run_empty.yaml"

if (!(Test-Path $planPath)) {
    Set-Content -Path $planPath -Value "{}" -Encoding UTF8
}

$items = @(
    "R2A_s202_cont_m4",
    "R2A_s303_cont_m4",
    "R2B_s202_cont_m4",
    "R2B_s303_cont_m4"
)

$manifest = @()
foreach ($name in $items) {
    $dstDir = Join-Path $batchRoot $name
    $db = Join-Path $dstDir "simulation.db"
    $cfg = Join-Path $dstDir "config.yaml"
    $stdout = Join-Path $dstDir "resume_stdout.log"
    $stderr = Join-Path $dstDir "resume_stderr.log"
    $runnerBat = Join-Path $dstDir "resume_run.cmd"

    if (Test-Path $stdout) { Remove-Item -Force $stdout }
    if (Test-Path $stderr) { Remove-Item -Force $stderr }

    $batLines = @(
        "@echo off",
        "set PYTHONPATH=$root",
        "python scripts/night_run.py --resume --db-path `"$db`" --base-config `"$cfg`" --plan `"$planPath`" --months 4"
    )
    Set-Content -Path $runnerBat -Value ($batLines -join "`r`n") -Encoding ASCII

    $proc = Start-Process -FilePath $runnerBat -WorkingDirectory $root -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
    $manifest += [pscustomobject]@{
        label      = $name
        pid        = $proc.Id
        db         = $db
        started_at = (Get-Date).ToString("s")
        stdout     = $stdout
        stderr     = $stderr
        runner     = $runnerBat
    }
}

$manifestPath = Join-Path $batchRoot "resume_manifest_retry.json"
$manifest | ConvertTo-Json -Depth 5 | Set-Content -Path $manifestPath -Encoding UTF8
Write-Output "started=$($manifest.Count)"
Get-Content $manifestPath
