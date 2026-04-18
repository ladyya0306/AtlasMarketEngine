param(
    [string]$Workspace = $(Split-Path -Parent $PSScriptRoot),
    [int]$Months = 6,
    [int]$Seed = 606,
    [switch]$Background
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$workspacePath = (Resolve-Path -LiteralPath $Workspace).Path
$pythonExe = "C:\Users\wyl\miniconda3\python.exe"
$scriptPath = Join-Path $workspacePath "scripts\run_release_compare_batch.py"

if ($Background) {
    Start-Process powershell.exe -ArgumentList @(
        "-NoExit",
        "-ExecutionPolicy", "Bypass",
        "-Command",
        "& '$pythonExe' -u '$scriptPath' --group g1g2 --workspace '$workspacePath' --months $Months --seed $Seed"
    ) -WorkingDirectory $workspacePath | Out-Null
    exit 0
}

& $pythonExe -u $scriptPath --group g1g2 --workspace $workspacePath --months $Months --seed $Seed
exit $LASTEXITCODE

