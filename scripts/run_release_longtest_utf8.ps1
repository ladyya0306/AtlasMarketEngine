param(
    [string]$Workspace = "D:\GitProj\visual_real_estate",
    [string]$SnapshotId = "spindle_large",
    [string]$MarketGoal = "balanced",
    [double]$DemandMultiplier = 1.00,
    [int]$Months = 6,
    [int]$Seed = 606,
    [string[]]$IncomeShock = @(),
    [string[]]$DeveloperSupply = @(),
    [string[]]$SupplyCut = @(),
    [switch]$Background
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding
if (Get-Variable -Name PSStyle -ErrorAction SilentlyContinue) {
    try {
        $PSStyle.OutputRendering = "PlainText"
    } catch {
    }
}

$workspacePath = (Resolve-Path -LiteralPath $Workspace).Path
Set-Location -LiteralPath $workspacePath

$pythonExe = "C:\Users\wyl\miniconda3\python.exe"
$sitePackages = "C:\Users\wyl\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\LocalCache\local-packages\Python313\site-packages"
$envFile = Join-Path $workspacePath ".env"
$env:PYTHONPATH = "$workspacePath;$sitePackages"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
$env:NO_COLOR = "1"

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir = Join-Path $workspacePath "logs"
$resultRoot = Join-Path $workspacePath "results\release_longtest"
$safeMultiplier = [string]$DemandMultiplier -replace '\.', 'p'
$projectDir = Join-Path $resultRoot ("release_longtest_{0}_{1}_m{2}_d{3}_s{4}" -f $stamp, $SnapshotId, $Months, $safeMultiplier, $Seed)
$logPath = Join-Path $logDir "release_longtest_utf8_$stamp.log"
New-Item -ItemType Directory -Force -Path $logDir, $resultRoot | Out-Null

function Write-Log {
    param([string]$Message)
    $Message | Tee-Object -FilePath $logPath -Append | Out-Host
}

function Quote-CmdArg {
    param([string]$Value)
    if ($null -eq $Value) {
        return '""'
    }
    $escaped = $Value -replace '"', '\"'
    return '"' + $escaped + '"'
}

cmd /c chcp 65001 > $null

$runnerArgs = @(
    "scripts/run_release_startup_smoke.py",
    "--snapshot-id", $SnapshotId,
    "--market-goal", $MarketGoal,
    "--months", [string]$Months,
    "--demand-multiplier", ([string]::Format("{0:0.00}", $DemandMultiplier)),
    "--seed", [string]$Seed,
    "--project-dir", $projectDir,
    "--no-default-interventions",
    "--no-llm-mock",
    "--no-disable-end-reports"
)

foreach ($spec in $IncomeShock) {
    if (-not [string]::IsNullOrWhiteSpace($spec)) {
        $runnerArgs += @("--income-shock", $spec)
    }
}
foreach ($spec in $DeveloperSupply) {
    if (-not [string]::IsNullOrWhiteSpace($spec)) {
        $runnerArgs += @("--developer-supply", $spec)
    }
}
foreach ($spec in $SupplyCut) {
    if (-not [string]::IsNullOrWhiteSpace($spec)) {
        $runnerArgs += @("--supply-cut", $spec)
    }
}

Write-Log "==== 发布前长测（UTF-8 CLI） ===="
Write-Log ("时间: " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Write-Log ("Workspace: " + $workspacePath)
Write-Log ("Python: " + $pythonExe)
Write-Log ("Log: " + $logPath)
Write-Log ("ProjectDir: " + $projectDir)
Write-Log ("SnapshotId: " + $SnapshotId)
Write-Log ("MarketGoal: " + $MarketGoal)
Write-Log ("DemandMultiplier: " + ([string]::Format("{0:0.00}", $DemandMultiplier)))
Write-Log ("Months: " + $Months)
Write-Log ("Seed: " + $Seed)
Write-Log ("IncomeShock: " + ($(if ($IncomeShock.Count -gt 0) { $IncomeShock -join ", " } else { "(none)" })))
Write-Log ("DeveloperSupply: " + ($(if ($DeveloperSupply.Count -gt 0) { $DeveloperSupply -join ", " } else { "(none)" })))
Write-Log ("SupplyCut: " + ($(if ($SupplyCut.Count -gt 0) { $SupplyCut -join ", " } else { "(none)" })))
Write-Log ("Console.InputEncoding: " + [Console]::InputEncoding.WebName)
Write-Log ("Console.OutputEncoding: " + [Console]::OutputEncoding.WebName)
Write-Log (((cmd /c chcp) | Out-String).Trim())
Write-Log ("SMART_API_KEY present: " + ($(if ($env:SMART_API_KEY -or $env:DEEPSEEK_API_KEY) { "yes" } else { "no" })))
Write-Log ("FAST_API_KEY present: " + ($(if ($env:FAST_API_KEY) { "yes" } else { "no" })))
Write-Log (".env present: " + ($(if (Test-Path $envFile) { "yes" } else { "no" })))
Write-Log "中文日志测试：发布前长测 / 梭子型固定供应盘 / 真实LLM / 无默认冲击 / checkpoint"
Write-Log ""
Write-Log ("命令: " + $pythonExe + " -u " + ($runnerArgs -join " "))

if ($Background) {
    $pidPath = Join-Path $projectDir "longtest.pid"
    $stdoutPath = Join-Path $projectDir "stdout.log"
    $stderrPath = Join-Path $projectDir "stderr.log"
    $allArgs = @("-u") + $runnerArgs
    New-Item -ItemType Directory -Force -Path $projectDir | Out-Null
    $proc = Start-Process $pythonExe -ArgumentList $allArgs -WorkingDirectory $workspacePath -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
    Set-Content -Path $pidPath -Value $proc.Id
    Write-Log ("LONGTEST_STARTED_PID=" + $proc.Id)
    Write-Log ("PID_FILE=" + $pidPath)
    Write-Log ("STDOUT_FILE=" + $stdoutPath)
    Write-Log ("STDERR_FILE=" + $stderrPath)
    exit 0
}

 $cmdArgs = @((Quote-CmdArg $pythonExe), '-u') + ($runnerArgs | ForEach-Object { Quote-CmdArg $_ })
 $cmdLine = ($cmdArgs -join ' ') + ' 2>&1'
 & cmd.exe /d /c $cmdLine | Tee-Object -FilePath $logPath -Append | Out-Host
$exitCode = $LASTEXITCODE
Write-Log ("LONGTEST_EXIT_CODE=" + $exitCode)

exit $exitCode
