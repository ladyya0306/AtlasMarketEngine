param(
    [string]$Workspace = $(Split-Path -Parent $PSScriptRoot),
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ScriptArgs
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
$env:PYTHONPATH = "$workspacePath;$sitePackages"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
$env:NO_COLOR = "1"

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir = Join-Path $workspacePath "logs"
$logPath = Join-Path $logDir "web_release_smoke_utf8_$stamp.log"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

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

Write-Log "==== Web Release Smoke（UTF-8 CLI） ===="
Write-Log ("时间: " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Write-Log ("Workspace: " + $workspacePath)
Write-Log ("Python: " + $pythonExe)
Write-Log ("Log: " + $logPath)
Write-Log ("Console.InputEncoding: " + [Console]::InputEncoding.WebName)
Write-Log ("Console.OutputEncoding: " + [Console]::OutputEncoding.WebName)
Write-Log (((cmd /c chcp) | Out-String).Trim())
Write-Log "中文日志测试：Web 固定供应盘 / 需求倍率 / 预排冲击 / checkpoint / 发布入口"

$defaultArgs = @(
    "scripts/run_web_release_smoke.py",
    "--base-url", "http://127.0.0.1:8011",
    "--snapshot-id", "spindle_minimum",
    "--market-goal", "balanced",
    "--months", "3",
    "--demand-multiplier", "0.10",
    "--seed", "606"
)

$runnerArgs = if ($ScriptArgs -and $ScriptArgs.Count -gt 0) {
    @("scripts/run_web_release_smoke.py") + $ScriptArgs
} else {
    $defaultArgs
}

Write-Log ""
Write-Log ("命令: " + $pythonExe + " -u " + ($runnerArgs -join " "))

$cmdArgs = @((Quote-CmdArg $pythonExe), '-u') + ($runnerArgs | ForEach-Object { Quote-CmdArg $_ })
$cmdLine = ($cmdArgs -join ' ') + ' 2>&1'
& cmd.exe /d /c $cmdLine | Tee-Object -FilePath $logPath -Append | Out-Host
$exitCode = $LASTEXITCODE
Write-Log ("WEB_SMOKE_EXIT_CODE=" + $exitCode)

exit $exitCode
