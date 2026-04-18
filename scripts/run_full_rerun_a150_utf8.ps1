param(
    [string]$Workspace = "D:\GitProj\visual_real_estate"
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
$outRoot = Join-Path $workspacePath "results\line_a_natural_activation_bridge\fixed_supply_spindle_medium_seller_demandheavy1p5x_m3_fullrerun_a150_20260417"
$batchDir = Join-Path $outRoot "forced_role_batch_$stamp"
$caseDir = Join-Path $batchDir "V2_s606_m3_a150"
$runtimeDir = Join-Path $caseDir "runtime_run"
$checkpointRoot = Join-Path $runtimeDir "monthly_checkpoints"
$logPath = Join-Path $logDir "full_rerun_a150_utf8_$stamp.log"
$packPath = Join-Path $workspacePath "results\line_a_natural_activation_bridge\demand_heavy_profile_packs_20260417\spindle_medium_seller_demandheavy_1p5x_scarce_a150.yaml"
$validationPath = Join-Path $batchDir "profile_pack_validation.json"

New-Item -ItemType Directory -Force -Path $logDir, $outRoot, $batchDir | Out-Null

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

function Write-Section {
    param([string]$Title)
    Write-Log ""
    Write-Log ("==== " + $Title + " ====")
}

cmd /c chcp 65001 > $null

Write-Section "中国住房市场推演完整重跑 A150"
Write-Log ("时间: " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Write-Log ("Workspace: " + $workspacePath)
Write-Log ("Python: " + $pythonExe)
Write-Log ("Log: " + $logPath)
Write-Log ("BatchDir: " + $batchDir)
Write-Log ("CaseDir: " + $caseDir)
Write-Log ("PackPath: " + $packPath)
Write-Log ("Console.InputEncoding: " + [Console]::InputEncoding.WebName)
Write-Log ("Console.OutputEncoding: " + [Console]::OutputEncoding.WebName)
Write-Log (((cmd /c chcp) | Out-String).Trim())
Write-Log "中文日志测试：住房市场推演 / 真热扩散 / 生命周期标签 / checkpoint"

Write-Section "网络环境"
$proxyVars = Get-ChildItem Env: | Where-Object {
    $_.Name -match "^(HTTP|HTTPS|ALL|NO)_PROXY$|^http_proxy$|^https_proxy$|^all_proxy$|^no_proxy$"
} | Sort-Object Name
if ($proxyVars) {
    foreach ($item in $proxyVars) {
        Write-Log ($item.Name + "=" + $item.Value)
    }
} else {
    Write-Log "(no proxy env vars found)"
}
Write-Log ("OPENAI_API_KEY present: " + ($(if ($env:OPENAI_API_KEY) { "yes" } else { "no" })))
$ipSummary = ipconfig | Select-String -Pattern "IPv4 Address|IPv4 地址|Default Gateway|默认网关"
if ($ipSummary) {
    foreach ($line in $ipSummary) {
        Write-Log $line.Line.Trim()
    }
}
try {
    $tnc = Test-NetConnection -ComputerName "api.openai.com" -Port 443 -WarningAction SilentlyContinue
    Write-Log ("api.openai.com:443 TcpTestSucceeded=" + [string]::Format("{0}", $tnc.TcpTestSucceeded))
    if ($tnc.RemoteAddress) {
        Write-Log ("api.openai.com remote address: " + $tnc.RemoteAddress.IPAddressToString)
    }
} catch {
    Write-Log ("Test-NetConnection failed: " + $_.Exception.Message)
}

Write-Section "输入包校验"
$validateArgs = @(
    "scripts/validate_line_b_profile_pack.py",
    "--profile-pack-path", $packPath,
    "--out-path", $validationPath,
    "--strict"
)
Write-Log ($pythonExe + " " + ($validateArgs -join " "))
$validateCmdArgs = @((Quote-CmdArg $pythonExe)) + ($validateArgs | ForEach-Object { Quote-CmdArg $_ })
$validateCmdLine = ($validateCmdArgs -join ' ') + ' 2>&1'
& cmd.exe /d /c $validateCmdLine | Tee-Object -FilePath $logPath -Append | Out-Host
$validateExit = $LASTEXITCODE
Write-Log ("VALIDATE_EXIT_CODE=" + $validateExit)
if ($validateExit -ne 0) {
    throw "Profile pack validation failed."
}

Write-Section "完整重跑启动"
$runnerArgs = @(
    "scripts/run_line_b_forced_role.py",
    "--months", "3",
    "--agent-count", "150",
    "--groups", "V2",
    "--seeds", "606",
    "--out-dir", $batchDir,
    "--income-adjustment-rate", "1.15",
    "--initial-listing-rate", "0.02",
    "--enable-profiled-market-mode",
    "--profile-pack-path", $packPath,
    "--profile-experiment-mode", "scarce",
    "--activation-mode", "natural",
    "--enable-buyer-seller-intent-split"
)
Write-Log ($pythonExe + " " + ($runnerArgs -join " "))
$runnerCmdArgs = @((Quote-CmdArg $pythonExe)) + ($runnerArgs | ForEach-Object { Quote-CmdArg $_ })
$runnerCmdLine = ($runnerCmdArgs -join ' ') + ' 2>&1'
& cmd.exe /d /c $runnerCmdLine | Tee-Object -FilePath $logPath -Append | Out-Host
$runExit = $LASTEXITCODE
Write-Log ("RUN_EXIT_CODE=" + $runExit)

Write-Section "Checkpoint 验证"
for ($month = 1; $month -le 3; $month++) {
    $monthLabel = "{0:D2}" -f $month
    $monthDir = Join-Path $checkpointRoot ("month_" + $monthLabel)
    $checkpointDb = Join-Path $monthDir "simulation.db"
    $checkpointMeta = Join-Path $monthDir "checkpoint_meta.json"
    $statusSnapshot = Join-Path $monthDir "status_snapshot.json"
    $ok = (Test-Path -LiteralPath $checkpointDb) -and (Test-Path -LiteralPath $checkpointMeta) -and (Test-Path -LiteralPath $statusSnapshot)
    Write-Log ("month_" + $monthLabel + " checkpoint_ok=" + ($(if ($ok) { "true" } else { "false" })))
    Write-Log ("  db=" + $checkpointDb)
    Write-Log ("  meta=" + $checkpointMeta)
    Write-Log ("  status=" + $statusSnapshot)
}

Write-Section "结果路径"
Write-Log ("BatchDir: " + $batchDir)
Write-Log ("CaseDir: " + $caseDir)
Write-Log ("Runtime DB: " + (Join-Path $runtimeDir "simulation.db"))
Write-Log ("Stdout: " + (Join-Path $caseDir "stdout.log"))
Write-Log ("Stderr: " + (Join-Path $caseDir "stderr.log"))
Write-Log ("LogFile: " + $logPath)

if ($runExit -eq 0) {
    Write-Section "运行完成"
    Write-Log "完整重跑已结束，窗口保留，便于继续查看日志。"
} else {
    Write-Section "运行失败"
    Write-Log "完整重跑返回非零退出码，请直接检查上方日志与 case 目录。"
}
