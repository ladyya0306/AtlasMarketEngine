param(
    [Parameter(Mandatory = $true)]
    [string]$CaseDir
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
cmd /c chcp 65001 > $null

$resolvedCase = (Resolve-Path -LiteralPath $CaseDir).Path
$stdoutPath = Join-Path $resolvedCase "stdout.log"
$stderrPath = Join-Path $resolvedCase "stderr.log"

function Write-Line {
    param([string]$Text)
    Write-Host $Text
}

function Flush-NewLines {
    param(
        [string]$Path,
        [string]$Prefix,
        [int]$KnownCount
    )
    if (-not (Test-Path -LiteralPath $Path)) {
        return $KnownCount
    }
    $lines = Get-Content -LiteralPath $Path
    $currentCount = @($lines).Count
    if ($currentCount -le $KnownCount) {
        return $KnownCount
    }
    $newLines = @($lines)[$KnownCount..($currentCount - 1)]
    foreach ($line in $newLines) {
        Write-Line ($Prefix + $line)
    }
    return $currentCount
}

Write-Line "==== Live Case Tail ===="
Write-Line ("CaseDir: " + $resolvedCase)
Write-Line ("Stdout: " + $stdoutPath)
Write-Line ("Stderr: " + $stderrPath)
Write-Line "Live UTF-8 log tail started."

$stdoutPrefix = '[stdout] '
$stderrPrefix = '[stderr] '
$stdoutKnown = 0
$stderrKnown = 0

while ($true) {
    $stdoutKnown = Flush-NewLines -Path $stdoutPath -Prefix $stdoutPrefix -KnownCount $stdoutKnown
    $stderrKnown = Flush-NewLines -Path $stderrPath -Prefix $stderrPrefix -KnownCount $stderrKnown
    Start-Sleep -Seconds 2
}
