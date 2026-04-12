param(
  [string]$Group = "",
  [int]$Seed = 0,
  [int]$Repeats = 3,
  [switch]$AllGroups,
  [string]$OutDir = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = "C:\Users\wyl\AppData\Local\Microsoft\WindowsApps\python.exe"

if (-not (Test-Path $python)) {
  throw "Python not found at $python"
}

$args = @("scripts/run_research_experiments.py")

if ($Group) {
  $args += @("--group", $Group)
}
if ($Seed -gt 0) {
  $args += @("--seed", $Seed)
}
if ($Repeats -gt 0) {
  $args += @("--repeats", $Repeats)
}
if ($AllGroups) {
  $args += "--all-groups"
}
if ($OutDir) {
  $args += @("--out-dir", $OutDir)
}

Write-Host "Python: $python"
Write-Host "Repo: $repoRoot"
Write-Host "Args: $($args -join ' ')"

Push-Location $repoRoot
try {
  & $python @args
} finally {
  Pop-Location
}
