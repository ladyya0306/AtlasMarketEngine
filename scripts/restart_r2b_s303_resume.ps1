$ErrorActionPreference = "Stop"

$root = (Resolve-Path ".").Path
$src = Join-Path $root "results/run_20260323_065503"
$dst = Join-Path $root "results/night_plan/night_gate_20260322_210527/03_stability1_resume_m4_selected/R2B_s303_cont_m4"

Copy-Item -Force (Join-Path $src "simulation.db") (Join-Path $dst "simulation.db")
Copy-Item -Force (Join-Path $src "config.yaml") (Join-Path $dst "config.yaml")

$stdout = Join-Path $dst "resume_stdout.log"
$stderr = Join-Path $dst "resume_stderr.log"
if (Test-Path $stdout) { Remove-Item -Force $stdout }
if (Test-Path $stderr) { Remove-Item -Force $stderr }

$runner = Join-Path $dst "resume_run.cmd"
$proc = Start-Process -FilePath $runner -WorkingDirectory $root -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
Write-Output ("restarted_pid=" + $proc.Id)
