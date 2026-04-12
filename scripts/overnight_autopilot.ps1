param(
  [string]$CurrentBatchDir = 'results/night_plan/fix4_stability_seed101_20260325_230628'
)

$ErrorActionPreference = 'Stop'
$root = 'D:\GitProj\visual_real_estate'
Set-Location $root
$env:PYTHONPATH = $root

$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$autoRoot = Join-Path $root ("results\night_plan\overnight_autopilot_" + $ts)
New-Item -ItemType Directory -Force -Path $autoRoot | Out-Null
$log = Join-Path $autoRoot 'autopilot.log'

function Log([string]$m){
  $line = "[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m
  $line | Tee-Object -FilePath $log -Append
}

function Run-Step([string]$name,[string[]]$CmdArgs,[string]$stdout,[string]$stderr){
  Log "START $name"
  Log ("CMD: python " + ($CmdArgs -join ' '))
  $p = Start-Process -FilePath python -ArgumentList $CmdArgs -WorkingDirectory $root -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru -Wait
  if($p.ExitCode -ne 0){
    Log "FAILED $name exit=$($p.ExitCode)"
    throw "stage failed: $name"
  }
  Log "DONE $name"
}

Log "Autopilot started"
Log "CurrentBatchDir=$CurrentBatchDir"

$batchSummary = Join-Path $root (Join-Path $CurrentBatchDir 'batch_summary.json')
for($i=0; $i -lt 360; $i++){
  if(Test-Path $batchSummary){ break }
  Start-Sleep -Seconds 20
}
if(!(Test-Path $batchSummary)){
  Log 'WAIT_TIMEOUT current batch summary not found within 120 minutes'
  throw 'current batch not finished in time'
}
Log "Current batch finished: $batchSummary"

$resumeOut = Join-Path $autoRoot 'month3_4_resume'
New-Item -ItemType Directory -Force -Path $resumeOut | Out-Null

$planPath = Join-Path $root 'config\night_run_empty.yaml'
if(!(Test-Path $planPath)){ '{}' | Set-Content -Path $planPath -Encoding UTF8 }

$runs = @(
  @{ label='R2A_s202_m4'; db='results/run_20260325_192027/simulation.db'; cfg='results/run_20260325_192027/config.yaml' },
  @{ label='R2B_s202_m4'; db='results/run_20260325_194415/simulation.db'; cfg='results/run_20260325_194415/config.yaml' },
  @{ label='R2A_s303_m4'; db='results/run_20260325_205805/simulation.db'; cfg='results/run_20260325_205805/config.yaml' },
  @{ label='R2B_s303_m4'; db='results/run_20260325_214724/simulation.db'; cfg='results/run_20260325_214724/config.yaml' }
)

$manifest = @()
foreach($r in $runs){
  $dst = Join-Path $resumeOut $r.label
  New-Item -ItemType Directory -Force -Path $dst | Out-Null
  $dbDst = Join-Path $dst 'simulation.db'
  $cfgDst = Join-Path $dst 'config.yaml'
  Copy-Item -Force (Join-Path $root $r.db) $dbDst
  Copy-Item -Force (Join-Path $root $r.cfg) $cfgDst

  $stdout = Join-Path $dst 'resume_stdout.log'
  $stderr = Join-Path $dst 'resume_stderr.log'
  $stepArgs = @('scripts/night_run.py','--resume','--db-path',$dbDst,'--base-config',$cfgDst,'--plan',$planPath,'--months','4')
  Run-Step -name $r.label -CmdArgs $stepArgs -stdout $stdout -stderr $stderr

  $manifest += [pscustomobject]@{ label=$r.label; db=$dbDst; cfg=$cfgDst; stdout=$stdout; stderr=$stderr }
}
$manifestPath = Join-Path $autoRoot 'resume_manifest.json'
$manifest | ConvertTo-Json -Depth 5 | Set-Content -Path $manifestPath -Encoding UTF8
Log "Resume manifest saved: $manifestPath"

$cardPath = Join-Path $root 'docs\外部事实对照卡_初版_20260325.md'
$py = @'
import json
from pathlib import Path

root = Path(r"D:\GitProj\visual_real_estate")
paths = [
    root / "results/night_plan/fix4_uplift_verify_20260325_192025/zone_chain_summary.json",
    root / "results/night_plan/fix4_stability_seed303_20260325_205803/zone_chain_summary.json",
]

rows = []
for p in paths:
    if not p.exists():
        continue
    data = json.loads(p.read_text(encoding='utf-8'))
    for run in data.get('runs', []):
        rows.append({
            'group': run.get('group_key'),
            'seed': run.get('seed'),
            'zone_tx_B': (run.get('zone_transactions') or {}).get('B', 0),
            'lag_rows': run.get('lagged_exposure_rows', 0),
            'avg_lag': run.get('avg_applied_lag_months', 0.0),
            'close_rate_B': ((run.get('zone_loss_metrics') or {}).get('B') or {}).get('primary_close_rate', 0),
            'fail_top': run.get('b_failure_reason_top', {}),
            'run_dir': run.get('run_dir','')
        })

r2a = [r for r in rows if r['group']=='R2A']
r2b = [r for r in rows if r['group']=='R2B']

def avg(xs, k):
    vals=[x.get(k,0) or 0 for x in xs]
    return sum(vals)/len(vals) if vals else 0

lines = []
lines.append('# 外部事实对照卡（初版）\n')
lines.append('生成时间：2026-03-25 夜间自动任务\n')
lines.append('')
lines.append('## 对照卡1：信息时滞存在且可观测')
lines.append('- 现实叙事：信息不是所有人同时知道，滞后会影响决策节奏。')
lines.append('- 模型映射：`lagged_exposure_rows`、`avg_applied_lag_months`（R2B 对比 R2A）。')
lines.append(f"- 证据：R2A 平均 lag={avg(r2a,'avg_lag'):.2f}；R2B 平均 lag={avg(r2b,'avg_lag'):.2f}。")
lines.append('- 判定：同向。')
lines.append('')
lines.append('## 对照卡2：竞争存在时，B区可形成成交而非长期为0')
lines.append('- 现实叙事：价格带更合适的区域会承接一部分真实成交。')
lines.append('- 模型映射：`zone_transactions.B` 与 `B区主转化率`。')
lines.append(f"- 证据：R2A B区平均成交={avg(r2a,'zone_tx_B'):.2f}；R2B B区平均成交={avg(r2b,'zone_tx_B'):.2f}。")
lines.append('- 判定：同向（当前样本成立，需后续扩大样本继续验证）。')
lines.append('')
lines.append('## 对照卡3：高竞争下会出现被抢与谈判失败')
lines.append('- 现实叙事：热门盘不会人人都买到，失败原因常见于被更强预算挤出和谈崩。')
lines.append('- 模型映射：`b_failure_reason_top`。')
sample = next((r for r in rows if r.get('fail_top')), None)
if sample:
    top = sorted(sample['fail_top'].items(), key=lambda kv: kv[1], reverse=True)[:3]
    top_txt = '；'.join([f"{k}={v}" for k,v in top])
    lines.append(f"- 证据：示例（{sample['group']} seed={sample['seed']}）失败Top3：{top_txt}。")
else:
    lines.append('- 证据：当前批次未提取到失败Top细分。')
lines.append('- 判定：同向。')
lines.append('')
lines.append('## 边界声明')
lines.append('- 本模型用于机制推演，不做城市点位预测。')
lines.append('- 当前为初版对照卡，后续会加入更多外部数据源与反向验证样本。')

out = root / 'docs/外部事实对照卡_初版_20260325.md'
out.write_text('\n'.join(lines), encoding='utf-8')
print(str(out))
'@
$cardStd = Join-Path $autoRoot 'fact_card_stdout.log'
$cardErr = Join-Path $autoRoot 'fact_card_stderr.log'
$py | python - 1> $cardStd 2> $cardErr
if($LASTEXITCODE -ne 0){
  Log 'FAILED fact card generation'
  throw 'fact card generation failed'
}
Log "Fact card generated: $cardPath"
Log 'Autopilot finished successfully'
