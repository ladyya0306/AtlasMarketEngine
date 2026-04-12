param(
  [string]$ProjectRoot = 'D:\GitProj\visual_real_estate'
)

Set-Location $ProjectRoot
$ErrorActionPreference = 'Stop'

function Invoke-Batch {
  param(
    [string]$OutDir,
    [string[]]$Seeds,
    [string[]]$Scenarios,
    [int]$Months = 6,
    [int]$AgentCount = 120
  )
  $seedArgs = $Seeds -join ' '
  $scArgs = $Scenarios -join ' '
  Write-Host "[AUTOPILOT] start batch out=$OutDir seeds=$seedArgs scenarios=$scArgs"
  python scripts/run_macro_matrix.py --months $Months --agent-count $AgentCount --seeds $Seeds --scenarios $Scenarios --out-dir $OutDir
}

function Invoke-GateCheck {
  param(
    [string]$MatrixDir,
    [string]$OutJson
  )
  $py = @"
import json,re,sqlite3
from pathlib import Path
m=Path(r'''$MatrixDir''')
obj=json.loads((m/'matrix_summary.json').read_text(encoding='utf-8'))
rows=[]
for r in obj['results']:
    txt=Path(r['stdout']).read_text(encoding='utf-8',errors='ignore')
    run_m=re.search(r'Run directory:\s*([^\r\n]+)',txt)
    db_m=re.search(r'DB path:\s*([^\r\n]+)',txt)
    if not run_m or not db_m:
        raise RuntimeError(f"missing run/db markers in stdout: {r.get('stdout')}")
    run=run_m.group(1).replace('\\\\','/')
    db=db_m.group(1).replace('\\\\','/')
    rp=Path(run); dp=Path(db)
    if not rp.is_absolute(): rp=Path('.')/rp
    if not dp.is_absolute(): dp=Path('.')/dp
    con=sqlite3.connect(str(dp)); c=con.cursor()
    all_orders=c.execute("select count(*) from transaction_orders").fetchone()[0]
    filled=c.execute("select count(*) from transaction_orders where status='filled'").fetchone()[0]
    pending=c.execute("select count(*) from transaction_orders where status='pending_settlement'").fetchone()[0]
    b_den=c.execute("select count(*) from transaction_orders o join properties_static p on p.property_id=o.property_id where upper(p.zone)='B'").fetchone()[0]
    b_num=c.execute("select count(*) from transaction_orders o join properties_static p on p.property_id=o.property_id where upper(p.zone)='B' and o.status in ('filled','pending_settlement')").fetchone()[0]
    dti=c.execute("select count(*) from transaction_orders where lower(coalesce(close_reason,'')) like '%dti exceeded%'").fetchone()[0]
    unv=c.execute("select count(*) from transaction_orders where lower(coalesce(close_reason,'')) like '%property unavailable%'").fetchone()[0]
    con.close()
    total_tx=filled+pending
    t1=max(10,120*0.12); t2=max(3,all_orders*0.08); t3=max(1,total_tx*0.05)
    g1=total_tx>=t1
    g2=b_den>=t2
    g3=b_num>=t3
    g4=(b_num/b_den)>=0.10 if b_den>0 else False
    g9=dti==0
    g10=unv==0
    key=(r.get('exit_code',1)==0 and g1 and g2 and g3 and g4 and g9 and g10)
    rows.append({'scenario':r['scenario'],'seed':r['seed'],'key_pass':key,'tx':total_tx,'b_num':b_num,'b_den':b_den,'dti':dti,'unv':unv})
pass_cnt=sum(1 for x in rows if x['key_pass'])
out={'matrix_dir':str(m),'pass':pass_cnt,'total':len(rows),'pass_ratio':(pass_cnt/len(rows) if rows else 0.0),'rows':rows}
Path(r'''$OutJson''').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
print(pass_cnt, len(rows), out['pass_ratio'])
"@
  $res = $py | python -
  Write-Host "[AUTOPILOT] gate result: $res"
}

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$nightRoot = "results/night_plan/autopilot_loop_$stamp"
New-Item -ItemType Directory -Force -Path $nightRoot | Out-Null

# Round 1: S-专项补强（新增seed）
$r1 = "$nightRoot/round1_sminus_seeds606_707"
Invoke-Batch -OutDir $r1 -Seeds @('606','707') -Scenarios @('S-')
$g1 = "$r1/gate_check.json"
Invoke-GateCheck -MatrixDir $r1 -OutJson $g1
$g1Obj = Get-Content $g1 -Raw | ConvertFrom-Json

if ([double]$g1Obj.pass_ratio -lt 0.7) {
  # Auto remediation: add two more seeds, same scenario
  $r1b = "$nightRoot/round1b_sminus_seeds808_909"
  Invoke-Batch -OutDir $r1b -Seeds @('808','909') -Scenarios @('S-')
  $g1b = "$r1b/gate_check.json"
  Invoke-GateCheck -MatrixDir $r1b -OutJson $g1b
}

# Round 2: full-cycle single-seed verification if no hard failures in round1 gate
if ([double]$g1Obj.pass_ratio -ge 0.5) {
  $r2 = "$nightRoot/round2_fullcycle_seed606"
  Invoke-Batch -OutDir $r2 -Seeds @('606') -Scenarios @('S+','N0','S-')
  $g2 = "$r2/gate_check.json"
  Invoke-GateCheck -MatrixDir $r2 -OutJson $g2
}

Write-Host "[AUTOPILOT] completed. root=$nightRoot"
