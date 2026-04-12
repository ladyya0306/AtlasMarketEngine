param([string]$Root='results/night_plan/autopilot_loop_20260401_005021')
Set-Location 'D:\GitProj\visual_real_estate'
$ErrorActionPreference='Stop'
function Gate($MatrixDir,$OutJson){
$py=@"
import json,re,sqlite3
from pathlib import Path
m=Path(r'''$MatrixDir''')
obj=json.loads((m/'matrix_summary.json').read_text(encoding='utf-8'))
rows=[]
for r in obj['results']:
    txt=Path(r['stdout']).read_text(encoding='utf-8',errors='ignore')
    rm=re.search(r'Run directory:\s*([^\r\n]+)',txt)
    dm=re.search(r'DB path:\s*([^\r\n]+)',txt)
    if not rm or not dm:
        raise RuntimeError(f"missing markers in {r['stdout']}")
    db=dm.group(1).replace('\\\\','/')
    dp=Path(db)
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
    key=(r.get('exit_code',1)==0 and total_tx>=t1 and b_den>=t2 and b_num>=t3 and (b_num/b_den>=0.10 if b_den>0 else False) and dti==0 and unv==0)
    rows.append({'scenario':r['scenario'],'seed':r['seed'],'key_pass':key,'tx':total_tx,'b_num':b_num,'b_den':b_den,'dti':dti,'unv':unv})
pass_cnt=sum(1 for x in rows if x['key_pass'])
out={'matrix_dir':str(m),'pass':pass_cnt,'total':len(rows),'pass_ratio':(pass_cnt/len(rows) if rows else 0.0),'rows':rows}
Path(r'''$OutJson''').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
print(out['pass'],out['total'],out['pass_ratio'])
"@
$py | python -
}

$r1 = Join-Path $Root 'round1_sminus_seeds606_707'
$g1 = Join-Path $r1 'gate_check.json'
if(!(Test-Path $g1)){ Gate $r1 $g1 | Out-Host }
$g1o = Get-Content $g1 -Raw | ConvertFrom-Json

if([double]$g1o.pass_ratio -lt 0.7){
  $r1b = Join-Path $Root 'round1b_sminus_seeds808_909'
  python scripts/run_macro_matrix.py --months 6 --agent-count 120 --seeds 808 909 --scenarios S- --out-dir $r1b
  $g1b = Join-Path $r1b 'gate_check.json'
  Gate $r1b $g1b | Out-Host
}

if([double]$g1o.pass_ratio -ge 0.5){
  $r2 = Join-Path $Root 'round2_fullcycle_seed606'
  python scripts/run_macro_matrix.py --months 6 --agent-count 120 --seeds 606 --scenarios S+ N0 S- --out-dir $r2
  $g2 = Join-Path $r2 'gate_check.json'
  Gate $r2 $g2 | Out-Host
}
Write-Host "[AUTOPILOT-CONT] done root=$Root"
