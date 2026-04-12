#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
R 门槛单变量调参脚本（1个月）

用途：
1. 指定组别（V2 或 V3）
2. 只调整一个参数路径（如 simulation.agent.income_adjustment_rate）
3. 对多个 seed 跑 1 个月并输出 R_order 门槛结果
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_market_state_matrix import _market_state_plan  # type: ignore

BASELINE = ROOT / "config" / "baseline.yaml"
NIGHT_RUN = ROOT / "scripts" / "night_run.py"


def _set_nested(d: Dict[str, Any], path: str, value: Any) -> None:
    keys = path.split(".")
    cur = d
    for k in keys[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _extract_db(stdout: Path) -> str:
    if not stdout.exists():
        return ""
    for ln in reversed(stdout.read_text(encoding="utf-8", errors="ignore").splitlines()):
        if "DB path:" in ln:
            return ln.split("DB path:", 1)[1].strip()
    return ""


def _metrics(db_path: Path) -> Dict[str, Any]:
    if not db_path.exists():
        return {"l0": 0, "b0_order": 0, "r_order": 0.0, "matches_m1": 0, "orders_m1": 0, "tx_m1": 0}
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        l0 = int(cur.execute("SELECT COUNT(*) FROM properties_market WHERE listing_month=0 AND status='for_sale'").fetchone()[0] or 0)
        b0 = int(cur.execute("SELECT COUNT(DISTINCT buyer_id) FROM transaction_orders WHERE created_month=1").fetchone()[0] or 0)
        matches = int(cur.execute("SELECT COUNT(*) FROM property_buyer_matches WHERE month=1").fetchone()[0] or 0)
        orders = int(cur.execute("SELECT COUNT(*) FROM transaction_orders WHERE created_month=1").fetchone()[0] or 0)
        tx = int(cur.execute("SELECT COUNT(*) FROM transactions WHERE month=1").fetchone()[0] or 0)
    finally:
        conn.close()
    return {
        "l0": l0,
        "b0_order": b0,
        "r_order": round((b0 / l0) if l0 > 0 else 0.0, 4),
        "matches_m1": matches,
        "orders_m1": orders,
        "tx_m1": tx,
    }


def _pass(group: str, m: Dict[str, Any]) -> bool:
    if m["matches_m1"] <= 0 or m["orders_m1"] <= 0:
        return False
    if group.upper().startswith("V2"):
        return m["r_order"] < 1.0
    if group.upper().startswith("V3"):
        return m["r_order"] > 1.0
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Single-variable R-gate tuning.")
    parser.add_argument("--group", required=True, choices=["V2", "V3"])
    parser.add_argument("--var-path", required=True, help="Dot path in plan, e.g. simulation.agent.income_adjustment_rate")
    parser.add_argument("--values", nargs="+", required=True, help="Values list, e.g. 1.10 1.20 1.30")
    parser.add_argument("--seeds", nargs="+", type=int, default=[606, 607, 608])
    parser.add_argument("--months", type=int, default=1)
    parser.add_argument("--agent-count", type=int, default=50)
    parser.add_argument("--out-dir", default="")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).resolve() if args.out_dir else ROOT / "results" / "market_state_matrix" / f"r_gate_tuning_{args.group}_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []
    for raw in args.values:
        # parse number if possible
        try:
            if "." in raw:
                val: Any = float(raw)
            else:
                val = int(raw)
        except Exception:
            val = raw
        for seed in args.seeds:
            plan = _market_state_plan(args.group, seed, args.months)
            _set_nested(plan, args.var_path, val)
            case = f"{args.group}_{args.var_path.replace('.','_')}_{str(val).replace('.','p')}_s{seed}_m{args.months}"
            case_dir = out_dir / case
            case_dir.mkdir(parents=True, exist_ok=True)
            plan_path = case_dir / "plan.yaml"
            _write_yaml(plan_path, plan)
            stdout = case_dir / "stdout.log"
            stderr = case_dir / "stderr.log"

            cmd = [
                sys.executable,
                str(NIGHT_RUN),
                "--base-config",
                str(BASELINE),
                "--plan",
                str(plan_path),
                "--seed",
                str(seed),
                "--months",
                str(args.months),
                "--agent-count",
                str(args.agent_count),
            ]
            env = dict(os.environ)
            prev_py = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = str(ROOT) if not prev_py else (str(ROOT) + ";" + prev_py)
            with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
                proc = subprocess.run(cmd, cwd=str(ROOT), stdout=out, stderr=err, env=env)
            db_rel = _extract_db(stdout)
            db_abs = (ROOT / db_rel).resolve() if db_rel else Path("")
            m = _metrics(db_abs)
            row = {
                "group": args.group,
                "seed": seed,
                "var_path": args.var_path,
                "value": val,
                "status": "success" if proc.returncode == 0 else "failed",
                "exit_code": proc.returncode,
                "run_dir": str(case_dir.resolve()),
                "plan_path": str(plan_path.resolve()),
                "db_path": db_rel,
                **m,
                "r_gate_pass": _pass(args.group, m),
            }
            rows.append(row)
            print(f"[{args.group}] {args.var_path}={val} seed={seed} R={m['r_order']:.4f} pass={row['r_gate_pass']}")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "group": args.group,
        "var_path": args.var_path,
        "values": args.values,
        "seeds": args.seeds,
        "rows": rows,
    }
    (out_dir / "tuning_summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"batch={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

