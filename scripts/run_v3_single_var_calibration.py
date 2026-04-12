#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
V3 单变量修复校准脚本

目标：
1. 只做 V3 校准，不改 V2。
2. 按单变量方式分别扫描：
   - 挂牌率 market.initial_listing_rate
   - 按揭覆盖率 market_pulse.seed_existing_mortgage_ratio
3. 统一口径输出每组每个 seed 的 L0 / B0_order / R_order。
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
from typing import Any, Dict, List, Optional, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "config" / "baseline.yaml"
NIGHT_RUN = ROOT / "scripts" / "night_run.py"


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _extract_db_from_stdout(stdout_path: Path) -> Optional[str]:
    if not stdout_path.exists():
        return None
    lines = stdout_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for ln in reversed(lines):
        if "DB path:" in ln:
            return ln.split("DB path:", 1)[1].strip()
    return None


def _v3_base_plan(seed: int, months: int) -> Dict[str, Any]:
    return {
        "simulation": {
            "random_seed": int(seed),
            "months": int(months),
            "enable_intervention_panel": False,
            "agent": {"income_adjustment_rate": 1.10},
        },
        "macro_environment": {"override_mode": "stable"},
        "mortgage": {
            "down_payment_ratio": 0.20,
            "max_dti_ratio": 0.64,
            "annual_interest_rate": 0.034,
        },
        "market": {
            "initial_listing_rate": 0.038,
            "zones": {
                "A": {"supply_band_ratio": {"low": 0.20, "mid": 0.45, "high": 0.35}},
                "B": {"supply_band_ratio": {"low": 0.20, "mid": 0.45, "high": 0.35}},
            },
        },
        "market_pulse": {
            "enabled": True,
            "seed_existing_mortgage_ratio": 0.46,
        },
        "smart_agent": {
            "activation_batch_size": 10,
            "role_decision_optimization": {
                "adaptive_batch_size_enabled": False,
                "enable_model_dual_routing": False,
                "default_model_type": "smart",
                "gray_model_type": "smart",
            },
            "info_delay_enabled": False,
            "info_delay_ratio": 0.00,
            "info_delay_max_months": 0,
        },
    }


def _build_calibration_groups() -> List[Tuple[str, Dict[str, Any]]]:
    """
    单变量扫描：
    - listing 轴：只改 initial_listing_rate，其他保持基线
    - mortgage 轴：只改 seed_existing_mortgage_ratio，其他保持基线
    """
    groups: List[Tuple[str, Dict[str, Any]]] = []
    groups.append(("V3_BASE", {"initial_listing_rate": 0.038, "seed_existing_mortgage_ratio": 0.46}))
    groups.append(("V3_L020", {"initial_listing_rate": 0.020, "seed_existing_mortgage_ratio": 0.46}))
    groups.append(("V3_L030", {"initial_listing_rate": 0.030, "seed_existing_mortgage_ratio": 0.46}))
    groups.append(("V3_L050", {"initial_listing_rate": 0.050, "seed_existing_mortgage_ratio": 0.46}))
    groups.append(("V3_M035", {"initial_listing_rate": 0.038, "seed_existing_mortgage_ratio": 0.35}))
    groups.append(("V3_M040", {"initial_listing_rate": 0.038, "seed_existing_mortgage_ratio": 0.40}))
    groups.append(("V3_M055", {"initial_listing_rate": 0.038, "seed_existing_mortgage_ratio": 0.55}))
    return groups


def _run_one(
    group_id: str,
    seed: int,
    months: int,
    agent_count: int,
    out_dir: Path,
    knobs: Dict[str, Any],
) -> Dict[str, Any]:
    case_name = f"{group_id}_s{seed}_m{months}"
    case_dir = out_dir / case_name
    case_dir.mkdir(parents=True, exist_ok=True)

    plan = _v3_base_plan(seed=seed, months=months)
    plan["market"]["initial_listing_rate"] = float(knobs["initial_listing_rate"])
    plan["market_pulse"]["seed_existing_mortgage_ratio"] = float(knobs["seed_existing_mortgage_ratio"])

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
        str(months),
        "--agent-count",
        str(agent_count),
    ]

    env = dict(os.environ)
    prev_py = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not prev_py else (str(ROOT) + ";" + prev_py)

    with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
        proc = subprocess.run(cmd, cwd=str(ROOT), stdout=out, stderr=err, env=env)

    db_path = _extract_db_from_stdout(stdout)
    return {
        "group": group_id,
        "seed": int(seed),
        "status": "success" if proc.returncode == 0 else "failed",
        "exit_code": int(proc.returncode),
        "run_dir": str(case_dir.resolve()),
        "plan_path": str(plan_path.resolve()),
        "stdout": str(stdout.resolve()),
        "stderr": str(stderr.resolve()),
        "db_path": db_path,
        "initial_listing_rate": float(knobs["initial_listing_rate"]),
        "seed_existing_mortgage_ratio": float(knobs["seed_existing_mortgage_ratio"]),
    }


def _count_err(stderr_path: Path, pat: str) -> int:
    if not stderr_path.exists():
        return 0
    txt = stderr_path.read_text(encoding="utf-8", errors="ignore")
    return txt.count(pat)


def _compute_metrics(db_path: Path) -> Dict[str, Any]:
    if not db_path.exists():
        return {
            "l0": 0,
            "b0_role": 0,
            "b0_order": 0,
            "r_order": 0.0,
            "matches_m1": 0,
            "orders_m1": 0,
            "tx_m1": 0,
        }
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        l0 = int(cur.execute("SELECT COUNT(*) FROM properties_market WHERE listing_month=0 AND status='for_sale'").fetchone()[0] or 0)
        b0_role = int(
            cur.execute(
                """
                SELECT COUNT(DISTINCT agent_id)
                FROM decision_logs
                WHERE event_type='ROLE_DECISION' AND month=1 AND decision IN ('BUYER','BUYER_SELLER')
                """
            ).fetchone()[0]
            or 0
        )
        b0_order = int(cur.execute("SELECT COUNT(DISTINCT buyer_id) FROM transaction_orders WHERE created_month=1").fetchone()[0] or 0)
        matches_m1 = int(cur.execute("SELECT COUNT(*) FROM property_buyer_matches WHERE month=1").fetchone()[0] or 0)
        orders_m1 = int(cur.execute("SELECT COUNT(*) FROM transaction_orders WHERE created_month=1").fetchone()[0] or 0)
        tx_m1 = int(cur.execute("SELECT COUNT(*) FROM transactions WHERE month=1").fetchone()[0] or 0)
    finally:
        conn.close()
    r_order = (float(b0_order) / float(l0)) if l0 > 0 else 0.0
    return {
        "l0": l0,
        "b0_role": b0_role,
        "b0_order": b0_order,
        "r_order": round(r_order, 4),
        "matches_m1": matches_m1,
        "orders_m1": orders_m1,
        "tx_m1": tx_m1,
    }


def _write_outputs(out_dir: Path, rows: List[Dict[str, Any]]) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_type": "v3_single_var_calibration",
        "run_count": len(rows),
        "success_count": sum(1 for r in rows if r.get("status") == "success"),
        "failed_count": sum(1 for r in rows if r.get("status") != "success"),
        "runs": rows,
    }
    (out_dir / "calibration_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# V3 单变量修复校准",
        "",
        f"- 生成时间: {payload['generated_at']}",
        "",
        "| 组别 | seed | listing_rate | mortgage_ratio | 状态 | L0 | B0_order | R_order | matches_m1 | orders_m1 | tx_m1 | conn_err |",
        "| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in rows:
        lines.append(
            f"| {r['group']} | {r['seed']} | {r['initial_listing_rate']:.3f} | {r['seed_existing_mortgage_ratio']:.2f} | "
            f"{r['status']} | {r['l0']} | {r['b0_order']} | {r['r_order']:.4f} | {r['matches_m1']} | {r['orders_m1']} | {r['tx_m1']} | {r['stderr_connection_error']} |"
        )
    (out_dir / "calibration_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V3 single-variable calibration sweep.")
    parser.add_argument("--months", type=int, default=1)
    parser.add_argument("--agent-count", type=int, default=50)
    parser.add_argument("--seeds", nargs="+", type=int, default=[606, 607, 608])
    parser.add_argument("--out-dir", default="")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).resolve() if args.out_dir else ROOT / "results" / "market_state_matrix" / f"v3_single_var_calib_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []
    groups = _build_calibration_groups()
    for group_id, knobs in groups:
        for seed in args.seeds:
            run = _run_one(
                group_id=group_id,
                seed=int(seed),
                months=int(args.months),
                agent_count=int(args.agent_count),
                out_dir=out_dir,
                knobs=knobs,
            )
            db_path = Path(str(ROOT / run["db_path"])) if run.get("db_path") else Path("")
            if run.get("db_path") and Path(run["db_path"]).is_absolute():
                db_path = Path(run["db_path"])
            metrics = _compute_metrics(db_path if db_path else Path(""))
            run.update(metrics)
            run["stderr_connection_error"] = _count_err(Path(run["stderr"]), "Connection error")
            run["stderr_circuit_open"] = _count_err(Path(run["stderr"]), "circuit_open")
            run["stderr_breaker_open"] = _count_err(Path(run["stderr"]), "Async LLM breaker open")
            rows.append(run)
            print(
                f"[{group_id}] seed={seed} status={run['status']} "
                f"L0={run['l0']} B0_order={run['b0_order']} R_order={run['r_order']:.4f}"
            )

    _write_outputs(out_dir=out_dir, rows=rows)
    print(f"batch={out_dir}")
    return 0 if all(r["status"] == "success" for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())

