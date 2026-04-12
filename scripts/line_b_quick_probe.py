#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
课题线 B 快速实验环境

用途：
1. init-only：只做初始化，快速检查 L0 / 初始化供给窗口 / 初始挂牌增量，不进入完整月度实验。
2. precheck：使用 mock LLM 跑 1 个月低 token 预检，快速验证 forced_role / B0 / L0 / R_order 是否值得进入正式批次。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_line_b_forced_role import (
    BASELINE,
    LINE_B_ROOT,
    analyze_db,
    apply_overrides,
    build_plan,
)

QUICK_ROOT = LINE_B_ROOT / "quick_probe"
INIT_LISTINGS_RE = re.compile(r"Created\s+(?P<count>\d+)\s+initial listings", re.IGNORECASE)


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data if isinstance(data, dict) else {}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fast line-B init/precheck probes.")
    parser.add_argument("--mode", choices=["init-only", "precheck"], default="init-only")
    parser.add_argument("--months", type=int, default=1)
    parser.add_argument("--agent-count", type=int, default=50)
    parser.add_argument("--groups", nargs="+", default=["V3"])
    parser.add_argument("--seeds", nargs="+", type=int, default=[606, 607, 608])
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--mock-llm", action="store_true", default=False)
    parser.add_argument("--initial-listing-rate", type=float, default=None)
    parser.add_argument("--quota-buyer", type=int, default=None)
    parser.add_argument("--quota-seller", type=int, default=None)
    parser.add_argument("--quota-buyer-seller", type=int, default=None)
    parser.add_argument("--init-min-for-sale-floor-zone-a", type=int, default=None)
    parser.add_argument("--init-min-for-sale-floor-zone-b", type=int, default=None)
    parser.add_argument("--init-min-tradable-floor-total", type=int, default=None)
    parser.add_argument("--init-min-for-sale-ratio-zone-a", type=float, default=None)
    parser.add_argument("--init-min-for-sale-ratio-zone-b", type=float, default=None)
    parser.add_argument("--disable-init-multi-owner-listings", action="store_true")
    parser.add_argument("--enable-profiled-market-mode", action="store_true")
    parser.add_argument("--profile-pack-path", default="")
    parser.add_argument("--profile-background-library-path", default="")
    parser.add_argument("--profile-experiment-mode", default="abundant")
    parser.add_argument("--enable-hard-bucket-matcher", action="store_true")
    parser.add_argument("--hard-bucket-include-soft-buckets", action="store_true")
    parser.add_argument("--hard-bucket-require-profiled-buyer", action="store_true")
    parser.add_argument("--disable-hard-bucket-strict-unmapped", action="store_true")
    parser.add_argument("--enable-mock-stub-select", action="store_true")
    parser.add_argument("--worker", action="store_true", default=False)
    parser.add_argument("--plan-path", default="")
    parser.add_argument("--db-path", default="")
    return parser


def _run_worker(args: argparse.Namespace) -> int:
    if args.mock_llm:
        os.environ["LLM_MOCK_MODE"] = "true"

    from config.config_loader import SimulationConfig
    from simulation_runner import SimulationRunner

    plan_path = Path(args.plan_path)
    db_path = Path(args.db_path)
    config = SimulationConfig(str(BASELINE))
    _deep_merge(config._config, _load_yaml(plan_path))
    config.update("simulation.enable_intervention_panel", False)

    runner = SimulationRunner(
        agent_count=int(args.agent_count),
        months=int(args.months),
        seed=int(args.seeds[0]) if isinstance(args.seeds, list) and args.seeds else int(config.get("simulation.random_seed", 42) or 42),
        resume=False,
        config=config,
        db_path=str(db_path),
    )
    try:
        if args.mode == "init-only":
            runner.initialize()
        else:
            runner.run_one_month()
    finally:
        runner.close()
    print(f"db_path={db_path}")
    return 0


def _extract_created_initial_listings(log_path: Path) -> int:
    if not log_path.exists():
        return 0
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    match = INIT_LISTINGS_RE.search(text)
    if not match:
        return 0
    return int(match.group("count") or 0)


def _fetch_zone_counts(cur: sqlite3.Cursor, where_sql: str, params: tuple = ()) -> Dict[str, int]:
    query = f"""
        SELECT ps.zone, COUNT(*)
        FROM properties_market pm
        JOIN properties_static ps ON ps.property_id = pm.property_id
        WHERE {where_sql}
        GROUP BY ps.zone
        ORDER BY ps.zone
    """
    rows = cur.execute(query, params).fetchall() or []
    result = {"A": 0, "B": 0}
    for zone, count in rows:
        result[str(zone)] = int(count or 0)
    return result


def _summarize_probe(db_path: Path, run_dir: Path, months: int) -> Dict[str, Any]:
    metrics = analyze_db(db_path, months=months, run_dir=run_dir)
    summary: Dict[str, Any] = {
        "metrics": metrics,
        "created_initial_listings": _extract_created_initial_listings(run_dir / "simulation_run.log"),
    }
    if not db_path.exists():
        return summary

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        summary["init_listing_rows_total"] = int(
            cur.execute(
                """
                SELECT COUNT(*)
                FROM properties_market
                WHERE owner_id != -1 AND status='for_sale' AND listing_month=0
                """
            ).fetchone()[0]
            or 0
        )
        summary["init_listing_rows_by_zone"] = _fetch_zone_counts(
            cur,
            "pm.owner_id != -1 AND pm.status='for_sale' AND pm.listing_month=0",
        )
        summary["for_sale_rows_all_by_zone"] = _fetch_zone_counts(
            cur,
            "pm.owner_id != -1 AND pm.status='for_sale'",
        )
        summary["orders_m1_distinct_buyers"] = int(
            cur.execute(
                "SELECT COUNT(DISTINCT buyer_id) FROM transaction_orders WHERE created_month=1"
            ).fetchone()[0]
            or 0
        )
        summary["orders_m1_total"] = int(
            cur.execute("SELECT COUNT(*) FROM transaction_orders WHERE created_month=1").fetchone()[0]
            or 0
        )
        summary["transactions_m1_total"] = int(
            cur.execute("SELECT COUNT(*) FROM transactions WHERE month=1").fetchone()[0] or 0
        )
        role_rows = cur.execute(
            """
            SELECT decision, COUNT(DISTINCT agent_id)
            FROM decision_logs
            WHERE month=1 AND event_type='ROLE_DECISION'
            GROUP BY decision
            ORDER BY decision
            """
        ).fetchall()
        summary["role_decision_m1"] = {str(role): int(count or 0) for role, count in role_rows}
    finally:
        conn.close()
    return summary


def _worker_cmd(args: argparse.Namespace, plan_path: Path, db_path: Path, seed: int) -> List[str]:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--mode",
        str(args.mode),
        "--plan-path",
        str(plan_path),
        "--db-path",
        str(db_path),
        "--agent-count",
        str(int(args.agent_count)),
        "--months",
        str(int(args.months)),
        "--seeds",
        str(int(seed)),
    ]
    if args.mock_llm:
        cmd.append("--mock-llm")
    return cmd


def _run_case(batch_dir: Path, args: argparse.Namespace, group_id: str, seed: int) -> Dict[str, Any]:
    case_name = f"{group_id}_s{int(seed)}_m{int(args.months)}_a{int(args.agent_count)}"
    case_dir = batch_dir / case_name
    if case_dir.exists():
        shutil.rmtree(case_dir, ignore_errors=True)
    case_dir.mkdir(parents=True, exist_ok=True)

    plan = build_plan(group_id=group_id, seed=seed, months=args.months, agent_count=args.agent_count)
    plan = apply_overrides(plan, args)
    plan_path = case_dir / "plan.yaml"
    _write_yaml(plan_path, plan)

    db_path = case_dir / "simulation.db"
    stdout_path = case_dir / "stdout.log"
    stderr_path = case_dir / "stderr.log"
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    prev_py = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not prev_py else f"{ROOT}{os.pathsep}{prev_py}"

    cmd = _worker_cmd(args=args, plan_path=plan_path, db_path=db_path, seed=seed)
    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            stdout=stdout_handle,
            stderr=stderr_handle,
            env=env,
        )

    summary = _summarize_probe(db_path=db_path, run_dir=case_dir, months=int(args.months))
    return {
        "group": str(group_id),
        "seed": int(seed),
        "mode": str(args.mode),
        "status": "success" if proc.returncode == 0 else "failed",
        "exit_code": int(proc.returncode),
        "mock_llm": bool(args.mock_llm),
        "case_dir": str(case_dir.resolve()),
        "plan_path": str(plan_path.resolve()),
        "db_path": str(db_path.resolve()),
        "stdout": str(stdout_path.resolve()),
        "stderr": str(stderr_path.resolve()),
        **summary,
    }


def _write_batch_outputs(batch_dir: Path, runs: List[Dict[str, Any]], args: argparse.Namespace) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_type": "line_b_quick_probe",
        "mode": str(args.mode),
        "mock_llm": bool(args.mock_llm),
        "months": int(args.months),
        "agent_count": int(args.agent_count),
        "runs": runs,
    }
    (batch_dir / "batch_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# 课题线B 快速实验摘要",
        "",
        f"- 模式: `{args.mode}`",
        f"- mock_llm: `{bool(args.mock_llm)}`",
        f"- 月数: `{int(args.months)}`",
        "",
        "| 组别 | seed | 状态 | L0 | B0_order | R_order | init_rows | extra_init_listings | A0 | B0 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for run in runs:
        metrics = run.get("metrics", {}) or {}
        by_zone = run.get("init_listing_rows_by_zone", {}) or {}
        lines.append(
            f"| {run['group']} | {run['seed']} | {run['status']} | "
            f"{metrics.get('l0', 0)} | {metrics.get('b0_order', 0)} | {float(metrics.get('r_order', 0.0) or 0.0):.4f} | "
            f"{run.get('init_listing_rows_total', 0)} | {run.get('created_initial_listings', 0)} | "
            f"{int(by_zone.get('A', 0) or 0)} | {int(by_zone.get('B', 0) or 0)} |"
        )
    (batch_dir / "batch_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.worker:
        return _run_worker(args)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = (
        Path(args.out_dir).resolve()
        if args.out_dir
        else QUICK_ROOT / f"{args.mode.replace('-', '_')}_{stamp}"
    )
    batch_dir.mkdir(parents=True, exist_ok=True)

    runs: List[Dict[str, Any]] = []
    for group_id in args.groups:
        for seed in args.seeds:
            run = _run_case(batch_dir=batch_dir, args=args, group_id=str(group_id), seed=int(seed))
            runs.append(run)
            metrics = run.get("metrics", {}) or {}
            print(
                f"[{group_id}] seed={seed} status={run['status']} "
                f"L0={metrics.get('l0', 0)} B0_order={metrics.get('b0_order', 0)} "
                f"R_order={float(metrics.get('r_order', 0.0) or 0.0):.4f} "
                f"init_rows={run.get('init_listing_rows_total', 0)}"
            )

    _write_batch_outputs(batch_dir=batch_dir, runs=runs, args=args)
    print(f"batch={batch_dir}")
    return 0 if all(item["status"] == "success" for item in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
