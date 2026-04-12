#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
连续阶段最小矩阵脚本

目标：
1. 用最小矩阵验证“上升期 / 转折期 / 下行期”的量价趋势是否与全国现实大体同向。
2. 不改核心交易代码，只通过外生场景参数、市场环境与开发商设定做阶段切换。
3. 输出兼容 batch_summary.json 的结果，方便继续复用：
   - scripts/generate_stage_trend_report.py
   - scripts/analyze_experiment_batch.py

默认矩阵：
1. 阶段：UP / TURN / DOWN
2. 开发商：NONE / DHI（高价新增供给）
3. seeds：606 / 607 / 608
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
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

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner

NIGHT_RUN = ROOT / "scripts" / "night_run.py"
BASELINE = ROOT / "config" / "baseline.yaml"
STAGE_TREND = ROOT / "scripts" / "generate_stage_trend_report.py"
ZONE_CHAIN = ROOT / "scripts" / "analyze_experiment_batch.py"


def _extract_db_from_stdout(stdout_path: Path) -> Optional[str]:
    if not stdout_path.exists():
        return None
    lines = stdout_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for ln in reversed(lines):
        if "DB path:" in ln:
            return ln.split("DB path:", 1)[1].strip()
    return None


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _build_init_snapshot_signature(seed: int, agent_count: Optional[int]) -> str:
    payload = {
        "seed": int(seed),
        "agent_count": int(agent_count) if agent_count is not None else None,
        "baseline_path": str(BASELINE.resolve()),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _snapshot_healthy(db_path: Path, expected_agents: Optional[int]) -> bool:
    if not db_path.exists():
        return False
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        agents = int(cur.execute("SELECT COUNT(*) FROM agents_static").fetchone()[0] or 0)
        props = int(cur.execute("SELECT COUNT(*) FROM properties_static").fetchone()[0] or 0)
        conn.close()
    except Exception:
        return False
    if props <= 0:
        return False
    if expected_agents is None:
        return agents > 0
    return agents >= max(1, int(expected_agents * 0.9))


def ensure_init_snapshot(seed: int, agent_count: Optional[int], snapshot_root: Path) -> Dict[str, str]:
    signature = _build_init_snapshot_signature(seed=seed, agent_count=agent_count)
    suffix = f"s{int(seed)}"
    if agent_count is not None:
        suffix += f"_a{int(agent_count)}"
    snapshot_dir = snapshot_root / f"stage_init_{signature[:12]}_{suffix}"
    snapshot_db = snapshot_dir / "simulation.db"
    snapshot_cfg = snapshot_dir / "config.yaml"
    snapshot_meta = snapshot_dir / "snapshot_meta.json"

    if snapshot_db.exists() and snapshot_cfg.exists() and _snapshot_healthy(snapshot_db, agent_count):
        return {
            "snapshot_dir": str(snapshot_dir.resolve()),
            "db_path": str(snapshot_db.resolve()),
            "config_path": str(snapshot_cfg.resolve()),
        }

    try:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
    except Exception:
        pass
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(BASELINE), str(snapshot_cfg))

    config = SimulationConfig(str(snapshot_cfg))
    config.update("simulation.random_seed", int(seed))
    config.update("simulation.enable_intervention_panel", False)
    config.save()

    runner = SimulationRunner(
        agent_count=int(agent_count) if agent_count is not None else int(config.get("simulation.agent_count", 50) or 50),
        months=1,
        seed=int(seed),
        resume=False,
        config=config,
        db_path=str(snapshot_db),
    )
    try:
        runner.initialize()
    finally:
        runner.close()

    snapshot_meta.write_text(
        json.dumps(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "seed": int(seed),
                "agent_count": int(agent_count) if agent_count is not None else None,
                "snapshot_dir": str(snapshot_dir.resolve()),
                "db_path": str(snapshot_db.resolve()),
                "config_path": str(snapshot_cfg.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "snapshot_dir": str(snapshot_dir.resolve()),
        "db_path": str(snapshot_db.resolve()),
        "config_path": str(snapshot_cfg.resolve()),
    }


def _developer_supply_patch(dev_mode: str) -> List[Dict[str, Any]]:
    if dev_mode == "NONE":
        return []
    if dev_mode == "DHI":
        return [
            {
                "action_type": "developer_supply",
                "month": 1,
                "zone": "A",
                "count": 24,
                "price_per_sqm": 42000.0,
                "school_units": 12,
                "build_year": 2026,
            }
        ]
    raise ValueError(f"unknown developer mode: {dev_mode}")


def _stage_plan(stage: str, dev_mode: str, seed: int, months: int) -> Dict[str, Any]:
    if stage == "UP":
        plan = {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {
                    "income_adjustment_rate": 1.20,
                },
                "preplanned_interventions": _developer_supply_patch(dev_mode),
            },
            "macro_environment": {
                "override_mode": "optimistic",
            },
            "mortgage": {
                "down_payment_ratio": 0.16,
                "max_dti_ratio": 0.68,
                "annual_interest_rate": 0.032,
            },
            "market_pulse": {
                "enabled": False,
                "seed_existing_mortgage_ratio": 0.45,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        }
        return plan
    if stage == "TURN":
        plan = {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {
                    "income_adjustment_rate": 0.92,
                },
                "preplanned_interventions": _developer_supply_patch(dev_mode),
            },
            "macro_environment": {
                "override_mode": "stable",
            },
            "mortgage": {
                "down_payment_ratio": 0.36,
                "max_dti_ratio": 0.42,
                "annual_interest_rate": 0.042,
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.56,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.70,
                "info_delay_ratio_multiplier": 1.35,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.30,
                "info_delay_max_months": 2,
            },
        }
        return plan
    if stage == "DOWN":
        plan = {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {
                    "income_adjustment_rate": 0.72,
                },
                "preplanned_interventions": _developer_supply_patch(dev_mode),
            },
            "macro_environment": {
                "override_mode": "pessimistic",
            },
            "mortgage": {
                "down_payment_ratio": 0.52,
                "max_dti_ratio": 0.28,
                "annual_interest_rate": 0.060,
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.72,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.95,
                "info_delay_ratio_multiplier": 1.70,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.60,
                "info_delay_max_months": 3,
            },
        }
        return plan
    raise ValueError(f"unknown stage: {stage}")


def _run_one(
    stage: str,
    dev_mode: str,
    seed: int,
    months: int,
    out_dir: Path,
    py_exec: str,
    agent_count: Optional[int],
    use_init_snapshot: bool,
    init_snapshot_root: Optional[Path],
) -> Dict[str, Any]:
    case_name = f"{stage}_{dev_mode}_s{seed}_m{months}"
    case_dir = out_dir / case_name
    case_dir.mkdir(parents=True, exist_ok=True)

    plan = _stage_plan(stage, dev_mode, seed, months)
    plan_path = case_dir / "plan.yaml"
    _write_yaml(plan_path, plan)

    case_db_path = case_dir / "simulation.db"
    base_config_path = BASELINE
    snapshot_info: Dict[str, str] | None = None
    resume = False
    if use_init_snapshot:
        snapshot_root = init_snapshot_root or (ROOT / "results" / "init_snapshots")
        snapshot_info = ensure_init_snapshot(
            seed=int(seed),
            agent_count=int(agent_count) if agent_count is not None else None,
            snapshot_root=snapshot_root,
        )
        shutil.copy2(snapshot_info["db_path"], str(case_db_path))
        copied_cfg = case_dir / "snapshot_base_config.yaml"
        shutil.copy2(snapshot_info["config_path"], str(copied_cfg))
        base_config_path = copied_cfg
        resume = True

    stdout = case_dir / "stdout.log"
    stderr = case_dir / "stderr.log"
    cmd = [
        py_exec,
        str(NIGHT_RUN),
        "--base-config",
        str(base_config_path),
        "--plan",
        str(plan_path),
        "--seed",
        str(seed),
        "--months",
        str(months),
    ]
    if resume:
        cmd += ["--resume", "--db-path", str(case_db_path)]
    if agent_count is not None:
        cmd += ["--agent-count", str(int(agent_count))]

    env = dict(os.environ)
    prev_py = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not prev_py else (str(ROOT) + ";" + prev_py)

    with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
        proc = subprocess.run(cmd, cwd=str(ROOT), stdout=out, stderr=err, env=env)

    db_path = _extract_db_from_stdout(stdout)
    return {
        "group_key": f"{stage}_{dev_mode}",
        "group_name": f"{stage}_{dev_mode}",
        "stage": stage,
        "developer_mode": dev_mode,
        "seed": int(seed),
        "status": "success" if proc.returncode == 0 else "failed",
        "exit_code": int(proc.returncode),
        "run_dir": str(case_dir),
        "plan_path": str(plan_path),
        "stdout": str(stdout),
        "stderr": str(stderr),
        "db_path": db_path,
        "months": int(months),
        "agent_count": int(agent_count) if agent_count is not None else None,
        "init_snapshot_used": bool(use_init_snapshot),
        "init_snapshot_dir": snapshot_info["snapshot_dir"] if snapshot_info else None,
    }


def write_batch_summary(out_dir: Path, runs: List[Dict[str, Any]], months: int, seeds: List[int], developer_modes: List[str]) -> None:
    summary_path = out_dir / "batch_summary.json"
    existing_runs: List[Dict[str, Any]] = []
    if summary_path.exists():
        try:
            existing_payload = json.loads(summary_path.read_text(encoding="utf-8"))
            existing_runs = list(existing_payload.get("runs", []) or [])
        except Exception:
            existing_runs = []

    merged: Dict[str, Dict[str, Any]] = {}
    for item in existing_runs + runs:
        key = f"{item.get('stage','')}::{item.get('developer_mode','')}::{item.get('seed','')}::{item.get('months','')}"
        merged[key] = item
    merged_runs = list(merged.values())
    merged_runs.sort(key=lambda item: (str(item.get("stage", "")), str(item.get("developer_mode", "")), int(item.get("seed", 0) or 0)))

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_type": "stage_matrix",
        "months": int(months),
        "seeds": sorted({int(x) for x in ([*seeds] + [int(item.get("seed", 0) or 0) for item in merged_runs])}),
        "developer_modes": sorted({str(x) for x in ([*developer_modes] + [str(item.get("developer_mode", "")) for item in merged_runs])}),
        "run_count": len(merged_runs),
        "success_count": sum(1 for item in merged_runs if item["status"] == "success"),
        "failed_count": sum(1 for item in merged_runs if item["status"] != "success"),
        "runs": merged_runs,
    }
    summary_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# 连续阶段最小矩阵",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 月数: {months}",
        f"- seeds: {', '.join(str(x) for x in seeds)}",
        f"- 开发商设定: {', '.join(developer_modes)}",
        "",
        "| 阶段 | 开发商 | seed | 状态 | run_dir |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in merged_runs:
        lines.append(
            f"| {item['stage']} | {item['developer_mode']} | {item['seed']} | {item['status']} | {item['run_dir']} |"
        )
    (out_dir / "batch_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_post_report(script_path: Path, out_dir: Path, label: str) -> None:
    if not script_path.exists():
        print(f"[warn] {label} 脚本不存在，跳过: {script_path}")
        return
    try:
        subprocess.run(
            [sys.executable, str(script_path), str(out_dir)],
            check=True,
            cwd=str(ROOT),
        )
        print(f"{label}已写入: {out_dir}")
    except Exception as exc:
        print(f"[warn] {label}生成失败: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the minimum stage matrix.")
    parser.add_argument("--months", type=int, default=6, help="Simulation months for each case.")
    parser.add_argument("--agent-count", type=int, default=None, help="Optional agent count override.")
    parser.add_argument("--seeds", nargs="+", type=int, default=[606, 607, 608], help="Seeds to run.")
    parser.add_argument("--stages", nargs="+", default=["UP", "TURN", "DOWN"], help="Stage list.")
    parser.add_argument("--developer-modes", nargs="+", default=["NONE", "DHI"], help="Developer control list.")
    parser.add_argument("--out-dir", default="", help="Optional output root directory.")
    parser.add_argument("--use-init-snapshot", action="store_true", help="Reuse a month-0 initialization snapshot for the same seed.")
    parser.add_argument(
        "--init-snapshot-root",
        default=str(ROOT / "results" / "init_snapshots"),
        help="Initialization snapshot directory.",
    )
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).resolve() if args.out_dir else ROOT / "results" / "stage_matrix" / f"stage_matrix_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    py_exec = sys.executable
    runs: List[Dict[str, Any]] = []
    init_snapshot_root = Path(args.init_snapshot_root).resolve()
    for stage in args.stages:
        for dev_mode in args.developer_modes:
            for seed in args.seeds:
                result = _run_one(
                    stage=str(stage),
                    dev_mode=str(dev_mode),
                    seed=int(seed),
                    months=int(args.months),
                    out_dir=out_dir,
                    py_exec=py_exec,
                    agent_count=int(args.agent_count) if args.agent_count is not None else None,
                    use_init_snapshot=bool(args.use_init_snapshot),
                    init_snapshot_root=init_snapshot_root,
                )
                runs.append(result)
                print(f"[{stage}/{dev_mode}] seed={seed} exit={result['exit_code']}")

    write_batch_summary(
        out_dir=out_dir,
        runs=runs,
        months=int(args.months),
        seeds=[int(x) for x in args.seeds],
        developer_modes=[str(x) for x in args.developer_modes],
    )
    _run_post_report(STAGE_TREND, out_dir, "阶段量价报告")
    _run_post_report(ZONE_CHAIN, out_dir, "区域链路摘要")
    print(f"batch={out_dir}")
    return 0 if all(item["status"] == "success" for item in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
