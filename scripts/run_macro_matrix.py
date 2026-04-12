#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
宏观冲击测试矩阵（不改核心代码）：
N0: 自然长跑（无外部冲击）
S+: 工资普涨
S-: 工资普降
S_STRICT: 工资普降 + 融资收紧（首付上调、DTI下调）
D+: 开发商偏高价供给
D-: 开发商偏低价供给

实现方式：调用 scripts/night_run.py + 临时 plan 覆盖参数。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml


ROOT = Path(__file__).resolve().parents[1]
NIGHT_RUN = ROOT / "scripts" / "night_run.py"
BASELINE = ROOT / "config" / "baseline.yaml"

# Research default point (tune-v1): fixed for comparability across matrix scenarios.
TUNE_V1_SMART_AGENT_DEFAULTS = {
    "location_scarcity_weight": 0.24,
    "shortlist_location_bonus_weight": 0.25,
    "cross_zone_discount_threshold": 0.35,
}


def _scenario_plan(name: str) -> Dict[str, Any]:
    if name == "N0":
        return {}
    if name == "S+":
        return {"simulation": {"agent": {"income_adjustment_rate": 1.10}}}
    if name == "S-":
        return {"simulation": {"agent": {"income_adjustment_rate": 0.90}}}
    if name == "S_STRICT":
        return {
            "simulation": {"agent": {"income_adjustment_rate": 0.90}},
            "mortgage": {
                "down_payment_ratio": 0.38,
                "max_dti_ratio": 0.40,
            },
        }
    if name == "D+":
        return {
            "simulation": {
                "preplanned_interventions": [
                    {
                        "action_type": "developer_supply",
                        "month": 1,
                        "zone": "A",
                        "count": 24,
                        "price_per_sqm": 42000.0,
                        "school_units": 14,
                        "build_year": 2026,
                    }
                ]
            }
        }
    if name == "D-":
        return {
            "simulation": {
                "preplanned_interventions": [
                    {
                        "action_type": "developer_supply",
                        "month": 1,
                        "zone": "B",
                        "count": 24,
                        "price_per_sqm": 12000.0,
                        "school_units": 4,
                        "build_year": 2026,
                    }
                ]
            }
        }
    raise ValueError(f"unknown scenario: {name}")


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _run_one(
    seed: int,
    months: int,
    scenario: str,
    out_dir: Path,
    py_exec: str,
    agent_count: int | None = None,
) -> Dict[str, Any]:
    scenario_dir = out_dir / f"{scenario}_s{seed}_m{months}"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    plan = _scenario_plan(scenario)
    # fixed controls for matrix comparability
    plan.setdefault("simulation", {})
    plan["simulation"]["enable_intervention_panel"] = False
    plan["simulation"]["random_seed"] = int(seed)
    plan["simulation"]["months"] = int(months)
    plan.setdefault("smart_agent", {})
    for key, value in TUNE_V1_SMART_AGENT_DEFAULTS.items():
        plan["smart_agent"][key] = float(value)
    plan_path = scenario_dir / "plan.yaml"
    _write_yaml(plan_path, plan)

    stdout = scenario_dir / "stdout.log"
    stderr = scenario_dir / "stderr.log"
    cmd = [
        py_exec,
        str(NIGHT_RUN),
        "--base-config",
        str(BASELINE),
        "--plan",
        str(plan_path),
        "--seed",
        str(seed),
        "--months",
        str(months),
    ]
    if agent_count is not None:
        cmd += ["--agent-count", str(int(agent_count))]
    env = dict(**__import__("os").environ)
    prev_py = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not prev_py else (str(ROOT) + ";" + prev_py)
    with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
        proc = subprocess.run(cmd, cwd=str(ROOT), stdout=out, stderr=err, env=env)

    result = {
        "scenario": scenario,
        "seed": seed,
        "months": months,
        "exit_code": int(proc.returncode),
        "plan": str(plan_path),
        "stdout": str(stdout),
        "stderr": str(stderr),
    }
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description="Run macro matrix by scenario x seed.")
    ap.add_argument("--months", type=int, default=8, help="Simulation months for each run.")
    ap.add_argument("--agent-count", type=int, default=None, help="Optional agent count override for each run.")
    ap.add_argument("--seeds", nargs="+", type=int, default=[101], help="Seed list.")
    ap.add_argument(
        "--scenarios",
        nargs="+",
        default=["N0", "S+", "S-", "D+", "D-"],
        help="Scenario list",
    )
    ap.add_argument("--out-dir", default="", help="Optional output root dir.")
    args = ap.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).resolve() if args.out_dir else ROOT / "results" / "night_plan" / f"macro_matrix_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "months": int(args.months),
        "seeds": [int(s) for s in args.seeds],
        "scenarios": list(args.scenarios),
        "results": [],
    }

    py_exec = sys.executable
    for seed in args.seeds:
        for scenario in args.scenarios:
            r = _run_one(
                int(seed),
                int(args.months),
                str(scenario),
                out_dir,
                py_exec,
                int(args.agent_count) if args.agent_count is not None else None,
            )
            summary["results"].append(r)
            print(f"[{scenario}] seed={seed} exit={r['exit_code']}")
            if r["exit_code"] != 0:
                # keep running next cases; final caller decides whether to stop
                pass

    ok = sum(1 for x in summary["results"] if x["exit_code"] == 0)
    fail = len(summary["results"]) - ok
    summary["ok"] = ok
    summary["fail"] = fail

    summary_path = out_dir / "matrix_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
