#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Minimal public smoke test for AtlasMarketEngine.

Goal:
1. verify the repo can boot after clone
2. avoid real LLM/API dependencies by forcing mock mode
3. produce a small report that can also be used in CI
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path
import re
from typing import Any

os.environ["LLM_MOCK_MODE"] = "true"
os.environ.setdefault("PYTHONUTF8", "1")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.config_loader import SimulationConfig  # noqa: E402
from simulation_runner import SimulationRunner  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a minimal clone verification smoke test.")
    parser.add_argument("--rounds", type=int, default=1, help="How many rounds to simulate. Default: 1.")
    parser.add_argument("--agent-count", type=int, default=8, help="How many agents to generate. Default: 8.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed. Default: 42.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory to keep smoke artifacts. Defaults to a temporary directory.",
    )
    return parser.parse_args()


def _safe_scalar(conn: sqlite3.Connection, sql: str, default: Any = 0) -> Any:
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return default
        value = row[0]
        return default if value is None else value
    except Exception:
        return default


def _write_markdown_report(report_path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Public Smoke Test Report",
        "",
        "## Scenario",
        f"- Status: `{summary['status']}`",
        f"- Rounds requested: `{summary['rounds_requested']}`",
        f"- Agent count: `{summary['agent_count']}`",
        f"- Random seed: `{summary['seed']}`",
        f"- Mock mode: `{summary['mock_mode']}`",
        "",
        "## Outcome",
        f"- Completed rounds: `{summary['completed_rounds']}`",
        f"- Runtime DB: `{summary['db_path']}`",
        f"- Runtime log: `{summary['log_path']}`",
        f"- Checkpoint count: `{summary['checkpoint_count']}`",
        f"- Transactions: `{summary['transaction_count']}`",
        f"- Active listings now: `{summary['active_listings_now']}`",
        "",
        "## Interpretation",
        "- This smoke is only meant to answer whether a fresh clone can boot, run, write a database, and finish the requested round count.",
        "- It does not validate market quality or release-grade realism.",
        "",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")


def _read_completed_rounds_from_log(log_path: Path) -> int:
    if not log_path.exists():
        return 0
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    matches = re.findall(r"(?:Month|Round)\s+(\d+)\s+Complete", text)
    if not matches:
        return 0
    return max(int(item) for item in matches)


def main() -> int:
    args = parse_args()

    base_output = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else Path(tempfile.mkdtemp(prefix="atlas_public_smoke_"))
    )
    base_output.mkdir(parents=True, exist_ok=True)

    db_path = base_output / "simulation.db"
    config = SimulationConfig(str(REPO_ROOT / "config" / "baseline.yaml"))
    config.update("simulation.enable_intervention_panel", False)
    config.update("simulation.preplanned_interventions", [])

    runner = SimulationRunner(
        agent_count=int(args.agent_count),
        months=int(args.rounds),
        seed=int(args.seed),
        resume=False,
        config=config,
        db_path=str(db_path),
    )

    try:
        runner.run_night()
    finally:
        if runner.conn is not None:
            runner.close()

    runtime_db = Path(runner.db_path)
    runtime_dir = Path(getattr(runner, "_run_dir", base_output))
    checkpoint_root = runtime_dir / "monthly_checkpoints"
    checkpoint_count = 0
    if checkpoint_root.exists():
        checkpoint_count = sum(1 for item in checkpoint_root.iterdir() if item.is_dir())

    conn = sqlite3.connect(str(runtime_db))
    try:
        transaction_count = int(_safe_scalar(conn, "SELECT COUNT(*) FROM transactions", 0))
        active_listings_now = int(
            _safe_scalar(conn, "SELECT COUNT(*) FROM properties_market WHERE status='for_sale'", 0)
        )
        completed_rounds_db = max(
            int(_safe_scalar(conn, "SELECT MAX(month) FROM transactions", 0)),
            int(_safe_scalar(conn, "SELECT MAX(listing_month) FROM properties_market", 0)),
        )
    finally:
        conn.close()

    log_path = runtime_dir / "simulation_run.log"
    completed_rounds_log = _read_completed_rounds_from_log(log_path)
    completed_rounds = max(completed_rounds_db, completed_rounds_log)
    parameter_report_path = runtime_dir / "parameter_assumption_report.md"
    motivation_report_path = runtime_dir / "motivation_agent_report.md"
    summary = {
        "status": (
            "completed"
            if runtime_db.exists()
            and completed_rounds >= int(args.rounds)
            and parameter_report_path.exists()
            and motivation_report_path.exists()
            else "failed"
        ),
        "timestamp": datetime.now().isoformat(),
        "rounds_requested": int(args.rounds),
        "agent_count": int(args.agent_count),
        "seed": int(args.seed),
        "mock_mode": True,
        "artifact_dir": str(base_output),
        "runtime_dir": str(runtime_dir),
        "db_path": str(runtime_db),
        "log_path": str(log_path),
        "checkpoint_count": checkpoint_count,
        "completed_rounds": completed_rounds,
        "completed_rounds_log": completed_rounds_log,
        "completed_rounds_db": completed_rounds_db,
        "transaction_count": transaction_count,
        "active_listings_now": active_listings_now,
        "parameter_report_path": str(parameter_report_path),
        "motivation_report_path": str(motivation_report_path),
    }

    json_path = base_output / "public_smoke_report.json"
    md_path = base_output / "public_smoke_report.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown_report(md_path, summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
