#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
为已有 run 目录补生成动机分层与 Agent 明细报告。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simulation_runner import SimulationRunner


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill motivation/agent report for existing simulation DB")
    parser.add_argument("--db-path", required=True, help="Path to simulation.db")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    runner = SimulationRunner(resume=True, db_path=str(db_path))
    try:
        paths = runner.write_motivation_agent_report()
        print(paths["markdown_path"])
        print(paths["json_path"])
    finally:
        runner.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
