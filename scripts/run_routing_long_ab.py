#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
长测 A/B：灰区路由开关对比（同参数、同种子）
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import project_manager
from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner


def _to_bool(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _run_once(label: str, months: int, agents: int, seed: int, routing_enabled: bool) -> dict:
    proj_dir, config_path, db_path = project_manager.create_new_project("config/baseline.yaml")
    config = SimulationConfig(config_path)
    config.update("simulation.months", int(months))
    config.update("simulation.agent_count", int(agents))
    config.update("simulation.random_seed", int(seed))
    config.update("smart_agent.buyer_match_dual_routing_enabled", bool(routing_enabled))
    config.update("smart_agent.negotiation_dual_routing_enabled", bool(routing_enabled))
    config._config["enable_llm_portraits"] = False
    config.update("life_events.llm_reasoning_enabled", False)
    config.update("system.market_bulletin.post_settlement_llm_analysis_enabled", False)
    config.update("system.market_bulletin.model_type", "fast")
    config.update("reporting.enable_end_reports", False)
    config.update("system.reporting.portrait_model_type", "fast")
    config.save()

    t0 = time.perf_counter()
    runner = SimulationRunner(
        agent_count=int(agents),
        months=int(months),
        seed=int(seed),
        resume=False,
        config=config,
        db_path=db_path,
    )
    runner.run()
    elapsed = round(time.perf_counter() - t0, 2)

    metrics = {
        "label": label,
        "routing_enabled": bool(routing_enabled),
        "months": int(months),
        "agents": int(agents),
        "seed": int(seed),
        "elapsed_sec": elapsed,
        "run_dir": str(Path(proj_dir).resolve()),
        "db_path": str(Path(db_path).resolve()),
    }

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(SUM(transaction_volume),0) FROM market_bulletin")
        metrics["tx_total"] = int((cur.fetchone() or [0])[0] or 0)
    except Exception:
        metrics["tx_total"] = 0

    try:
        cur.execute(
            """
            SELECT json_extract(context_metrics, '$.llm_route_model') AS m, COUNT(*)
            FROM decision_logs
            WHERE event_type='BUYER_MATCH'
            GROUP BY m
            ORDER BY m
            """
        )
        metrics["buyer_match_route_counts"] = cur.fetchall()
    except Exception:
        metrics["buyer_match_route_counts"] = []

    try:
        cur.execute(
            """
            SELECT json_extract(context_metrics, '$.negotiation_route_model') AS m, COUNT(*)
            FROM decision_logs
            WHERE event_type='NEGOTIATION_ROUTE'
            GROUP BY m
            ORDER BY m
            """
        )
        metrics["negotiation_route_counts"] = cur.fetchall()
    except Exception:
        metrics["negotiation_route_counts"] = []
    conn.close()
    return metrics


def main() -> int:
    ap = argparse.ArgumentParser(description="灰区路由长测 A/B")
    ap.add_argument("--months", type=int, default=10)
    ap.add_argument("--agents", type=int, default=120)
    ap.add_argument("--seed", type=int, default=303)
    args = ap.parse_args()

    started = datetime.now().isoformat(timespec="seconds")
    on = _run_once("routing_on", args.months, args.agents, args.seed, True)
    off = _run_once("routing_off", args.months, args.agents, args.seed, False)

    summary = {
        "started_at": started,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "params": {"months": args.months, "agents": args.agents, "seed": args.seed},
        "routing_on": on,
        "routing_off": off,
        "delta": {
            "elapsed_sec_off_minus_on": round(float(off["elapsed_sec"]) - float(on["elapsed_sec"]), 2),
            "tx_off_minus_on": int(off.get("tx_total", 0)) - int(on.get("tx_total", 0)),
        },
    }

    out = ROOT / "results" / f"routing_ab_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"summary_file": str(out.resolve()), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
