#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
路径一致性校验：
比较两条执行路径（例如 自动批跑 vs real_estate_demo）在同一实验上的“方向是否一致”。

用法（推荐，方向校验）：
python scripts/check_path_consistency.py \
  --a-control D:/.../run_auto_r2a \
  --a-treatment D:/.../run_auto_r2b \
  --b-control D:/.../run_demo_r2a \
  --b-treatment D:/.../run_demo_r2b

输出：
1) 控制组/处理组关键配置是否一致
2) 两条路径的 delta(处理-控制) 是否同号
3) JSON 报告（默认写到 results/path_consistency_*.json）
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


CONFIG_KEYS = [
    "simulation.months",
    "simulation.random_seed",
    "simulation.agent_count",
    "simulation.base_year",
    "market_pulse.enabled",
    "market_pulse.seed_existing_mortgage_ratio",
    "smart_agent.info_delay_enabled",
    "smart_agent.info_delay_ratio",
    "smart_agent.info_delay_max_months",
    "smart_agent.effective_bid_floor_ratio",
    "smart_agent.price_adjustment_llm_min_duration",
    "smart_agent.price_adjustment_monthly_llm_cap",
    "smart_agent.regime_engine_v1_enabled",
    "smart_agent.regime_v1_raise_release_force_sample_enabled",
    "smart_agent.regime_v1_raise_release_force_sample_ratio",
    "smart_agent.price_adjustment_llm_min_calls_per_month",
]


def _get_nested(d: Dict[str, Any], dotted: str) -> Any:
    cur: Any = d
    for k in dotted.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _load_config(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "config.yaml"
    if not p.exists():
        raise FileNotFoundError(f"missing config: {p}")
    # run config may contain python tuple tags (e.g., !!python/tuple)
    return yaml.unsafe_load(p.read_text(encoding="utf-8")) or {}


def _open_db(run_dir: Path) -> sqlite3.Connection:
    p = run_dir / "simulation.db"
    if not p.exists():
        raise FileNotFoundError(f"missing db: {p}")
    return sqlite3.connect(str(p))


def _q(cur: sqlite3.Cursor, sql: str) -> float:
    v = cur.execute(sql).fetchone()[0]
    if v is None:
        return 0.0
    return float(v)


def _metrics(run_dir: Path) -> Dict[str, float]:
    conn = _open_db(run_dir)
    cur = conn.cursor()
    total_tx = _q(cur, "select count(*) from transactions")
    avg_price = _q(cur, "select avg(final_price) from transactions")
    b_tx = _q(
        cur,
        """
        select count(*)
        from transactions t
        join properties_static p on p.property_id=t.property_id
        where p.zone='B'
        """,
    )
    b_share = (b_tx / total_tx) if total_tx > 0 else 0.0
    b_orders = _q(
        cur,
        """
        select count(*)
        from transaction_orders o
        join properties_static p on p.property_id=o.property_id
        where p.zone='B'
        """,
    )
    b_close_rate = (b_tx / b_orders) if b_orders > 0 else 0.0
    ef_total = _q(
        cur,
        """
        select count(*)
        from decision_logs
        where event_type='PRICE_ADJUSTMENT'
          and trim(decision) in ('E','F')
        """,
    )
    ef_llm_total = _q(
        cur,
        """
        select count(*)
        from decision_logs
        where event_type='PRICE_ADJUSTMENT'
          and trim(decision) in ('E','F')
          and coalesce(llm_called,0)=1
        """,
    )
    conn.close()
    return {
        "tx_total": total_tx,
        "avg_price": avg_price,
        "b_tx_share": b_share,
        "b_close_rate": b_close_rate,
        "ef_total": ef_total,
        "ef_llm_total": ef_llm_total,
    }


def _config_subset(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _get_nested(cfg, k) for k in CONFIG_KEYS}


def _compare_cfg(a: Dict[str, Any], b: Dict[str, Any]) -> List[Dict[str, Any]]:
    diffs = []
    for k in CONFIG_KEYS:
        va, vb = a.get(k), b.get(k)
        if va != vb:
            diffs.append({"key": k, "a": va, "b": vb})
    return diffs


def _sign(x: float, eps: float = 1e-9) -> int:
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def _delta(control: Dict[str, float], treatment: Dict[str, float]) -> Dict[str, float]:
    return {k: float(treatment.get(k, 0.0)) - float(control.get(k, 0.0)) for k in control.keys()}


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare directional consistency across execution paths.")
    ap.add_argument("--a-control", required=True, help="Path A control run dir")
    ap.add_argument("--a-treatment", required=True, help="Path A treatment run dir")
    ap.add_argument("--b-control", required=True, help="Path B control run dir")
    ap.add_argument("--b-treatment", required=True, help="Path B treatment run dir")
    ap.add_argument("--out", default="", help="Optional output json path")
    args = ap.parse_args()

    a_c = Path(args.a_control).resolve()
    a_t = Path(args.a_treatment).resolve()
    b_c = Path(args.b_control).resolve()
    b_t = Path(args.b_treatment).resolve()

    a_c_cfg = _config_subset(_load_config(a_c))
    a_t_cfg = _config_subset(_load_config(a_t))
    b_c_cfg = _config_subset(_load_config(b_c))
    b_t_cfg = _config_subset(_load_config(b_t))

    cfg_diff_control = _compare_cfg(a_c_cfg, b_c_cfg)
    cfg_diff_treatment = _compare_cfg(a_t_cfg, b_t_cfg)

    a_c_m = _metrics(a_c)
    a_t_m = _metrics(a_t)
    b_c_m = _metrics(b_c)
    b_t_m = _metrics(b_t)

    a_delta = _delta(a_c_m, a_t_m)
    b_delta = _delta(b_c_m, b_t_m)

    direction_checks: Dict[str, Dict[str, Any]] = {}
    for k in a_delta.keys():
        sa = _sign(a_delta[k])
        sb = _sign(b_delta[k])
        direction_checks[k] = {
            "delta_a": a_delta[k],
            "delta_b": b_delta[k],
            "sign_a": sa,
            "sign_b": sb,
            "same_direction": bool(sa == sb),
        }

    same_dir_ratio = sum(1 for v in direction_checks.values() if v["same_direction"]) / max(
        len(direction_checks), 1
    )
    passed = (len(cfg_diff_control) == 0 and len(cfg_diff_treatment) == 0 and same_dir_ratio >= 0.7)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "path_a": {"control": str(a_c), "treatment": str(a_t)},
        "path_b": {"control": str(b_c), "treatment": str(b_t)},
        "config_diffs": {
            "control": cfg_diff_control,
            "treatment": cfg_diff_treatment,
        },
        "metrics": {
            "a_control": a_c_m,
            "a_treatment": a_t_m,
            "b_control": b_c_m,
            "b_treatment": b_t_m,
            "a_delta": a_delta,
            "b_delta": b_delta,
        },
        "direction_checks": direction_checks,
        "same_direction_ratio": same_dir_ratio,
        "passed": passed,
        "rule": "config_diffs==0 and same_direction_ratio>=0.70",
    }

    if args.out:
        out_path = Path(args.out).resolve()
    else:
        out_path = (
            Path(__file__).resolve().parents[1]
            / "results"
            / f"path_consistency_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"PASS={passed} same_direction_ratio={same_dir_ratio:.2f}")
    print(f"report={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
