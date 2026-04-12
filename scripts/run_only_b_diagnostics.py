import argparse
import json
import os
import sqlite3
import subprocess
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml


ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG = ROOT / "config" / "baseline.yaml"
NIGHT_RUN = ROOT / "scripts" / "night_run.py"
RESULTS_ROOT = ROOT / "results" / "diagnostics"


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return data


def write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, allow_unicode=True, sort_keys=False)


def only_b_overrides(agent_count: int, months: int, seed: int, core_llm_enabled: bool) -> Dict[str, Any]:
    return {
        "simulation": {
            "agent_count": int(agent_count),
            "months": int(months),
            "random_seed": int(seed),
            "enable_intervention_panel": False,
        },
        "market": {
            "zones": {
                "A": {
                    "property_count": {
                        "quality_1": 0,
                        "quality_2": 0,
                        "quality_3": 0,
                    }
                }
            }
        },
        "life_events": {
            "llm_reasoning_enabled": False,
        },
        "system": {
            "market_bulletin": {
                "post_settlement_llm_analysis_enabled": False,
            },
            "enable_end_reports": False,
        },
        "smart_agent": {
            "location_scarcity_weight": 0.24,
            "shortlist_location_bonus_weight": 0.25,
            "cross_zone_discount_threshold": 0.35,
            # Keep seller price-adjustment LLM off for the small real run too;
            # the purpose here is candidate exposure and core buy/negotiation behavior.
            "price_adjustment_llm_enabled_for_smart": False,
        },
        "diagnostic": {
            "scenario": "only_b_inventory",
            "core_llm_enabled": bool(core_llm_enabled),
        },
    }


def run_case(case_name: str, config_path: Path, db_path: Path, agent_count: int, months: int, seed: int, mock_mode: bool) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["LLM_MOCK_MODE"] = "true" if mock_mode else "false"
    existing_pythonpath = str(env.get("PYTHONPATH", "") or "").strip()
    env["PYTHONPATH"] = str(ROOT) if not existing_pythonpath else f"{ROOT}{os.pathsep}{existing_pythonpath}"
    cmd = [
        sys.executable,
        str(NIGHT_RUN),
        "--base-config",
        str(config_path),
        "--db-path",
        str(db_path),
        "--agent-count",
        str(agent_count),
        "--months",
        str(months),
        "--seed",
        str(seed),
    ]
    print(f"\n=== Running {case_name} ===")
    print(" ".join(cmd))
    subprocess.run(cmd, cwd=str(ROOT), env=env, check=True)


def scalar(cur: sqlite3.Cursor, query: str, params=()) -> Any:
    row = cur.execute(query, params).fetchone()
    if not row:
        return None
    if isinstance(row, sqlite3.Row):
        return list(row)[0]
    return row[0]


def fetch_all_dicts(cur: sqlite3.Cursor, query: str, params=()):
    rows = cur.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def count_log_events(log_path: Path, needle: str) -> int:
    if not log_path.exists():
        return 0
    count = 0
    with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if needle in line:
                count += 1
    return count


def summarize_case(run_dir: Path) -> Dict[str, Any]:
    db_path = run_dir / "simulation.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    total_b_properties = scalar(
        cur,
        "SELECT COUNT(*) FROM properties_static WHERE zone='B'",
    ) or 0
    exposed_b_properties = scalar(
        cur,
        """
        SELECT COUNT(DISTINCT m.property_id)
        FROM property_buyer_matches m
        JOIN properties_static ps ON ps.property_id=m.property_id
        WHERE ps.zone='B'
        """,
    ) or 0
    total_a_properties = scalar(
        cur,
        "SELECT COUNT(*) FROM properties_static WHERE zone='A'",
    ) or 0
    b_orders_total = scalar(
        cur,
        """
        SELECT COUNT(*)
        FROM transaction_orders o
        JOIN properties_static ps ON ps.property_id=o.property_id
        WHERE ps.zone='B'
        """,
    ) or 0
    b_orders_success = scalar(
        cur,
        """
        SELECT COUNT(*)
        FROM transaction_orders o
        JOIN properties_static ps ON ps.property_id=o.property_id
        WHERE ps.zone='B' AND o.status IN ('filled','pending_settlement')
        """,
    ) or 0
    shortlist_only_total = scalar(
        cur,
        "SELECT COUNT(*) FROM property_buyer_matches WHERE final_outcome='SHORTLIST_ONLY'",
    ) or 0
    recovery_states = fetch_all_dicts(
        cur,
        "SELECT state, COUNT(*) AS cnt FROM buyer_recovery_queue GROUP BY state ORDER BY cnt DESC",
    )
    top_b_seen_buyers = fetch_all_dicts(
        cur,
        """
        SELECT m.buyer_id,
               COUNT(*) AS exposure_rows,
               COUNT(DISTINCT m.property_id) AS distinct_seen_properties,
               COUNT(DISTINCT m.month) AS active_months,
               SUM(CASE WHEN m.final_outcome='SHORTLIST_ONLY' THEN 1 ELSE 0 END) AS shortlist_only_rows,
               SUM(CASE WHEN m.order_id IS NOT NULL THEN 1 ELSE 0 END) AS order_rows
        FROM property_buyer_matches m
        JOIN properties_static ps ON ps.property_id=m.property_id
        WHERE ps.zone='B'
        GROUP BY m.buyer_id
        ORDER BY distinct_seen_properties DESC, active_months DESC, m.buyer_id
        LIMIT 8
        """,
    )
    top_b_exposed_properties = fetch_all_dicts(
        cur,
        """
        SELECT ps.property_id,
               ROUND(pm.listed_price, 0) AS listed_price,
               ps.zone_price_tier,
               ps.property_type,
               COUNT(m.match_id) AS exposure_rows,
               COUNT(DISTINCT m.buyer_id) AS distinct_buyers
        FROM properties_static ps
        JOIN properties_market pm ON pm.property_id=ps.property_id
        LEFT JOIN property_buyer_matches m ON m.property_id=ps.property_id
        WHERE ps.zone='B'
        GROUP BY ps.property_id, pm.listed_price, ps.zone_price_tier, ps.property_type
        HAVING COUNT(m.match_id) > 0
        ORDER BY exposure_rows DESC, distinct_buyers DESC, ps.property_id
        LIMIT 8
        """,
    )
    top_close_reasons = fetch_all_dicts(
        cur,
        """
        SELECT COALESCE(close_reason, '(null)') AS close_reason, COUNT(*) AS cnt
        FROM transaction_orders
        WHERE status IN ('cancelled','expired','breached')
        GROUP BY COALESCE(close_reason, '(null)')
        ORDER BY cnt DESC
        LIMIT 8
        """,
    )
    monthly_b_exposure = fetch_all_dicts(
        cur,
        """
        SELECT m.month,
               COUNT(*) AS exposure_rows,
               COUNT(DISTINCT m.property_id) AS exposed_props,
               COUNT(DISTINCT m.buyer_id) AS buyers
        FROM property_buyer_matches m
        JOIN properties_static ps ON ps.property_id=m.property_id
        WHERE ps.zone='B'
        GROUP BY m.month
        ORDER BY m.month
        """,
    )
    conn.close()

    order_log = run_dir / "order_lifecycle.log"
    sim_log = run_dir / "simulation_run.log"
    log_counter = Counter(
        {
            "BUYER_UNSEEN_CANDIDATE_FOCUS": count_log_events(order_log, "BUYER_UNSEEN_CANDIDATE_FOCUS"),
            "PENDING_CAP_REACHED": count_log_events(order_log, "PENDING_CAP_REACHED"),
            "BUYER_RECOVERY_DEFERRED": count_log_events(order_log, "BUYER_RECOVERY_DEFERRED"),
            "lowered_price": count_log_events(sim_log, "lowered price"),
            "调价至": count_log_events(sim_log, "调价至"),
        }
    )

    return {
        "run_dir": str(run_dir),
        "db_path": str(db_path),
        "total_a_properties": int(total_a_properties),
        "total_b_properties": int(total_b_properties),
        "exposed_b_properties": int(exposed_b_properties),
        "zero_exposure_b_properties": int(max(0, total_b_properties - exposed_b_properties)),
        "b_orders_total": int(b_orders_total),
        "b_orders_success": int(b_orders_success),
        "b_order_success_ratio": (float(b_orders_success) / float(b_orders_total)) if b_orders_total else 0.0,
        "shortlist_only_total": int(shortlist_only_total),
        "log_counts": dict(log_counter),
        "recovery_states": recovery_states,
        "top_b_seen_buyers": top_b_seen_buyers,
        "top_b_exposed_properties": top_b_exposed_properties,
        "top_close_reasons": top_close_reasons,
        "monthly_b_exposure": monthly_b_exposure,
    }


def write_report(output_dir: Path, mechanism: Dict[str, Any], real_sample: Dict[str, Any], manifest: Dict[str, Any]) -> None:
    json_path = output_dir / "only_b_diagnostics_summary.json"
    md_path = output_dir / "only_b_diagnostics_summary.md"

    payload = {
        "manifest": manifest,
        "mechanism_mock": mechanism,
        "real_sample": real_sample,
    }
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    def rows_to_bullets(rows, keys):
        lines = []
        for row in rows:
            parts = [f"{k}={row.get(k)}" for k in keys]
            lines.append("- " + ", ".join(parts))
        return "\n".join(lines) if lines else "- (none)"

    md = f"""# only-B 诊断摘要

## 实验配置

- 输出目录: `{output_dir}`
- 机制测试: `only B + mock LLM`
- 真实测试: `only B + small real LLM`
- 固定参数:
  - `smart_agent.location_scarcity_weight=0.24`
  - `smart_agent.shortlist_location_bonus_weight=0.25`
  - `smart_agent.cross_zone_discount_threshold=0.35`

## 机制测试摘要

- run_dir: `{mechanism['run_dir']}`
- A区房源数: `{mechanism['total_a_properties']}`
- B区房源数: `{mechanism['total_b_properties']}`
- B区曝光房源数: `{mechanism['exposed_b_properties']}`
- B区零曝光房源数: `{mechanism['zero_exposure_b_properties']}`
- B区订单: `{mechanism['b_orders_success']}/{mechanism['b_orders_total']}`
- SHORTLIST_ONLY 总数: `{mechanism['shortlist_only_total']}`
- 日志计数: `{json.dumps(mechanism['log_counts'], ensure_ascii=False)}`

### 机制测试 B区买家曝光样本
{rows_to_bullets(mechanism['top_b_seen_buyers'], ['buyer_id', 'distinct_seen_properties', 'active_months', 'exposure_rows', 'shortlist_only_rows', 'order_rows'])}

### 机制测试 B区曝光房源
{rows_to_bullets(mechanism['top_b_exposed_properties'], ['property_id', 'listed_price', 'zone_price_tier', 'property_type', 'exposure_rows', 'distinct_buyers'])}

## 小样本真实测试摘要

- run_dir: `{real_sample['run_dir']}`
- A区房源数: `{real_sample['total_a_properties']}`
- B区房源数: `{real_sample['total_b_properties']}`
- B区曝光房源数: `{real_sample['exposed_b_properties']}`
- B区零曝光房源数: `{real_sample['zero_exposure_b_properties']}`
- B区订单: `{real_sample['b_orders_success']}/{real_sample['b_orders_total']}`
- SHORTLIST_ONLY 总数: `{real_sample['shortlist_only_total']}`
- 日志计数: `{json.dumps(real_sample['log_counts'], ensure_ascii=False)}`

### 真实测试 B区买家曝光样本
{rows_to_bullets(real_sample['top_b_seen_buyers'], ['buyer_id', 'distinct_seen_properties', 'active_months', 'exposure_rows', 'shortlist_only_rows', 'order_rows'])}

### 真实测试 B区曝光房源
{rows_to_bullets(real_sample['top_b_exposed_properties'], ['property_id', 'listed_price', 'zone_price_tier', 'property_type', 'exposure_rows', 'distinct_buyers'])}

## 失败原因 Top

### 机制测试
{rows_to_bullets(mechanism['top_close_reasons'], ['close_reason', 'cnt'])}

### 真实测试
{rows_to_bullets(real_sample['top_close_reasons'], ['close_reason', 'cnt'])}
"""
    with md_path.open("w", encoding="utf-8") as handle:
        handle.write(md)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run only-B diagnostics with mock-LLM and real-LLM comparison.")
    parser.add_argument("--seed", type=int, default=606)
    parser.add_argument("--mock-agents", type=int, default=50)
    parser.add_argument("--mock-months", type=int, default=6)
    parser.add_argument("--real-agents", type=int, default=20)
    parser.add_argument("--real-months", type=int, default=6)
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = RESULTS_ROOT / f"only_b_diag_{ts}"
    config_dir = output_dir / "configs"
    mechanism_run_dir = output_dir / "only_b_mock"
    real_run_dir = output_dir / "only_b_real"
    output_dir.mkdir(parents=True, exist_ok=True)

    base_cfg = load_yaml(BASE_CONFIG)

    mechanism_cfg = deep_merge(json.loads(json.dumps(base_cfg)), only_b_overrides(args.mock_agents, args.mock_months, args.seed, core_llm_enabled=False))
    real_cfg = deep_merge(json.loads(json.dumps(base_cfg)), only_b_overrides(args.real_agents, args.real_months, args.seed, core_llm_enabled=True))

    mechanism_cfg_path = config_dir / "only_b_mock.yaml"
    real_cfg_path = config_dir / "only_b_real.yaml"
    write_yaml(mechanism_cfg_path, mechanism_cfg)
    write_yaml(real_cfg_path, real_cfg)

    manifest = {
        "seed": int(args.seed),
        "mock_agents": int(args.mock_agents),
        "mock_months": int(args.mock_months),
        "real_agents": int(args.real_agents),
        "real_months": int(args.real_months),
        "mechanism_config_path": str(mechanism_cfg_path),
        "real_config_path": str(real_cfg_path),
    }
    with (output_dir / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)

    run_case(
        case_name="only_b_mock",
        config_path=mechanism_cfg_path,
        db_path=mechanism_run_dir / "simulation.db",
        agent_count=args.mock_agents,
        months=args.mock_months,
        seed=args.seed,
        mock_mode=True,
    )
    mechanism_summary = summarize_case(mechanism_run_dir)

    run_case(
        case_name="only_b_real",
        config_path=real_cfg_path,
        db_path=real_run_dir / "simulation.db",
        agent_count=args.real_agents,
        months=args.real_months,
        seed=args.seed,
        mock_mode=False,
    )
    real_summary = summarize_case(real_run_dir)

    write_report(output_dir, mechanism_summary, real_summary, manifest)

    print("\n=== only-B diagnostics complete ===")
    print(f"Output dir: {output_dir}")
    print(f"Mechanism run: {mechanism_run_dir}")
    print(f"Real run: {real_run_dir}")
    print(f"Mechanism exposed B props: {mechanism_summary['exposed_b_properties']}/{mechanism_summary['total_b_properties']}")
    print(f"Real exposed B props: {real_summary['exposed_b_properties']}/{real_summary['total_b_properties']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
