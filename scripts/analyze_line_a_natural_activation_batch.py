#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
课题线 A：自然激活与行为一致性批次分析

输入：包含 batch_summary.json 的批次目录
输出：
1) line_a_natural_activation_analysis.json
2) line_a_natural_activation_analysis.md
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
BUY_ROLES = {"BUYER", "BUYER_SELLER"}
SELL_ROLES = {"SELLER", "BUYER_SELLER"}


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_json_loads(text: Optional[str]) -> Dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        return {}


def _resolve_db_path(run_item: Dict[str, Any]) -> Optional[Path]:
    raw = str(run_item.get("db_path") or "").strip()
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_absolute() else (ROOT / raw).resolve()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _query_one_int(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    try:
        row = conn.execute(sql, params).fetchone()
    except Exception:
        return 0
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def _extract_chain_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    l0 = _query_one_int(
        conn,
        "SELECT COUNT(DISTINCT property_id) FROM properties_market WHERE listing_month = 0",
    )
    b0_role = _query_one_int(
        conn,
        """
        SELECT COUNT(DISTINCT agent_id)
        FROM decision_logs
        WHERE month = 1 AND event_type = 'ROLE_DECISION' AND decision IN ('BUYER', 'BUYER_SELLER')
        """,
    )
    s0_role = _query_one_int(
        conn,
        """
        SELECT COUNT(DISTINCT agent_id)
        FROM decision_logs
        WHERE month = 1 AND event_type = 'ROLE_DECISION' AND decision IN ('SELLER', 'BUYER_SELLER')
        """,
    )
    b0_order = _query_one_int(
        conn,
        "SELECT COUNT(DISTINCT buyer_id) FROM transaction_orders WHERE created_month = 1",
    )
    return {
        "l0": int(l0),
        "b0_role": int(b0_role),
        "s0_role": int(s0_role),
        "b0_order": int(b0_order),
        "r_role": round((float(b0_role) / float(l0)) if l0 > 0 else 0.0, 4),
        "r_order": round((float(b0_order) / float(l0)) if l0 > 0 else 0.0, 4),
    }


def _extract_buyer_path_breakdown(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        buyer_roles = {
            int(row["agent_id"])
            for row in conn.execute(
                """
                SELECT DISTINCT agent_id
                FROM decision_logs
                WHERE month = 1 AND event_type = 'ROLE_DECISION' AND decision IN ('BUYER', 'BUYER_SELLER')
                """
            ).fetchall()
        }
    except Exception:
        buyer_roles = set()

    try:
        shortlist_buyers = {
            int(row["buyer_id"])
            for row in conn.execute(
                "SELECT DISTINCT buyer_id FROM property_buyer_matches WHERE month = 1"
            ).fetchall()
        }
    except Exception:
        shortlist_buyers = set()

    try:
        selected_buyers = {
            int(row["buyer_id"])
            for row in conn.execute(
                "SELECT DISTINCT buyer_id FROM property_buyer_matches WHERE month = 1 AND selected_in_shortlist = 1"
            ).fetchall()
        }
    except Exception:
        selected_buyers = set()

    try:
        neg_buyers = {
            int(row["buyer_id"])
            for row in conn.execute(
                "SELECT DISTINCT buyer_id FROM property_buyer_matches WHERE month = 1 AND proceeded_to_negotiation = 1"
            ).fetchall()
        }
    except Exception:
        neg_buyers = set()

    try:
        order_buyers = {
            int(row["buyer_id"])
            for row in conn.execute(
                "SELECT DISTINCT buyer_id FROM transaction_orders WHERE created_month = 1"
            ).fetchall()
        }
    except Exception:
        order_buyers = set()

    no_selection = Counter()
    try:
        rows = conn.execute(
            """
            SELECT thought_process
            FROM decision_logs
            WHERE month = 1 AND event_type = 'BUYER_MATCH_SUMMARY'
            """
        ).fetchall()
    except Exception:
        rows = []
    for row in rows:
        thought = _safe_json_loads(row["thought_process"])
        code = str(thought.get("no_selection_reason_code", "") or "").strip()
        if code and code != "HAS_SELECTION":
            no_selection[code] += 1

    buyer_role_count = int(len(buyer_roles))
    shortlist_count = int(len(shortlist_buyers))
    selected_count = int(len(selected_buyers))
    neg_count = int(len(neg_buyers))
    order_count = int(len(order_buyers))
    return {
        "buyer_roles": buyer_role_count,
        "shortlist_buyers": shortlist_count,
        "selected_buyers": selected_count,
        "negotiation_buyers": neg_count,
        "order_buyers": order_count,
        "role_to_shortlist": round((float(shortlist_count) / float(buyer_role_count)) if buyer_role_count > 0 else 0.0, 4),
        "shortlist_to_selected": round((float(selected_count) / float(shortlist_count)) if shortlist_count > 0 else 0.0, 4),
        "selected_to_negotiation": round((float(neg_count) / float(selected_count)) if selected_count > 0 else 0.0, 4),
        "negotiation_to_order": round((float(order_count) / float(neg_count)) if neg_count > 0 else 0.0, 4),
        "role_gap_to_shortlist": int(max(0, buyer_role_count - shortlist_count)),
        "top_no_selection_codes": dict(no_selection.most_common(8)),
    }


def _extract_routing_summary(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        row = conn.execute(
            """
            SELECT context_metrics
            FROM decision_logs
            WHERE event_type = 'ROLE_ACTIVATION_ROUTING_SUMMARY'
            ORDER BY log_id DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return {}
    return _safe_json_loads(row["context_metrics"])


def _extract_rule_correction_counts(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        rows = conn.execute(
            """
            SELECT reason, thought_process
            FROM decision_logs
            WHERE month = 1 AND event_type = 'ROLE_DECISION'
            """
        ).fetchall()
    except Exception:
        rows = []
    hard_constraint = 0
    overridden_buy_lock = 0
    route_counter = Counter()
    for row in rows:
        reason = str(row["reason"] or "")
        thought = _safe_json_loads(row["thought_process"])
        route_source = str(thought.get("_decision_origin", "llm_batch") or "llm_batch")
        route_counter[route_source] += 1
        if "System constrained" in reason:
            hard_constraint += 1
        if str(thought.get("override_reason_code", "") or "") == "OUTBID_BUY_TASK_LOCK":
            overridden_buy_lock += 1
    return {
        "hard_constraint_corrections": int(hard_constraint),
        "buy_lock_overrides": int(overridden_buy_lock),
        "route_source_counts": dict(route_counter),
    }


def _extract_price_adjustment_breakdown(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        rows = conn.execute(
            """
            SELECT
                dl.decision,
                dl.llm_called,
                dl.context_metrics,
                COALESCE(ast.agent_type, 'normal') AS agent_type
            FROM decision_logs dl
            LEFT JOIN agents_static ast ON ast.agent_id = dl.agent_id
            WHERE dl.event_type = 'PRICE_ADJUSTMENT'
            """
        ).fetchall()
    except Exception:
        rows = []
    by_type: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "total": 0,
            "llm_called": 0,
            "actions": Counter(),
            "pricing_modes": Counter(),
        }
    )
    for row in rows:
        agent_type = str(row["agent_type"] or "normal").lower()
        action = str(row["decision"] or "").upper()
        metrics = _safe_json_loads(row["context_metrics"])
        pricing_mode = str(metrics.get("pricing_mode", "unknown") or "unknown")
        by_type[agent_type]["total"] += 1
        by_type[agent_type]["actions"][action] += 1
        by_type[agent_type]["pricing_modes"][pricing_mode] += 1
        if bool(row["llm_called"]):
            by_type[agent_type]["llm_called"] += 1

    payload: Dict[str, Any] = {}
    for agent_type, item in by_type.items():
        total = int(item["total"])
        actions = dict(item["actions"])
        llm_called = int(item["llm_called"])
        payload[agent_type] = {
            "total": total,
            "llm_called": llm_called,
            "llm_called_share": round((float(llm_called) / float(total)) if total > 0 else 0.0, 4),
            "actions": actions,
            "pricing_modes": dict(item["pricing_modes"]),
            "raise_actions": int(actions.get("E", 0) + actions.get("F", 0)),
            "defer_or_raise_actions": int(actions.get("D", 0) + actions.get("E", 0) + actions.get("F", 0)),
        }
    return payload


def _extract_listing_action_breakdown(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        rows = conn.execute(
            """
            SELECT
                dl.decision,
                dl.llm_called,
                dl.thought_process,
                dl.context_metrics,
                COALESCE(ast.agent_type, 'normal') AS agent_type
            FROM decision_logs dl
            LEFT JOIN agents_static ast ON ast.agent_id = dl.agent_id
            WHERE dl.event_type = 'LISTING_ACTION'
            """
        ).fetchall()
    except Exception:
        rows = []
    by_type: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "total": 0,
            "llm_called": 0,
            "decisions": Counter(),
            "strategies": Counter(),
        }
    )
    for row in rows:
        agent_type = str(row["agent_type"] or "normal").lower()
        decision = str(row["decision"] or "").upper()
        thought = _safe_json_loads(row["thought_process"])
        metrics = _safe_json_loads(row["context_metrics"])
        strategy = str(
            thought.get("strategy")
            or metrics.get("strategy")
            or "UNKNOWN"
        ).upper()
        by_type[agent_type]["total"] += 1
        by_type[agent_type]["decisions"][decision] += 1
        by_type[agent_type]["strategies"][strategy] += 1
        if bool(row["llm_called"]):
            by_type[agent_type]["llm_called"] += 1

    output: Dict[str, Any] = {}
    for agent_type, item in by_type.items():
        total = int(item["total"])
        llm_called = int(item["llm_called"])
        output[agent_type] = {
            "total": total,
            "llm_called": llm_called,
            "llm_called_share": round((float(llm_called) / float(total)) if total > 0 else 0.0, 4),
            "decisions": dict(item["decisions"]),
            "strategies": dict(item["strategies"]),
        }
    return output


def _extract_role_profile_breakdown(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        rows = conn.execute(
            """
            SELECT
                dl.decision,
                dl.context_metrics,
                COALESCE(ast.agent_type, 'normal') AS agent_type
            FROM decision_logs dl
            LEFT JOIN agents_static ast ON ast.agent_id = dl.agent_id
            WHERE dl.month = 1 AND dl.event_type = 'ROLE_DECISION'
            """
        ).fetchall()
    except Exception:
        rows = []
    by_type: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "total": 0,
            "roles": Counter(),
            "delayed": 0,
        }
    )
    for row in rows:
        agent_type = str(row["agent_type"] or "normal").lower()
        metrics = _safe_json_loads(row["context_metrics"])
        role = str(row["decision"] or "UNKNOWN").upper()
        by_type[agent_type]["total"] += 1
        by_type[agent_type]["roles"][role] += 1
        if int(metrics.get("m14_info_delay_months", 0) or 0) > 0:
            by_type[agent_type]["delayed"] += 1
    output: Dict[str, Any] = {}
    for agent_type, item in by_type.items():
        total = int(item["total"])
        output[agent_type] = {
            "total": total,
            "roles": dict(item["roles"]),
            "delayed_count": int(item["delayed"]),
            "delayed_share": round((float(item["delayed"]) / float(total)) if total > 0 else 0.0, 4),
        }
    return output


def _grade_run(run_metrics: Dict[str, Any]) -> Dict[str, Any]:
    price = run_metrics.get("price_adjustment", {})
    normal = price.get("normal", {})
    listing = run_metrics.get("listing_action", {})
    normal_listing = listing.get("normal", {})
    chain = run_metrics.get("chain_metrics", {})
    buyer_path = run_metrics.get("buyer_path", {})

    reasons: List[str] = []
    pass_flag = True

    if int(chain.get("b0_role", 0)) <= 0 or int(chain.get("l0", 0)) <= 0:
        pass_flag = False
        reasons.append("missing_chain_pressure")
    if int(buyer_path.get("buyer_roles", 0)) > 0 and float(buyer_path.get("role_to_shortlist", 0.0)) < 0.3:
        pass_flag = False
        reasons.append("buyer_role_to_shortlist_too_low")
    if int(normal_listing.get("total", 0)) > 0 and int(normal_listing.get("llm_called", 0)) <= 0:
        pass_flag = False
        reasons.append("normal_listing_never_called_llm")
    if int(normal.get("total", 0)) <= 0:
        pass_flag = False
        reasons.append("no_normal_price_adjustment_sample_in_month1")
    if int(normal.get("total", 0)) > 0 and int(normal.get("llm_called", 0)) <= 0:
        pass_flag = False
        reasons.append("normal_price_adjustment_never_called_llm")
    if int(normal.get("total", 0)) > 0 and int(normal.get("defer_or_raise_actions", 0)) <= 0:
        pass_flag = False
        reasons.append("normal_price_adjustment_only_a_b_c")

    return {
        "status": "pass" if pass_flag else "fail",
        "reasons": reasons,
    }


def _summarize_groups(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for run in runs:
        groups[run["group"]].append(run)

    summary: Dict[str, Any] = {}
    for group, items in groups.items():
        summary[group] = {
            "seed_count": len(items),
            "avg_r_role": round(sum(float(x["chain_metrics"]["r_role"]) for x in items) / max(1, len(items)), 4),
            "avg_r_order": round(sum(float(x["chain_metrics"]["r_order"]) for x in items) / max(1, len(items)), 4),
            "pass_count": sum(1 for x in items if x["grade"]["status"] == "pass"),
        }
    return summary


def _render_md(payload: Dict[str, Any]) -> str:
    lines = [
        "# 课题线 A：自然激活与行为一致性分析",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 批次目录: {payload['batch_dir']}",
        f"- 成功样本数: {payload['run_count']}",
        "",
        "## 一、逐运行链路指标",
        "",
        "| 组别 | seed | L0 | B0_role | B0_order | R_role | R_order | normal_listing_llm | normal_price_llm | normal_D/E/F | 判定 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for run in payload["runs"]:
        chain = run["chain_metrics"]
        normal_listing = run.get("listing_action", {}).get("normal", {})
        normal = run.get("price_adjustment", {}).get("normal", {})
        lines.append(
            f"| {run['group']} | {run['seed']} | {chain['l0']} | {chain['b0_role']} | {chain['b0_order']} | "
            f"{chain['r_role']:.4f} | {chain['r_order']:.4f} | {normal_listing.get('llm_called', 0)} | "
            f"{normal.get('llm_called', 0)} | "
            f"{normal.get('defer_or_raise_actions', 0)} | {run['grade']['status']} |"
        )

    lines.extend(
        [
            "",
            "## 二、Buyer Path",
            "",
            "| 组别 | seed | buyer_roles | shortlist_buyers | selected_buyers | negotiation_buyers | order_buyers | role_to_shortlist |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for run in payload["runs"]:
        buyer_path = run.get("buyer_path", {})
        lines.append(
            f"| {run['group']} | {run['seed']} | {buyer_path.get('buyer_roles', 0)} | "
            f"{buyer_path.get('shortlist_buyers', 0)} | {buyer_path.get('selected_buyers', 0)} | "
            f"{buyer_path.get('negotiation_buyers', 0)} | {buyer_path.get('order_buyers', 0)} | "
            f"{buyer_path.get('role_to_shortlist', 0.0):.4f} |"
        )

    lines.extend(
        [
            "",
            "## 三、组别汇总",
            "",
            "```json",
            json.dumps(payload["group_summary"], ensure_ascii=False, indent=2),
            "```",
            "",
            "## 四、说明",
            "",
            "1. `normal_listing_llm` 表示普通卖家在挂牌入口样本中 `llm_called=1` 的次数，用于确认普通路径是否真的进入 LLM 挂牌决策。",
            "2. `normal_price_llm` 表示普通卖家调价样本中 `llm_called=1` 的次数。",
            "3. `role_to_shortlist` 用于衡量买方角色是否真正进入候选房源/shortlist 阶段；若这一段过低，说明有效供需压力在 shortlist 之前就被耗散。",
            "4. `normal_D/E/F` 表示普通卖家在调价阶段出现 `D/E/F` 的次数，用于判断是否仍被 `A/B/C` 单边压制。",
            "5. 若 `grade` 出现 `no_normal_price_adjustment_sample_in_month1`，表示固定 1 个月矩阵尚未自然走到 repricing 阶段，不能据此宣布普通卖家调价修复已闭环。",
            "6. `grade` 只用于课题线 A 的行为一致性筛查，不代表市场发布结论。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze line-A natural activation batch.")
    parser.add_argument("batch_dir", help="Batch directory that contains batch_summary.json")
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir).resolve()
    summary_path = batch_dir / "batch_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"batch_summary.json not found: {summary_path}")

    batch_summary = _load_json(summary_path)
    runs_out: List[Dict[str, Any]] = []

    for item in batch_summary.get("runs", []):
        if str(item.get("status")) != "success":
            continue
        db_path = _resolve_db_path(item)
        if not db_path or not db_path.exists():
            continue
        conn = _connect(db_path)
        try:
            run_metrics = {
                "group": str(item.get("group_key", "")),
                "seed": int(item.get("seed", 0)),
                "run_dir": str(item.get("run_dir", "")),
                "db_path": str(db_path),
                "chain_metrics": _extract_chain_metrics(conn),
                "routing_summary": _extract_routing_summary(conn),
                "rule_corrections": _extract_rule_correction_counts(conn),
                "buyer_path": _extract_buyer_path_breakdown(conn),
                "price_adjustment": _extract_price_adjustment_breakdown(conn),
                "listing_action": _extract_listing_action_breakdown(conn),
                "role_profiles": _extract_role_profile_breakdown(conn),
            }
            run_metrics["grade"] = _grade_run(run_metrics)
            runs_out.append(run_metrics)
        finally:
            conn.close()

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_dir": str(batch_dir),
        "run_count": len(runs_out),
        "runs": runs_out,
        "group_summary": _summarize_groups(runs_out),
    }

    json_path = batch_dir / "line_a_natural_activation_analysis.json"
    md_path = batch_dir / "line_a_natural_activation_analysis.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_md(payload), encoding="utf-8")
    print(f"line-a analysis written: {json_path}")
    print(f"line-a analysis written: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
