#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按批次汇总研究实验结果，并重点生成 A/B 区链路指标。

用途：
1. 面向当前研究验证阶段，自动从 batch_summary.json 遍历每个 run 的 simulation.db。
2. 输出对比友好的 JSON / Markdown 摘要，重点观察：
   - listing / match / negotiation / order / transaction 的 A/B 区分布
   - 成交漏斗：matches -> negotiations -> orders -> transactions
   - 动机分层成交（按买方 purchase_motive_primary）
   - B区 是否进入主成交链，以及在哪个环节损失
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List


def _fetch_scalar(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> Any:
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def _table_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _count_by_zone(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> Dict[str, int]:
    cur.execute(sql, params)
    rows = cur.fetchall()
    return {str(zone): int(count or 0) for zone, count in rows}


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _count_by_key(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> Dict[str, int]:
    cur.execute(sql, params)
    rows = cur.fetchall()
    result: Dict[str, int] = {}
    for key, count in rows:
        norm = str(key) if key not in (None, "") else "UNKNOWN"
        result[norm] = int(count or 0)
    return result


def analyze_run(run_item: Dict[str, Any]) -> Dict[str, Any]:
    db_path = Path(run_item["db_path"])
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        pbm_cols = _table_columns(cur, "property_buyer_matches")
        has_match_context = "match_context" in pbm_cols
        has_selected_in_shortlist = "selected_in_shortlist" in pbm_cols
        tables = {row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        zone_end_active_listings = _count_by_zone(
            cur,
            """
            SELECT ps.zone, COUNT(*)
            FROM properties_market pm
            JOIN properties_static ps ON ps.property_id = pm.property_id
            WHERE pm.status='for_sale'
            GROUP BY ps.zone
            """,
        )
        zone_period_listings = _count_by_zone(
            cur,
            """
            SELECT ps.zone, COUNT(*)
            FROM properties_market pm
            JOIN properties_static ps ON ps.property_id = pm.property_id
            WHERE pm.listing_month IS NOT NULL
            GROUP BY ps.zone
            """,
        )
        zone_matches = _count_by_zone(
            cur,
            """
            SELECT ps.zone, COUNT(*)
            FROM property_buyer_matches pbm
            JOIN properties_static ps ON ps.property_id = pbm.property_id
            GROUP BY ps.zone
            """,
        )
        zone_negotiations = _count_by_zone(
            cur,
            """
            SELECT ps.zone, COUNT(*)
            FROM negotiations n
            JOIN properties_static ps ON ps.property_id = n.property_id
            GROUP BY ps.zone
            """,
        )
        zone_orders = _count_by_zone(
            cur,
            """
            SELECT ps.zone, COUNT(*)
            FROM transaction_orders o
            JOIN properties_static ps ON ps.property_id = o.property_id
            GROUP BY ps.zone
            """,
        )
        zone_transactions = _count_by_zone(
            cur,
            """
            SELECT ps.zone, COUNT(*)
            FROM transactions t
            JOIN properties_static ps ON ps.property_id = t.property_id
            GROUP BY ps.zone
            """,
        )
        zone_avg_price = _count_by_zone(
            cur,
            """
            SELECT ps.zone, ROUND(AVG(t.final_price), 2)
            FROM transactions t
            JOIN properties_static ps ON ps.property_id = t.property_id
            GROUP BY ps.zone
            """,
        )
        b_shortlist_selected = 0
        if has_selected_in_shortlist:
            b_shortlist_selected = int(
                _fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM property_buyer_matches pbm
                    JOIN properties_static ps ON ps.property_id = pbm.property_id
                    WHERE ps.zone='B' AND COALESCE(pbm.selected_in_shortlist, 0)=1
                    """,
                )
                or 0
            )

        b_match_context_rows = 0
        if has_match_context:
            b_match_context_rows = int(
                _fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM property_buyer_matches pbm
                    JOIN properties_static ps ON ps.property_id = pbm.property_id
                    WHERE ps.zone='B' AND COALESCE(pbm.match_context, '') <> ''
                    """,
                )
                or 0
            )

        total_matches = int(_fetch_scalar(cur, "SELECT COUNT(*) FROM property_buyer_matches") or 0)
        total_negotiations = int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(*) FROM property_buyer_matches WHERE COALESCE(proceeded_to_negotiation, 0)=1",
            )
            or 0
        )
        total_orders = int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(*) FROM property_buyer_matches WHERE order_id IS NOT NULL",
            )
            or 0
        )
        total_pending_settlement_orders = int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(*) FROM transaction_orders WHERE status='pending_settlement'",
            )
            or 0
        )
        total_transactions = int(_fetch_scalar(cur, "SELECT COUNT(*) FROM transactions") or 0)
        is_short_window = int(run_item.get("months", 0) or 0) <= 2
        primary_close_metric = (
            "order_to_effective_close_rate" if is_short_window else "order_to_tx_rate"
        )

        zone_loss_metrics: Dict[str, Dict[str, float | int]] = {}
        for zone in ("A", "B"):
            period_listings = int(zone_period_listings.get(zone, 0))
            end_active_listings = int(zone_end_active_listings.get(zone, 0))
            matches = int(
                _fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM property_buyer_matches pbm
                    JOIN properties_static ps ON ps.property_id = pbm.property_id
                    WHERE ps.zone=?
                    """,
                    (zone,),
                )
                or 0
            )
            negotiations = int(
                _fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM property_buyer_matches pbm
                    JOIN properties_static ps ON ps.property_id = pbm.property_id
                    WHERE ps.zone=? AND COALESCE(pbm.proceeded_to_negotiation, 0)=1
                    """,
                    (zone,),
                )
                or 0
            )
            orders = int(
                _fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM property_buyer_matches pbm
                    JOIN properties_static ps ON ps.property_id = pbm.property_id
                    WHERE ps.zone=? AND pbm.order_id IS NOT NULL
                    """,
                    (zone,),
                )
                or 0
            )
            pending_settlement_orders = int(
                _fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM transaction_orders o
                    JOIN properties_static ps ON ps.property_id = o.property_id
                    WHERE ps.zone=? AND o.status='pending_settlement'
                    """,
                    (zone,),
                )
                or 0
            )
            transactions = int(zone_transactions.get(zone, 0))

            zone_loss_metrics[zone] = {
                "period_listings": period_listings,
                "end_active_listings": end_active_listings,
                "matches": matches,
                "negotiations": negotiations,
                "orders": orders,
                "pending_settlement_orders": pending_settlement_orders,
                "transactions": transactions,
                "match_to_neg_rate": round(_safe_div(negotiations, matches), 4),
                "neg_to_order_rate": round(_safe_div(orders, negotiations), 4),
                "order_to_tx_rate": round(_safe_div(transactions, orders), 4),
                "order_to_effective_close_rate": round(
                    _safe_div(transactions + pending_settlement_orders, orders),
                    4,
                ),
                "match_to_tx_rate": round(_safe_div(transactions, matches), 4),
                "match_to_period_listing_rate": round(_safe_div(matches, period_listings), 4),
                "tx_to_period_listing_rate": round(_safe_div(transactions, period_listings), 4),
                "primary_close_metric": primary_close_metric,
                "primary_close_rate": round(
                    _safe_div(transactions + pending_settlement_orders, orders)
                    if is_short_window
                    else _safe_div(transactions, orders),
                    4,
                ),
            }

        motivation_transactions: Dict[str, int] = {}
        if "agents_static" in tables:
            motivation_transactions = _count_by_key(
                cur,
                """
                SELECT COALESCE(a.purchase_motive_primary, 'UNKNOWN') AS motive, COUNT(*) AS cnt
                FROM transactions t
                JOIN agents_static a ON a.agent_id = t.buyer_id
                GROUP BY motive
                ORDER BY cnt DESC
                """,
            )

        top_motivation = "UNKNOWN"
        if motivation_transactions:
            top_motivation = max(motivation_transactions.items(), key=lambda kv: kv[1])[0]

        price_adjustment_total = 0
        price_adjustment_llm_called = 0
        price_adjustment_e_count = 0
        demand_heat_distribution: Dict[str, int] = {}
        style_price_adjust_actions: Dict[str, Dict[str, int]] = {}
        style_price_adjust_summary: Dict[str, Dict[str, float | int]] = {}
        if "decision_logs" in tables:
            price_adjustment_total = int(
                _fetch_scalar(
                    cur,
                    "SELECT COUNT(*) FROM decision_logs WHERE event_type='PRICE_ADJUSTMENT'",
                )
                or 0
            )
            price_adjustment_llm_called = int(
                _fetch_scalar(
                    cur,
                    "SELECT COUNT(*) FROM decision_logs WHERE event_type='PRICE_ADJUSTMENT' AND COALESCE(llm_called,0)=1",
                )
                or 0
            )
            price_adjustment_e_count = int(
                _fetch_scalar(
                    cur,
                    "SELECT COUNT(*) FROM decision_logs WHERE event_type='PRICE_ADJUSTMENT' AND UPPER(COALESCE(decision,''))='E'",
                )
                or 0
            )
            cur.execute(
                "SELECT context_metrics FROM decision_logs WHERE event_type='PRICE_ADJUSTMENT' AND context_metrics IS NOT NULL"
            )
            for (raw_metrics,) in cur.fetchall() or []:
                if not raw_metrics:
                    continue
                try:
                    metrics = json.loads(raw_metrics)
                except Exception:
                    continue
                recent_ctx = metrics.get("recent_demand_context", {}) if isinstance(metrics, dict) else {}
                band = str((recent_ctx or {}).get("band", "UNKNOWN") or "UNKNOWN").upper()
                demand_heat_distribution[band] = int(demand_heat_distribution.get(band, 0)) + 1

            if "agents_static" in tables:
                cur.execute(
                    """
                    SELECT LOWER(COALESCE(a.investment_style, 'unknown')) AS style_key,
                           UPPER(COALESCE(dl.decision, 'UNKNOWN')) AS action_key,
                           COUNT(*) AS cnt
                    FROM decision_logs dl
                    JOIN agents_static a ON a.agent_id = dl.agent_id
                    WHERE dl.event_type='PRICE_ADJUSTMENT'
                    GROUP BY style_key, action_key
                    """
                )
                for style_key, action_key, cnt in cur.fetchall() or []:
                    s = str(style_key or "unknown")
                    a_key = str(action_key or "UNKNOWN")
                    style_price_adjust_actions.setdefault(s, {})
                    style_price_adjust_actions[s][a_key] = int(cnt or 0)

                for style_key, action_counts in style_price_adjust_actions.items():
                    total = int(sum(int(v or 0) for v in action_counts.values()))
                    e_cnt = int(action_counts.get("E", 0))
                    cd_cnt = int(action_counts.get("C", 0) + action_counts.get("D", 0))
                    style_price_adjust_summary[style_key] = {
                        "total": total,
                        "e_count": e_cnt,
                        "cd_count": cd_cnt,
                        "e_share": round(_safe_div(e_cnt, total), 4),
                        "cd_share": round(_safe_div(cd_cnt, total), 4),
                    }

        aggressive_summary = style_price_adjust_summary.get("aggressive", {})
        non_aggressive_total = 0
        non_aggressive_e = 0
        non_aggressive_cd = 0
        for style_key in ("balanced", "conservative"):
            item = style_price_adjust_summary.get(style_key, {})
            non_aggressive_total += int(item.get("total", 0) or 0)
            non_aggressive_e += int(item.get("e_count", 0) or 0)
            non_aggressive_cd += int(item.get("cd_count", 0) or 0)
        non_aggressive_summary = {
            "total": int(non_aggressive_total),
            "e_count": int(non_aggressive_e),
            "cd_count": int(non_aggressive_cd),
            "e_share": round(_safe_div(non_aggressive_e, non_aggressive_total), 4),
            "cd_share": round(_safe_div(non_aggressive_cd, non_aggressive_total), 4),
        }

        b_failure_top: Dict[str, int] = {}
        if "property_buyer_matches" in tables:
            b_failure_top = _count_by_key(
                cur,
                """
                SELECT pbm.failure_reason, COUNT(*) AS cnt
                FROM property_buyer_matches pbm
                JOIN properties_static ps ON ps.property_id = pbm.property_id
                WHERE ps.zone='B'
                  AND COALESCE(TRIM(pbm.failure_reason), '') <> ''
                GROUP BY pbm.failure_reason
                ORDER BY cnt DESC
                LIMIT 8
                """,
            )

        bulletin_exposure_rows = 0
        lagged_exposure_rows = 0
        exposure_agent_count = 0
        avg_applied_lag_months = 0.0
        avg_visible_bulletins = 0.0
        max_seen_bulletin_month = 0
        if "bulletin_exposure_log" in tables:
            bulletin_exposure_rows = int(
                _fetch_scalar(cur, "SELECT COUNT(*) FROM bulletin_exposure_log")
                or 0
            )
            lagged_exposure_rows = int(
                _fetch_scalar(cur, "SELECT COUNT(*) FROM bulletin_exposure_log WHERE COALESCE(applied_lag_months,0) > 0")
                or 0
            )
            exposure_agent_count = int(
                _fetch_scalar(cur, "SELECT COUNT(DISTINCT agent_id) FROM bulletin_exposure_log")
                or 0
            )
            avg_applied_lag_months = float(
                _fetch_scalar(cur, "SELECT ROUND(AVG(COALESCE(applied_lag_months,0)), 4) FROM bulletin_exposure_log")
                or 0.0
            )
            avg_visible_bulletins = float(
                _fetch_scalar(cur, "SELECT ROUND(AVG(COALESCE(visible_bulletins,0)), 4) FROM bulletin_exposure_log")
                or 0.0
            )
            max_seen_bulletin_month = int(
                _fetch_scalar(cur, "SELECT COALESCE(MAX(seen_bulletin_month),0) FROM bulletin_exposure_log")
                or 0
            )

        return {
            "group_key": run_item.get("group_key", "UNKNOWN"),
            "group_name": run_item.get("group_name", "UNKNOWN"),
            "seed": run_item.get("seed", 0),
            "status": run_item.get("status", "unknown"),
            "run_dir": run_item.get("run_dir", ""),
            "db_path": run_item.get("db_path", ""),
            "months": run_item.get("months", 0),
            "agent_count": run_item.get("agent_count", 0),
            "property_count": run_item.get("property_count", 0),
            "elapsed_seconds": run_item.get("elapsed_seconds", 0),
            "zone_period_listings": zone_period_listings,
            "zone_end_active_listings": zone_end_active_listings,
            "zone_matches": zone_matches,
            "zone_negotiations": zone_negotiations,
            "zone_orders": zone_orders,
            "zone_transactions": zone_transactions,
            "zone_avg_price": zone_avg_price,
            "funnel": {
                "matches": total_matches,
                "negotiation_entries": total_negotiations,
                "orders": total_orders,
                "pending_settlement_orders": total_pending_settlement_orders,
                "transactions": total_transactions,
                "match_to_neg_rate": round(_safe_div(total_negotiations, total_matches), 4),
                "neg_to_order_rate": round(_safe_div(total_orders, total_negotiations), 4),
                "order_to_tx_rate": round(_safe_div(total_transactions, total_orders), 4),
                "order_to_effective_close_rate": round(
                    _safe_div(total_transactions + total_pending_settlement_orders, total_orders),
                    4,
                ),
                "match_to_tx_rate": round(_safe_div(total_transactions, total_matches), 4),
                "primary_close_metric": primary_close_metric,
                "primary_close_rate": round(
                    _safe_div(total_transactions + total_pending_settlement_orders, total_orders)
                    if is_short_window
                    else _safe_div(total_transactions, total_orders),
                    4,
                ),
            },
            "zone_loss_metrics": zone_loss_metrics,
            "motivation_transactions": motivation_transactions,
            "top_motivation": top_motivation,
            "price_adjustment_total": price_adjustment_total,
            "price_adjustment_llm_called": price_adjustment_llm_called,
            "price_adjustment_e_count": price_adjustment_e_count,
            "price_adjustment_demand_heat_distribution": demand_heat_distribution,
            "style_price_adjust_actions": style_price_adjust_actions,
            "style_price_adjust_summary": style_price_adjust_summary,
            "aggressive_price_adjust_summary": aggressive_summary,
            "non_aggressive_price_adjust_summary": non_aggressive_summary,
            "b_failure_reason_top": b_failure_top,
            "b_shortlist_selected": b_shortlist_selected,
            "b_match_context_rows": b_match_context_rows,
            "has_match_context": has_match_context,
            "has_selected_in_shortlist": has_selected_in_shortlist,
            "bulletin_exposure_rows": bulletin_exposure_rows,
            "lagged_exposure_rows": lagged_exposure_rows,
            "exposure_agent_count": exposure_agent_count,
            "avg_applied_lag_months": avg_applied_lag_months,
            "avg_visible_bulletins": avg_visible_bulletins,
            "max_seen_bulletin_month": max_seen_bulletin_month,
        }
    finally:
        conn.close()


def write_outputs(batch_dir: Path, batch_payload: Dict[str, Any], run_reports: List[Dict[str, Any]]) -> None:
    json_path = batch_dir / "zone_chain_summary.json"
    md_path = batch_dir / "zone_chain_summary.md"

    payload = {
        "generated_at": batch_payload.get("generated_at"),
        "batch_dir": str(batch_dir.resolve()),
        "run_count": len(run_reports),
        "runs": run_reports,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: List[str] = [
        "# 区域链路汇总",
        "",
        f"- 批次目录: {batch_dir.resolve()}",
        f"- 轮数: {len(run_reports)}",
        "",
    ]
    for item in run_reports:
        lines.extend(
            [
                f"## {item['group_name']} / seed {item['seed']}",
                "",
                f"- 状态: {item['status']}",
                f"- 耗时(秒): {item['elapsed_seconds']}",
                f"- 运行目录: {item['run_dir']}",
                f"- B区 shortlist 选中条数: {item['b_shortlist_selected']}",
                f"- B区 match_context 记录条数: {item['b_match_context_rows']}",
                f"- 公报暴露记录数: {item['bulletin_exposure_rows']}",
                f"- 滞后暴露记录数: {item['lagged_exposure_rows']}",
                f"- 暴露Agent数: {item['exposure_agent_count']}",
                f"- 平均滞后月数: {item['avg_applied_lag_months']}",
                f"- 平均可见公报条数: {item['avg_visible_bulletins']}",
                f"- 可见最新公报月: {item['max_seen_bulletin_month']}",
                "",
                f"- 期间挂牌量: {json.dumps(item['zone_period_listings'], ensure_ascii=False)}",
                f"- 期末在售量: {json.dumps(item['zone_end_active_listings'], ensure_ascii=False)}",
                f"- 匹配: {json.dumps(item['zone_matches'], ensure_ascii=False)}",
                f"- 谈判: {json.dumps(item['zone_negotiations'], ensure_ascii=False)}",
                f"- 订单: {json.dumps(item['zone_orders'], ensure_ascii=False)}",
                f"- 成交: {json.dumps(item['zone_transactions'], ensure_ascii=False)}",
                f"- 分区均价: {json.dumps(item['zone_avg_price'], ensure_ascii=False)}",
                "",
                f"- 漏斗: {json.dumps(item['funnel'], ensure_ascii=False)}",
                f"- 动机分层成交: {json.dumps(item['motivation_transactions'], ensure_ascii=False)}",
                f"- 主导动机: {item['top_motivation']}",
                f"- 分区损失诊断: {json.dumps(item['zone_loss_metrics'], ensure_ascii=False)}",
                f"- 调价日志: total={item['price_adjustment_total']}, llm_called={item['price_adjustment_llm_called']}, E={item['price_adjustment_e_count']}",
                f"- 调价需求热度分布: {json.dumps(item['price_adjustment_demand_heat_distribution'], ensure_ascii=False)}",
                f"- 风格调价动作分布: {json.dumps(item['style_price_adjust_actions'], ensure_ascii=False)}",
                f"- 激进风格调价摘要: {json.dumps(item['aggressive_price_adjust_summary'], ensure_ascii=False)}",
                f"- 非激进风格调价摘要: {json.dumps(item['non_aggressive_price_adjust_summary'], ensure_ascii=False)}",
                f"- B区失败原因Top: {json.dumps(item['b_failure_reason_top'], ensure_ascii=False)}",
                "",
            ]
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="分析研究实验批次的区域链路")
    parser.add_argument("batch_dir", help="批次目录，需包含 batch_summary.json")
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir).resolve()
    batch_summary = batch_dir / "batch_summary.json"
    payload = json.loads(batch_summary.read_text(encoding="utf-8"))
    run_reports = [analyze_run(item) for item in payload.get("runs", [])]
    write_outputs(batch_dir, payload, run_reports)
    print(batch_dir / "zone_chain_summary.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
