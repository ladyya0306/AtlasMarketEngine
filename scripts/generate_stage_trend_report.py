#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从批次结果中自动提取“量价主判断”报告。

用途：
1. 面向当前阶段复现任务，自动从 batch_summary.json 遍历每个 run 的 simulation.db。
2. 固定提取：
   - 成交套数（transactions）
   - 成交均价（transactions.final_price）
   - 有效成交口径（transactions + pending_settlement）
   - 月度成交套数序列
   - 月度成交均价序列
3. 为“量价是否同向”提供最小证据面，不替研究员做最终现实判断。

用法：
  python scripts/generate_stage_trend_report.py <batch_dir>
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _fetch_scalar(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> Any:
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def _fetch_month_series(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    cur.execute(sql, params)
    rows = cur.fetchall()
    result: List[Dict[str, Any]] = []
    for month, value in rows:
        result.append(
            {
                "month": int(month or 0),
                "value": float(value or 0.0),
            }
        )
    return result


def analyze_run(run_item: Dict[str, Any]) -> Dict[str, Any]:
    db_path = Path(run_item["db_path"]).resolve()
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        transaction_count = int(
            _fetch_scalar(cur, "SELECT COUNT(*) FROM transactions") or 0
        )
        pending_settlement_count = int(
            _fetch_scalar(cur, "SELECT COUNT(*) FROM transaction_orders WHERE status='pending_settlement'") or 0
        )
        effective_close_count = int(transaction_count + pending_settlement_count)

        avg_transaction_price = float(
            _fetch_scalar(cur, "SELECT ROUND(AVG(final_price), 2) FROM transactions WHERE COALESCE(final_price, 0) > 0")
            or 0.0
        )
        avg_effective_order_price = float(
            _fetch_scalar(
                cur,
                """
                SELECT ROUND(AVG(agreed_price), 2)
                FROM transaction_orders
                WHERE status IN ('filled', 'pending_settlement')
                  AND COALESCE(agreed_price, 0) > 0
                """,
            )
            or 0.0
        )

        monthly_transaction_count = _fetch_month_series(
            cur,
            """
            SELECT month, COUNT(*)
            FROM transactions
            GROUP BY month
            ORDER BY month
            """,
        )
        monthly_transaction_avg_price = _fetch_month_series(
            cur,
            """
            SELECT month, ROUND(AVG(final_price), 2)
            FROM transactions
            WHERE COALESCE(final_price, 0) > 0
            GROUP BY month
            ORDER BY month
            """,
        )
        monthly_pending_settlement_count = _fetch_month_series(
            cur,
            """
            SELECT settlement_due_month, COUNT(*)
            FROM transaction_orders
            WHERE status='pending_settlement'
            GROUP BY settlement_due_month
            ORDER BY settlement_due_month
            """,
        )

        b_transaction_count = int(
            _fetch_scalar(
                cur,
            """
            SELECT COUNT(*)
            FROM transactions t
            JOIN properties_static ps ON ps.property_id = t.property_id
            WHERE UPPER(ps.zone)='B'
                """,
            )
            or 0
        )
        b_end_active_listing_count = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM properties_market pm
                JOIN properties_static ps ON ps.property_id = pm.property_id
                WHERE UPPER(ps.zone)='B'
                  AND pm.status='for_sale'
                """,
            )
            or 0
        )
        b_period_listing_count = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM properties_market pm
                JOIN properties_static ps ON ps.property_id = pm.property_id
                WHERE UPPER(ps.zone)='B'
                  AND pm.listing_month IS NOT NULL
                """,
            )
            or 0
        )
        b_avg_transaction_price = float(
            _fetch_scalar(
                cur,
                """
                SELECT ROUND(AVG(t.final_price), 2)
                FROM transactions t
                JOIN properties_static ps ON ps.property_id = t.property_id
                WHERE UPPER(ps.zone)='B'
                  AND COALESCE(t.final_price, 0) > 0
                """,
            )
            or 0.0
        )
        b_match_count = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM property_buyer_matches pbm
                JOIN properties_static ps ON ps.property_id = pbm.property_id
                WHERE UPPER(ps.zone)='B'
                """,
            )
            or 0
        )
        a_transaction_count = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM transactions t
                JOIN properties_static ps ON ps.property_id = t.property_id
                WHERE UPPER(ps.zone)='A'
                """,
            )
            or 0
        )
        a_end_active_listing_count = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM properties_market pm
                JOIN properties_static ps ON ps.property_id = pm.property_id
                WHERE UPPER(ps.zone)='A'
                  AND pm.status='for_sale'
                """,
            )
            or 0
        )
        a_period_listing_count = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM properties_market pm
                JOIN properties_static ps ON ps.property_id = pm.property_id
                WHERE UPPER(ps.zone)='A'
                  AND pm.listing_month IS NOT NULL
                """,
            )
            or 0
        )
        a_avg_transaction_price = float(
            _fetch_scalar(
                cur,
                """
                SELECT ROUND(AVG(t.final_price), 2)
                FROM transactions t
                JOIN properties_static ps ON ps.property_id = t.property_id
                WHERE UPPER(ps.zone)='A'
                  AND COALESCE(t.final_price, 0) > 0
                """,
            )
            or 0.0
        )
        a_match_count = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(*)
                FROM property_buyer_matches pbm
                JOIN properties_static ps ON ps.property_id = pbm.property_id
                WHERE UPPER(ps.zone)='A'
                """,
            )
            or 0
        )
        avg_price_to_initial_value_ratio = float(
            _fetch_scalar(
                cur,
                """
                SELECT ROUND(AVG(t.final_price / NULLIF(ps.initial_value, 0)), 4)
                FROM transactions t
                JOIN properties_static ps ON ps.property_id = t.property_id
                WHERE COALESCE(t.final_price, 0) > 0
                  AND COALESCE(ps.initial_value, 0) > 0
                """,
            )
            or 0.0
        )
        a_price_to_initial_value_ratio = float(
            _fetch_scalar(
                cur,
                """
                SELECT ROUND(AVG(t.final_price / NULLIF(ps.initial_value, 0)), 4)
                FROM transactions t
                JOIN properties_static ps ON ps.property_id = t.property_id
                WHERE UPPER(ps.zone)='A'
                  AND COALESCE(t.final_price, 0) > 0
                  AND COALESCE(ps.initial_value, 0) > 0
                """,
            )
            or 0.0
        )
        b_price_to_initial_value_ratio = float(
            _fetch_scalar(
                cur,
                """
                SELECT ROUND(AVG(t.final_price / NULLIF(ps.initial_value, 0)), 4)
                FROM transactions t
                JOIN properties_static ps ON ps.property_id = t.property_id
                WHERE UPPER(ps.zone)='B'
                  AND COALESCE(t.final_price, 0) > 0
                  AND COALESCE(ps.initial_value, 0) > 0
                """,
            )
            or 0.0
        )

        return {
            "group_key": run_item.get("group_key"),
            "group_name": run_item.get("group_name"),
            "seed": int(run_item.get("seed", 0) or 0),
            "status": run_item.get("status"),
            "run_dir": run_item.get("run_dir"),
            "db_path": str(db_path),
            "months": int(run_item.get("months", 0) or 0),
            "transaction_count": transaction_count,
            "pending_settlement_count": pending_settlement_count,
            "effective_close_count": effective_close_count,
            "avg_transaction_price": round(avg_transaction_price, 2),
            "avg_effective_order_price": round(avg_effective_order_price, 2),
            "monthly_transaction_count": monthly_transaction_count,
            "monthly_transaction_avg_price": monthly_transaction_avg_price,
            "monthly_pending_settlement_count": monthly_pending_settlement_count,
            "a_end_active_listing_count": a_end_active_listing_count,
            "a_period_listing_count": a_period_listing_count,
            "a_match_count": a_match_count,
            "a_transaction_count": a_transaction_count,
            "a_avg_transaction_price": round(a_avg_transaction_price, 2),
            "a_transaction_share": round(_safe_div(a_transaction_count, transaction_count), 4),
            "a_match_to_listing_ratio": round(_safe_div(a_match_count, a_period_listing_count), 4),
            "a_tx_to_listing_ratio": round(_safe_div(a_transaction_count, a_period_listing_count), 4),
            "b_end_active_listing_count": b_end_active_listing_count,
            "b_period_listing_count": b_period_listing_count,
            "b_match_count": b_match_count,
            "b_transaction_count": b_transaction_count,
            "b_avg_transaction_price": round(b_avg_transaction_price, 2),
            "b_transaction_share": round(_safe_div(b_transaction_count, transaction_count), 4),
            "b_match_to_listing_ratio": round(_safe_div(b_match_count, b_period_listing_count), 4),
            "b_tx_to_listing_ratio": round(_safe_div(b_transaction_count, b_period_listing_count), 4),
            "avg_price_to_initial_value_ratio": round(avg_price_to_initial_value_ratio, 4),
            "a_price_to_initial_value_ratio": round(a_price_to_initial_value_ratio, 4),
            "b_price_to_initial_value_ratio": round(b_price_to_initial_value_ratio, 4),
        }
    finally:
        conn.close()


def write_outputs(batch_dir: Path, batch_payload: Dict[str, Any], run_reports: List[Dict[str, Any]]) -> None:
    out_json = batch_dir / "stage_trend_report.json"
    out_md = batch_dir / "stage_trend_report.md"
    stage_rank = {"UP": 0, "TURN": 1, "DOWN": 2}

    valid_reports = [item for item in run_reports if int(item.get("transaction_count", 0) or 0) > 0]
    if valid_reports:
        ref_a_share = round(sum(float(item.get("a_transaction_share", 0.0) or 0.0) for item in valid_reports) / len(valid_reports), 4)
        ref_b_share = round(sum(float(item.get("b_transaction_share", 0.0) or 0.0) for item in valid_reports) / len(valid_reports), 4)
    else:
        ref_a_share = 0.5
        ref_b_share = 0.5

    if ref_a_share + ref_b_share <= 0:
        ref_a_share = 0.5
        ref_b_share = 0.5

    for item in run_reports:
        structure_adjusted_avg = (
            float(item.get("a_avg_transaction_price", 0.0) or 0.0) * ref_a_share
            + float(item.get("b_avg_transaction_price", 0.0) or 0.0) * ref_b_share
        )
        item["structure_adjusted_avg_price"] = round(structure_adjusted_avg, 2)
    run_reports.sort(key=lambda item: (stage_rank.get(str(item.get("stage", "")), 99), str(item.get("developer_mode", "")), int(item.get("seed", 0) or 0)))

    reference_zone_mix = {
        "A_share": ref_a_share,
        "B_share": ref_b_share,
    }

    payload = {
        "generated_at": batch_payload.get("generated_at"),
        "batch_dir": str(batch_dir.resolve()),
        "run_count": len(run_reports),
        "runs": run_reports,
        "reference_zone_mix": reference_zone_mix,
        "note": "Primary indicators plus zone-adjusted price views.",
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: List[str] = [
        "# 阶段量价主判断报告",
        "",
        "用途：先看成交套数和成交均价，再决定这一轮是否值得进入更细结构解释。",
        "",
        f"- 批次目录: {batch_dir.resolve()}",
        f"- 轮数: {len(run_reports)}",
        f"- 结构校正参考权重: A={ref_a_share:.4f}, B={ref_b_share:.4f}",
        "",
        "## 当前判读规则",
        "",
        "先分别看 A 区和 B 区：",
        "1. A区自己的成交套数、成交价/初始值、期间挂牌与成交关系是否按阶段变弱。",
        "2. B区自己的成交套数、成交价/初始值、期间挂牌与成交关系是否按阶段变弱。",
        "3. 只有 A/B 各自都大体同向后，才看综合成交量和综合价格。",
        "",
        "## 主指标总表",
        "",
        "| 组别 | seed | 成交套数 | 成交均价 | 结构校正均价 | 价/初始值 | A区套数 | A区均价 | B区套数 | B区均价 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in run_reports:
        lines.append(
            f"| {item['group_name']} | {item['seed']} | {item['transaction_count']} | "
            f"{item['avg_transaction_price']:.2f} | {item['structure_adjusted_avg_price']:.2f} | "
            f"{item['avg_price_to_initial_value_ratio']:.4f} | "
            f"{item['a_transaction_count']} | {item['a_avg_transaction_price']:.2f} | "
            f"{item['b_transaction_count']} | {item['b_avg_transaction_price']:.2f} |"
        )

    if len(run_reports) >= 2:
        lines.extend(
            [
                "",
                "## 分区横向对照",
                "",
                "| 阶段 | A区套数 | A区价/初始值 | A区成交/期间挂牌 | B区套数 | B区价/初始值 | B区成交/期间挂牌 | 综合结构校正均价 |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in run_reports:
            lines.append(
                f"| {item['group_name']} | {item['a_transaction_count']} | {item['a_price_to_initial_value_ratio']:.4f} | "
                f"{item['a_tx_to_listing_ratio']:.4f} | {item['b_transaction_count']} | "
                f"{item['b_price_to_initial_value_ratio']:.4f} | {item['b_tx_to_listing_ratio']:.4f} | "
                f"{item['structure_adjusted_avg_price']:.2f} |"
            )

    lines.extend(
        [
            "",
            "## 每轮最小复盘模板",
            "",
            "每轮至少回答下面六句：",
            "1. A区的成交套数和价格趋势是否同向。",
            "2. B区的成交套数和价格趋势是否同向。",
            "3. A区期间挂牌与成交的关系是在变热还是变冷。",
            "4. B区期间挂牌与成交的关系是在变热还是变冷。",
            "5. 综合成交套数是上升、持平还是下降。",
            "6. 综合价格口径是否与全国现实该阶段同向。",
            "",
        ]
    )

    for item in run_reports:
        lines.extend(
            [
                f"## {item['group_name']} / seed {item['seed']}",
                "",
                f"- 运行目录: {item['run_dir']}",
                f"- 数据库: {item['db_path']}",
                f"- 成交套数: {item['transaction_count']}",
                f"- 待交割套数: {item['pending_settlement_count']}",
                f"- 有效成交口径: {item['effective_close_count']}",
                f"- 成交均价: {item['avg_transaction_price']:.2f}",
                f"- 结构校正均价: {item['structure_adjusted_avg_price']:.2f}",
                f"- 有效订单均价: {item['avg_effective_order_price']:.2f}",
                f"- 成交价 / 初始值: {item['avg_price_to_initial_value_ratio']:.4f}",
                f"- A区期间挂牌量: {item['a_period_listing_count']}",
                f"- A区期末在售量: {item['a_end_active_listing_count']}",
                f"- A区匹配数: {item['a_match_count']}",
                f"- A区成交套数: {item['a_transaction_count']}",
                f"- A区成交占比: {item['a_transaction_share']:.4f}",
                f"- A区成交均价: {item['a_avg_transaction_price']:.2f}",
                f"- A区成交价 / 初始值: {item['a_price_to_initial_value_ratio']:.4f}",
                f"- A区匹配/期间挂牌: {item['a_match_to_listing_ratio']:.4f}",
                f"- A区成交/期间挂牌: {item['a_tx_to_listing_ratio']:.4f}",
                f"- B区期间挂牌量: {item['b_period_listing_count']}",
                f"- B区期末在售量: {item['b_end_active_listing_count']}",
                f"- B区匹配数: {item['b_match_count']}",
                f"- B区成交套数: {item['b_transaction_count']}",
                f"- B区成交占比: {item['b_transaction_share']:.4f}",
                f"- B区成交均价: {item['b_avg_transaction_price']:.2f}",
                f"- B区成交价 / 初始值: {item['b_price_to_initial_value_ratio']:.4f}",
                f"- B区匹配/期间挂牌: {item['b_match_to_listing_ratio']:.4f}",
                f"- B区成交/期间挂牌: {item['b_tx_to_listing_ratio']:.4f}",
                f"- 月度成交套数: {json.dumps(item['monthly_transaction_count'], ensure_ascii=False)}",
                f"- 月度成交均价: {json.dumps(item['monthly_transaction_avg_price'], ensure_ascii=False)}",
                f"- 月度待交割套数: {json.dumps(item['monthly_pending_settlement_count'], ensure_ascii=False)}",
                "",
            ]
        )

    out_md.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate stage trend report from a batch directory.")
    parser.add_argument("batch_dir", help="Batch directory containing batch_summary.json")
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir).resolve()
    batch_summary_path = batch_dir / "batch_summary.json"
    payload = json.loads(batch_summary_path.read_text(encoding="utf-8"))
    run_reports = [analyze_run(item) for item in payload.get("runs", [])]
    write_outputs(batch_dir, payload, run_reports)
    print(batch_dir / "stage_trend_report.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
