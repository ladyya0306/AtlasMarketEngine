import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

import yaml


BUCKET_LABELS = {
    "Y_ENTRY_SOLO_COST": "入门单身刚需",
    "Y_ENTRY_FAMILY_SCHOOL": "入门家庭学区刚需",
    "Y_ENTRY_FAMILY_NONSCHOOL": "入门家庭非学区刚需",
    "UPGRADE_YOUNG_SCHOOL": "年轻家庭学区改善",
    "UPGRADE_YOUNG_COMFORT": "年轻家庭舒适改善",
    "MID_DUALCARE_BALANCED": "上有老下有小平衡改善",
    "MULTI_CHILD_EXPAND": "多孩扩容家庭",
    "HOUSEHOLD_SPLIT_MULTIUNIT": "家庭分居/一套变多套",
    "SILVER_COMFORT_HEALTH": "银发养老改善",
    "HNW_CORE_PRESERVATION": "高净值核心保值",
    "HNW_PRIVATE_EDU": "高净值私校教育",
    "ASSET_REALLOCATOR": "资产重配型",
}

BUCKET_ORDER = [
    "Y_ENTRY_SOLO_COST",
    "Y_ENTRY_FAMILY_SCHOOL",
    "Y_ENTRY_FAMILY_NONSCHOOL",
    "UPGRADE_YOUNG_SCHOOL",
    "UPGRADE_YOUNG_COMFORT",
    "MID_DUALCARE_BALANCED",
    "MULTI_CHILD_EXPAND",
    "HOUSEHOLD_SPLIT_MULTIUNIT",
    "SILVER_COMFORT_HEALTH",
    "HNW_CORE_PRESERVATION",
    "HNW_PRIVATE_EDU",
    "ASSET_REALLOCATOR",
]


def _parse_args():
    parser = argparse.ArgumentParser(description="Analyze a fixed-snapshot replay run.")
    parser.add_argument("--db", required=True, help="Path to replay simulation.db")
    parser.add_argument("--init-db", required=True, help="Path to frozen init snapshot simulation.db")
    parser.add_argument("--plan", required=True, help="Path to full plan.yaml used for replay")
    parser.add_argument("--output-dir", required=True, help="Directory for markdown/csv outputs")
    return parser.parse_args()


def _bucket_sort_key(bucket_id: str):
    if bucket_id in BUCKET_ORDER:
        return (0, BUCKET_ORDER.index(bucket_id))
    return (1, str(bucket_id))


def _safe_bucket_label(bucket_id: str) -> str:
    return BUCKET_LABELS.get(bucket_id, str(bucket_id))


def _load_plan(plan_path: Path) -> dict:
    payload = yaml.safe_load(plan_path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _resolve_profile_pack(plan_payload: dict) -> dict:
    smart_agent = dict(plan_payload.get("smart_agent", {}) or {})
    profiled = dict(smart_agent.get("profiled_market_mode", {}) or {})
    pack_path = str(profiled.get("profile_pack_path", "") or "").strip()
    if not pack_path:
        return {}
    payload = yaml.safe_load(Path(pack_path).read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}
    pack = payload.get("profiled_market_mode", payload)
    return dict(pack) if isinstance(pack, dict) else {}


def _load_alignment_map(profile_pack: dict) -> dict:
    alignment_map = {}
    rules = profile_pack.get("bucket_alignment_rules", []) or []
    for item in rules:
        if not isinstance(item, dict):
            continue
        agent_bucket = str(item.get("agent_bucket_id", "") or "").strip()
        if not agent_bucket:
            continue
        allowed = {
            str(x).strip()
            for x in (item.get("eligible_property_buckets", []) or [])
            if str(x).strip()
        }
        alignment_map[agent_bucket] = allowed
    return alignment_map


def _load_bucket_maps(conn: sqlite3.Connection):
    cur = conn.cursor()
    buyer_bucket_map = {
        int(agent_id): str(bucket_id)
        for agent_id, bucket_id in cur.execute(
            "SELECT agent_id, bucket_id FROM profiled_market_agent_buckets"
        ).fetchall()
    }
    property_bucket_map = {
        int(property_id): str(bucket_id)
        for property_id, bucket_id in cur.execute(
            "SELECT property_id, bucket_id FROM profiled_market_property_buckets"
        ).fetchall()
    }
    population_by_bucket = defaultdict(set)
    for agent_id, bucket_id, role_side in cur.execute(
        "SELECT agent_id, bucket_id, role_side FROM profiled_market_agent_buckets"
    ).fetchall():
        if str(role_side or "").lower() in {"buyer", "buyer_seller"}:
            population_by_bucket[str(bucket_id)].add(int(agent_id))
    return buyer_bucket_map, property_bucket_map, population_by_bucket


def _load_initial_for_sale(init_db: Path) -> set[int]:
    conn = sqlite3.connect(str(init_db))
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT property_id FROM properties_market WHERE status='for_sale'"
    ).fetchall()
    conn.close()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def _load_listing_logs(conn: sqlite3.Connection) -> dict[int, set[int]]:
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT month, thought_process
        FROM decision_logs
        WHERE event_type='LISTING_ACTION'
        ORDER BY month, log_id
        """
    ).fetchall()
    listed_by_month = defaultdict(set)
    for month, thought_process in rows:
        try:
            payload = json.loads(thought_process or "{}")
        except Exception:
            payload = {}
        for property_id in payload.get("properties_to_sell") or []:
            try:
                listed_by_month[int(month)].add(int(property_id))
            except Exception:
                continue
    return listed_by_month


def _load_role_decisions(conn: sqlite3.Connection, buyer_bucket_map: dict[int, str]):
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT month, agent_id, decision, context_metrics
        FROM decision_logs
        WHERE event_type='ROLE_DECISION'
        ORDER BY month, agent_id
        """
    ).fetchall()
    broad_by_month = defaultdict(lambda: defaultdict(set))
    immediate_by_month = defaultdict(lambda: defaultdict(set))
    for month, agent_id, decision, context_metrics in rows:
        bucket_id = str(buyer_bucket_map.get(int(agent_id), "") or "")
        chain_mode = ""
        if context_metrics:
            try:
                metrics = json.loads(context_metrics)
                bucket_id = str(metrics.get("profile_bucket_id") or bucket_id or "")
                chain_mode = str(metrics.get("chain_mode") or "").strip().lower()
            except Exception:
                pass
        if not bucket_id:
            continue
        decision_upper = str(decision or "").upper()
        if decision_upper in {"BUYER", "BUYER_SELLER"}:
            broad_by_month[int(month)][bucket_id].add(int(agent_id))
        if decision_upper == "BUYER" or (decision_upper == "BUYER_SELLER" and chain_mode == "buy_first"):
            immediate_by_month[int(month)][bucket_id].add(int(agent_id))
    return broad_by_month, immediate_by_month


def _load_orders_and_transactions(conn: sqlite3.Connection, buyer_bucket_map: dict[int, str]):
    cur = conn.cursor()
    order_rows = cur.execute(
        """
        SELECT order_id, created_month, close_month, status, buyer_id, property_id, agreed_price, offer_price
        FROM transaction_orders
        ORDER BY created_month, order_id
        """
    ).fetchall()
    tx_rows = cur.execute(
        """
        SELECT transaction_id, month, buyer_id, property_id, final_price, order_id
        FROM transactions
        ORDER BY month, transaction_id
        """
    ).fetchall()
    orders_by_month = defaultdict(lambda: defaultdict(set))
    tx_by_month = defaultdict(lambda: defaultdict(set))
    for _order_id, created_month, _close_month, _status, buyer_id, _property_id, _agreed_price, _offer_price in order_rows:
        bucket_id = str(buyer_bucket_map.get(int(buyer_id), "") or "")
        if bucket_id:
            orders_by_month[int(created_month)][bucket_id].add(int(buyer_id))
    for _transaction_id, month, buyer_id, _property_id, _final_price, _order_id in tx_rows:
        bucket_id = str(buyer_bucket_map.get(int(buyer_id), "") or "")
        if bucket_id:
            tx_by_month[int(month)][bucket_id].add(int(buyer_id))
    return order_rows, tx_rows, orders_by_month, tx_by_month


def _load_match_stages(conn: sqlite3.Connection, buyer_bucket_map: dict[int, str]):
    cur = conn.cursor()
    shortlist_by_month = defaultdict(lambda: defaultdict(set))
    negotiation_by_month = defaultdict(lambda: defaultdict(set))
    rows = cur.execute(
        """
        SELECT month, buyer_id, proceeded_to_negotiation
        FROM property_buyer_matches
        """
    ).fetchall()
    for month, buyer_id, proceeded_to_negotiation in rows:
        bucket_id = str(buyer_bucket_map.get(int(buyer_id), "") or "")
        if not bucket_id:
            continue
        shortlist_by_month[int(month)][bucket_id].add(int(buyer_id))
        if int(proceeded_to_negotiation or 0) == 1:
            negotiation_by_month[int(month)][bucket_id].add(int(buyer_id))
    return shortlist_by_month, negotiation_by_month


def _build_monthly_state(initial_for_sale_ids: set[int], listing_by_month: dict[int, set[int]], order_rows, tx_rows):
    months = sorted(
        set(list(listing_by_month.keys()))
        | {int(row[1]) for row in order_rows}
        | {int(row[1]) for row in tx_rows}
    )
    if not months:
        months = [1]

    visible_by_month = {}
    pending_properties_by_month = {}
    sold_properties_by_month = {}

    listed_cumulative = set(int(x) for x in initial_for_sale_ids)
    sold_cumulative = set()
    for month in months:
        listed_cumulative.update(int(x) for x in listing_by_month.get(month, set()))
        pending_props = set()
        for _order_id, created_month, close_month, _status, _buyer_id, property_id, _agreed_price, _offer_price in order_rows:
            if int(created_month or 0) > month:
                continue
            close_month_i = int(close_month) if close_month is not None else None
            if close_month_i is None or close_month_i > month:
                pending_props.add(int(property_id))
        sold_cumulative.update(int(row[3]) for row in tx_rows if int(row[1]) <= month)
        pending_properties_by_month[month] = set(pending_props)
        sold_properties_by_month[month] = set(sold_cumulative)
        visible_by_month[month] = set(listed_cumulative) - pending_props - sold_cumulative

    return months, visible_by_month, pending_properties_by_month, sold_properties_by_month


def _build_bucket_rows(
    months,
    buyer_bucket_map,
    property_bucket_map,
    population_by_bucket,
    alignment_map,
    visible_by_month,
    broad_by_month,
    immediate_by_month,
    orders_by_month,
    tx_by_month,
    shortlist_by_month,
    negotiation_by_month,
    order_rows,
    tx_rows,
):
    pending_buyers_end = defaultdict(lambda: defaultdict(set))
    filled_buyers_end = defaultdict(lambda: defaultdict(set))
    filled_cumulative = defaultdict(set)
    for month in months:
        for _tx_id, tx_month, buyer_id, _property_id, _final_price, _order_id in tx_rows:
            if int(tx_month) <= month:
                bucket_id = str(buyer_bucket_map.get(int(buyer_id), "") or "")
                if bucket_id:
                    filled_cumulative[bucket_id].add(int(buyer_id))
        for bucket_id, buyers in filled_cumulative.items():
            filled_buyers_end[month][bucket_id] = set(int(x) for x in buyers)

        for _order_id, created_month, close_month, _status, buyer_id, _property_id, _agreed_price, _offer_price in order_rows:
            if int(created_month or 0) > month:
                continue
            close_month_i = int(close_month) if close_month is not None else None
            if close_month_i is None or close_month_i > month:
                bucket_id = str(buyer_bucket_map.get(int(buyer_id), "") or "")
                if bucket_id:
                    pending_buyers_end[month][bucket_id].add(int(buyer_id))

    all_buckets = set(population_by_bucket.keys()) | set(alignment_map.keys()) | set(BUCKET_LABELS.keys())
    cumulative_broad = defaultdict(set)
    cumulative_immediate = defaultdict(set)
    rows = []
    for month in months:
        visible_stock_ids = set(visible_by_month.get(month, set()))
        for bucket_id in sorted(all_buckets, key=_bucket_sort_key):
            cumulative_broad[bucket_id].update(broad_by_month.get(month, {}).get(bucket_id, set()))
            cumulative_immediate[bucket_id].update(immediate_by_month.get(month, {}).get(bucket_id, set()))
            pending_now = set(pending_buyers_end.get(month, {}).get(bucket_id, set()))
            filled_now = set(filled_buyers_end.get(month, {}).get(bucket_id, set()))
            broad_stock = set(cumulative_broad[bucket_id]) - pending_now - filled_now
            immediate_stock = set(cumulative_immediate[bucket_id]) - pending_now - filled_now

            allowed_property_buckets = set(alignment_map.get(bucket_id, set()))
            matching_supply_ids = {
                int(pid)
                for pid in visible_stock_ids
                if str(property_bucket_map.get(int(pid), "") or "") in allowed_property_buckets
            }
            supply_count = len(matching_supply_ids)
            row = {
                "month": int(month),
                "profile_bucket_id": bucket_id,
                "profile_label": _safe_bucket_label(bucket_id),
                "matching_supply_count": int(supply_count),
                "matching_supply_property_ids": ",".join(str(x) for x in sorted(matching_supply_ids)),
                "population_total": int(len(population_by_bucket.get(bucket_id, set()))),
                "new_activated_broad": int(len(broad_by_month.get(month, {}).get(bucket_id, set()))),
                "new_activated_immediate": int(len(immediate_by_month.get(month, {}).get(bucket_id, set()))),
                "month_end_pending_or_locked": int(len(pending_now)),
                "month_end_filled": int(len(filled_now)),
                "month_end_broad_effective_stock": int(len(broad_stock)),
                "month_end_immediate_stock": int(len(immediate_stock)),
                "shortlist_buyers": int(len(shortlist_by_month.get(month, {}).get(bucket_id, set()))),
                "negotiation_buyers": int(len(negotiation_by_month.get(month, {}).get(bucket_id, set()))),
                "order_buyers": int(len(orders_by_month.get(month, {}).get(bucket_id, set()))),
                "transaction_buyers": int(len(tx_by_month.get(month, {}).get(bucket_id, set()))),
            }
            row["broad_stock_to_supply_ratio"] = round(
                float(row["month_end_broad_effective_stock"]) / float(supply_count),
                4,
            ) if supply_count > 0 else 0.0
            row["immediate_stock_to_supply_ratio"] = round(
                float(row["month_end_immediate_stock"]) / float(supply_count),
                4,
            ) if supply_count > 0 else 0.0
            rows.append(row)
    return rows


def _build_macro_rows(months, initial_for_sale_ids, listing_by_month, order_rows, tx_rows, broad_by_month, immediate_by_month):
    cumulative_broad = set()
    cumulative_immediate = set()
    monthly_rows = []
    listed_cumulative = set(int(x) for x in initial_for_sale_ids)
    sold_cumulative = set()
    sold_buyers_cumulative = set()
    for month in months:
        listed_cumulative.update(int(x) for x in listing_by_month.get(month, set()))
        cumulative_broad.update(
            int(agent_id)
            for bucket_items in broad_by_month.get(month, {}).values()
            for agent_id in bucket_items
        )
        cumulative_immediate.update(
            int(agent_id)
            for bucket_items in immediate_by_month.get(month, {}).values()
            for agent_id in bucket_items
        )
        pending_buyers = set()
        pending_properties = set()
        for _order_id, created_month, close_month, _status, buyer_id, property_id, _agreed_price, _offer_price in order_rows:
            if int(created_month or 0) > month:
                continue
            close_month_i = int(close_month) if close_month is not None else None
            if close_month_i is None or close_month_i > month:
                pending_buyers.add(int(buyer_id))
                pending_properties.add(int(property_id))
        tx_in_month = [row for row in tx_rows if int(row[1]) == month]
        sold_cumulative.update(int(row[3]) for row in tx_rows if int(row[1]) <= month)
        sold_buyers_cumulative.update(int(row[2]) for row in tx_rows if int(row[1]) <= month)
        visible_unlocked_stock = set(listed_cumulative) - pending_properties - sold_cumulative
        tx_count = len(tx_in_month)
        avg_tx_price = round(sum(float(row[4]) for row in tx_in_month) / tx_count, 2) if tx_count else 0.0
        monthly_rows.append(
            {
                "month": int(month),
                "initial_for_sale_count": int(len(initial_for_sale_ids)),
                "new_distinct_listings": int(len(listing_by_month.get(month, set()))),
                "month_end_visible_unlocked_stock": int(len(visible_unlocked_stock)),
                "month_end_locked_properties": int(len(pending_properties)),
                "month_end_cumulative_sold_properties": int(len(sold_cumulative)),
                "new_activated_broad_buyers": int(
                    sum(len(v) for v in broad_by_month.get(month, {}).values())
                ),
                "new_activated_immediate_buyers": int(
                    sum(len(v) for v in immediate_by_month.get(month, {}).values())
                ),
                "month_end_pending_buyers": int(len(pending_buyers)),
                "month_end_cumulative_bought_buyers": int(len(sold_buyers_cumulative)),
                "month_end_broad_free_buyers": int(len(cumulative_broad - pending_buyers - sold_buyers_cumulative)),
                "month_end_immediate_free_buyers": int(len(cumulative_immediate - pending_buyers - sold_buyers_cumulative)),
                "order_count": int(len([row for row in order_rows if int(row[1]) == month])),
                "transaction_count": int(tx_count),
                "avg_transaction_price": float(avg_tx_price),
                "rough_months_to_clear": round(float(len(visible_unlocked_stock)) / float(tx_count), 2) if tx_count else None,
            }
        )
    return monthly_rows


def _query_price_to_list(conn: sqlite3.Connection):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        WITH tx_base AS (
            SELECT
                t.month,
                t.transaction_id,
                t.final_price,
                COALESCE(m.listing_price, o.agreed_price, o.offer_price) AS reference_listing_price
            FROM transactions t
            LEFT JOIN transaction_orders o ON o.order_id = t.order_id
            LEFT JOIN property_buyer_matches m ON m.order_id = t.order_id
        )
        SELECT
            month,
            COUNT(*) AS transaction_count,
            SUM(CASE WHEN final_price > reference_listing_price THEN 1 ELSE 0 END) AS above_list_count,
            SUM(CASE WHEN ABS(final_price - reference_listing_price) < 1e-6 THEN 1 ELSE 0 END) AS equal_list_count,
            SUM(CASE WHEN final_price < reference_listing_price THEN 1 ELSE 0 END) AS below_list_count,
            ROUND(AVG(final_price / NULLIF(reference_listing_price, 0)), 4) AS avg_price_to_list_ratio
        FROM tx_base
        WHERE reference_listing_price IS NOT NULL
        GROUP BY month
        ORDER BY month
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _write_csv(path: Path, rows):
    if not rows:
        return
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, macro_rows, bucket_rows, price_rows):
    lines = ["# 固定快照复跑月度诊断", "", "## 1. 月度总表", ""]
    lines.append("| 月份 | 新激活广义买家 | 新激活立即买家 | 月末自由广义买家 | 月末自由立即买家 | 月末可售房 | 当月订单 | 当月成交 | 月均成交价 | 粗略去化月数 |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in macro_rows:
        clear = "-" if row["rough_months_to_clear"] is None else row["rough_months_to_clear"]
        lines.append(
            f"| {row['month']} | {row['new_activated_broad_buyers']} | {row['new_activated_immediate_buyers']} | "
            f"{row['month_end_broad_free_buyers']} | {row['month_end_immediate_free_buyers']} | "
            f"{row['month_end_visible_unlocked_stock']} | {row['order_count']} | {row['transaction_count']} | "
            f"{row['avg_transaction_price']:.2f} | {clear} |"
        )

    lines.extend(["", "## 2. 价格相对挂牌", "", "| 月份 | 高于挂牌 | 等于挂牌 | 低于挂牌 | 平均成交/挂牌比 |", "| --- | --- | --- | --- | --- |"])
    for row in price_rows:
        lines.append(
            f"| {row['month']} | {row['above_list_count']} | {row['equal_list_count']} | {row['below_list_count']} | {row['avg_price_to_list_ratio']} |"
        )

    lines.extend(["", "## 3. 画像层月末有效需求 vs 可承接在售房（只列供需比>=1或最接近的）", ""])
    for month in sorted({int(r["month"]) for r in bucket_rows}):
        month_rows = [r for r in bucket_rows if int(r["month"]) == month]
        ranked = sorted(
            month_rows,
            key=lambda item: (
                -float(item["broad_stock_to_supply_ratio"]),
                -int(item["month_end_broad_effective_stock"]),
                _bucket_sort_key(item["profile_bucket_id"]),
            ),
        )
        lines.append(f"### 第 {month} 月")
        lines.append("")
        lines.append("| 画像 | 月末自由买家 | 可承接在售房 | 广义供需比 | 立即供需比 | 当月看房 | 当月下单 | 当月成交 |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in ranked[:6]:
            lines.append(
                f"| {row['profile_label']} | {row['month_end_broad_effective_stock']} | {row['matching_supply_count']} | "
                f"{row['broad_stock_to_supply_ratio']} | {row['immediate_stock_to_supply_ratio']} | "
                f"{row['shortlist_buyers']} | {row['order_buyers']} | {row['transaction_buyers']} |"
            )
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    args = _parse_args()
    db_path = Path(args.db)
    init_db_path = Path(args.init_db)
    plan_path = Path(args.plan)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    plan_payload = _load_plan(plan_path)
    profile_pack = _resolve_profile_pack(plan_payload)
    alignment_map = _load_alignment_map(profile_pack)

    conn = sqlite3.connect(str(db_path))
    buyer_bucket_map, property_bucket_map, population_by_bucket = _load_bucket_maps(conn)
    initial_for_sale_ids = _load_initial_for_sale(init_db_path)
    listing_by_month = _load_listing_logs(conn)
    broad_by_month, immediate_by_month = _load_role_decisions(conn, buyer_bucket_map)
    order_rows, tx_rows, orders_by_month, tx_by_month = _load_orders_and_transactions(conn, buyer_bucket_map)
    shortlist_by_month, negotiation_by_month = _load_match_stages(conn, buyer_bucket_map)
    months, visible_by_month, _pending_properties_by_month, _sold_properties_by_month = _build_monthly_state(
        initial_for_sale_ids=initial_for_sale_ids,
        listing_by_month=listing_by_month,
        order_rows=order_rows,
        tx_rows=tx_rows,
    )
    bucket_rows = _build_bucket_rows(
        months=months,
        buyer_bucket_map=buyer_bucket_map,
        property_bucket_map=property_bucket_map,
        population_by_bucket=population_by_bucket,
        alignment_map=alignment_map,
        visible_by_month=visible_by_month,
        broad_by_month=broad_by_month,
        immediate_by_month=immediate_by_month,
        orders_by_month=orders_by_month,
        tx_by_month=tx_by_month,
        shortlist_by_month=shortlist_by_month,
        negotiation_by_month=negotiation_by_month,
        order_rows=order_rows,
        tx_rows=tx_rows,
    )
    macro_rows = _build_macro_rows(
        months=months,
        initial_for_sale_ids=initial_for_sale_ids,
        listing_by_month=listing_by_month,
        order_rows=order_rows,
        tx_rows=tx_rows,
        broad_by_month=broad_by_month,
        immediate_by_month=immediate_by_month,
    )
    price_rows = _query_price_to_list(conn)
    conn.close()

    _write_csv(output_dir / "profile_monthly_demand_supply_breakdown.csv", bucket_rows)
    _write_csv(output_dir / "seller_market_monthly_macro.csv", macro_rows)
    _write_csv(output_dir / "seller_market_price_to_list.csv", price_rows)
    _write_markdown(output_dir / "fixed_snapshot_replay_diagnosis.md", macro_rows, bucket_rows, price_rows)


if __name__ == "__main__":
    main()
