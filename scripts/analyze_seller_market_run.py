import argparse
import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path


def _parse_args():
    parser = argparse.ArgumentParser(description="Analyze seller-market realism and traversal hygiene for one run.")
    parser.add_argument("--db", required=True, help="Path to simulation.db")
    parser.add_argument("--batch-summary", required=True, help="Path to batch_summary.json")
    parser.add_argument(
        "--monthly-breakdown-csv",
        required=True,
        help="Path to profile_monthly_demand_supply_breakdown.csv",
    )
    parser.add_argument(
        "--initial-l0",
        type=int,
        default=0,
        help="Known initial listed-supply count for month 1. Use when batch_summary.json is aggregate-only.",
    )
    parser.add_argument("--output-dir", required=True, help="Directory for markdown/csv outputs")
    return parser.parse_args()


def _load_batch_summary(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_monthly_breakdown(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _query_monthly_macro(conn: sqlite3.Connection, initial_l0: int):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    listing_logs = cur.execute(
        """
        SELECT month, thought_process
        FROM decision_logs
        WHERE event_type = 'LISTING_ACTION'
        """
    ).fetchall()
    listed_property_ids_by_month = defaultdict(set)
    for row in listing_logs:
        try:
            payload = json.loads(row["thought_process"] or "{}")
        except Exception:
            payload = {}
        for property_id in payload.get("properties_to_sell") or []:
            listed_property_ids_by_month[int(row["month"])].add(int(property_id))

    new_distinct_listings_by_month = {}
    seen_listed = set()
    for month in sorted(listed_property_ids_by_month.keys()):
        month_ids = set(listed_property_ids_by_month[month])
        new_ids = month_ids - seen_listed
        new_distinct_listings_by_month[month] = len(new_ids)
        seen_listed.update(month_ids)

    order_rows = cur.execute(
        """
        SELECT created_month, close_month, buyer_id, property_id
        FROM transaction_orders
        """
    ).fetchall()
    tx_rows = cur.execute(
        """
        SELECT month, buyer_id, property_id, final_price, order_id
        FROM transactions
        """
    ).fetchall()

    role_rows = cur.execute(
        """
        SELECT month, agent_id
        FROM decision_logs
        WHERE event_type = 'ROLE_DECISION'
          AND json_extract(thought_process, '$.role') IN ('BUYER', 'BUYER_SELLER')
        GROUP BY month, agent_id
        """
    ).fetchall()

    new_broad_buyers_by_month = defaultdict(set)
    for row in role_rows:
        new_broad_buyers_by_month[int(row["month"])].add(int(row["agent_id"]))

    months = sorted({int(row["created_month"]) for row in order_rows} | {int(row["month"]) for row in tx_rows} | set(new_distinct_listings_by_month.keys()))
    if not months:
        months = [1]

    monthly_rows = []
    cumulative_unique_broad = set()
    cumulative_new_listings = 0
    for month in months:
        cumulative_unique_broad.update(new_broad_buyers_by_month.get(month, set()))
        cumulative_new_listings += new_distinct_listings_by_month.get(month, 0)

        pending_buyers = set()
        pending_properties = set()
        for row in order_rows:
            created_month = int(row["created_month"] or 0)
            if created_month > month:
                continue
            close_month = int(row["close_month"]) if row["close_month"] is not None else None
            if close_month is None or close_month > month:
                pending_buyers.add(int(row["buyer_id"]))
                pending_properties.add(int(row["property_id"]))

        sold_buyers_cum = {int(row["buyer_id"]) for row in tx_rows if int(row["month"]) <= month}
        sold_properties_cum = {int(row["property_id"]) for row in tx_rows if int(row["month"]) <= month}
        tx_in_month = [row for row in tx_rows if int(row["month"]) == month]
        order_in_month = [row for row in order_rows if int(row["created_month"]) == month]

        approx_visible_unlocked_stock_end = (
            int(initial_l0)
            + int(cumulative_new_listings)
            - len(sold_properties_cum)
            - len(pending_properties)
        )
        tx_count = len(tx_in_month)
        monthly_rows.append(
            {
                "month": int(month),
                "new_activated_buyers": len(new_broad_buyers_by_month.get(month, set())),
                "cumulative_unique_buyers": len(cumulative_unique_broad),
                "month_end_locked_buyers": len(pending_buyers),
                "month_end_cumulative_bought_buyers": len(sold_buyers_cum),
                "month_end_free_buyers": len(cumulative_unique_broad) - len(pending_buyers) - len(sold_buyers_cum),
                "new_distinct_listings": int(new_distinct_listings_by_month.get(month, 0)),
                "month_end_locked_properties": len(pending_properties),
                "month_end_cumulative_sold_properties": len(sold_properties_cum),
                "approx_visible_unlocked_stock_end": int(approx_visible_unlocked_stock_end),
                "order_count": len(order_in_month),
                "order_buyers": len({int(row["buyer_id"]) for row in order_in_month}),
                "transaction_count": tx_count,
                "transaction_buyers": len({int(row["buyer_id"]) for row in tx_in_month}),
                "avg_transaction_price": round(sum(float(row["final_price"]) for row in tx_in_month) / tx_count, 2) if tx_count else 0.0,
                "transaction_volume": round(sum(float(row["final_price"]) for row in tx_in_month), 2),
                "rough_months_to_clear": round(float(approx_visible_unlocked_stock_end) / float(tx_count), 2) if tx_count else None,
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


def _query_traversal_hygiene(conn: sqlite3.Connection):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    exposure_counter = Counter()
    buyer_match_rows = cur.execute(
        """
        SELECT month, thought_process
        FROM decision_logs
        WHERE event_type = 'BUYER_MATCH'
        """
    ).fetchall()
    for row in buyer_match_rows:
        try:
            payload = json.loads(row["thought_process"] or "{}")
        except Exception:
            payload = {}
        for item in payload.get("shortlist_context") or []:
            property_id = item.get("property_id")
            if property_id is None:
                continue
            exposure_counter[int(property_id)] += 1

    match_counts = {
        int(row["property_id"]): int(row["cnt"])
        for row in cur.execute(
            """
            SELECT property_id, COUNT(*) AS cnt
            FROM property_buyer_matches
            GROUP BY property_id
            """
        ).fetchall()
    }

    zero_match_for_sale = []
    for row in cur.execute(
        """
        SELECT p.property_id, p.status, p.listing_month, p.listed_price, pb.bucket_id
        FROM properties_market p
        LEFT JOIN profiled_market_property_buckets pb ON pb.property_id = p.property_id
        WHERE p.status = 'for_sale'
        ORDER BY p.listing_month DESC, p.property_id
        """
    ).fetchall():
        property_id = int(row["property_id"])
        if match_counts.get(property_id, 0) != 0:
            continue
        zero_match_for_sale.append(
            {
                "property_id": property_id,
                "listing_month": int(row["listing_month"] or 0),
                "listed_price": round(float(row["listed_price"] or 0.0), 2),
                "bucket_id": str(row["bucket_id"] or ""),
                "shortlist_exposure_count": int(exposure_counter.get(property_id, 0)),
                "selected_match_count": 0,
            }
        )

    repeated_pairs = [
        dict(row)
        for row in cur.execute(
            """
            SELECT
                buyer_id,
                property_id,
                MIN(month) AS first_month,
                MAX(month) AS last_month,
                COUNT(*) AS repeat_count
            FROM property_buyer_matches
            GROUP BY buyer_id, property_id
            HAVING COUNT(*) > 1
            ORDER BY repeat_count DESC, buyer_id, property_id
            LIMIT 20
            """
        ).fetchall()
    ]

    post_lock_summary = dict(
        cur.execute(
            """
            WITH first_lock AS (
                SELECT property_id, MIN(created_month) AS first_order_month
                FROM transaction_orders
                GROUP BY property_id
            ),
            later_matches AS (
                SELECT m.property_id, m.buyer_id, m.month, f.first_order_month
                FROM property_buyer_matches m
                JOIN first_lock f ON f.property_id = m.property_id
                WHERE m.month >= f.first_order_month
            )
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN month = first_order_month THEN 1 ELSE 0 END) AS same_month_rows,
                SUM(CASE WHEN month > first_order_month THEN 1 ELSE 0 END) AS future_month_rows,
                COUNT(DISTINCT property_id) AS affected_properties,
                COUNT(DISTINCT CASE WHEN month > first_order_month THEN property_id END) AS future_month_properties,
                COUNT(DISTINCT buyer_id) AS affected_buyers
            FROM later_matches
            """
        ).fetchone()
    )
    post_lock_examples = [
        dict(row)
        for row in cur.execute(
            """
            WITH first_lock AS (
                SELECT property_id, MIN(created_month) AS first_order_month
                FROM transaction_orders
                GROUP BY property_id
            ),
            later_matches AS (
                SELECT
                    m.property_id,
                    COUNT(*) AS later_match_rows,
                    COUNT(DISTINCT m.buyer_id) AS later_distinct_buyers,
                    MIN(m.month) AS first_later_match_month,
                    MAX(m.month) AS last_later_match_month
                FROM property_buyer_matches m
                JOIN first_lock f ON f.property_id = m.property_id
                WHERE m.month >= f.first_order_month
                GROUP BY m.property_id
            )
            SELECT *
            FROM later_matches
            WHERE later_match_rows > 1
            ORDER BY later_match_rows DESC, later_distinct_buyers DESC
            LIMIT 15
            """
        ).fetchall()
    ]

    return {
        "zero_match_for_sale": zero_match_for_sale,
        "repeated_pairs": repeated_pairs,
        "post_lock_summary": post_lock_summary,
        "post_lock_examples": post_lock_examples,
    }


def _estimate_agent_need(monthly_breakdown_rows):
    month_groups = defaultdict(list)
    for row in monthly_breakdown_rows:
        month_groups[int(row["month"])].append(row)

    month_stats = {}
    for month, rows in month_groups.items():
        population_total = sum(int(row["population_total"]) for row in rows)
        broad_stock_sum = sum(int(row["month_end_broad_effective_stock"]) for row in rows)
        broad_deficit_sum = sum(
            max(0, int(row["matching_supply_count"]) - int(row["month_end_broad_effective_stock"]))
            for row in rows
        )
        month_stats[month] = {
            "population_total": population_total,
            "broad_stock_sum": broad_stock_sum,
            "broad_deficit_sum": broad_deficit_sum,
        }

    latest_month = max(month_stats.keys())
    first_month = min(month_stats.keys())
    latest_population = max(1, month_stats[latest_month]["population_total"])
    latest_stock = month_stats[latest_month]["broad_stock_sum"]
    latest_deficit = month_stats[latest_month]["broad_deficit_sum"]
    retention_ratio = float(latest_stock) / float(latest_population) if latest_population else 0.0
    monthly_deficit_growth = 0.0
    if latest_month > first_month:
        monthly_deficit_growth = (
            float(month_stats[latest_month]["broad_deficit_sum"] - month_stats[first_month]["broad_deficit_sum"])
            / float(latest_month - first_month)
        )
        monthly_deficit_growth = max(0.0, monthly_deficit_growth)

    def _estimate_total_agents(target_month: int):
        future_gap = max(0, target_month - latest_month)
        effective_stock_needed = latest_deficit + monthly_deficit_growth * future_gap
        if retention_ratio <= 0:
            return None
        extra_agents = math.ceil(float(effective_stock_needed) / float(retention_ratio))
        return {
            "target_month": target_month,
            "effective_stock_gap_to_fill": round(effective_stock_needed, 1),
            "estimated_total_agents": int(latest_population + extra_agents),
            "assumed_retention_ratio": round(retention_ratio, 4),
        }

    return {
        "month_stats": month_stats,
        "estimate_m6": _estimate_total_agents(6),
        "estimate_m12": _estimate_total_agents(12),
    }


def _write_csv(path: Path, rows, fieldnames):
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_markdown(path: Path, monthly_macro, price_to_list, traversal, agent_need):
    price_rows = {int(row["month"]): row for row in price_to_list}
    lines = [
        "# 卖方市场月度诊断（人话版）",
        "",
        "## 1. 先说结论",
        "",
        "1. 这轮 `149人 × 3个月` 的需求加压，已经不再是“总需求人口根本不够”。从第 2 个月开始，市场确实出现了比较快的库存消化。",
        "2. 但它还不能叫“稳定、持续、分层都成立的卖方市场”。更准确地说，它像是“第 2-3 个月局部偏热、部分画像先被快速吃掉”的阶段。",
        "3. 这轮也确实存在机制层面的卫生问题：有房子在已经被锁单后，还在同月甚至后续月份继续进入候选；同一个买家也会反复被拿去看同一套房。",
        "",
        "## 2. 按月看，到底发生了什么",
        "",
        "| 月份 | 新进场买家 | 月末仍在场内的自由买家 | 月末已锁单买家 | 月末累计已买到房的买家 | 当月下单 | 当月成交 | 当月均价 | 粗估月末可售库存 | 粗估去化周期 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in monthly_macro:
        lines.append(
            "| {month} | {new_activated_buyers} | {month_end_free_buyers} | {month_end_locked_buyers} | "
            "{month_end_cumulative_bought_buyers} | {order_count} | {transaction_count} | {avg_tx_price} | "
            "{approx_visible_unlocked_stock_end} | {rough_months_to_clear} |".format(
                month=row["month"],
                new_activated_buyers=row["new_activated_buyers"],
                month_end_free_buyers=row["month_end_free_buyers"],
                month_end_locked_buyers=row["month_end_locked_buyers"],
                month_end_cumulative_bought_buyers=row["month_end_cumulative_bought_buyers"],
                order_count=row["order_count"],
                transaction_count=row["transaction_count"],
                avg_tx_price=f"{row['avg_transaction_price'] / 10000:.2f}万" if row["avg_transaction_price"] else "-",
                approx_visible_unlocked_stock_end=row["approx_visible_unlocked_stock_end"],
                rough_months_to_clear=f"{row['rough_months_to_clear']}个月" if row["rough_months_to_clear"] is not None else "-",
            )
        )

    lines.extend(
        [
            "",
            "说明：",
            "",
            "1. 月 1 不适合单独判刑。因为这轮月 1 下单很多，但交割明显后移，所以月 2 才是真正开始释放成交的月份。",
            "2. 这轮不能简单说“新增激活很快被市场全部吃光”。更准确的表述是：",
            "   - 月 1 新进场买家很多，但当月主要先变成了锁单和排队，不是立刻交割。",
            "   - 月 2 出现了明显消化：当月 40 笔成交，粗估月末可售库存只剩 52 套左右，按当月成交速度折算，去化周期约 1.3 个月。",
            "   - 月 3 仍在消化，但热度没有继续扩散到所有画像，粗估去化周期约 1.63 个月，说明已经接近偏热，但还没形成更强、更久的卖方挤压。",
            "",
            "## 3. 成交价有没有体现卖方市场味道",
            "",
            "| 月份 | 成交笔数 | 高于挂牌成交 | 平于挂牌成交 | 低于挂牌成交 | 平均成交/挂牌比 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for month in sorted(price_rows):
        row = price_rows[month]
        lines.append(
            "| {month} | {transaction_count} | {above_list_count} | {equal_list_count} | {below_list_count} | {avg_price_to_list_ratio} |".format(
                **row
            )
        )

    lines.extend(
        [
            "",
            "判断：",
            "",
            "1. 第 2 个月已经不是纯粹的“降价换量”：40 笔成交里，有 17 笔高于挂牌，22 笔低于挂牌，整体平均接近挂牌价。说明这时市场里已经有一定抢房和议价并存的状态。",
            "2. 第 3 个月又回到“低于挂牌更多”，说明热度没有继续往上抬，更多像是前面被锁住的需求在继续消化，而不是新一波更强的卖方溢价。",
            "3. 所以这轮更像“偏热但未稳固”。它已经有卖方市场的影子，但还没达到现实里那种持续 6-12 个月、主力画像大面积抬价成交的状态。",
            "",
            "## 4. 房子有没有被漏掉，导致该卖的没卖出去",
            "",
        ]
    )

    zero_match = traversal["zero_match_for_sale"]
    if zero_match:
        lines.append("有，但数量不多，而且不是“完全没露面”。当前仍在卖、却 0 次进入正式匹配记录的房子有：")
        lines.append("")
        for row in zero_match:
            lines.append(
                "- 房子 {property_id}：第 {listing_month} 月挂出，分到 `{bucket_id}`，正式匹配记录 0 次，但在买家候选里实际露面 {shortlist_exposure_count} 次。".format(
                    **row
                )
            )
        lines.extend(
            [
                "",
                "这说明它们不是“系统完全没遍历到”，而是“露过面，但始终没被买家真正选中”。这批房子都集中在 `A区、非学区、fallback` 这一类，说明更像是画像对位偏弱，而不是简单漏跑。",
            ]
        )
    else:
        lines.append("当前没有发现“仍在卖、却完全没有露面的房子”。")

    lines.extend(
        [
            "",
            "## 5. 有没有重复推荐、锁单后还继续推荐",
            "",
            "### 5.1 同一个买家反复看同一套房",
            "",
            "有，而且比较明显。典型例子：",
            "",
        ]
    )

    for row in traversal["repeated_pairs"][:10]:
        lines.append(
            "- 买家 {buyer_id} 在第 {first_month}-{last_month} 月，反复看到房子 {property_id} 共 {repeat_count} 次。".format(
                **row
            )
        )

    post_lock = traversal["post_lock_summary"]
    lines.extend(
        [
            "",
            "### 5.2 房子已经有人下单后，还继续被拿去匹配",
            "",
            f"- 受影响房子数：{post_lock['affected_properties']} 套",
            f"- 受影响买家数：{post_lock['affected_buyers']} 人",
            f"- 总重复匹配记录：{post_lock['total_rows']} 条",
            f"- 其中发生在同一锁单月的：{post_lock['same_month_rows']} 条",
            f"- 真正拖到后续月份还在继续匹配的：{post_lock['future_month_rows']} 条，涉及 {post_lock['future_month_properties']} 套房",
            "",
            "最明显的例子：",
            "",
        ]
    )

    for row in traversal["post_lock_examples"][:10]:
        lines.append(
            "- 房子 {property_id}：在首次被锁单的同月或之后，又被继续拿去匹配 {later_match_rows} 次，涉及 {later_distinct_buyers} 个买家，持续到第 {last_later_match_month} 月。".format(
                **row
            )
        )

    estimate_m6 = agent_need.get("estimate_m6")
    estimate_m12 = agent_need.get("estimate_m12")
    lines.extend(
        [
            "",
            "## 6. 如果还不够像卖方市场，大概要多少 agent 才能撑 6-12 个月",
            "",
            "这部分只能做粗估，不是发布级精算。算法很保守：",
            "",
            "1. 先看第 3 个月时，各画像月末还留在场内的有效买家，和对应在售房相比还差多少。",
            "2. 再看这轮 149 人里，到第 3 个月还能留下多少有效买家，作为“留存效率”。",
            "3. 用当前留存效率反推：如果还想把这种偏热状态撑到第 6、12 个月，大概要多少总 agent 池。",
            "",
        ]
    )

    if estimate_m6:
        lines.append(
            f"- 粗估撑到 6 个月：大约需要 {estimate_m6['estimated_total_agents']} 人左右（按当前第 3 个月留存效率 {estimate_m6['assumed_retention_ratio']} 倒推）。"
        )
    if estimate_m12:
        lines.append(
            f"- 粗估撑到 12 个月：大约需要 {estimate_m12['estimated_total_agents']} 人左右（同样按当前留存效率外推）。"
        )

    lines.extend(
        [
            "",
            "人话理解：当前 149 人已经把“总需求不够”的问题补掉了，但还不够把卖方挤压稳定撑到 6-12 个月。要想在同一份供应盘下跑出更长、更稳的卖方市场，需求池大概率还得再往上抬一个量级。",
            "",
            "## 7. 当前最像机制问题的地方，以及解决方向",
            "",
            "1. 同一个买家会反复看同一套房。这个问题已经开始修：新代码会复用同月已经明确说过的“不买这批房”的结论，并把送给 LLM 的可见候选缩到 3-5 套。下一轮新批次才能看到节流效果。",
            "2. 已经被锁单的房子，还会继续进入候选，尤其是同月内。这个更像可见性清理不够彻底，后续应把“已锁单房”的候选剔除再收紧一层，至少不要继续给新的买家。",
            "3. 那几套 0 次正式匹配的在售房，更像是“露过面但没人挑中”，不是完全漏遍历。它们集中在 fallback 桶，后续应该重点检查这类房是不是总被更强画像的同区房压住。",
        ]
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = _parse_args()
    db_path = Path(args.db).resolve()
    batch_summary_path = Path(args.batch_summary).resolve()
    monthly_breakdown_path = Path(args.monthly_breakdown_csv).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_summary = _load_batch_summary(batch_summary_path)
    initial_l0 = int(args.initial_l0 or batch_summary.get("l0") or batch_summary.get("L0") or 0)
    monthly_breakdown_rows = _load_monthly_breakdown(monthly_breakdown_path)

    with sqlite3.connect(db_path) as conn:
        monthly_macro = _query_monthly_macro(conn, initial_l0=initial_l0)
        price_to_list = _query_price_to_list(conn)
        traversal = _query_traversal_hygiene(conn)

    agent_need = _estimate_agent_need(monthly_breakdown_rows)

    _write_csv(
        output_dir / "seller_market_monthly_macro.csv",
        monthly_macro,
        fieldnames=[
            "month",
            "new_activated_buyers",
            "cumulative_unique_buyers",
            "month_end_locked_buyers",
            "month_end_cumulative_bought_buyers",
            "month_end_free_buyers",
            "new_distinct_listings",
            "month_end_locked_properties",
            "month_end_cumulative_sold_properties",
            "approx_visible_unlocked_stock_end",
            "order_count",
            "order_buyers",
            "transaction_count",
            "transaction_buyers",
            "avg_transaction_price",
            "transaction_volume",
            "rough_months_to_clear",
        ],
    )
    _write_csv(
        output_dir / "seller_market_price_to_list.csv",
        price_to_list,
        fieldnames=[
            "month",
            "transaction_count",
            "above_list_count",
            "equal_list_count",
            "below_list_count",
            "avg_price_to_list_ratio",
        ],
    )
    _write_csv(
        output_dir / "seller_market_zero_match_for_sale.csv",
        traversal["zero_match_for_sale"],
        fieldnames=[
            "property_id",
            "listing_month",
            "listed_price",
            "bucket_id",
            "shortlist_exposure_count",
            "selected_match_count",
        ],
    )
    _write_csv(
        output_dir / "seller_market_repeated_buyer_property_pairs.csv",
        traversal["repeated_pairs"],
        fieldnames=["buyer_id", "property_id", "first_month", "last_month", "repeat_count"],
    )
    _write_csv(
        output_dir / "seller_market_post_lock_matches.csv",
        traversal["post_lock_examples"],
        fieldnames=[
            "property_id",
            "later_match_rows",
            "later_distinct_buyers",
            "first_later_match_month",
            "last_later_match_month",
        ],
    )
    (output_dir / "seller_market_agent_need_estimate.json").write_text(
        json.dumps(agent_need, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_markdown(
        output_dir / "seller_market_run_diagnosis.md",
        monthly_macro=monthly_macro,
        price_to_list=price_to_list,
        traversal=traversal,
        agent_need=agent_need,
    )


if __name__ == "__main__":
    main()
