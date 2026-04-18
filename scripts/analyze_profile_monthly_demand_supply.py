import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path


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
    parser = argparse.ArgumentParser(description="Analyze monthly demand vs supply by profile bucket.")
    parser.add_argument("--db", required=True, help="Path to simulation.db")
    parser.add_argument("--monthly-bucket-funnel", required=True, help="Path to monthly_bucket_funnel.csv")
    parser.add_argument("--output-dir", required=True, help="Directory for CSV/Markdown outputs")
    return parser.parse_args()


def _bucket_sort_key(bucket_id: str):
    if bucket_id in BUCKET_ORDER:
        return (0, BUCKET_ORDER.index(bucket_id))
    return (1, bucket_id)


def _safe_bucket_label(bucket_id: str) -> str:
    return BUCKET_LABELS.get(bucket_id, bucket_id)


def _load_monthly_bucket_funnel(path: Path):
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(row)
    months = sorted({int(r["month_index"]) for r in rows})
    bucket_rows = {
        (int(r["month_index"]), str(r["profile_bucket_id"])): r
        for r in rows
    }
    return months, bucket_rows


def _load_bucket_map(conn: sqlite3.Connection):
    bucket_by_agent = {}
    broad_population = defaultdict(set)
    rows = conn.execute(
        """
        SELECT agent_id, bucket_id, role_side
        FROM profiled_market_agent_buckets
        """
    ).fetchall()
    for agent_id, bucket_id, role_side in rows:
        bucket_id = str(bucket_id or "")
        if bucket_id:
            bucket_by_agent[int(agent_id)] = bucket_id
        role_side = str(role_side or "").lower()
        if role_side in {"buyer", "buyer_seller"} and bucket_id:
            broad_population[bucket_id].add(int(agent_id))
    return bucket_by_agent, broad_population


def _load_role_decisions(conn: sqlite3.Connection, bucket_by_agent):
    broad_by_month = defaultdict(lambda: defaultdict(set))
    immediate_by_month = defaultdict(lambda: defaultdict(set))
    rows = conn.execute(
        """
        SELECT month, agent_id, decision, context_metrics
        FROM decision_logs
        WHERE event_type = 'ROLE_DECISION'
        ORDER BY month, agent_id
        """
    ).fetchall()
    for month, agent_id, decision, context_metrics in rows:
        bucket_id = bucket_by_agent.get(int(agent_id), "")
        chain_mode = ""
        if context_metrics:
            try:
                payload = json.loads(context_metrics)
                bucket_id = str(payload.get("profile_bucket_id") or bucket_id or "")
                chain_mode = str(payload.get("chain_mode") or "").strip().lower()
            except Exception:
                pass
        if not bucket_id:
            continue
        decision = str(decision or "").upper()
        if decision in {"BUYER", "BUYER_SELLER"}:
            broad_by_month[int(month)][bucket_id].add(int(agent_id))
        if decision == "BUYER" or (decision == "BUYER_SELLER" and chain_mode == "buy_first"):
            immediate_by_month[int(month)][bucket_id].add(int(agent_id))
    return broad_by_month, immediate_by_month


def _load_match_stages(conn: sqlite3.Connection, bucket_by_agent):
    shortlist_by_month = defaultdict(lambda: defaultdict(set))
    negotiation_by_month = defaultdict(lambda: defaultdict(set))
    rows = conn.execute(
        """
        SELECT month, buyer_id, proceeded_to_negotiation
        FROM property_buyer_matches
        """
    ).fetchall()
    for month, buyer_id, proceeded_to_negotiation in rows:
        bucket_id = bucket_by_agent.get(int(buyer_id), "")
        if not bucket_id:
            continue
        shortlist_by_month[int(month)][bucket_id].add(int(buyer_id))
        if int(proceeded_to_negotiation or 0) == 1:
            negotiation_by_month[int(month)][bucket_id].add(int(buyer_id))
    return shortlist_by_month, negotiation_by_month


def _load_orders_and_transactions(conn: sqlite3.Connection, bucket_by_agent):
    orders_by_month = defaultdict(lambda: defaultdict(set))
    order_rows = conn.execute(
        """
        SELECT order_id, created_month, close_month, status, buyer_id
        FROM transaction_orders
        """
    ).fetchall()
    for _order_id, created_month, _close_month, _status, buyer_id in order_rows:
        bucket_id = bucket_by_agent.get(int(buyer_id), "")
        if not bucket_id:
            continue
        orders_by_month[int(created_month)][bucket_id].add(int(buyer_id))

    tx_by_month = defaultdict(lambda: defaultdict(set))
    tx_rows = conn.execute(
        """
        SELECT month, buyer_id
        FROM transactions
        """
    ).fetchall()
    for month, buyer_id in tx_rows:
        bucket_id = bucket_by_agent.get(int(buyer_id), "")
        if not bucket_id:
            continue
        tx_by_month[int(month)][bucket_id].add(int(buyer_id))

    return order_rows, orders_by_month, tx_by_month


def _pending_and_filled_by_month(order_rows, tx_by_month, bucket_by_agent, months):
    pending_end = defaultdict(lambda: defaultdict(set))
    filled_end = defaultdict(lambda: defaultdict(set))
    filled_cumulative = defaultdict(set)

    for month in months:
        for bucket_id, buyers in tx_by_month.get(month, {}).items():
            filled_cumulative[bucket_id].update(int(x) for x in buyers)
        for bucket_id, buyers in filled_cumulative.items():
            filled_end[month][bucket_id] = set(int(x) for x in buyers)

        for _order_id, created_month, close_month, status, buyer_id in order_rows:
            created_month = int(created_month or 0)
            if created_month > month:
                continue
            close_month = int(close_month) if close_month is not None else None
            bucket_id = bucket_by_agent.get(int(buyer_id), "")
            if not bucket_id:
                continue
            is_pending_at_month_end = close_month is None or close_month > month
            if is_pending_at_month_end:
                pending_end[month][bucket_id].add(int(buyer_id))

    return pending_end, filled_end


def _build_rows(months, bucket_rows, broad_population, broad_by_month, immediate_by_month, shortlist_by_month,
                negotiation_by_month, orders_by_month, tx_by_month, pending_end, filled_end):
    all_buckets = set(BUCKET_LABELS.keys())
    all_buckets.update(broad_population.keys())
    all_buckets.update(bucket for _, bucket in bucket_rows.keys())

    cumulative_broad = defaultdict(set)
    cumulative_immediate = defaultdict(set)
    output_rows = []

    for month in months:
        for bucket_id in sorted(all_buckets, key=_bucket_sort_key):
            cumulative_broad[bucket_id].update(broad_by_month.get(month, {}).get(bucket_id, set()))
            cumulative_immediate[bucket_id].update(immediate_by_month.get(month, {}).get(bucket_id, set()))

            pending_now = set(pending_end.get(month, {}).get(bucket_id, set()))
            filled_now = set(filled_end.get(month, {}).get(bucket_id, set()))

            broad_stock = set(cumulative_broad[bucket_id]) - pending_now - filled_now
            immediate_stock = set(cumulative_immediate[bucket_id]) - pending_now - filled_now

            funnel_row = dict(bucket_rows.get((month, bucket_id), {}))
            supply_count = int(funnel_row.get("eligible_supply_count", 0) or 0)
            population_total = len(broad_population.get(bucket_id, set()))
            row = {
                "month": int(month),
                "profile_bucket_id": bucket_id,
                "profile_label": _safe_bucket_label(bucket_id),
                "matching_supply_count": int(supply_count),
                "population_total": int(population_total),
                "new_activated_broad": len(broad_by_month.get(month, {}).get(bucket_id, set())),
                "new_activated_immediate": len(immediate_by_month.get(month, {}).get(bucket_id, set())),
                "month_end_pending_or_locked": len(pending_now),
                "month_end_filled": len(filled_now),
                "month_end_broad_effective_stock": len(broad_stock),
                "month_end_immediate_stock": len(immediate_stock),
                "shortlist_buyers": len(shortlist_by_month.get(month, {}).get(bucket_id, set())),
                "negotiation_buyers": len(negotiation_by_month.get(month, {}).get(bucket_id, set())),
                "order_buyers": len(orders_by_month.get(month, {}).get(bucket_id, set())),
                "transaction_buyers": len(tx_by_month.get(month, {}).get(bucket_id, set())),
            }
            row["broad_stock_to_supply_ratio"] = round(
                float(row["month_end_broad_effective_stock"]) / float(supply_count),
                4,
            ) if supply_count > 0 else 0.0
            row["immediate_stock_to_supply_ratio"] = round(
                float(row["month_end_immediate_stock"]) / float(supply_count),
                4,
            ) if supply_count > 0 else 0.0
            output_rows.append(row)
    return output_rows


def _write_csv(path: Path, rows):
    fieldnames = [
        "month",
        "profile_bucket_id",
        "profile_label",
        "matching_supply_count",
        "population_total",
        "new_activated_broad",
        "new_activated_immediate",
        "month_end_pending_or_locked",
        "month_end_filled",
        "month_end_broad_effective_stock",
        "broad_stock_to_supply_ratio",
        "month_end_immediate_stock",
        "immediate_stock_to_supply_ratio",
        "shortlist_buyers",
        "negotiation_buyers",
        "order_buyers",
        "transaction_buyers",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, rows, monthly_bucket_funnel_path: Path, db_path: Path):
    lines = []
    lines.append("# 逐月画像需求-供给拆解")
    lines.append("")
    lines.append(f"- 数据库: `{db_path}`")
    lines.append(f"- 月度画像漏斗: `{monthly_bucket_funnel_path}`")
    lines.append("- 口径说明:")
    lines.append("  - `新激活广义买家`: 当月新进入买房链路的人，包含纯买家和买卖双身份。")
    lines.append("  - `新激活即时买家`: 当月一激活就能直接走买房链路的人，这里只算纯买家和极少数明确先买的双身份。")
    lines.append("  - `月末广义有效需求`: 截至当月末，历史上已激活过、且当前没有待交割订单、也还没真正成交的买房需求。")
    lines.append("  - `月末即时需求`: 同上，但只看能立刻以买家身份出手的人。")
    lines.append("  - `匹配在售房`: 使用同批次 `monthly_bucket_funnel.csv` 里的当月画像可承接在售房口径。")
    lines.append("")

    rows_by_month = defaultdict(list)
    for row in rows:
        rows_by_month[int(row["month"])].append(row)

    for month in sorted(rows_by_month):
        lines.append(f"## 第 {month} 月")
        lines.append("")
        lines.append("| 画像 | 匹配在售房 | 总潜在人口 | 新激活广义买家 | 新激活即时买家 | 月末广义有效需求 | 广义需求/供给 | 月末即时需求 | 即时需求/供给 | 看房人数 | 谈价人数 | 下单人数 | 成交人数 | 月末待交割/锁定 | 累计已成交 |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for row in sorted(rows_by_month[month], key=lambda item: _bucket_sort_key(item["profile_bucket_id"])):
            lines.append(
                f"| {row['profile_label']} | {row['matching_supply_count']} | {row['population_total']} | "
                f"{row['new_activated_broad']} | {row['new_activated_immediate']} | "
                f"{row['month_end_broad_effective_stock']} | {row['broad_stock_to_supply_ratio']:.4f} | "
                f"{row['month_end_immediate_stock']} | {row['immediate_stock_to_supply_ratio']:.4f} | "
                f"{row['shortlist_buyers']} | {row['negotiation_buyers']} | {row['order_buyers']} | "
                f"{row['transaction_buyers']} | {row['month_end_pending_or_locked']} | {row['month_end_filled']} |"
            )
        lines.append("")

        if month == 1:
            sorted_rows = sorted(rows_by_month[month], key=lambda item: (-item["month_end_broad_effective_stock"], item["profile_label"]))
            top_rows = sorted_rows[:3]
            weak_rows = sorted(rows_by_month[month], key=lambda item: (item["month_end_broad_effective_stock"], item["profile_label"]))[:3]
            lines.append("### 当月观察")
            lines.append("")
            lines.append(
                f"- 当月广义需求最厚的三类是："
                + "，".join(
                    f"{row['profile_label']}({row['month_end_broad_effective_stock']}/{row['matching_supply_count']})"
                    for row in top_rows
                )
                + "。"
            )
            lines.append(
                f"- 当月最薄的三类是："
                + "，".join(
                    f"{row['profile_label']}({row['month_end_broad_effective_stock']}/{row['matching_supply_count']})"
                    for row in weak_rows
                )
                + "。"
            )
            lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    args = _parse_args()
    db_path = Path(args.db)
    monthly_bucket_funnel_path = Path(args.monthly_bucket_funnel)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    months, bucket_rows = _load_monthly_bucket_funnel(monthly_bucket_funnel_path)
    conn = sqlite3.connect(str(db_path))
    bucket_by_agent, broad_population = _load_bucket_map(conn)
    broad_by_month, immediate_by_month = _load_role_decisions(conn, bucket_by_agent)
    shortlist_by_month, negotiation_by_month = _load_match_stages(conn, bucket_by_agent)
    order_rows, orders_by_month, tx_by_month = _load_orders_and_transactions(conn, bucket_by_agent)
    pending_end, filled_end = _pending_and_filled_by_month(order_rows, tx_by_month, bucket_by_agent, months)
    rows = _build_rows(
        months,
        bucket_rows,
        broad_population,
        broad_by_month,
        immediate_by_month,
        shortlist_by_month,
        negotiation_by_month,
        orders_by_month,
        tx_by_month,
        pending_end,
        filled_end,
    )
    conn.close()

    csv_path = output_dir / "profile_monthly_demand_supply_breakdown.csv"
    md_path = output_dir / "profile_monthly_demand_supply_breakdown.md"
    _write_csv(csv_path, rows)
    _write_markdown(md_path, rows, monthly_bucket_funnel_path, db_path)

    print(json.dumps({"csv": str(csv_path), "markdown": str(md_path), "row_count": len(rows)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
