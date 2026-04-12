import logging
import json
import sqlite3
from typing import Dict, List

from models import Market
from property_initializer import convert_to_v2_tuples, initialize_market_properties

logger = logging.getLogger(__name__)


class MarketService:
    def __init__(self, config, db_conn: sqlite3.Connection):
        self.config = config
        self.conn = db_conn
        self.consecutive_trend = 0
        self.market = None  # Initialized later

    def initialize_market(self):
        """Initialize market properties based on configuration."""
        user_prop_count = getattr(self.config, 'user_property_count', None)

        if user_prop_count:
            logger.info(f"Initializing market with User Defined Property Count: {user_prop_count}")
            properties = initialize_market_properties(target_total_count=user_prop_count, config=self.config)
        else:
            properties = initialize_market_properties(config=self.config)

        # Sort properties by value descending for targeted distribution
        properties.sort(key=lambda x: x['base_value'], reverse=True)

        self.market = Market(properties)

        # Persist to DB (V2)
        # Note: Owner IDs are None initially. AgentService updates them later.
        # But we must insert the properties first so AgentService can update them.
        self._persist_properties(properties)

        return properties

    def _persist_properties(self, properties: List[Dict]):
        cursor = self.conn.cursor()
        batch_static = []
        batch_market = []

        for p in properties:
            s_data, m_data = convert_to_v2_tuples(p)
            batch_static.append(tuple(s_data.values()))
            batch_market.append(tuple(m_data.values()))

        cursor.executemany("""
            INSERT OR IGNORE INTO properties_static
            (property_id, zone, quality, building_area, property_type, is_school_district, school_tier, price_per_sqm, zone_price_tier, initial_value, build_year, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_static)

        cursor.executemany("""
            INSERT OR IGNORE INTO properties_market
            (property_id, owner_id, status, current_valuation, listed_price, min_price, rental_price, rental_yield, listing_month, last_transaction_month)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_market)

        self.conn.commit()
        logger.info(f"Persisted {len(properties)} properties to DB (V2).")

    def load_market_from_db(self, agents: List):
        """Load market properties from database and link to owners."""
        cursor = self.conn.cursor()
        # Use V2 Schema: Join properties_static with properties_market
        cursor.execute("""
            SELECT ps.*, pm.status, pm.owner_id, pm.listed_price, pm.current_valuation
            FROM properties_static ps
            LEFT JOIN properties_market pm ON ps.property_id = pm.property_id
        """)
        # V1 table 'properties' contains everything. V2 split checks?
        # SimulationRunner.load_from_db checks 'properties'.
        # Since we just verified V1 cleanup, 'properties' table might still exist?
        # Wait, I removed 'create table properties' but did I remove 'properties' table?
        # In simulation_runner, I removed 'property_listings'.
        # 'properties' table creation was at line 159.
        # I did NOT remove 'properties' table creation in my V1 Cleanups?
        # Let's check my edits.

        # Step 1038 edit:
        # I removed 'cursor.execute("DROP TABLE IF EXISTS property_listings")' and creation.

        # Did I remove 'properties' table?
        # The edit showed removing property_listings.
        # Line 158 in simulation_runner starts 'CREATE TABLE properties'.

        # If 'properties' table still exists, it's V1 legcay.
        # Ideally we should use properties_static and properties_market.
        # For now, let's assume we still use 'properties' for reading if it wasn't deleted.
        # BUT, the goal is architecture evolution.
        # I should probably switch to V2 tables here if possible.

        # Let's assume loading from V2 for now if possible, else fallback.
        # But 'properties' table creation was NOT removed in the previous step?
        # I replaced lines 130-141. 'properties' creation starts at 158.
        # Ah, I might have missed removing 'properties' creation?
        # Task said "V1 Table Cleanup: Audit property_listings... Remove creation of property_listings..."
        # It didn't explicitly say remove 'properties' table yet, but it's part of V1.
        # However, verifying script checks 'property_listings'.

        # Correct approach: Read from properties_static + properties_market.
        cursor.execute("SELECT ps.*, pm.status, pm.owner_id, pm.listed_price, pm.current_valuation FROM properties_static ps LEFT JOIN properties_market pm ON ps.property_id = pm.property_id")
        rows = cursor.fetchall()

        properties = []
        for row in rows:
            # Need to map columns correctly. 'properties_static' has headers?
            # Sqlite Row factory useful here.
            # Assuming row is dict-like if configured, but here we passed db_conn.
            # We should probably configure row_factory in Runner or here.
            pass
            # Implementation detail: simulation_runner sets row_factory.

        # To avoid complexity in this first draft, I will assume we can query 'properties' if it exists, or V2.
        # But wait, if I am decoupling, I should do it right.
        # Let's stick to 'properties' for now if I didn't delete it, OR implement V2 loading.
        # Since I verified verify_db_results targets properties_market, I should use V2.

        cursor.execute("""
            SELECT ps.property_id, ps.zone, ps.quality, ps.building_area, ps.property_type,
                   ps.is_school_district, ps.school_tier, ps.initial_value as base_value,
                   ps.build_year,
                   pm.owner_id, pm.status, pm.listed_price, pm.min_price, pm.current_valuation
            FROM properties_static ps
            LEFT JOIN properties_market pm ON ps.property_id = pm.property_id
        """)

        # Convert to dict
        columns = [d[0] for d in cursor.description]
        properties = [dict(zip(columns, row)) for row in cursor.fetchall()]

        # Link to agents
        for p in properties:
            if p.get('owner_id') and p['owner_id'] > 0:
                agent = next((x for x in agents if x.id == p['owner_id']), None)
                if agent:
                    agent.owned_properties.append(p)

        self.market = Market(properties)
        logger.info(f"Loaded {len(properties)} properties from DB (V2).")

    def get_recent_bulletins(self, current_month: int, n: int = 3) -> List[Dict]:
        """
        Fetched recent market bulletins from DB for LLM Context.
        Returns list of dicts: [{'month': m, 'avg_price': p, 'volume': v, 'trend': t}]
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT month, avg_price, transaction_volume, trend_signal
            FROM market_bulletin
            WHERE month < ?
            ORDER BY month DESC
            LIMIT ?
        """, (current_month, n))

        rows = cursor.fetchall()
        # Return in chronological order
        return [{'month': r[0], 'avg_price': r[1], 'volume': r[2], 'trend': r[3]} for r in reversed(rows)]

    def _compute_match_metrics(self, rows: List[sqlite3.Row]) -> Dict[str, float]:
        total = 0
        selected = 0
        smart_total = 0
        smart_selected = 0
        edu_deltas = []
        price_deltas = []

        for row in rows:
            total += 1
            decision = str(row[0] or "")
            reason = str(row[1] or "")
            thought_raw = row[2]
            metrics_raw = row[3]

            selected_ids = []
            try:
                thought_obj = json.loads(thought_raw) if thought_raw else {}
                selected_ids = thought_obj.get("selected_property_ids", []) or []
            except Exception:
                # Fallback parse from reason: "selected=[1,2]"
                if "selected=[" in reason:
                    try:
                        bracket = reason.split("selected=", 1)[1].strip()
                        selected_ids = json.loads(bracket.replace("'", "\""))
                    except Exception:
                        selected_ids = []
            if selected_ids:
                selected += 1

            is_smart_profile = decision.startswith("smart_")
            if is_smart_profile:
                smart_total += 1
                if selected_ids:
                    smart_selected += 1
                try:
                    metrics_obj = json.loads(metrics_raw) if metrics_raw else {}
                    base_edu = metrics_obj.get("base_education_weight")
                    eff_edu = metrics_obj.get("effective_education_weight")
                    base_price = metrics_obj.get("base_price_sensitivity")
                    eff_price = metrics_obj.get("effective_price_sensitivity")
                    if base_edu is not None and eff_edu is not None:
                        edu_deltas.append(float(eff_edu) - float(base_edu))
                    if base_price is not None and eff_price is not None:
                        price_deltas.append(float(eff_price) - float(base_price))
                except Exception:
                    pass

        return {
            "match_total": int(total),
            "match_selected": int(selected),
            "match_hit_rate": float(selected / total) if total > 0 else 0.0,
            "smart_match_total": int(smart_total),
            "smart_match_selected": int(smart_selected),
            "smart_match_hit_rate": float(smart_selected / smart_total) if smart_total > 0 else 0.0,
            "avg_edu_weight_delta": float(sum(edu_deltas) / len(edu_deltas)) if edu_deltas else 0.0,
            "avg_price_sensitivity_delta": float(sum(price_deltas) / len(price_deltas)) if price_deltas else 0.0,
        }

    def _collect_buyer_match_metrics(self, report_month: int) -> Dict[str, float]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT decision, reason, thought_process, context_metrics
            FROM decision_logs
            WHERE month=? AND event_type='BUYER_MATCH'
            """,
            (report_month,),
        )
        rows = cursor.fetchall() or []
        metrics = self._compute_match_metrics(rows)
        metrics.update(self._collect_m16_guard_metrics(report_month))
        metrics.update(self._collect_order_flow_metrics(report_month))
        metrics.update(self._collect_market_pulse_metrics(report_month))
        return metrics

    def _collect_m16_guard_metrics(self, report_month: int) -> Dict[str, float]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM decision_logs
            WHERE month=?
              AND event_type='BUYER_MATCH'
              AND decision='M16_BLOCK_DOWNTREND_ACCUMULATION'
            """,
            (report_month,),
        )
        blocks = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute(
            """
            SELECT context_metrics
            FROM decision_logs
            WHERE month=?
              AND event_type='M16_GUARD'
              AND decision='OFFER_CLAMP'
            """,
            (report_month,),
        )
        rows = cursor.fetchall() or []
        clamp_total = 0
        for row in rows:
            raw = row[0]
            if not raw:
                clamp_total += 1
                continue
            try:
                obj = json.loads(raw)
                clamp_total += int(obj.get("m16_offer_clamp_count", obj.get("clamp_count", 1)) or 0)
            except Exception:
                clamp_total += 1

        cursor.execute(
            """
            SELECT context_metrics
            FROM decision_logs
            WHERE month=?
              AND event_type='ROLE_DECISION'
            """,
            (report_month,),
        )
        rows = cursor.fetchall() or []
        sell_cap_total = 0
        for row in rows:
            raw = row[0]
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            if bool(obj.get("m16_sell_cap_applied", False)):
                sell_cap_total += 1

        return {
            "m16_blocks_count": int(blocks),
            "m16_offer_clamp_count": int(clamp_total),
            "m16_sell_cap_count": int(sell_cap_total),
        }

    def _collect_order_flow_metrics(self, report_month: int) -> Dict[str, float]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM decision_logs
            WHERE month=?
              AND event_type='ORDER_PRECHECK'
              AND decision='REJECT'
            """,
            (report_month,),
        )
        precheck_reject_count = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM decision_logs
            WHERE month=?
              AND event_type='BID_VALIDATION'
              AND decision='INVALID_BID'
            """,
            (report_month,),
        )
        invalid_bid_count = int((cursor.fetchone() or [0])[0] or 0)

        cursor.execute(
            """
            SELECT COALESCE(close_reason, ''), COUNT(*)
            FROM transaction_orders
            WHERE close_month=?
            GROUP BY COALESCE(close_reason, '')
            """,
            (report_month,),
        )
        rows = cursor.fetchall() or []
        settlement_fail_affordability_count = 0
        settlement_fail_dti_count = 0
        settlement_fail_fee_count = 0
        for reason, cnt in rows:
            r = str(reason or "").lower()
            c = int(cnt or 0)
            if (
                "settlement failed: buyer affordability" in r
                or "settlement failed: insufficient down payment" in r
                or "settlement failed: liquidity buffer" in r
                or "settlement failed: leverage cap exceeded" in r
            ):
                settlement_fail_affordability_count += c
            elif "settlement failed: dti exceeded" in r:
                settlement_fail_dti_count += c
            elif "settlement failed: fee shortfall" in r:
                settlement_fail_fee_count += c

        return {
            "precheck_reject_count": int(precheck_reject_count),
            "invalid_bid_count": int(invalid_bid_count),
            "settlement_fail_affordability_count": int(settlement_fail_affordability_count),
            "settlement_fail_dti_count": int(settlement_fail_dti_count),
            "settlement_fail_fee_count": int(settlement_fail_fee_count),
        }

    def _collect_market_pulse_metrics(self, report_month: int) -> Dict[str, float]:
        cursor = self.conn.cursor()
        metrics = {
            "mortgage_watch_count": 0,
            "mortgage_dpd30_count": 0,
            "mortgage_dpd60_count": 0,
            "mortgage_default_count": 0,
            "forced_sale_count": 0,
            "negative_equity_count": 0,
            "npl_ratio": 0.0,
            "zone_a_liquidity_index": 1.0,
            "zone_b_liquidity_index": 1.0,
        }
        try:
            cursor.execute(
                """
                SELECT delinquency_stage, COUNT(*)
                FROM mortgage_accounts
                WHERE status='active'
                GROUP BY delinquency_stage
                """
            )
            stage_counts = {str(r[0] or "").upper(): int(r[1] or 0) for r in cursor.fetchall() or []}
            metrics["mortgage_watch_count"] = int(stage_counts.get("WATCH", 0))
            metrics["mortgage_dpd30_count"] = int(stage_counts.get("DPD30", 0))
            metrics["mortgage_dpd60_count"] = int(stage_counts.get("DPD60", 0))

            cursor.execute("SELECT COUNT(*) FROM mortgage_accounts WHERE status='defaulted'")
            metrics["mortgage_default_count"] = int((cursor.fetchone() or [0])[0] or 0)

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM decision_logs
                WHERE month=?
                  AND event_type IN ('MARKET_PULSE', 'SELLER_DEADLINE')
                  AND decision='FORCED_SALE'
                """,
                (report_month,),
            )
            metrics["forced_sale_count"] = int((cursor.fetchone() or [0])[0] or 0)

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM mortgage_accounts ma
                JOIN properties_market pm ON pm.property_id = ma.property_id
                WHERE ma.status IN ('active', 'defaulted')
                  AND COALESCE(ma.remaining_principal, 0) > COALESCE(pm.current_valuation, 0)
                """
            )
            metrics["negative_equity_count"] = int((cursor.fetchone() or [0])[0] or 0)

            cursor.execute("SELECT COUNT(*) FROM mortgage_accounts WHERE status IN ('active', 'defaulted')")
            loan_population = int((cursor.fetchone() or [0])[0] or 0)
            npl_num = (
                int(metrics["mortgage_dpd30_count"])
                + int(metrics["mortgage_dpd60_count"])
                + int(metrics["mortgage_default_count"])
            )
            metrics["npl_ratio"] = float(npl_num / loan_population) if loan_population > 0 else 0.0

            for zone in ("A", "B"):
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM properties_market pm
                    JOIN properties_static ps ON ps.property_id = pm.property_id
                    WHERE pm.status='for_sale' AND ps.zone=?
                    """,
                    (zone,),
                )
                total_sale = int((cursor.fetchone() or [0])[0] or 0)
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM properties_market pm
                    JOIN properties_static ps ON ps.property_id = pm.property_id
                    WHERE pm.status='for_sale'
                      AND ps.zone=?
                      AND COALESCE(pm.last_price_update_reason, '') LIKE 'Market Pulse forced sale%'
                    """,
                    (zone,),
                )
                forced = int((cursor.fetchone() or [0])[0] or 0)
                liquidity_idx = max(0.05, 1.0 - (forced / max(1, total_sale)))
                if zone == "A":
                    metrics["zone_a_liquidity_index"] = float(liquidity_idx)
                else:
                    metrics["zone_b_liquidity_index"] = float(liquidity_idx)

        except sqlite3.OperationalError:
            # Older DB without market pulse tables/columns.
            pass
        return metrics

    async def generate_market_bulletin(
        self,
        month: int,
        extra_news: List[str] = None,
        observed_month: int = None,
        use_llm_analysis: bool = True,
    ) -> str:
        """
        Generate monthly market bulletin with LLM analysis.
        Includes Phase 5: Unit Price Trends.
        """
        from utils.llm_client import safe_call_llm_async

        cursor = self.conn.cursor()

        observed_month = max(0, int(month - 1 if observed_month is None else observed_month))

        # 1. Query observed month's transactions (Volume & Total Price)
        # JOIN with properties_static to fetch AREA for unit price calculation
        cursor.execute("""
            SELECT
                COUNT(*) as count,
                AVG(t.final_price) as avg_price,
                SUM(t.final_price) as total_volume,
                SUM(t.final_price) / SUM(p.building_area) as avg_unit_price
            FROM transactions t
            JOIN properties_static p ON t.property_id = p.property_id
            WHERE t.month = ?
        """, (observed_month,))

        last_month_stats = cursor.fetchone()
        transaction_count = last_month_stats[0] if last_month_stats else 0
        avg_price = last_month_stats[1] if last_month_stats and last_month_stats[1] else 0
        avg_unit_price = last_month_stats[3] if last_month_stats and last_month_stats[3] else 0

        # Handle case where no transactions occurred
        if not avg_unit_price:
            avg_unit_price = 0

        # 2. Calculate price change (MoM for Unit Price)
        price_change_pct = 0.0
        unit_price_change_pct = 0.0

        if month > 1:
            cursor.execute("SELECT avg_price, avg_unit_price FROM market_bulletin WHERE month = ?", (month - 1,))
            prev_bulletin = cursor.fetchone()

            # Avg Price Change
            if prev_bulletin and prev_bulletin[0] and prev_bulletin[0] > 0:
                price_change_pct = ((avg_price - prev_bulletin[0]) / prev_bulletin[0]) * 100

            # Unit Price Change
            if prev_bulletin and len(prev_bulletin) > 1 and prev_bulletin[1] and prev_bulletin[1] > 0 and avg_unit_price > 0:
                unit_price_change_pct = ((avg_unit_price - prev_bulletin[1]) / prev_bulletin[1]) * 100

        # 2.5 Smart strategy observability (from previous month's buyer matching logs)
        match_metrics = self._collect_buyer_match_metrics(observed_month)

        # 3. Calculate zone heat
        cursor.execute("PRAGMA table_info(active_participants)")
        active_cols = {r[1] for r in cursor.fetchall()}
        has_month = "month" in active_cols

        def calc_zone_heat(zone):
            cursor.execute("SELECT COUNT(*) FROM properties_market WHERE status = 'for_sale' AND property_id IN (SELECT property_id FROM properties_static WHERE zone = ?)", (zone,))
            result = cursor.fetchone()
            listings = result[0] if result else 0

            if has_month:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM active_participants ap
                    JOIN (
                        SELECT agent_id, MAX(month) AS max_month
                        FROM active_participants
                        GROUP BY agent_id
                    ) latest
                    ON ap.agent_id = latest.agent_id
                    AND (
                        ap.month = latest.max_month
                        OR (ap.month IS NULL AND latest.max_month IS NULL)
                    )
                    WHERE ap.role IN ('BUYER', 'BUYER_SELLER') AND ap.target_zone = ?
                """, (zone,))
            else:
                cursor.execute(
                    "SELECT COUNT(*) FROM active_participants WHERE role IN ('BUYER', 'BUYER_SELLER') AND target_zone = ?",
                    (zone,)
                )
            result = cursor.fetchone()
            buyers = result[0] if result else 0

            if buyers == 0:
                return "COLD" if listings > 5 else "BALANCED"
            ratio = listings / max(buyers, 1)
            return "COLD" if ratio > 1.5 else ("HOT" if ratio < 0.7 else "BALANCED")

        zone_a_heat = calc_zone_heat('A')
        zone_b_heat = calc_zone_heat('B')

        # 4. Determine trend signal
        change_to_use = unit_price_change_pct if avg_unit_price > 0 else price_change_pct

        if change_to_use > 2.0:
            self.consecutive_trend = self.consecutive_trend + 1 if self.consecutive_trend > 0 else 1
            trend_signal = "UP"
        elif change_to_use < -2.0:
            self.consecutive_trend = self.consecutive_trend - 1 if self.consecutive_trend < 0 else -1
            trend_signal = "DOWN"
        else:
            self.consecutive_trend = 0
            trend_signal = "STABLE"

        if self.consecutive_trend <= -2:
            trend_signal = "PANIC"

        # 5. 查询开发商房产信息（V3新增）
        cursor.execute("""
            SELECT 
                COUNT(*) as dev_count,
                AVG(pm.listed_price) as dev_avg_price,
                MIN(pm.listed_price) as dev_min_price,
                MAX(pm.listed_price) as dev_max_price
            FROM properties_market pm
            WHERE pm.owner_id = -1 AND pm.status = 'for_sale'
        """)
        dev_stats = cursor.fetchone()
        dev_count = dev_stats[0] if dev_stats else 0
        dev_avg_price = dev_stats[1] if dev_stats and dev_stats[1] else 0
        dev_min_price = dev_stats[2] if dev_stats and dev_stats[2] else 0
        dev_max_price = dev_stats[3] if dev_stats and dev_stats[3] else 0
        
        # 计算开发商房产单价（用于与市场价对比）
        dev_avg_unit_price = 0
        if dev_count > 0:
            cursor.execute("""
                SELECT AVG(pm.listed_price / ps.building_area) as avg_unit_price
                FROM properties_market pm
                JOIN properties_static ps ON pm.property_id = ps.property_id
                WHERE pm.owner_id = -1 AND pm.status = 'for_sale'
            """)
            dev_unit = cursor.fetchone()
            dev_avg_unit_price = dev_unit[0] if dev_unit and dev_unit[0] else 0

        # 6. Generate LLM Analysis
        
        # 计算开发商房产价格优势 (Moved out for global usage)
        dev_discount_pct = 0
        if dev_avg_unit_price > 0 and avg_unit_price > 0:
            dev_discount_pct = ((dev_avg_unit_price - avg_unit_price) / avg_unit_price) * 100

        # 5.5 Order/Settlement Metrics (M18/M19 observability)
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM transaction_orders
            WHERE created_month = ?
            """,
            (observed_month,),
        )
        orders_created = cursor.fetchone()[0] or 0

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM transaction_orders
            WHERE status = 'pending_settlement'
            """
        )
        orders_pending_settlement = cursor.fetchone()[0] or 0

        cursor.execute(
            """
            SELECT COUNT(*), COALESCE(AVG(close_month - created_month), 0)
            FROM transaction_orders
            WHERE status = 'filled' AND close_month = ?
            """,
            (observed_month,),
        )
        settled_row = cursor.fetchone()
        settlements_completed = settled_row[0] if settled_row else 0
        avg_settlement_lag_months = float(settled_row[1] if settled_row and settled_row[1] is not None else 0.0)

        cursor.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(penalty_amount), 0)
            FROM transaction_orders
            WHERE status IN ('breached', 'expired') AND close_month = ?
            """,
            (observed_month,),
        )
        breach_row = cursor.fetchone()
        breaches_count = breach_row[0] if breach_row else 0
        breach_penalty_total = float(breach_row[1] if breach_row and breach_row[1] is not None else 0.0)

        current_month_report = observed_month == month and month > 0
        period_label = "本月" if current_month_report else "上月"

        if observed_month == 0:
            llm_analysis_text = "市场初始化完成，暂无历史数据可供分析。建议关注后续月份的市场动态。"
        else:
            # 开发商房产信息（如果有）
            dev_info = ""
            if dev_count > 0:
                dev_info = f"""
                🏗️ **开发商特价房源**:
                - 在售数量: {dev_count}套
                - 均价: ¥{dev_avg_price:,.0f} (¥{dev_avg_unit_price:,.0f}/㎡)
                - 价格区间: ¥{dev_min_price:,.0f} - ¥{dev_max_price:,.0f}
                - 价格优势: 较市场均价{dev_discount_pct:+.1f}% {'🔥超值!' if dev_discount_pct < -20 else '✅有优势' if dev_discount_pct < 0 else ''}
                """
            
            base_stats = f"""
            第{month}月市场数据（统计口径: {period_label}）：
            - 成交量: {transaction_count}套
            - 成交均价: {avg_price:,.0f}元
            - 📏 单位均价: {avg_unit_price:,.0f} 元/㎡ (环比 {unit_price_change_pct:+.1f}%)
            - 📦 新建订单: {orders_created}
            - ⏳ 待交割库存: {orders_pending_settlement}
            - ✅ 交割完成: {settlements_completed}
            - ⚠️ 违约/过期: {breaches_count} (罚金合计 ¥{breach_penalty_total:,.0f})
            - 🕒 平均交割时滞: {avg_settlement_lag_months:.2f} 个月
            - 🧠 聪明策略命中率: {match_metrics['smart_match_hit_rate'] * 100:.1f}% ({match_metrics['smart_match_selected']}/{match_metrics['smart_match_total']})
            - 🎚️ 学区权重平均变化: {match_metrics['avg_edu_weight_delta']:+.2f}
            - 🎚️ 价格敏感平均变化: {match_metrics['avg_price_sensitivity_delta']:+.2f}
            - 🛡️ M16下跌拦截: {int(match_metrics.get('m16_blocks_count', 0))}
            - 🛡️ M16出价钳制: {int(match_metrics.get('m16_offer_clamp_count', 0))}
            - 🛡️ M16卖方限售: {int(match_metrics.get('m16_sell_cap_count', 0))}
            - 🧪 订单预检拦截: {int(match_metrics.get('precheck_reject_count', 0))}
            - 🧪 无效出价拦截: {int(match_metrics.get('invalid_bid_count', 0))}
            - ⚠️ 交割失败(首付): {int(match_metrics.get('settlement_fail_affordability_count', 0))}
            - ⚠️ 交割失败(DTI): {int(match_metrics.get('settlement_fail_dti_count', 0))}
            - ⚠️ 交割失败(税费): {int(match_metrics.get('settlement_fail_fee_count', 0))}
            - 🚨 个贷观察: WATCH {int(match_metrics.get('mortgage_watch_count', 0))}, DPD30 {int(match_metrics.get('mortgage_dpd30_count', 0))}, DPD60 {int(match_metrics.get('mortgage_dpd60_count', 0))}
            - 🚨 不良比例(NPL): {float(match_metrics.get('npl_ratio', 0.0)):.2%} | 违约总量: {int(match_metrics.get('mortgage_default_count', 0))}
            - 🧨 强平挂牌: {int(match_metrics.get('forced_sale_count', 0))} | 负资产: {int(match_metrics.get('negative_equity_count', 0))}
            - 🌊 抵押物流动性指数: A区 {float(match_metrics.get('zone_a_liquidity_index', 1.0)):.2f}, B区 {float(match_metrics.get('zone_b_liquidity_index', 1.0)):.2f}
            - A区热度: {zone_a_heat}
            - B区热度: {zone_b_heat}
            - 趋势: {trend_signal} (连续 {abs(self.consecutive_trend)} 个月)
            - 政策新闻: {", ".join(extra_news) if extra_news else "无"}
            {dev_info}
            """

            prompt = f"""
            你是一位资深房地产分析师。请根据以下市场核心数据，撰写一份简短犀利的【市场分析点评】（LLM Analysis）。

            {base_stats}

            请包含：
            1. 核心观点（一句话概括当前形势，重点关注单价变化）
            2. 对买家的建议（观望/入手/砍价）{' - **如果有开发商特价房源，重点提示买家这个超值机会！**' if dev_count > 0 and dev_discount_pct < -10 else ''}
            3. 对卖家的建议（降价/坚守/惜售）
            4. 可能会对{", ".join(extra_news) if extra_news else "当前环境"}产生什么解读。

            输出纯文本，控制在150字以内。
            """

            default_analysis = (
                f"市场{trend_signal}，{period_label}成交{transaction_count}套，"
                f"单价{avg_unit_price:,.0f}，建议谨慎操作。"
            )
            if use_llm_analysis:
                bulletin_model_type = str(
                    self.config.get("system.market_bulletin.model_type", "fast")
                ).strip().lower()
                if bulletin_model_type not in {"fast", "smart"}:
                    bulletin_model_type = "fast"
                llm_analysis_text = await safe_call_llm_async(
                    prompt,
                    default_analysis,
                    model_type=bulletin_model_type,
                )
            else:
                llm_analysis_text = default_analysis

        if isinstance(llm_analysis_text, dict):
            llm_analysis_text = str(llm_analysis_text)

        # 6. Save to database
        policy_news_str = "\\n".join(extra_news) if extra_news else ""

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO market_bulletin
                (month, transaction_volume, avg_price, avg_unit_price,
                 orders_created, orders_pending_settlement, settlements_completed,
                 breaches_count, breach_penalty_total, avg_settlement_lag_months,
                 smart_match_total, smart_match_selected, smart_match_hit_rate,
                 avg_edu_weight_delta, avg_price_sensitivity_delta,
                m16_blocks_count, m16_offer_clamp_count, m16_sell_cap_count,
                precheck_reject_count, invalid_bid_count,
                settlement_fail_affordability_count, settlement_fail_dti_count, settlement_fail_fee_count,
                mortgage_watch_count, mortgage_dpd30_count, mortgage_dpd60_count,
                mortgage_default_count, forced_sale_count, negative_equity_count, npl_ratio,
                zone_a_liquidity_index, zone_b_liquidity_index,
                price_change_pct, zone_a_heat, zone_b_heat, trend_signal, consecutive_direction, policy_news, llm_analysis)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                month, transaction_count, avg_price, avg_unit_price,
                orders_created, orders_pending_settlement, settlements_completed,
                breaches_count, breach_penalty_total, avg_settlement_lag_months,
                int(match_metrics["smart_match_total"]),
                int(match_metrics["smart_match_selected"]),
                float(match_metrics["smart_match_hit_rate"]),
                float(match_metrics["avg_edu_weight_delta"]),
                float(match_metrics["avg_price_sensitivity_delta"]),
                int(match_metrics.get("m16_blocks_count", 0)),
                int(match_metrics.get("m16_offer_clamp_count", 0)),
                int(match_metrics.get("m16_sell_cap_count", 0)),
                int(match_metrics.get("precheck_reject_count", 0)),
                int(match_metrics.get("invalid_bid_count", 0)),
                int(match_metrics.get("settlement_fail_affordability_count", 0)),
                int(match_metrics.get("settlement_fail_dti_count", 0)),
                int(match_metrics.get("settlement_fail_fee_count", 0)),
                int(match_metrics.get("mortgage_watch_count", 0)),
                int(match_metrics.get("mortgage_dpd30_count", 0)),
                int(match_metrics.get("mortgage_dpd60_count", 0)),
                int(match_metrics.get("mortgage_default_count", 0)),
                int(match_metrics.get("forced_sale_count", 0)),
                int(match_metrics.get("negative_equity_count", 0)),
                float(match_metrics.get("npl_ratio", 0.0)),
                float(match_metrics.get("zone_a_liquidity_index", 1.0)),
                float(match_metrics.get("zone_b_liquidity_index", 1.0)),
                float(unit_price_change_pct),
                zone_a_heat, zone_b_heat, trend_signal, int(self.consecutive_trend), policy_news_str, llm_analysis_text
            ))
            self.conn.commit()
        except sqlite3.OperationalError as e:
            print(f"Error saving market bulletin: {e}")

        trend_emoji = {"UP": "📈", "DOWN": "📉", "STABLE": "➡️", "PANIC": "⚠️"}.get(trend_signal, "")
        
        # 开发商房产显示
        dev_display = ""
        if dev_count > 0:
            discount_indicator = "🔥超值特价" if dev_discount_pct < -20 else "✅价格优势" if dev_discount_pct < 0 else ""
            dev_display = f"""
        【🏗️ 开发商特供房源】
        📦 在售: {dev_count}套 | 💰 均价: ¥{dev_avg_price:,.0f}
        📏 单价: ¥{dev_avg_unit_price:,.0f}/㎡ (较市场{dev_discount_pct:+.1f}%) {discount_indicator}
        """

        result_text = f"""
        【📊 市场公报 - 第{month}月】
        ━━━━━━━━━━━━━━━━━━━━━━━
        📈 {period_label}成交: {transaction_count} 套
        💰 成交均价: ¥{avg_price:,.0f}
        📏 单位均价: ¥{avg_unit_price:,.0f}/㎡ ({unit_price_change_pct:+.1f}%)
        🏢 A区热度: {zone_a_heat} | B区热度: {zone_b_heat}
        📊 趋势信号: {trend_signal} {trend_emoji}
        🧠 聪明策略命中: {match_metrics['smart_match_hit_rate'] * 100:.1f}% ({match_metrics['smart_match_selected']}/{match_metrics['smart_match_total']})
        🎚️ 调权变化: 学区{match_metrics['avg_edu_weight_delta']:+.2f} / 价格敏感{match_metrics['avg_price_sensitivity_delta']:+.2f}
        🛡️ M16守护: 下跌拦截{int(match_metrics.get('m16_blocks_count', 0))}次 / 出价钳制{int(match_metrics.get('m16_offer_clamp_count', 0))}次 / 卖方限售{int(match_metrics.get('m16_sell_cap_count', 0))}次
        🧪 订单预检: 拦截{int(match_metrics.get('precheck_reject_count', 0))}次 / 无效出价{int(match_metrics.get('invalid_bid_count', 0))}次
        ⚠️ 交割失败细分: 首付{int(match_metrics.get('settlement_fail_affordability_count', 0))} / DTI{int(match_metrics.get('settlement_fail_dti_count', 0))} / 税费{int(match_metrics.get('settlement_fail_fee_count', 0))}
        🚨 个贷脉冲: WATCH{int(match_metrics.get('mortgage_watch_count', 0))} / DPD30{int(match_metrics.get('mortgage_dpd30_count', 0))} / DPD60{int(match_metrics.get('mortgage_dpd60_count', 0))} / 默认{int(match_metrics.get('mortgage_default_count', 0))}
        🧨 强平与负资产: 强平{int(match_metrics.get('forced_sale_count', 0))} / 负资产{int(match_metrics.get('negative_equity_count', 0))} / NPL {float(match_metrics.get('npl_ratio', 0.0)):.2%}
        🌊 抵押物流动性: A区{float(match_metrics.get('zone_a_liquidity_index', 1.0)):.2f} / B区{float(match_metrics.get('zone_b_liquidity_index', 1.0)):.2f}
        {dev_display}
        【📝 专家点评】
        {llm_analysis_text.strip()}

        【🔔 政策动态】
        {policy_news_str if policy_news_str else "本月无重大政策变动"}
        ━━━━━━━━━━━━━━━━━━━━━━━
        """

        return result_text

    def get_market_trend(self, month):
        cursor = self.conn.cursor()
        cursor.execute("SELECT trend_signal FROM market_bulletin WHERE month = ?", (month,))
        trend_row = cursor.fetchone()
        return trend_row[0] if trend_row else "STABLE"
