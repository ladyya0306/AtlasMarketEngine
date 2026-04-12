import json
import logging
import random
import sqlite3
from typing import Dict, List, Optional

from mortgage_system import calculate_monthly_payment

logger = logging.getLogger(__name__)


class MortgageRiskService:
    """
    Market Pulse engine:
    - seed existing mortgages for owner agents
    - process monthly repayment/delinquency/default cycle
    - trigger forced-sale listing for defaulted collateral
    """

    def __init__(self, config, db_conn: sqlite3.Connection):
        self.config = config
        self.conn = db_conn

    def _enabled(self) -> bool:
        raw = self.config.get("market_pulse.enabled", False)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _cfg_float(self, key: str, default: float) -> float:
        try:
            return float(self.config.get(f"market_pulse.{key}", default))
        except Exception:
            return float(default)

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            return int(self.config.get(f"market_pulse.{key}", default))
        except Exception:
            return int(default)

    def _append_pulse_log(self, month: int, agent_id: int, decision: str, reason: str, metrics: Dict):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO decision_logs
                (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
                VALUES (?, ?, 'MARKET_PULSE', ?, ?, ?, ?, 0)
                """,
                (
                    int(agent_id),
                    int(month),
                    str(decision),
                    str(reason),
                    None,
                    json.dumps(metrics or {}, ensure_ascii=False),
                ),
            )
        except Exception as e:
            logger.debug(f"failed to append MARKET_PULSE log: {e}")

    def seed_existing_mortgages(self, agents: List, month: int = 0) -> int:
        """
        Seed legacy stock mortgages for existing owner agents.
        """
        if not self._enabled():
            return 0

        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM mortgage_accounts")
        existing = int((cursor.fetchone() or [0])[0] or 0)
        if existing > 0:
            return 0

        seed_ratio = max(0.0, min(1.0, self._cfg_float("seed_existing_mortgage_ratio", 0.55)))
        min_ltv = max(0.1, min(0.95, self._cfg_float("seed_ltv_min", 0.35)))
        max_ltv = max(min_ltv, min(0.98, self._cfg_float("seed_ltv_max", 0.78)))
        base_rate = max(0.0, self._cfg_float("seed_rate_base", 0.045))
        rate_jitter = max(0.0, self._cfg_float("seed_rate_jitter", 0.008))
        term_years = max(10, self._cfg_int("seed_term_years", 30))
        min_age_months = max(0, self._cfg_int("seed_loan_age_min_months", 12))
        max_age_months = max(min_age_months, self._cfg_int("seed_loan_age_max_months", 180))

        rows_to_insert = []
        for agent in agents:
            aid = int(getattr(agent, "id", -1))
            if aid <= 0:
                continue
            props = list(getattr(agent, "owned_properties", []) or [])
            if not props:
                continue
            for p in props:
                if random.random() > seed_ratio:
                    continue
                pid = int(p.get("property_id", -1))
                if pid <= 0:
                    continue
                value = float(
                    p.get("current_valuation")
                    or p.get("base_value")
                    or p.get("initial_value")
                    or 0.0
                )
                if value <= 0:
                    continue
                ltv = random.uniform(min_ltv, max_ltv)
                original_loan = max(1.0, value * ltv)
                age_m = random.randint(min_age_months, max_age_months)
                total_term_m = term_years * 12
                rem_term_m = max(12, total_term_m - age_m)
                annual_rate = max(0.0001, base_rate + random.uniform(-rate_jitter, rate_jitter))
                monthly_pay = calculate_monthly_payment(original_loan, annual_rate, max(1, rem_term_m // 12))
                # Recalculate for month-level term accuracy when term not divisible by 12.
                if rem_term_m % 12 != 0:
                    monthly_rate = annual_rate / 12.0
                    factor = (1 + monthly_rate) ** rem_term_m
                    monthly_pay = original_loan * (monthly_rate * factor) / max(1e-8, (factor - 1))
                remaining_principal = max(1.0, original_loan * (1.0 - min(0.95, age_m / float(total_term_m + 1))))
                rows_to_insert.append(
                    (
                        aid,
                        pid,
                        original_loan,
                        remaining_principal,
                        annual_rate,
                        rem_term_m,
                        float(monthly_pay),
                        int(month),
                        int(month + 1),
                    )
                )

        if not rows_to_insert:
            return 0

        cursor.executemany(
            """
            INSERT INTO mortgage_accounts
            (agent_id, property_id, original_loan_amount, remaining_principal,
             annual_interest_rate, remaining_term_months, monthly_payment,
             start_month, next_due_month)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        self.sync_agent_finance_from_mortgages()
        self.conn.commit()
        logger.info(f"Market Pulse: seeded {len(rows_to_insert)} mortgage accounts.")
        return len(rows_to_insert)

    def register_new_mortgage(
        self,
        agent_id: int,
        property_id: int,
        loan_amount: float,
        annual_rate: float,
        remaining_term_years: int,
        monthly_payment: float,
        month: int,
    ) -> None:
        if not self._enabled():
            return
        if float(loan_amount or 0.0) <= 0:
            return
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO mortgage_accounts
            (agent_id, property_id, original_loan_amount, remaining_principal,
             annual_interest_rate, remaining_term_months, monthly_payment,
             start_month, next_due_month)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(agent_id),
                int(property_id),
                float(loan_amount),
                float(loan_amount),
                float(annual_rate),
                max(1, int(remaining_term_years) * 12),
                float(monthly_payment),
                int(month),
                int(month + 1),
            ),
        )
        self.sync_agent_finance_from_mortgages(agent_ids=[int(agent_id)])

    def close_seller_mortgage_by_property(self, seller_id: int, property_id: int, month: int):
        if not self._enabled():
            return
        if seller_id is None or int(seller_id) <= 0:
            return
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE mortgage_accounts
            SET status='closed', is_active=0, delinquency_stage='CLOSED', updated_at=CURRENT_TIMESTAMP
            WHERE agent_id=? AND property_id=? AND status='active'
            """,
            (int(seller_id), int(property_id)),
        )
        if cursor.rowcount > 0:
            self._append_pulse_log(
                month=month,
                agent_id=int(seller_id),
                decision="MORTGAGE_CLOSED",
                reason="Property sold, close linked mortgage account",
                metrics={"property_id": int(property_id), "closed_count": int(cursor.rowcount)},
            )
        self.sync_agent_finance_from_mortgages(agent_ids=[int(seller_id)])

    def sync_agent_finance_from_mortgages(self, agent_ids: Optional[List[int]] = None) -> None:
        """
        Keep agents_finance aggregates aligned with mortgage_accounts source of truth.
        """
        cursor = self.conn.cursor()
        where_clause = ""
        params: List = []
        if agent_ids:
            valid_ids = [int(x) for x in agent_ids if int(x) > 0]
            if not valid_ids:
                return
            placeholders = ",".join("?" for _ in valid_ids)
            where_clause = f"WHERE agent_id IN ({placeholders})"
            params.extend(valid_ids)

        cursor.execute(
            f"""
            SELECT agent_id,
                   COALESCE(SUM(CASE WHEN status='active' THEN remaining_principal ELSE 0 END), 0) AS debt_active,
                   COALESCE(SUM(CASE WHEN status='active' THEN monthly_payment ELSE 0 END), 0) AS pay_active
            FROM mortgage_accounts
            {where_clause}
            GROUP BY agent_id
            """,
            tuple(params),
        )
        agg = {int(r[0]): (float(r[1] or 0.0), float(r[2] or 0.0)) for r in cursor.fetchall() or []}

        if agent_ids:
            target_ids = [int(x) for x in agent_ids if int(x) > 0]
        else:
            cursor.execute("SELECT agent_id FROM agents_finance WHERE agent_id > 0")
            target_ids = [int(r[0]) for r in cursor.fetchall() or []]

        updates = []
        for aid in target_ids:
            debt, pay = agg.get(aid, (0.0, 0.0))
            updates.append((debt, pay, aid))
        if updates:
            cursor.executemany(
                """
                UPDATE agents_finance
                SET total_debt=?, mortgage_monthly_payment=?
                WHERE agent_id=?
                """,
                updates,
            )

    def process_monthly_cycle(self, month: int, agent_map: Dict[int, object], market_properties: Optional[List[Dict]] = None) -> Dict[str, float]:
        """
        Run monthly mortgage stress cycle and forced-sale trigger.
        """
        if not self._enabled():
            return {
                "watch_count": 0,
                "dpd30_count": 0,
                "dpd60_count": 0,
                "default_count": 0,
                "forced_sale_count": 0,
            }

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT mortgage_id, agent_id, property_id, remaining_principal, annual_interest_rate,
                   remaining_term_months, monthly_payment, missed_payments, status
            FROM mortgage_accounts
            WHERE is_active=1
            """
        )
        rows = cursor.fetchall() or []
        if not rows:
            return {
                "watch_count": 0,
                "dpd30_count": 0,
                "dpd60_count": 0,
                "default_count": 0,
                "forced_sale_count": 0,
            }

        forced_sale_discount = max(0.3, min(0.98, self._cfg_float("forced_sale_discount_ratio", 0.78)))
        forced_min_ratio = max(0.5, min(1.0, self._cfg_float("forced_sale_min_ratio", 0.92)))
        stage_watch = self._cfg_int("watch_missed_payments", 1)
        stage_dpd30 = self._cfg_int("dpd30_missed_payments", 2)
        stage_dpd60 = self._cfg_int("dpd60_missed_payments", 3)
        stage_default = self._cfg_int("default_missed_payments", 4)

        metrics = {"watch_count": 0, "dpd30_count": 0, "dpd60_count": 0, "default_count": 0, "forced_sale_count": 0}

        for row in rows:
            mid = int(row[0])
            aid = int(row[1])
            pid = int(row[2])
            remaining = float(row[3] or 0.0)
            annual_rate = float(row[4] or 0.0)
            rem_term = max(0, int(row[5] or 0))
            monthly_payment = max(0.0, float(row[6] or 0.0))
            missed = max(0, int(row[7] or 0))
            status = str(row[8] or "active").lower()
            if status != "active":
                continue

            agent = agent_map.get(aid)
            if not agent:
                continue

            interest = remaining * max(0.0, annual_rate / 12.0)
            paid = False
            if agent.cash >= monthly_payment and monthly_payment > 0:
                agent.cash -= monthly_payment
                principal_paid = max(0.0, monthly_payment - interest)
                remaining = max(0.0, remaining - principal_paid)
                rem_term = max(0, rem_term - 1)
                missed = 0
                paid = True
            else:
                missed += 1
                remaining = max(0.0, remaining + interest)

            stage = "NORMAL"
            if missed >= stage_default:
                stage = "DPD90_DEFAULT"
            elif missed >= stage_dpd60:
                stage = "DPD60"
            elif missed >= stage_dpd30:
                stage = "DPD30"
            elif missed >= stage_watch:
                stage = "WATCH"

            new_status = "active"
            is_active = 1
            if remaining <= 1e-6 or rem_term <= 0:
                stage = "CLOSED"
                new_status = "closed"
                is_active = 0
            elif stage == "DPD90_DEFAULT":
                new_status = "defaulted"
                is_active = 0
                metrics["default_count"] += 1
                self._append_pulse_log(
                    month=month,
                    agent_id=aid,
                    decision="DEFAULT_TRIGGERED",
                    reason="Mortgage account moved to DPD90 default",
                    metrics={"mortgage_id": mid, "property_id": pid, "missed_payments": missed, "remaining_principal": remaining},
                )
                self._trigger_forced_sale(cursor, month, aid, pid, forced_sale_discount, forced_min_ratio, market_properties)
                metrics["forced_sale_count"] += 1
            elif stage == "DPD60":
                metrics["dpd60_count"] += 1
            elif stage == "DPD30":
                metrics["dpd30_count"] += 1
            elif stage == "WATCH":
                metrics["watch_count"] += 1

            cursor.execute(
                """
                UPDATE mortgage_accounts
                SET remaining_principal=?, remaining_term_months=?, missed_payments=?, days_past_due=?,
                    delinquency_stage=?, status=?, is_active=?, last_payment_month=?, next_due_month=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE mortgage_id=?
                """,
                (
                    float(remaining),
                    int(rem_term),
                    int(missed),
                    int(missed * 30),
                    stage,
                    new_status,
                    int(is_active),
                    int(month) if paid else None,
                    int(month + 1),
                    mid,
                ),
            )

        self.sync_agent_finance_from_mortgages(agent_ids=list(agent_map.keys()))
        for aid, agent in agent_map.items():
            if aid <= 0:
                continue
            cursor.execute(
                "SELECT total_debt, mortgage_monthly_payment FROM agents_finance WHERE agent_id=?",
                (int(aid),),
            )
            row = cursor.fetchone()
            if row:
                agent.total_debt = float(row[0] or 0.0)
                agent.mortgage_monthly_payment = float(row[1] or 0.0)
                agent.net_cashflow = float(agent.monthly_income) - float(agent.mortgage_monthly_payment)
                agent.total_assets = float(agent.net_worth)
                cursor.execute(
                    "UPDATE agents_finance SET cash=?, net_cashflow=?, total_assets=? WHERE agent_id=?",
                    (float(agent.cash), float(agent.net_cashflow), float(agent.total_assets), int(aid)),
                )
        self.conn.commit()
        return metrics

    def _trigger_forced_sale(
        self,
        cursor,
        month: int,
        agent_id: int,
        property_id: int,
        forced_sale_discount: float,
        forced_min_ratio: float,
        market_properties: Optional[List[Dict]] = None,
    ):
        cursor.execute(
            """
            SELECT owner_id, status, current_valuation
            FROM properties_market
            WHERE property_id=?
            """,
            (int(property_id),),
        )
        row = cursor.fetchone()
        if not row:
            return
        owner_id = int(row[0]) if row[0] is not None else -1
        if owner_id != int(agent_id):
            return
        status = str(row[1] or "")
        valuation = float(row[2] or 0.0)
        if valuation <= 0:
            valuation = 500000.0
        listed = max(1.0, valuation * forced_sale_discount)
        min_price = max(1.0, listed * forced_min_ratio)

        cursor.execute(
            """
            UPDATE properties_market
            SET status='for_sale', listed_price=?, min_price=?, listing_month=?, last_price_update_month=?,
                last_price_update_reason='Market Pulse forced sale'
            WHERE property_id=?
            """,
            (listed, min_price, int(month), int(month), int(property_id)),
        )
        if market_properties is not None:
            target = next((p for p in market_properties if int(p.get("property_id", -1)) == int(property_id)), None)
            if target:
                target["status"] = "for_sale"
                target["listed_price"] = listed
                target["min_price"] = min_price
        self._append_pulse_log(
            month=month,
            agent_id=agent_id,
            decision="FORCED_SALE",
            reason="Defaulted mortgage collateral listed by Market Pulse",
            metrics={
                "property_id": int(property_id),
                "listed_price": float(listed),
                "min_price": float(min_price),
                "forced_sale_discount": float(forced_sale_discount),
            },
        )

