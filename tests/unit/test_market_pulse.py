import os
import sqlite3
import tempfile
import unittest

from database import init_db
from models import Agent
from services.mortgage_risk_service import MortgageRiskService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestMarketPulse(unittest.TestCase):
    def _mkdb(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        init_db(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return path, conn

    def test_seed_existing_mortgages_updates_finance(self):
        path, conn = self._mkdb()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO agents_static(agent_id, name, birth_year, marital_status, children_ages, occupation, background_story, investment_style)
                VALUES (1, 'A1', 1990, 'single', '[]', 'engineer', 'bg', 'balanced')
                """
            )
            cur.execute(
                """
                INSERT INTO agents_finance(agent_id, monthly_income, cash, total_assets, total_debt, mortgage_monthly_payment, net_cashflow)
                VALUES (1, 40000, 1000000, 2000000, 0, 0, 40000)
                """
            )
            cur.execute(
                """
                INSERT INTO properties_static(property_id, zone, quality, building_area, property_type, is_school_district, school_tier, price_per_sqm, zone_price_tier, initial_value)
                VALUES (100, 'A', 2, 90, 'residential', 0, 0, 20000, 'standard', 1800000)
                """
            )
            cur.execute(
                """
                INSERT INTO properties_market(property_id, owner_id, status, current_valuation, listed_price, min_price, listing_month)
                VALUES (100, 1, 'off_market', 1800000, NULL, NULL, 0)
                """
            )
            conn.commit()

            cfg = _Cfg(
                {
                    "market_pulse.enabled": True,
                    "market_pulse.seed_existing_mortgage_ratio": 1.0,
                    "market_pulse.seed_ltv_min": 0.6,
                    "market_pulse.seed_ltv_max": 0.6,
                    "market_pulse.seed_loan_age_min_months": 24,
                    "market_pulse.seed_loan_age_max_months": 24,
                    "market_pulse.seed_term_years": 30,
                    "market_pulse.seed_rate_base": 0.045,
                    "market_pulse.seed_rate_jitter": 0.0,
                }
            )
            svc = MortgageRiskService(cfg, conn)
            a = Agent(id=1, name="A1", age=35, marital_status="single", cash=1000000, monthly_income=40000)
            a.owned_properties = [{"property_id": 100, "current_valuation": 1800000, "base_value": 1800000}]
            seeded = svc.seed_existing_mortgages([a], month=0)
            self.assertGreaterEqual(seeded, 1)

            cur.execute("SELECT COUNT(*) FROM mortgage_accounts WHERE agent_id=1")
            self.assertGreaterEqual(int(cur.fetchone()[0]), 1)
            cur.execute("SELECT total_debt, mortgage_monthly_payment FROM agents_finance WHERE agent_id=1")
            debt, mmp = cur.fetchone()
            self.assertGreater(float(debt or 0), 0.0)
            self.assertGreater(float(mmp or 0), 0.0)
        finally:
            conn.close()
            os.remove(path)

    def test_monthly_cycle_triggers_default_and_forced_sale(self):
        path, conn = self._mkdb()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO agents_static(agent_id, name, birth_year, marital_status, children_ages, occupation, background_story, investment_style)
                VALUES (1, 'A1', 1990, 'single', '[]', 'engineer', 'bg', 'balanced')
                """
            )
            cur.execute(
                """
                INSERT INTO agents_finance(agent_id, monthly_income, cash, total_assets, total_debt, mortgage_monthly_payment, net_cashflow)
                VALUES (1, 50000, 1000, 2000000, 0, 0, 50000)
                """
            )
            cur.execute(
                """
                INSERT INTO properties_static(property_id, zone, quality, building_area, property_type, is_school_district, school_tier, price_per_sqm, zone_price_tier, initial_value)
                VALUES (100, 'B', 2, 80, 'residential', 0, 0, 12000, 'budget', 960000)
                """
            )
            cur.execute(
                """
                INSERT INTO properties_market(property_id, owner_id, status, current_valuation, listed_price, min_price, listing_month)
                VALUES (100, 1, 'off_market', 960000, NULL, NULL, 0)
                """
            )
            cur.execute(
                """
                INSERT INTO mortgage_accounts
                (agent_id, property_id, original_loan_amount, remaining_principal, annual_interest_rate,
                 remaining_term_months, monthly_payment, start_month, next_due_month)
                VALUES (1, 100, 700000, 680000, 0.05, 300, 60000, 0, 1)
                """
            )
            conn.commit()

            cfg = _Cfg(
                {
                    "market_pulse.enabled": True,
                    "market_pulse.watch_missed_payments": 1,
                    "market_pulse.dpd30_missed_payments": 2,
                    "market_pulse.dpd60_missed_payments": 3,
                    "market_pulse.default_missed_payments": 4,
                    "market_pulse.forced_sale_discount_ratio": 0.75,
                    "market_pulse.forced_sale_min_ratio": 0.9,
                }
            )
            svc = MortgageRiskService(cfg, conn)
            a = Agent(id=1, name="A1", age=35, marital_status="single", cash=1000, monthly_income=50000)
            a.owned_properties = [{"property_id": 100, "current_valuation": 960000, "base_value": 960000}]
            agent_map = {1: a}
            for m in range(1, 5):
                metrics = svc.process_monthly_cycle(m, agent_map)
            self.assertGreaterEqual(int(metrics.get("default_count", 0)), 1)
            self.assertGreaterEqual(int(metrics.get("forced_sale_count", 0)), 1)

            cur.execute("SELECT status, delinquency_stage FROM mortgage_accounts WHERE agent_id=1 AND property_id=100")
            status, stage = cur.fetchone()
            self.assertEqual(str(status), "defaulted")
            self.assertEqual(str(stage), "DPD90_DEFAULT")

            cur.execute("SELECT status, listed_price, min_price FROM properties_market WHERE property_id=100")
            p_status, listed, min_price = cur.fetchone()
            self.assertEqual(str(p_status), "for_sale")
            self.assertGreater(float(listed or 0), 0.0)
            self.assertGreater(float(min_price or 0), 0.0)
        finally:
            conn.close()
            os.remove(path)


if __name__ == "__main__":
    unittest.main()
