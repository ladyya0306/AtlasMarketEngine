import asyncio
import sqlite3
import unittest
from unittest.mock import patch

from services.transaction_service import TransactionService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}
        self.mortgage = {
            "down_payment_ratio": 0.3,
            "annual_interest_rate": 0.05,
            "loan_term_years": 30,
            "max_dti_ratio": 0.5,
        }

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestPriceAdjustmentRouting(unittest.TestCase):
    def test_rule_fallback_normal_seller_can_follow_observed_heat(self):
        svc = TransactionService(_Cfg(), sqlite3.connect(":memory:"))

        result, metrics = svc._rule_price_adjustment_decision(
            current_price=1_000_000.0,
            listing_duration=2,
            market_trend="STABLE",
            observed_market_trend="BOOM",
            recent_demand_context={
                "band": "HIGH",
                "valid_bids": 2,
                "outbid_losses": 1,
                "negotiation_entries": 1,
                "best_valid_bid": 1_010_000.0,
            },
            decision_profile="normal",
            deadline_months_left=4,
            cfg={},
        )

        self.assertIn(result["action"], {"E", "F"})
        self.assertEqual(metrics["rule_observed_market_trend"], "BOOM")
        self.assertEqual(metrics["rule_decision_profile"], "normal")

    def test_process_listing_price_adjustment_passes_delay_and_profile_to_llm(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                owner_id INTEGER,
                status TEXT,
                listed_price REAL,
                min_price REAL,
                listing_month INTEGER,
                last_price_update_month INTEGER,
                last_price_update_reason TEXT,
                sell_deadline_month INTEGER,
                sell_deadline_total_months INTEGER,
                forced_sale_mode INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE agents_static (
                agent_id INTEGER PRIMARY KEY,
                name TEXT,
                investment_style TEXT,
                purchase_motive_primary TEXT,
                housing_stage TEXT,
                family_stage TEXT,
                education_path TEXT,
                financial_profile TEXT,
                seller_profile TEXT,
                agent_type TEXT,
                info_delay_months INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_static (
                property_id INTEGER PRIMARY KEY,
                zone TEXT,
                is_school_district INTEGER,
                building_area REAL,
                price_per_sqm REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE decision_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id INTEGER,
                month INTEGER,
                event_type TEXT,
                decision TEXT,
                reason TEXT,
                thought_process TEXT,
                context_metrics TEXT,
                llm_called BOOLEAN
            )
            """
        )
        cur.execute("CREATE TABLE market_bulletin (month INTEGER, trend_signal TEXT)")
        cur.execute(
            """
            INSERT INTO properties_market
            (property_id, owner_id, status, listed_price, min_price, listing_month, sell_deadline_month, sell_deadline_total_months, forced_sale_mode)
            VALUES (1, 1, 'for_sale', 1000000.0, 900000.0, 1, NULL, NULL, 0)
            """
        )
        cur.execute(
            """
            INSERT INTO agents_static
            (agent_id, name, investment_style, purchase_motive_primary, housing_stage, family_stage,
             education_path, financial_profile, seller_profile, agent_type, info_delay_months)
            VALUES (1, 'seller_1', 'balanced', '', '', '', '', '', '', 'normal', 1)
            """
        )
        cur.execute(
            """
            INSERT INTO properties_static
            (property_id, zone, is_school_district, building_area, price_per_sqm)
            VALUES (1, 'A', 0, 88.0, 20000.0)
            """
        )
        cur.execute("INSERT INTO market_bulletin(month, trend_signal) VALUES (2, 'UP')")
        conn.commit()

        svc = TransactionService(
            _Cfg(
                {
                    "smart_agent.normal_seller_rule_pricing_enabled": False,
                    "smart_agent.price_adjustment_llm_min_duration": 1,
                    "smart_agent.price_adjustment_monthly_llm_cap": 10,
                    "smart_agent.price_adjustment_llm_min_calls_per_month": 0,
                }
            ),
            conn,
        )

        observed = {}

        async def _fake_adjust(**kwargs):
            observed.update(kwargs)
            return ({"action": "A", "new_price": 1_000_000.0, "reason": "keep"}, {"source": "test"})

        with patch("services.transaction_service.decide_price_adjustment", side_effect=_fake_adjust):
            asyncio.run(svc.process_listing_price_adjustments(month=3, market_trend="DOWN"))

        self.assertEqual(observed["decision_profile"], "normal")
        self.assertEqual(observed["info_delay_months"], 1)
        self.assertEqual(observed["observed_market_trend"], "UP")
        conn.close()

    def test_collect_recent_listing_demand_heat_ignores_exposure_only_fake_heat(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE property_buyer_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                month INTEGER,
                property_id INTEGER,
                buyer_id INTEGER,
                selected_in_shortlist INTEGER,
                is_valid_bid INTEGER,
                proceeded_to_negotiation INTEGER,
                order_id INTEGER,
                buyer_bid REAL,
                failure_reason TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_static (
                property_id INTEGER PRIMARY KEY,
                zone TEXT,
                is_school_district INTEGER,
                building_area REAL,
                price_per_sqm REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE transactions (
                transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                month INTEGER,
                property_id INTEGER
            )
            """
        )
        cur.execute(
            """
            INSERT INTO properties_static
            (property_id, zone, is_school_district, building_area, price_per_sqm)
            VALUES (1, 'A', 0, 98.0, 25000.0)
            """
        )
        cur.executemany(
            """
            INSERT INTO property_buyer_matches
            (month, property_id, buyer_id, selected_in_shortlist, is_valid_bid, proceeded_to_negotiation, order_id, buyer_bid, failure_reason)
            VALUES (?, ?, ?, 0, 0, 0, NULL, NULL, NULL)
            """,
            [(1, 1, i) for i in range(1, 9)],
        )
        conn.commit()

        svc = TransactionService(
            _Cfg(
                {
                    "smart_agent.price_adjustment_demand_lookback_months": 2,
                    "smart_agent.price_adjustment_demand_cfg": {},
                }
            ),
            conn,
        )

        heat = svc._collect_recent_listing_demand_heat(cur, month=3, property_id=1, cfg={})

        self.assertEqual(heat["matches"], 8)
        self.assertEqual(heat["exposure_matches"], 8)
        self.assertEqual(heat["commitment_buyers"], 0)
        self.assertEqual(heat["competition_buyers"], 0)
        self.assertEqual(heat["valid_bids"], 0)
        self.assertEqual(heat["negotiation_entries"], 0)
        self.assertEqual(heat["score"], 0.0)
        self.assertEqual(heat["band"], "LOW")
        conn.close()

    def test_process_listing_price_adjustment_blocks_raise_for_fake_hot_listing(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                owner_id INTEGER,
                status TEXT,
                listed_price REAL,
                min_price REAL,
                listing_month INTEGER,
                last_price_update_month INTEGER,
                last_price_update_reason TEXT,
                sell_deadline_month INTEGER,
                sell_deadline_total_months INTEGER,
                forced_sale_mode INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE agents_static (
                agent_id INTEGER PRIMARY KEY,
                name TEXT,
                investment_style TEXT,
                purchase_motive_primary TEXT,
                housing_stage TEXT,
                family_stage TEXT,
                education_path TEXT,
                financial_profile TEXT,
                seller_profile TEXT,
                agent_type TEXT,
                info_delay_months INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_static (
                property_id INTEGER PRIMARY KEY,
                zone TEXT,
                is_school_district INTEGER,
                building_area REAL,
                price_per_sqm REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE decision_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id INTEGER,
                month INTEGER,
                event_type TEXT,
                decision TEXT,
                reason TEXT,
                thought_process TEXT,
                context_metrics TEXT,
                llm_called BOOLEAN
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE property_buyer_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                month INTEGER,
                property_id INTEGER,
                buyer_id INTEGER,
                selected_in_shortlist INTEGER,
                is_valid_bid INTEGER,
                proceeded_to_negotiation INTEGER,
                order_id INTEGER,
                buyer_bid REAL,
                failure_reason TEXT
            )
            """
        )
        cur.execute("CREATE TABLE transactions (transaction_id INTEGER PRIMARY KEY AUTOINCREMENT, month INTEGER, property_id INTEGER)")
        cur.execute("CREATE TABLE market_bulletin (month INTEGER, trend_signal TEXT)")
        cur.execute(
            """
            INSERT INTO properties_market
            (property_id, owner_id, status, listed_price, min_price, listing_month, sell_deadline_month, sell_deadline_total_months, forced_sale_mode)
            VALUES (1, 1, 'for_sale', 1000000.0, 900000.0, 1, NULL, NULL, 0)
            """
        )
        cur.execute(
            """
            INSERT INTO agents_static
            (agent_id, name, investment_style, purchase_motive_primary, housing_stage, family_stage,
             education_path, financial_profile, seller_profile, agent_type, info_delay_months)
            VALUES (1, 'seller_1', 'balanced', '', '', '', '', '', '', 'smart', 0)
            """
        )
        cur.execute(
            """
            INSERT INTO properties_static
            (property_id, zone, is_school_district, building_area, price_per_sqm)
            VALUES (1, 'A', 0, 98.0, 25000.0)
            """
        )
        cur.executemany(
            """
            INSERT INTO property_buyer_matches
            (month, property_id, buyer_id, selected_in_shortlist, is_valid_bid, proceeded_to_negotiation, order_id, buyer_bid, failure_reason)
            VALUES (?, ?, ?, 0, 0, 0, NULL, NULL, NULL)
            """,
            [(1, 1, i) for i in range(1, 13)],
        )
        conn.commit()

        svc = TransactionService(
            _Cfg(
                {
                    "smart_agent.normal_seller_rule_pricing_enabled": False,
                    "smart_agent.price_adjustment_llm_min_duration": 1,
                    "smart_agent.price_adjustment_monthly_llm_cap": 10,
                    "smart_agent.price_adjustment_llm_min_calls_per_month": 0,
                    "smart_agent.fake_hot_block_raise_enabled": True,
                }
            ),
            conn,
        )

        async def _fake_adjust(**kwargs):
            return ({"action": "E", "new_price": 1_050_000.0, "coefficient": 1.05, "reason": "hot listing raise"}, {"recent_demand_context": kwargs.get("recent_demand_context", {})})

        with patch("services.transaction_service.decide_price_adjustment", side_effect=_fake_adjust):
            asyncio.run(svc.process_listing_price_adjustments(month=3, market_trend="UP"))

        listed_price = cur.execute("SELECT listed_price FROM properties_market WHERE property_id=1").fetchone()[0]
        self.assertEqual(float(listed_price), 1_000_000.0)
        conn.close()


if __name__ == "__main__":
    unittest.main()
