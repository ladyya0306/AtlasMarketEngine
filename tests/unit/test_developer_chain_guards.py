import asyncio
import json
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from services.transaction_service import TransactionService
from transaction_engine import developer_quick_sale, generate_seller_listing


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


class TestDeveloperChainGuards(unittest.TestCase):
    def test_developer_quick_sale_uses_affordability_with_fees(self):
        cfg = _Cfg(
            {
                "transaction_costs.buyer.brokerage_ratio": 0.01,
                "transaction_costs.buyer.tax_ratio": 0.007,
                "transaction_costs.buyer.misc_ratio": 0.003,
            }
        )
        buyer = SimpleNamespace(
            id=101,
            cash=360_000.0,
            monthly_income=30_000.0,
            mortgage_monthly_payment=0.0,
            total_debt=0.0,
            # intentionally omit total_assets to verify fallback path in check_affordability
            preference=SimpleNamespace(max_price=1_200_000.0),
        )
        listing = {"listed_price": 1_000_000.0, "min_price": 900_000.0}

        # fee-aware check should fail: down payment(300k) + fees(20k) > cash(360k)? false (320k <= 360k)
        ok = developer_quick_sale(buyer, listing, cfg, offered_price=1_000_000.0)
        self.assertEqual(ok["outcome"], "success")

        buyer_low_cash = SimpleNamespace(
            id=102,
            cash=310_000.0,
            monthly_income=30_000.0,
            mortgage_monthly_payment=0.0,
            total_debt=0.0,
            preference=SimpleNamespace(max_price=1_200_000.0),
        )
        failed = developer_quick_sale(buyer_low_cash, listing, cfg, offered_price=1_000_000.0)
        self.assertEqual(failed["outcome"], "failed")

    def test_listing_adjustment_skips_developer_and_keeps_min_coupled(self):
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
                last_price_update_reason TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE agents_static (
                agent_id INTEGER PRIMARY KEY,
                name TEXT,
                investment_style TEXT
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
        # normal seller listing
        cur.execute(
            """
            INSERT INTO properties_market
            (property_id, owner_id, status, listed_price, min_price, listing_month, last_price_update_month, last_price_update_reason)
            VALUES (1, 1, 'for_sale', 1000.0, 900.0, 1, NULL, NULL)
            """
        )
        # developer listing should not be adjusted by auto-price logic
        cur.execute(
            """
            INSERT INTO properties_market
            (property_id, owner_id, status, listed_price, min_price, listing_month, last_price_update_month, last_price_update_reason)
            VALUES (2, -1, 'for_sale', 800.0, 700.0, 1, NULL, NULL)
            """
        )
        cur.execute("INSERT INTO agents_static(agent_id, name, investment_style) VALUES (1, 'seller_1', 'balanced')")
        conn.commit()

        svc = TransactionService(_Cfg(), conn)

        async def _fake_adjust(**kwargs):
            return ({"action": "B", "new_price": 800.0, "reason": "test cut"}, {"x": 1})

        with patch("services.transaction_service.decide_price_adjustment", side_effect=_fake_adjust):
            asyncio.run(svc.process_listing_price_adjustments(month=3, market_trend="DOWN"))

        # seller listing updated and min_price stays <= listed_price
        row = conn.execute("SELECT listed_price, min_price FROM properties_market WHERE property_id=1").fetchone()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row[0], 800.0, places=2)
        self.assertAlmostEqual(row[1], 704.0, places=2)
        self.assertLessEqual(row[1], row[0])

        # developer listing unchanged
        dev = conn.execute("SELECT listed_price, min_price FROM properties_market WHERE property_id=2").fetchone()
        self.assertEqual(dev[0], 800.0)
        self.assertEqual(dev[1], 700.0)
        log = conn.execute(
            "SELECT decision, reason, context_metrics FROM decision_logs WHERE event_type='PRICE_ADJUSTMENT'"
        ).fetchone()
        self.assertIsNotNone(log)
        self.assertEqual(log[0], "B")
        metrics = json.loads(log[2])
        self.assertEqual(metrics["seller_persona_snapshot"]["purchase_motive_primary"], "")
        self.assertEqual(metrics["property_snapshot"]["property_id"], 1)
        self.assertEqual(metrics["property_snapshot"]["zone"], "")
        self.assertIn("action:B", metrics["selection_reason_tags"])
        self.assertIn("trend:down", metrics["selection_reason_tags"])
        conn.close()

    def test_generate_seller_listing_normalizes_min_price_not_above_listed(self):
        seller = SimpleNamespace(
            id=1,
            story=SimpleNamespace(background_story="test", selling_motivation="test"),
        )

        class _Market:
            def get_avg_price(self, zone):
                return 2_000_000.0

        property_data = {"property_id": 101, "zone": "A", "building_area": 80, "base_value": 2_000_000.0}

        with patch("transaction_engine.safe_call_llm", return_value={"listed_price": 1_800_000.0, "min_price": 1_900_000.0, "urgency": 0.6}):
            listing = generate_seller_listing(seller, property_data, _Market(), strategy_hint="balanced", pricing_coefficient=None)

        self.assertAlmostEqual(float(listing["listed_price"]), 1_800_000.0, places=2)
        self.assertAlmostEqual(float(listing["min_price"]), 1_800_000.0, places=2)
        self.assertLessEqual(float(listing["min_price"]), float(listing["listed_price"]))

    def test_repair_listing_price_invariants_for_resume_db(self):
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
                last_price_update_reason TEXT
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO properties_market
            (property_id, owner_id, status, listed_price, min_price, listing_month)
            VALUES (?, ?, 'for_sale', ?, ?, 1)
            """,
            [
                (1, -1, 500000.0, 800000.0),  # inverted
                (2, 1, 0.0, 300000.0),        # invalid listed
                (3, 2, 900000.0, 0.0),        # invalid min
                (4, 2, 950000.0, 900000.0),   # already valid
            ],
        )
        conn.commit()

        svc = TransactionService(_Cfg(), conn)
        svc._append_order_log = lambda *args, **kwargs: None
        repaired = svc._repair_listing_price_invariants(conn.cursor(), month=6)
        conn.commit()

        self.assertEqual(repaired, 3)
        rows = conn.execute(
            "SELECT property_id, listed_price, min_price FROM properties_market ORDER BY property_id"
        ).fetchall()
        for _, listed, minimum in rows:
            self.assertGreater(listed, 0.0)
            self.assertGreater(minimum, 0.0)
            self.assertLessEqual(minimum, listed)
        conn.close()


if __name__ == "__main__":
    unittest.main()
