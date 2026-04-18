import sqlite3
import unittest
from types import SimpleNamespace

from services.transaction_service import TransactionService


class _Cfg:
    def __init__(self, data):
        self._data = data
        self.mortgage = {
            "down_payment_ratio": 0.3,
            "annual_interest_rate": 0.05,
            "loan_term_years": 30,
            "max_dti_ratio": 0.5,
        }

    def get(self, key, default=None):
        return self._data.get(key, default)


class _Agent(SimpleNamespace):
    def to_v2_finance_dict(self):
        return {
            "mortgage_monthly_payment": float(getattr(self, "mortgage_monthly_payment", 0.0)),
            "cash": float(getattr(self, "cash", 0.0)),
            "total_assets": float(getattr(self, "total_assets", 0.0)),
            "total_debt": float(getattr(self, "total_debt", 0.0)),
            "net_cashflow": float(getattr(self, "monthly_income", 0.0))
            - float(getattr(self, "mortgage_monthly_payment", 0.0)),
        }


class TestSellerDeadlineForcedClear(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                owner_id INTEGER,
                status TEXT,
                current_valuation REAL,
                listed_price REAL,
                min_price REAL,
                sell_deadline_month INTEGER,
                sell_deadline_total_months INTEGER,
                forced_sale_mode INTEGER,
                last_transaction_month INTEGER,
                last_price_update_month INTEGER,
                last_price_update_reason TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE transaction_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                buyer_id INTEGER,
                property_id INTEGER,
                status TEXT,
                deposit_amount REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE transactions (
                transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                month INTEGER,
                order_id INTEGER,
                buyer_id INTEGER,
                seller_id INTEGER,
                property_id INTEGER,
                final_price REAL,
                down_payment REAL,
                loan_amount REAL,
                buyer_transaction_cost REAL,
                seller_transaction_cost REAL,
                negotiation_rounds INTEGER
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
            CREATE TABLE agents_finance (
                agent_id INTEGER PRIMARY KEY,
                mortgage_monthly_payment REAL,
                cash REAL,
                total_assets REAL,
                total_debt REAL,
                net_cashflow REAL
            )
            """
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_deadline_force_clear_uses_real_buyer_and_penalty(self):
        cfg = _Cfg(
            {
                "smart_agent.seller_deadline_enabled": True,
                "smart_agent.seller_deadline_hard_clear_enabled": True,
                "smart_agent.seller_deadline_force_price_ratio": 0.88,
                "smart_agent.seller_deadline_force_floor_ratio": 0.60,
                "smart_agent.seller_deadline_hard_floor_ratio": 0.01,
                "smart_agent.seller_deadline_force_step_ratio": 0.05,
                "smart_agent.seller_deadline_penalty_ratio": 0.08,
                "smart_agent.seller_deadline_terminal_zero_price_enabled": True,
                "transaction_costs.buyer.brokerage_ratio": 0.01,
                "transaction_costs.buyer.tax_ratio": 0.01,
                "transaction_costs.buyer.misc_ratio": 0.005,
                "transaction_costs.seller.brokerage_ratio": 0.01,
                "transaction_costs.seller.tax_ratio": 0.005,
                "transaction_costs.seller.misc_ratio": 0.002,
            }
        )
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()

        seller = _Agent(
            id=1,
            name="卖家1",
            cash=100000.0,
            monthly_income=0.0,
            mortgage_monthly_payment=0.0,
            total_assets=1000000.0,
            total_debt=0.0,
            owned_properties=[{"property_id": 11, "owner_id": 1, "status": "for_sale"}],
        )
        # Buyer intentionally low-cash so normal force-cut windows fail; terminal clear should still complete.
        buyer = _Agent(
            id=2,
            name="买家2",
            cash=0.0,
            monthly_income=12000.0,
            mortgage_monthly_payment=0.0,
            total_assets=200000.0,
            total_debt=0.0,
            owned_properties=[],
        )

        cur.execute(
            """
            INSERT INTO properties_market(
                property_id, owner_id, status, current_valuation, listed_price, min_price,
                sell_deadline_month, sell_deadline_total_months, forced_sale_mode
            ) VALUES (?, ?, 'for_sale', ?, ?, ?, ?, ?, 1)
            """,
            (11, 1, 820000.0, 800000.0, 760000.0, 2, 2),
        )
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (1, 0.0, seller.cash, seller.total_assets, seller.total_debt, seller.monthly_income),
        )
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (2, 0.0, buyer.cash, buyer.total_assets, buyer.total_debt, buyer.monthly_income),
        )
        self.conn.commit()

        props_map = {
            11: {
                "property_id": 11,
                "owner_id": 1,
                "status": "for_sale",
                "zone": "B",
                "building_area": 88.0,
                "is_school_district": False,
                "property_type": "apartment",
                "base_value": 820000.0,
            }
        }
        agent_map = {1: seller, 2: buyer}

        done = svc._force_sell_due_listings(
            cursor=cur,
            month=2,
            buyers=[buyer],
            agent_map=agent_map,
            props_map=props_map,
            market=None,
        )
        svc._flush_decision_log_buffer(cur)
        self.conn.commit()
        self.assertEqual(done, 1)

        cur.execute("SELECT owner_id, status FROM properties_market WHERE property_id=11")
        owner_id, status = cur.fetchone()
        self.assertEqual(owner_id, 2)
        self.assertEqual(status, "off_market")

        cur.execute("SELECT buyer_id, seller_id, property_id, final_price FROM transactions")
        tx = cur.fetchone()
        self.assertIsNotNone(tx)
        self.assertEqual(tx[0], 2)  # real buyer id, not virtual buyback
        self.assertEqual(tx[1], 1)
        self.assertEqual(tx[2], 11)
        self.assertGreaterEqual(float(tx[3]), 0.0)

        # penalty is based on listed price, so seller cash must be reduced.
        self.assertLess(float(seller.cash), 100000.0)

        cur.execute(
            """
            SELECT context_metrics
            FROM decision_logs
            WHERE event_type='SELLER_DEADLINE' AND decision='FORCED_SALE'
            ORDER BY log_id DESC LIMIT 1
            """
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        import json

        ctx = json.loads(row[0] or "{}")
        self.assertEqual(ctx.get("force_sell_mode"), "terminal_zero_price_clear")

    def test_deadline_force_clear_skips_buyers_with_pending_settlement(self):
        cfg = _Cfg(
            {
                "smart_agent.seller_deadline_enabled": True,
                "smart_agent.seller_deadline_hard_clear_enabled": True,
                "smart_agent.seller_deadline_force_price_ratio": 0.88,
                "smart_agent.seller_deadline_force_floor_ratio": 0.60,
                "smart_agent.seller_deadline_hard_floor_ratio": 0.01,
                "smart_agent.seller_deadline_force_step_ratio": 0.05,
                "smart_agent.seller_deadline_penalty_ratio": 0.08,
                "smart_agent.seller_deadline_terminal_zero_price_enabled": True,
                "transaction_costs.buyer.brokerage_ratio": 0.01,
                "transaction_costs.buyer.tax_ratio": 0.01,
                "transaction_costs.buyer.misc_ratio": 0.005,
                "transaction_costs.seller.brokerage_ratio": 0.01,
                "transaction_costs.seller.tax_ratio": 0.005,
                "transaction_costs.seller.misc_ratio": 0.002,
            }
        )
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()

        seller = _Agent(
            id=1,
            name="卖家1",
            cash=100000.0,
            monthly_income=0.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_000_000.0,
            total_debt=0.0,
            owned_properties=[{"property_id": 11, "owner_id": 1, "status": "for_sale"}],
        )
        blocked_buyer = _Agent(
            id=2,
            name="待交割买家",
            cash=3_000_000.0,
            monthly_income=30_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=5_000_000.0,
            total_debt=0.0,
            owned_properties=[],
        )
        fallback_buyer = _Agent(
            id=3,
            name="可选买家",
            cash=3_000_000.0,
            monthly_income=30_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=5_000_000.0,
            total_debt=0.0,
            owned_properties=[],
        )

        cur.execute(
            """
            INSERT INTO properties_market(
                property_id, owner_id, status, current_valuation, listed_price, min_price,
                sell_deadline_month, sell_deadline_total_months, forced_sale_mode
            ) VALUES (?, ?, 'for_sale', ?, ?, ?, ?, ?, 1)
            """,
            (11, 1, 820000.0, 800000.0, 760000.0, 2, 2),
        )
        cur.execute(
            "INSERT INTO transaction_orders(order_id, buyer_id, property_id, status, deposit_amount) VALUES (?, ?, ?, ?, ?)",
            (501, 2, 99, "pending_settlement", 100_000.0),
        )
        for agent in (seller, blocked_buyer, fallback_buyer):
            cur.execute(
                "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    agent.id,
                    float(agent.mortgage_monthly_payment),
                    float(agent.cash),
                    float(agent.total_assets),
                    float(agent.total_debt),
                    float(agent.monthly_income) - float(agent.mortgage_monthly_payment),
                ),
            )
        self.conn.commit()

        props_map = {
            11: {
                "property_id": 11,
                "owner_id": 1,
                "status": "for_sale",
                "zone": "B",
                "building_area": 88.0,
                "is_school_district": False,
                "property_type": "apartment",
                "base_value": 820000.0,
            }
        }
        agent_map = {1: seller, 2: blocked_buyer, 3: fallback_buyer}

        done = svc._force_sell_due_listings(
            cursor=cur,
            month=2,
            buyers=[blocked_buyer, fallback_buyer],
            agent_map=agent_map,
            props_map=props_map,
            market=None,
        )
        self.assertEqual(done, 1)

        cur.execute(
            "SELECT buyer_id, seller_id, property_id FROM transactions ORDER BY transaction_id DESC LIMIT 1"
        )
        tx = cur.fetchone()
        self.assertIsNotNone(tx)
        self.assertEqual(tx[0], 3)
        self.assertEqual(tx[1], 1)
        self.assertEqual(tx[2], 11)


if __name__ == "__main__":
    unittest.main()
