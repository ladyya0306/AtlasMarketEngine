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


class _Buyer(SimpleNamespace):
    def to_v2_finance_dict(self):
        return {
            "mortgage_monthly_payment": float(getattr(self, "mortgage_monthly_payment", 0.0)),
            "cash": float(getattr(self, "cash", 0.0)),
            "total_assets": float(getattr(self, "total_assets", 0.0)),
            "total_debt": float(getattr(self, "total_debt", 0.0)),
            "net_cashflow": float(getattr(self, "monthly_income", 0.0)) - float(getattr(self, "mortgage_monthly_payment", 0.0)),
        }


class TestOrderFundsConsistency(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        cur = self.conn.cursor()
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
            CREATE TABLE transaction_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_month INTEGER,
                expires_month INTEGER,
                settlement_due_month INTEGER,
                buyer_id INTEGER,
                seller_id INTEGER,
                property_id INTEGER,
                offer_price REAL,
                agreed_price REAL,
                negotiation_rounds INTEGER,
                deposit_amount REAL,
                penalty_amount REAL,
                status TEXT,
                close_month INTEGER,
                close_reason TEXT,
                agent_type TEXT,
                updated_at TIMESTAMP
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

    def test_cancel_and_breach_release_logic(self):
        cfg = _Cfg({"smart_agent.deposit_ratio": 0.1, "smart_agent.precheck_include_tax_and_fee": False})
        svc = TransactionService(cfg, self.conn)
        buyer = _Buyer(
            id=1,
            cash=1_000_000.0,
            monthly_income=60_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=2_000_000.0,
            total_debt=0.0,
            agent_type="smart",
        )
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (buyer.id, 0.0, buyer.cash, buyer.total_assets, buyer.total_debt, buyer.monthly_income),
        )
        self.conn.commit()

        matches = [{"buyer": buyer, "listing": {"property_id": 11, "listed_price": 800_000.0, "min_price": 760_000.0, "seller_id": 2}}]
        entries = svc._create_orders_for_matches(cur, month=1, buyer_matches=matches, agent_map={buyer.id: buyer})
        self.conn.commit()
        self.assertEqual(len(entries), 1)
        deposit = float(entries[0]["deposit_amount"])
        self.assertAlmostEqual(buyer.cash, 1_000_000.0 - deposit, places=2)

        svc._close_order(
            cur,
            month=1,
            order_id=int(entries[0]["order_id"]),
            buyer=buyer,
            status="cancelled",
            close_reason="Outbid",
            release_amount=deposit,
            penalty_amount=0.0,
        )
        self.conn.commit()
        self.assertAlmostEqual(buyer.cash, 1_000_000.0, places=2)

        # Create second order and apply breach penalty.
        entries2 = svc._create_orders_for_matches(cur, month=2, buyer_matches=matches, agent_map={buyer.id: buyer})
        self.conn.commit()
        self.assertEqual(len(entries2), 1)
        deposit2 = float(entries2[0]["deposit_amount"])
        penalty = round(deposit2 * 0.05, 2)
        release = deposit2 - penalty
        svc._close_order(
            cur,
            month=2,
            order_id=int(entries2[0]["order_id"]),
            buyer=buyer,
            status="breached",
            close_reason="Settlement failed: DTI exceeded",
            release_amount=release,
            penalty_amount=penalty,
        )
        self.conn.commit()
        self.assertAlmostEqual(buyer.cash, 1_000_000.0 - penalty, places=1)

    def test_single_active_order_per_buyer(self):
        cfg = _Cfg({"smart_agent.deposit_ratio": 0.1, "smart_agent.precheck_include_tax_and_fee": False})
        svc = TransactionService(cfg, self.conn)
        buyer = _Buyer(
            id=3,
            cash=2_000_000.0,
            monthly_income=80_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=4_000_000.0,
            total_debt=0.0,
            agent_type="smart",
        )
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (buyer.id, 0.0, buyer.cash, buyer.total_assets, buyer.total_debt, buyer.monthly_income),
        )
        self.conn.commit()

        matches = [
            {"buyer": buyer, "listing": {"property_id": 21, "listed_price": 700_000.0, "min_price": 650_000.0, "seller_id": 4}},
            {"buyer": buyer, "listing": {"property_id": 22, "listed_price": 720_000.0, "min_price": 670_000.0, "seller_id": 5}},
        ]
        entries = svc._create_orders_for_matches(cur, month=1, buyer_matches=matches, agent_map={buyer.id: buyer})
        self.conn.commit()
        self.assertEqual(len(entries), 1)
        cur.execute("SELECT COUNT(*) FROM transaction_orders WHERE status='pending' AND buyer_id=?", (buyer.id,))
        self.assertEqual(cur.fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
