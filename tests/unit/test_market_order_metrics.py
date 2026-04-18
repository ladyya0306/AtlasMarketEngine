import sqlite3
import unittest

from services.market_service import MarketService


class _Cfg:
    pass


class TestMarketOrderMetrics(unittest.TestCase):
    def test_collect_order_flow_metrics(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
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
                close_month INTEGER,
                close_reason TEXT
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO decision_logs
            (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, 5, "ORDER_PRECHECK", "REJECT", "INSUFFICIENT_DOWN_PAYMENT", "{}", "{}", 0),
                (2, 5, "BID_VALIDATION", "INVALID_BID", "INVALID_BID_BELOW_FLOOR", "{}", "{}", 0),
                (3, 5, "ORDER_PRECHECK", "PASS", "PASS", "{}", "{}", 0),
            ],
        )
        cur.executemany(
            "INSERT INTO transaction_orders(close_month, close_reason) VALUES (?, ?)",
            [
                (5, "Settlement failed: insufficient down payment"),
                (5, "Settlement failed: DTI exceeded"),
                (5, "Settlement failed: fee shortfall"),
                (5, "Settlement failed: buyer affordability"),
                (5, "Settlement failed: liquidity buffer"),
                (5, "Settlement failed: leverage cap exceeded"),
            ],
        )
        conn.commit()

        svc = MarketService(_Cfg(), db_conn=conn)
        m = svc._collect_order_flow_metrics(5)
        self.assertEqual(m["precheck_reject_count"], 1)
        self.assertEqual(m["invalid_bid_count"], 1)
        self.assertEqual(m["settlement_fail_affordability_count"], 4)
        self.assertEqual(m["settlement_fail_dti_count"], 1)
        self.assertEqual(m["settlement_fail_fee_count"], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()
