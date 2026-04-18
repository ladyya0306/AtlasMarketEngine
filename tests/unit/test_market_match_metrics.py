import unittest
import sqlite3

from services.market_service import MarketService


class _Cfg:
    pass


class TestMarketMatchMetrics(unittest.TestCase):
    def test_compute_match_metrics(self):
        svc = MarketService(_Cfg(), db_conn=None)
        rows = [
            (
                "smart_downturn_defensive",
                "selected=[101]",
                '{"strategy_profile":"smart_downturn_defensive","selected_property_ids":[101]}',
                '{"base_education_weight":8,"effective_education_weight":6,"base_price_sensitivity":5,"effective_price_sensitivity":7}',
            ),
            (
                "smart_stable_balanced",
                "selected=[]",
                '{"strategy_profile":"smart_stable_balanced","selected_property_ids":[]}',
                '{"base_education_weight":7,"effective_education_weight":7,"base_price_sensitivity":5,"effective_price_sensitivity":5}',
            ),
            (
                "normal_balanced",
                "selected=[11]",
                '{"strategy_profile":"normal_balanced","selected_property_ids":[11]}',
                '{}',
            ),
        ]
        m = svc._compute_match_metrics(rows)
        self.assertEqual(m["match_total"], 3)
        self.assertEqual(m["match_selected"], 2)
        self.assertAlmostEqual(m["match_hit_rate"], 2 / 3, places=5)
        self.assertEqual(m["smart_match_total"], 2)
        self.assertEqual(m["smart_match_selected"], 1)
        self.assertAlmostEqual(m["smart_match_hit_rate"], 0.5, places=5)
        self.assertAlmostEqual(m["avg_edu_weight_delta"], -1.0, places=5)
        self.assertAlmostEqual(m["avg_price_sensitivity_delta"], 1.0, places=5)

    def test_collect_m16_guard_metrics(self):
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
        cur.executemany(
            """
            INSERT INTO decision_logs
            (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, 3, "BUYER_MATCH", "M16_BLOCK_DOWNTREND_ACCUMULATION", "", "{}", "{}", 0),
                (2, 3, "BUYER_MATCH", "M16_BLOCK_DOWNTREND_ACCUMULATION", "", "{}", "{}", 0),
                (3, 3, "M16_GUARD", "OFFER_CLAMP", "", "{}", '{"m16_offer_clamp_count": 2}', 0),
                (4, 3, "M16_GUARD", "OFFER_CLAMP", "", "{}", '{"clamp_count": 1}', 0),
                (5, 3, "ROLE_DECISION", "SELLER", "", "{}", '{"m16_sell_cap_applied": true, "m16_sell_cap": 1}', 1),
                (6, 3, "ROLE_DECISION", "SELLER", "", "{}", '{"m16_sell_cap_applied": false, "m16_sell_cap": 1}', 1),
            ],
        )
        conn.commit()

        svc = MarketService(_Cfg(), db_conn=conn)
        m = svc._collect_m16_guard_metrics(3)
        self.assertEqual(m["m16_blocks_count"], 2)
        self.assertEqual(m["m16_offer_clamp_count"], 3)
        self.assertEqual(m["m16_sell_cap_count"], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()
