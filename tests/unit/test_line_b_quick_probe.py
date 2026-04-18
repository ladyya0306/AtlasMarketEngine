import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.line_b_quick_probe import _extract_created_initial_listings, _summarize_probe


class TestLineBQuickProbe(unittest.TestCase):
    def test_extract_created_initial_listings_from_log(self):
        with TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "simulation_run.log"
            log_path.write_text(
                "2026-04-10 00:00:00,000 - INFO - Created 3 initial listings (V2 properties_market).\n",
                encoding="utf-8",
            )
            self.assertEqual(_extract_created_initial_listings(log_path), 3)

    def test_summarize_probe_reads_init_rows_and_orders(self):
        with TemporaryDirectory() as tmp_dir:
            run_dir = Path(tmp_dir)
            db_path = run_dir / "simulation.db"
            (run_dir / "simulation_run.log").write_text(
                "\n".join(
                    [
                        "2026-04-10 00:00:00,000 - INFO - Created 3 initial listings (V2 properties_market).",
                        "2026-04-10 00:00:01,000 - INFO - Init supply coverage snapshot: "
                        "A_owned=38 B_owned=20 A_for_sale=4 B_for_sale=8 tradable=12 | "
                        "targets(A_owner=8, B_owner=20, A_for_sale=4, B_for_sale=8, tradable=10)",
                    ]
                ),
                encoding="utf-8",
            )

            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute("CREATE TABLE properties_static (property_id INTEGER PRIMARY KEY, zone TEXT)")
            cur.execute(
                "CREATE TABLE properties_market (property_id INTEGER PRIMARY KEY, owner_id INTEGER, status TEXT, listing_month INTEGER)"
            )
            cur.execute(
                "CREATE TABLE transaction_orders (order_id INTEGER PRIMARY KEY, created_month INTEGER, buyer_id INTEGER)"
            )
            cur.execute(
                "CREATE TABLE transactions (transaction_id INTEGER PRIMARY KEY, month INTEGER)"
            )
            cur.execute(
                "CREATE TABLE decision_logs (log_id INTEGER PRIMARY KEY, agent_id INTEGER, month INTEGER, event_type TEXT, decision TEXT, thought_process TEXT, context_metrics TEXT, llm_called INTEGER)"
            )
            cur.execute(
                "CREATE TABLE property_buyer_matches (match_id INTEGER PRIMARY KEY, month INTEGER, property_id INTEGER, buyer_id INTEGER, failure_reason TEXT, chain_mode TEXT)"
            )
            cur.execute(
                "CREATE TABLE active_participants (agent_id INTEGER, month INTEGER, role TEXT, chain_mode TEXT)"
            )
            cur.execute(
                "CREATE TABLE agents_static (agent_id INTEGER PRIMARY KEY, agent_type TEXT)"
            )
            cur.executemany(
                "INSERT INTO properties_static(property_id, zone) VALUES (?, ?)",
                [(1, "A"), (2, "A"), (3, "B"), (4, "B")],
            )
            cur.executemany(
                "INSERT INTO properties_market(property_id, owner_id, status, listing_month) VALUES (?, ?, ?, ?)",
                [
                    (1, 10, "for_sale", 0),
                    (2, 11, "off_market", 0),
                    (3, 12, "for_sale", 0),
                    (4, 13, "for_sale", 1),
                ],
            )
            cur.executemany(
                "INSERT INTO transaction_orders(order_id, created_month, buyer_id) VALUES (?, ?, ?)",
                [(1, 1, 101), (2, 1, 102)],
            )
            cur.execute("INSERT INTO transactions(transaction_id, month) VALUES (1, 1)")
            cur.executemany(
                "INSERT INTO decision_logs(log_id, agent_id, month, event_type, decision, thought_process, context_metrics, llm_called) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (1, 101, 1, "ROLE_DECISION", "BUYER", "{}", "{}", 0),
                    (2, 102, 1, "ROLE_DECISION", "BUYER_SELLER", "{}", "{}", 0),
                ],
            )
            cur.executemany(
                "INSERT INTO agents_static(agent_id, agent_type) VALUES (?, ?)",
                [(101, "normal"), (102, "normal")],
            )
            conn.commit()
            conn.close()

            summary = _summarize_probe(db_path=db_path, run_dir=run_dir, months=1)

        self.assertEqual(summary["created_initial_listings"], 3)
        self.assertEqual(summary["init_listing_rows_total"], 2)
        self.assertEqual(summary["init_listing_rows_by_zone"]["A"], 1)
        self.assertEqual(summary["init_listing_rows_by_zone"]["B"], 1)
        self.assertEqual(summary["orders_m1_distinct_buyers"], 2)
        self.assertEqual(summary["metrics"]["l0"], 12)


if __name__ == "__main__":
    unittest.main()
