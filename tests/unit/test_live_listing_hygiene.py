import sqlite3
import unittest

from services.transaction_service import TransactionService


class _Cfg:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.negotiation = {}
        self.mortgage = {
            "down_payment_ratio": 0.3,
            "annual_interest_rate": 0.05,
            "loan_term_years": 30,
            "max_dti_ratio": 0.5,
        }

    def get(self, key, default=None):
        return self.values.get(key, default)


class TestLiveListingHygiene(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE transaction_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                buyer_id INTEGER,
                seller_id INTEGER,
                property_id INTEGER,
                status TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                owner_id INTEGER,
                status TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE property_buyer_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                month INTEGER,
                property_id INTEGER,
                buyer_id INTEGER
            )
            """
        )
        cur.executemany(
            "INSERT INTO properties_market (property_id, owner_id, status) VALUES (?, ?, ?)",
            [
                (1, 11, "for_sale"),
                (2, 12, "for_sale"),
                (3, 13, "for_sale"),
                (4, 14, "pending_settlement"),
                (5, 99, "for_sale"),
            ],
        )
        cur.execute(
            "INSERT INTO transaction_orders (buyer_id, seller_id, property_id, status) VALUES (?, ?, ?, ?)",
            (31, 12, 2, "pending"),
        )
        self.conn.commit()
        self.svc = TransactionService(_Cfg({}), self.conn)

    def tearDown(self):
        self.conn.close()

    def test_filter_live_candidate_listings_excludes_locked_seen_and_self_owned(self):
        listings = [
            {"property_id": 1, "owner_id": 11, "status": "for_sale"},
            {"property_id": 2, "owner_id": 12, "status": "for_sale"},
            {"property_id": 3, "owner_id": 13, "status": "for_sale"},
            {"property_id": 4, "owner_id": 14, "status": "for_sale"},
            {"property_id": 5, "owner_id": 99, "status": "for_sale"},
        ]

        filtered, stats = self.svc._filter_live_candidate_listings(
            cursor=self.conn.cursor(),
            buyer_id=99,
            listings=listings,
            same_month_seen_ids={3},
        )

        self.assertEqual([int(item["property_id"]) for item in filtered], [1])
        self.assertEqual(int(stats["removed_locked"]), 1)
        self.assertEqual(int(stats["removed_not_for_sale"]), 1)
        self.assertEqual(int(stats["removed_same_month_seen"]), 1)
        self.assertEqual(int(stats["removed_self_owned"]), 1)


if __name__ == "__main__":
    unittest.main()
