import sqlite3
import unittest
from types import SimpleNamespace

from services.transaction_service import TransactionService
from transaction_engine import developer_quick_sale


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


class TestRushBuyQueue(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.listing = {"property_id": 1001, "listed_price": 1_000_000}

    def tearDown(self):
        self.conn.close()

    def test_rotation_tie_breaker(self):
        cfg = _Cfg({"smart_agent.tie_breaker_mode": "rotation", "smart_agent.bid_aggressiveness": 0.5})
        svc = TransactionService(cfg, self.conn)
        entries = [
            {"buyer": SimpleNamespace(id=2, agent_type="smart")},
            {"buyer": SimpleNamespace(id=1, agent_type="smart")},
            {"buyer": SimpleNamespace(id=3, agent_type="normal")},
        ]

        queue = svc._build_developer_priority_queue(month=1, listing=self.listing, interested_entries=entries)
        order = [row["buyer"].id for row in queue]

        # Smart buyers have same offered price; month=1 rotation should move [1,2] -> [2,1].
        self.assertEqual(order, [2, 1, 3])
        self.assertEqual(queue[0]["offered_price"], 1_025_000.0)
        self.assertEqual(queue[2]["offered_price"], 1_005_000.0)

    def test_random_tie_breaker_is_stable_for_same_inputs(self):
        cfg = _Cfg({"smart_agent.tie_breaker_mode": "random", "smart_agent.bid_aggressiveness": 0.5})
        svc = TransactionService(cfg, self.conn)
        entries = [
            {"buyer": SimpleNamespace(id=10, agent_type="smart")},
            {"buyer": SimpleNamespace(id=11, agent_type="smart")},
            {"buyer": SimpleNamespace(id=12, agent_type="smart")},
        ]

        queue_a = svc._build_developer_priority_queue(month=4, listing=self.listing, interested_entries=entries)
        queue_b = svc._build_developer_priority_queue(month=4, listing=self.listing, interested_entries=entries)

        self.assertEqual([x["buyer"].id for x in queue_a], [x["buyer"].id for x in queue_b])

    def test_developer_quick_sale_uses_offered_price_for_affordability(self):
        # At listed price this buyer is affordable; at +5% offered price the down payment fails.
        buyer = SimpleNamespace(
            id=1,
            cash=300_000,
            monthly_income=50_000,
            mortgage_monthly_payment=0.0,
        )
        listing = {"listed_price": 1_000_000}
        cfg = _Cfg({})

        ok = developer_quick_sale(buyer, listing, cfg, offered_price=1_000_000)
        not_ok = developer_quick_sale(buyer, listing, cfg, offered_price=1_050_000)

        self.assertEqual(ok["outcome"], "success")
        self.assertEqual(not_ok["outcome"], "failed")

    def test_recovery_refill_prefers_viable_same_zone_candidates(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE transaction_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                buyer_id INTEGER,
                property_id INTEGER,
                status TEXT
            )
            """
        )
        self.conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.deposit_ratio": 0.1,
                "smart_agent.precheck_include_tax_and_fee": False,
                "smart_agent.precheck_liquidity_buffer_months": 0,
                "smart_agent.candidate_quota_per_property_base": 3,
            }
        )
        svc = TransactionService(cfg, self.conn)
        buyer = SimpleNamespace(
            id=7,
            cash=600_000.0,
            monthly_income=40_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_500_000.0,
            total_debt=0.0,
            school_urgency=2,
            waited_months=0,
            max_wait_months=6,
            preference=SimpleNamespace(
                target_zone="B",
                need_school_district=True,
                min_bedrooms=1,
                max_price=1_200_000.0,
            ),
            story=SimpleNamespace(
                education_path="public_school_district_priority",
                purchase_motive_primary="education_driven",
            ),
        )
        active_listings = [
            {"property_id": 101, "listed_price": 900_000.0, "owner_id": 10},
            {"property_id": 102, "listed_price": 850_000.0, "owner_id": 11},
            {"property_id": 103, "listed_price": 700_000.0, "owner_id": 12},
        ]
        props_map = {
            101: {"property_id": 101, "zone": "B", "is_school_district": True, "building_area": 75.0, "property_type": "普通住宅"},
            102: {"property_id": 102, "zone": "A", "is_school_district": True, "building_area": 78.0, "property_type": "普通住宅"},
            103: {"property_id": 103, "zone": "B", "is_school_district": False, "building_area": 72.0, "property_type": "普通住宅"},
        }

        refill = svc._build_recovery_refill_candidates(
            cursor=cur,
            month=1,
            buyer=buyer,
            active_listings=active_listings,
            props_map=props_map,
            blocked_property_ids=set(),
            attempted_property_ids=set(),
            combined_pressure={},
            monthly_candidate_quota_used={},
            candidate_quota_cfg=svc._resolve_candidate_quota_controls(),
            limit=3,
        )

        self.assertEqual([row["property_id"] for row in refill], [101])

    def test_recovery_queue_state_supports_deferred_next_month(self):
        cfg = _Cfg({})
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()

        svc._ensure_recovery_tables(cur)
        svc._upsert_recovery_queue(cur, month=1, buyer_id=9, lock_reason="Outbid")
        svc._mark_recovery_queue_state(
            cur,
            month=1,
            buyer_id=9,
            state="deferred_next_month",
            progress_round=2,
        )
        self.conn.commit()

        row = cur.execute(
            "SELECT state, last_progress_round FROM buyer_recovery_queue WHERE month=1 AND buyer_id=9"
        ).fetchone()
        self.assertEqual(row, ("deferred_next_month", 2))

    def test_recovery_candidate_sort_penalizes_reused_promotion_slots(self):
        cfg = _Cfg({})
        svc = TransactionService(cfg, self.conn)
        candidates = [
            {
                "property_id": 201,
                "same_zone_score": 1.0,
                "school_match_score": 1.0,
                "affordability_headroom": 0.2,
                "price_fit_score": 0.2,
                "pending_count": 0,
                "used_count": 0,
                "pressure": 0.1,
            },
            {
                "property_id": 202,
                "same_zone_score": 1.0,
                "school_match_score": 1.0,
                "affordability_headroom": 0.2,
                "price_fit_score": 0.2,
                "pending_count": 0,
                "used_count": 0,
                "pressure": 0.1,
            },
        ]

        ranked = sorted(
            candidates,
            key=lambda item: svc._recovery_candidate_sort_key(item, {201: 1}),
            reverse=True,
        )

        self.assertEqual([item["property_id"] for item in ranked], [202, 201])

    def test_recovery_refill_prefers_uncontested_candidates_after_outbid(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE transaction_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                buyer_id INTEGER,
                property_id INTEGER,
                status TEXT
            )
            """
        )
        cur.executemany(
            "INSERT INTO transaction_orders (buyer_id, property_id, status) VALUES (?, ?, ?)",
            [
                (30, 301, "pending"),
                (31, 301, "pending_settlement"),
                (32, 303, "pending"),
            ],
        )
        self.conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.deposit_ratio": 0.1,
                "smart_agent.precheck_include_tax_and_fee": False,
                "smart_agent.precheck_liquidity_buffer_months": 0,
                "smart_agent.candidate_quota_per_property_base": 3,
            }
        )
        svc = TransactionService(cfg, self.conn)
        buyer = SimpleNamespace(
            id=9,
            cash=1_000_000.0,
            monthly_income=60_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=2_000_000.0,
            total_debt=0.0,
            school_urgency=1,
            waited_months=1,
            max_wait_months=6,
            preference=SimpleNamespace(
                target_zone="B",
                need_school_district=False,
                min_bedrooms=1,
                max_price=1_200_000.0,
            ),
            story=SimpleNamespace(
                education_path="",
                purchase_motive_primary="upgrade",
            ),
        )
        active_listings = [
            {"property_id": 301, "listed_price": 800_000.0, "owner_id": 21},
            {"property_id": 302, "listed_price": 820_000.0, "owner_id": 22},
            {"property_id": 303, "listed_price": 780_000.0, "owner_id": 23},
        ]
        props_map = {
            301: {"property_id": 301, "zone": "B", "is_school_district": False, "bedrooms": 2, "building_area": 80.0, "property_type": "普通住宅"},
            302: {"property_id": 302, "zone": "B", "is_school_district": False, "bedrooms": 2, "building_area": 78.0, "property_type": "普通住宅"},
            303: {"property_id": 303, "zone": "B", "is_school_district": False, "bedrooms": 2, "building_area": 82.0, "property_type": "普通住宅"},
        }

        refill = svc._build_recovery_refill_candidates(
            cursor=cur,
            month=2,
            buyer=buyer,
            active_listings=active_listings,
            props_map=props_map,
            blocked_property_ids=set(),
            attempted_property_ids=set(),
            combined_pressure={},
            monthly_candidate_quota_used={},
            candidate_quota_cfg=svc._resolve_candidate_quota_controls(),
            limit=3,
            recovery_reason="Outbid: winner had significantly stronger budget headroom",
        )

        self.assertEqual([row["property_id"] for row in refill], [302, 303])

    def test_recovery_refill_uses_limited_competition_fallback_when_uncontested_insufficient(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE transaction_orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                buyer_id INTEGER,
                property_id INTEGER,
                status TEXT
            )
            """
        )
        cur.executemany(
            "INSERT INTO transaction_orders (buyer_id, property_id, status) VALUES (?, ?, ?)",
            [
                (40, 401, "pending"),
                (41, 403, "pending"),
                (42, 403, "pending_settlement"),
            ],
        )
        self.conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.deposit_ratio": 0.1,
                "smart_agent.precheck_include_tax_and_fee": False,
                "smart_agent.precheck_liquidity_buffer_months": 0,
                "smart_agent.candidate_quota_per_property_base": 3,
            }
        )
        svc = TransactionService(cfg, self.conn)
        buyer = SimpleNamespace(
            id=10,
            cash=1_000_000.0,
            monthly_income=60_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=2_000_000.0,
            total_debt=0.0,
            school_urgency=1,
            waited_months=1,
            max_wait_months=6,
            preference=SimpleNamespace(
                target_zone="B",
                need_school_district=False,
                min_bedrooms=1,
                max_price=1_200_000.0,
            ),
            story=SimpleNamespace(
                education_path="",
                purchase_motive_primary="upgrade",
            ),
        )
        active_listings = [
            {"property_id": 401, "listed_price": 800_000.0, "owner_id": 21},
            {"property_id": 402, "listed_price": 820_000.0, "owner_id": 22},
            {"property_id": 403, "listed_price": 780_000.0, "owner_id": 23},
        ]
        props_map = {
            401: {"property_id": 401, "zone": "B", "is_school_district": False, "bedrooms": 2, "building_area": 80.0, "property_type": "普通住宅"},
            402: {"property_id": 402, "zone": "B", "is_school_district": False, "bedrooms": 2, "building_area": 78.0, "property_type": "普通住宅"},
            403: {"property_id": 403, "zone": "B", "is_school_district": False, "bedrooms": 2, "building_area": 82.0, "property_type": "普通住宅"},
        }

        refill = svc._build_recovery_refill_candidates(
            cursor=cur,
            month=2,
            buyer=buyer,
            active_listings=active_listings,
            props_map=props_map,
            blocked_property_ids=set(),
            attempted_property_ids=set(),
            combined_pressure={},
            monthly_candidate_quota_used={},
            candidate_quota_cfg=svc._resolve_candidate_quota_controls(),
            limit=2,
            recovery_reason="Outbid: lost in negotiation",
        )

        self.assertEqual([row["property_id"] for row in refill], [402, 401])

    def test_load_buyer_seen_property_ids_reads_shortlist_history(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE property_buyer_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                month INTEGER,
                property_id INTEGER,
                buyer_id INTEGER,
                listing_price REAL,
                buyer_bid REAL,
                is_valid_bid BOOLEAN,
                proceeded_to_negotiation BOOLEAN,
                order_id INTEGER,
                match_context TEXT,
                selection_reason TEXT,
                selected_in_shortlist BOOLEAN,
                final_outcome TEXT,
                failure_stage TEXT,
                failure_reason TEXT,
                final_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.executemany(
            "INSERT INTO property_buyer_matches (month, property_id, buyer_id, final_outcome) VALUES (?, ?, ?, ?)",
            [
                (1, 501, 77, "SHORTLIST_ONLY"),
                (2, 502, 77, "SHORTLIST_ONLY"),
                (3, 503, 88, "SHORTLIST_ONLY"),
            ],
        )
        self.conn.commit()

        svc = TransactionService(_Cfg({}), self.conn)
        seen_ids = svc._load_buyer_seen_property_ids(cur, buyer_id=77, through_month=2)

        self.assertEqual(seen_ids, {501, 502})

    def test_record_shortlist_exposure_rows_persists_non_selected_candidates(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE property_buyer_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                month INTEGER,
                property_id INTEGER,
                buyer_id INTEGER,
                listing_price REAL,
                buyer_bid REAL,
                is_valid_bid BOOLEAN,
                proceeded_to_negotiation BOOLEAN,
                order_id INTEGER,
                match_context TEXT,
                selection_reason TEXT,
                selected_in_shortlist BOOLEAN,
                final_outcome TEXT,
                failure_stage TEXT,
                failure_reason TEXT,
                final_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.commit()

        svc = TransactionService(_Cfg({}), self.conn)
        buyer = SimpleNamespace(id=55)
        svc._record_shortlist_exposure_rows(
            cursor=cur,
            month=3,
            buyer=buyer,
            shortlist_context=[
                {"property_id": 601, "listed_price": 900000.0},
                {"property_id": 602, "listed_price": 950000.0},
            ],
            selected_ids=[601],
            no_selection_code="NO_ORDER_CREATED",
            match_ctx={"selection_reason": "kept_searching"},
        )
        self.conn.commit()

        rows = cur.execute(
            "SELECT property_id, final_outcome, failure_stage, failure_reason FROM property_buyer_matches ORDER BY property_id"
        ).fetchall()

        self.assertEqual(rows, [(602, "SHORTLIST_ONLY", "DECISION", "NO_ORDER_CREATED")])


if __name__ == "__main__":
    unittest.main()
