import json
import sqlite3
import unittest
import asyncio
from types import SimpleNamespace

import transaction_engine
from services.transaction_service import TransactionService


class _Cfg:
    def __init__(self, data):
        self._data = data
        self.negotiation = data.get("negotiation", {})
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


class TestOrderPrecheck(unittest.TestCase):
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
            CREATE TABLE transactions (
                tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                owner_id INTEGER,
                status TEXT,
                last_transaction_month INTEGER,
                current_valuation REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_static (
                property_id INTEGER PRIMARY KEY,
                zone TEXT,
                building_area REAL,
                is_school_district INTEGER,
                property_type TEXT
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
        cur.execute(
            """
            CREATE TABLE active_participants (
                agent_id INTEGER PRIMARY KEY,
                role TEXT,
                chain_mode TEXT
            )
            """
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_precheck_reasons(self):
        cfg = _Cfg(
            {
                "smart_agent.precheck_include_tax_and_fee": True,
                "smart_agent.precheck_liquidity_buffer_months": 0,
            }
        )
        svc = TransactionService(cfg, self.conn)
        listing = {"property_id": 1, "listed_price": 1_000_000, "min_price": 950_000}

        buyer_down = _Buyer(id=1, cash=200_000, monthly_income=30_000, mortgage_monthly_payment=0, total_assets=500_000, total_debt=0, agent_type="smart")
        ok, reason, _ = svc._precheck_order_affordability(buyer_down, listing, 1_000_000)
        self.assertFalse(ok)
        self.assertEqual(reason, "INSUFFICIENT_DOWN_PAYMENT")

        buyer_fee = _Buyer(id=2, cash=305_000, monthly_income=30_000, mortgage_monthly_payment=0, total_assets=500_000, total_debt=0, agent_type="smart")
        ok, reason, _ = svc._precheck_order_affordability(buyer_fee, listing, 1_000_000)
        self.assertFalse(ok)
        self.assertEqual(reason, "FEE_SHORTFALL")

        buyer_dti = _Buyer(id=3, cash=1_000_000, monthly_income=2_000, mortgage_monthly_payment=0, total_assets=1_500_000, total_debt=0, agent_type="smart")
        ok, reason, _ = svc._precheck_order_affordability(buyer_dti, listing, 1_000_000)
        self.assertFalse(ok)
        self.assertEqual(reason, "DTI_EXCEEDED")

    def test_create_order_logs_precheck_reject(self):
        cfg = _Cfg(
            {
                "smart_agent.precheck_include_tax_and_fee": True,
                "smart_agent.precheck_liquidity_buffer_months": 0,
                "smart_agent.deposit_ratio": 0.1,
            }
        )
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()
        buyer = _Buyer(
            id=10,
            cash=250_000,
            monthly_income=20_000,
            mortgage_monthly_payment=0.0,
            total_assets=400_000,
            total_debt=0.0,
            agent_type="smart",
        )
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (buyer.id, 0.0, buyer.cash, buyer.total_assets, buyer.total_debt, buyer.monthly_income),
        )
        cur.execute(
            "INSERT INTO properties_market(property_id, owner_id, status, last_transaction_month, current_valuation) VALUES (?, ?, ?, ?, ?)",
            (101, 20, "for_sale", None, 1_000_000.0),
        )
        cur.execute(
            "INSERT INTO properties_static(property_id, zone, building_area, is_school_district, property_type) VALUES (?, ?, ?, ?, ?)",
            (101, "B", 90.0, 0, "residential"),
        )
        self.conn.commit()

        matches = [{"buyer": buyer, "listing": {"property_id": 101, "listed_price": 1_000_000, "min_price": 900_000, "seller_id": 20}}]
        entries = svc._create_orders_for_matches(cur, month=1, buyer_matches=matches, agent_map={buyer.id: buyer})
        self.conn.commit()

        self.assertEqual(len(entries), 0)
        cur.execute(
            """
            SELECT event_type, decision, reason
            FROM decision_logs
            WHERE agent_id=? AND month=1
            ORDER BY log_id DESC
            LIMIT 1
            """,
            (buyer.id,),
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "ORDER_PRECHECK")
        self.assertEqual(row[1], "REJECT")
        self.assertIn(
            row[2],
            {
                "INSUFFICIENT_DOWN_PAYMENT",
                "FEE_SHORTFALL",
                "DTI_EXCEEDED",
                "INSUFFICIENT_LIQUIDITY_BUFFER",
                "LEVERAGE_CAP_EXCEEDED",
            },
        )

    def test_precheck_uses_settlement_liquidity_floor(self):
        cfg = _Cfg(
            {
                "smart_agent.precheck_include_tax_and_fee": False,
                "smart_agent.precheck_liquidity_buffer_months": 3,
                "smart_agent.liquidity_floor_months": 6,
                "smart_agent.deposit_ratio": 0.1,
            }
        )
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()
        buyer = _Buyer(
            id=11,
            cash=350_000.0,  # down payment 300k => remaining 50k (passes 3-month floor, fails 6-month floor at income=20k)
            monthly_income=20_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=800_000.0,
            total_debt=0.0,
            agent_type="smart",
        )
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (buyer.id, 0.0, buyer.cash, buyer.total_assets, buyer.total_debt, buyer.monthly_income),
        )
        cur.execute(
            "INSERT INTO properties_market(property_id, owner_id, status, last_transaction_month, current_valuation) VALUES (?, ?, ?, ?, ?)",
            (202, 20, "for_sale", None, 1_000_000.0),
        )
        cur.execute(
            "INSERT INTO properties_static(property_id, zone, building_area, is_school_district, property_type) VALUES (?, ?, ?, ?, ?)",
            (202, "B", 88.0, 0, "residential"),
        )
        self.conn.commit()

        matches = [{"buyer": buyer, "listing": {"property_id": 202, "listed_price": 1_000_000, "min_price": 900_000, "seller_id": 20}}]
        entries = svc._create_orders_for_matches(cur, month=1, buyer_matches=matches, agent_map={buyer.id: buyer})
        self.conn.commit()
        self.assertEqual(len(entries), 0)

        cur.execute(
            """
            SELECT decision, reason
            FROM decision_logs
            WHERE agent_id=? AND month=1 AND event_type='ORDER_PRECHECK'
            ORDER BY log_id DESC
            LIMIT 1
            """,
            (buyer.id,),
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "REJECT")
        self.assertEqual(row[1], "INSUFFICIENT_LIQUIDITY_BUFFER")

    def test_buyer_state_drift_triggers_settlement_recheck_path(self):
        svc = TransactionService(_Cfg({}), self.conn)
        cur = self.conn.cursor()
        buyer = _Buyer(
            id=21,
            cash=500_000.0,
            monthly_income=25_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_000_000.0,
            total_debt=100_000.0,
            owned_properties=[{"property_id": 1}],
            agent_type="smart",
        )
        svc._persist_order_prequalification_snapshot(cur, order_id=999, buyer=buyer)
        self.assertFalse(
            svc._buyer_state_drifted_since_precheck(
                buyer=buyer,
                prequal_cash=500_000.0,
                prequal_total_debt=100_000.0,
                prequal_owned_property_count=1,
            )
        )

        buyer.cash = 320_000.0
        buyer.total_debt = 260_000.0
        buyer.owned_properties.append({"property_id": 2})
        self.assertTrue(
            svc._buyer_state_drifted_since_precheck(
                buyer=buyer,
                prequal_cash=500_000.0,
                prequal_total_debt=100_000.0,
                prequal_owned_property_count=1,
            )
        )

    def test_settlement_cash_consistency_only_checks_cash_floor(self):
        svc = TransactionService(
            _Cfg(
                {
                    "smart_agent.precheck_include_tax_and_fee": True,
                    "smart_agent.precheck_liquidity_buffer_months": 2,
                }
            ),
            self.conn,
        )
        buyer = _Buyer(
            id=22,
            cash=300_000.0,
            monthly_income=20_000.0,
            mortgage_monthly_payment=18_000.0,
            total_assets=900_000.0,
            total_debt=800_000.0,
            owned_properties=[{"property_id": 1}, {"property_id": 2}],
            agent_type="smart",
        )
        ok, reason, metrics = svc._precheck_settlement_cash_consistency(
            buyer=buyer,
            listing={"property_id": 303},
            offer_price=1_000_000.0,
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "CASH_SHORTFALL_PREQUALIFIED")
        self.assertGreater(metrics["required_cash"], buyer.cash)

    def test_derive_outbid_reason_marks_batch_tiebreak_for_near_equal_final_bids(self):
        svc = TransactionService(_Cfg({}), self.conn)
        winner = _Buyer(id=1, preference=SimpleNamespace(max_price=1_500_000.0))
        loser = _Buyer(id=2, preference=SimpleNamespace(max_price=900_000.0))
        listing = {"property_id": 10, "listed_price": 800_000.0}
        history = [
            {"action": "MODEL_ROUTE", "session_mode": "batch_bidding"},
            {"action": "FINAL_BID", "agent_id": 1, "price": 829_028.0},
            {"action": "FINAL_BID", "agent_id": 2, "price": 829_028.0},
            {"action": "WIN_BID", "buyer_id": 1, "price": 829_028.0},
        ]
        order_entries = [{"buyer": winner}, {"buyer": loser}]

        reason = svc._derive_outbid_reason(
            winner=winner,
            loser=loser,
            listing=listing,
            order_entries=order_entries,
            history=history,
            session_mode="batch_bidding",
        )

        self.assertEqual(reason, "Outbid: batch tie-break after near-equal final bids")

    def test_derive_negotiation_failure_reason_prefers_terminal_seller_action(self):
        svc = TransactionService(_Cfg({}), self.conn)
        buyer = _Buyer(id=2)
        history = [
            {"round": 1, "party": "buyer", "agent_id": 2, "action": "OFFER", "price": 520_000},
            {"round": 1, "party": "seller", "agent_id": 9, "action": "COUNTER", "price": 560_000},
            {"round": 2, "party": "buyer", "agent_id": 2, "action": "OFFER", "price": 540_000},
            {"round": 2, "party": "seller", "agent_id": 9, "action": "ACCEPT", "price": 560_000},
        ]

        reason = svc._derive_negotiation_failure_reason(
            session_reason="All negotiations failed",
            history=history,
            buyer=buyer,
            listing={"listed_price": 600_000.0, "min_price": 500_000.0},
        )

        self.assertEqual(reason, "All negotiations failed: accepted then blocked by downstream rule")

    def test_derive_negotiation_failure_reason_marks_accept_below_floor(self):
        svc = TransactionService(_Cfg({}), self.conn)
        buyer = _Buyer(id=2)
        history = [
            {"round": 1, "party": "buyer", "agent_id": 2, "action": "OFFER", "price": 520_000},
            {"round": 1, "party": "seller", "agent_id": 9, "action": "COUNTER", "price": 560_000},
            {"round": 2, "party": "buyer", "agent_id": 2, "action": "OFFER", "price": 540_000},
            {"round": 2, "party": "seller", "agent_id": 9, "action": "ACCEPT", "price": 560_000},
        ]

        reason = svc._derive_negotiation_failure_reason(
            session_reason="All negotiations failed",
            history=history,
            buyer=buyer,
            listing={"listed_price": 600_000.0, "min_price": 550_000.0},
        )

        self.assertEqual(reason, "All negotiations failed: seller accepted offer below floor rule")

    def test_build_buyer_match_summary_log_keeps_no_shortlist_reason(self):
        svc = TransactionService(_Cfg({}), self.conn)
        buyer = _Buyer(id=88, agent_type="normal")
        log_row = svc._build_buyer_match_summary_log(
            buyer=buyer,
            month=1,
            strategy_profile="normal_balanced",
            selected_ids=[],
            no_selection_code="NO_ACTIVE_LISTINGS",
            listings_for_buyer_count=0,
            shortlist_ids=[],
            quota_prefilter_enabled=True,
            quota_blocked_ids=[],
            quota_blocked_meta=[],
            quota_charge_ids=[],
            weight_payload={"base_price_sensitivity": 6},
            match_ctx={
                "selection_reason": "",
                "llm_called": True,
                "persona_snapshot": {"purchase_motive_primary": "upgrade_living"},
            },
            retry_trace=[{"attempt": 1, "result": "no_selection"}],
            retry_budget={"attempts_spent": 1, "attempt_budget": 3},
            pipeline_stage_trace=["collect_visible_pool", "apply_hard_filters"],
            pipeline_order_violation=False,
            market_trend="STABLE",
            observed_trend="STABLE",
            observed_month=0,
            observed_delay=0,
        )
        self.assertEqual(log_row[2], "BUYER_MATCH_SUMMARY")
        self.assertEqual(log_row[3], "normal_balanced")
        thought = json.loads(log_row[5])
        metrics = json.loads(log_row[6])
        self.assertEqual(thought["no_selection_reason_code"], "NO_ACTIVE_LISTINGS")
        self.assertEqual(thought["shortlist_count"], 0)
        self.assertEqual(thought["listings_for_buyer_count"], 0)
        self.assertTrue(thought["source_match_llm_called"])
        # New diagnostic fields are optional; when absent they should default safely.
        self.assertIn("no_buy_class", thought)
        self.assertIn("no_buy_branch", thought)
        self.assertEqual(metrics["shortlist_count"], 0)
        self.assertEqual(metrics["listings_for_buyer_count"], 0)
        self.assertTrue(metrics["source_match_llm_called"])
        self.assertIn("no_buy_class", metrics)
        self.assertIn("no_buy_branch", metrics)
        self.assertFalse(log_row[7])

    def test_negotiate_async_converts_invalid_accept_below_floor_to_counter(self):
        cfg = _Cfg(
            {
                "negotiation": {
                    "rounds_range": [3, 3],
                    "heuristic_gap_threshold": 0.2,
                    "market_conditions": {
                        "balanced": {"buyer_lowball": 0.9, "llm_hint": "balanced"},
                        "oversupply": {"buyer_lowball": 0.9, "llm_hint": "oversupply"},
                        "undersupply": {"buyer_lowball": 0.9, "llm_hint": "undersupply"},
                    },
                }
            }
        )
        buyer = _Buyer(
            id=1,
            preference=SimpleNamespace(max_price=1_000_000.0),
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        seller = _Buyer(
            id=2,
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        listing = {"property_id": 1, "listed_price": 1_000_000.0, "min_price": 950_000.0, "zone": "B"}
        market = SimpleNamespace(properties=[{"property_id": 1, "status": "for_sale", "zone": "B"}])

        responses = iter(
            [
                {"action": "OFFER", "offer_price": 900_000.0, "reason": "first low offer"},
                {"action": "ACCEPT", "counter_price": 0, "reason": "invalid accept below floor"},
                {"action": "ACCEPT", "offer_price": 950_000.0, "reason": "accept min price"},
            ]
        )

        original_async = transaction_engine.safe_call_llm_async
        original_randint = transaction_engine.random.randint
        original_build_macro = transaction_engine.build_macro_context
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return next(responses)

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            transaction_engine.random.randint = lambda a, b: 3
            transaction_engine.build_macro_context = lambda month, config=None: ""

            result = asyncio.run(
                transaction_engine.negotiate_async(
                    buyer=buyer,
                    seller=seller,
                    listing=listing,
                    market=market,
                    potential_buyers_count=1,
                    config=cfg,
                    llm_model_type="fast",
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async
            transaction_engine.random.randint = original_randint
            transaction_engine.build_macro_context = original_build_macro

        self.assertEqual(result["outcome"], "success")
        self.assertEqual(result["final_price"], 950_000.0)
        seller_events = [e for e in result["history"] if isinstance(e, dict) and e.get("party") == "seller"]
        self.assertGreaterEqual(len(seller_events), 1)
        self.assertEqual(seller_events[0]["action"], "COUNTER")
        self.assertEqual(seller_events[0]["price"], 950_000.0)
        self.assertIn("convert invalid ACCEPT to COUNTER@min_price", seller_events[0]["content"])

    def test_negotiate_async_floor_locked_buyer_invalid_offer_normalized_to_withdraw(self):
        cfg = _Cfg(
            {
                "negotiation": {
                    "rounds_range": [2, 2],
                    "heuristic_gap_threshold": 0.2,
                    "market_conditions": {
                        "balanced": {"buyer_lowball": 0.9, "llm_hint": "balanced"},
                        "oversupply": {"buyer_lowball": 0.9, "llm_hint": "oversupply"},
                        "undersupply": {"buyer_lowball": 0.9, "llm_hint": "undersupply"},
                    },
                }
            }
        )
        buyer = _Buyer(
            id=1,
            preference=SimpleNamespace(max_price=1_000_000.0),
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        seller = _Buyer(
            id=2,
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        listing = {"property_id": 1, "listed_price": 1_000_000.0, "min_price": 950_000.0, "zone": "B"}
        market = SimpleNamespace(properties=[{"property_id": 1, "status": "for_sale", "zone": "B"}])

        responses = iter(
            [
                {"action": "OFFER", "offer_price": 900_000.0, "reason": "first low offer"},
                {"action": "COUNTER", "counter_price": 950_000.0, "reason": "hold floor"},
                {"action": "OFFER", "offer_price": 900_000.0, "reason": "still lowball"},
            ]
        )

        original_async = transaction_engine.safe_call_llm_async
        original_randint = transaction_engine.random.randint
        original_build_macro = transaction_engine.build_macro_context
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return next(responses)

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            transaction_engine.random.randint = lambda a, b: 2
            transaction_engine.build_macro_context = lambda month, config=None: ""

            result = asyncio.run(
                transaction_engine.negotiate_async(
                    buyer=buyer,
                    seller=seller,
                    listing=listing,
                    market=market,
                    potential_buyers_count=1,
                    config=cfg,
                    llm_model_type="fast",
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async
            transaction_engine.random.randint = original_randint
            transaction_engine.build_macro_context = original_build_macro

        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(result["reason"], "Buyer withdrew")
        buyer_events = [e for e in result["history"] if isinstance(e, dict) and e.get("party") == "buyer"]
        self.assertEqual(buyer_events[-1]["action"], "WITHDRAW")
        self.assertIn("invalid new offer normalized to WITHDRAW", buyer_events[-1]["content"])

    def test_negotiate_async_final_round_counter_gets_buyer_last_look(self):
        cfg = _Cfg(
            {
                "negotiation": {
                    "rounds_range": [2, 2],
                    "heuristic_gap_threshold": 0.2,
                    "market_conditions": {
                        "balanced": {"buyer_lowball": 0.9, "llm_hint": "balanced"},
                        "oversupply": {"buyer_lowball": 0.9, "llm_hint": "oversupply"},
                        "undersupply": {"buyer_lowball": 0.9, "llm_hint": "undersupply"},
                    },
                }
            }
        )
        buyer = _Buyer(
            id=1,
            preference=SimpleNamespace(max_price=1_000_000.0),
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        seller = _Buyer(
            id=2,
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        listing = {"property_id": 1, "listed_price": 1_000_000.0, "min_price": 900_000.0, "zone": "B"}
        market = SimpleNamespace(properties=[{"property_id": 1, "status": "for_sale", "zone": "B"}])

        responses = iter(
            [
                {"action": "OFFER", "offer_price": 880_000.0, "reason": "initial offer"},
                {"action": "COUNTER", "counter_price": 950_000.0, "reason": "need more"},
                {"action": "OFFER", "offer_price": 930_000.0, "reason": "move closer"},
                {"action": "COUNTER", "counter_price": 940_000.0, "reason": "final counter"},
                {"action": "ACCEPT", "reason": "final accept"},
            ]
        )

        original_async = transaction_engine.safe_call_llm_async
        original_randint = transaction_engine.random.randint
        original_build_macro = transaction_engine.build_macro_context
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return next(responses)

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            transaction_engine.random.randint = lambda a, b: 2
            transaction_engine.build_macro_context = lambda month, config=None: ""

            result = asyncio.run(
                transaction_engine.negotiate_async(
                    buyer=buyer,
                    seller=seller,
                    listing=listing,
                    market=market,
                    potential_buyers_count=1,
                    config=cfg,
                    llm_model_type="fast",
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async
            transaction_engine.random.randint = original_randint
            transaction_engine.build_macro_context = original_build_macro

        self.assertEqual(result["outcome"], "success")
        self.assertEqual(result["final_price"], 940_000.0)
        buyer_final_events = [e for e in result["history"] if isinstance(e, dict) and e.get("party") == "buyer_final"]
        self.assertEqual(len(buyer_final_events), 1)
        self.assertEqual(buyer_final_events[0]["action"], "ACCEPT")

    def test_negotiate_async_final_round_reject_above_floor_converts_to_counter_last_look(self):
        cfg = _Cfg(
            {
                "smart_agent.regime_engine_v1_enabled": True,
                "smart_agent.regime_v1_negotiation_convergence_enabled": True,
                "smart_agent.regime_v1_reject_to_counter_close_priority_threshold": 68.0,
                "smart_agent.regime_v1_reject_to_counter_final_round_threshold": 40.0,
                "smart_agent.regime_v1_reject_to_counter_near_floor_ratio": 0.98,
                "negotiation": {
                    "rounds_range": [2, 2],
                    "heuristic_gap_threshold": 0.2,
                    "market_conditions": {
                        "balanced": {"buyer_lowball": 0.9, "llm_hint": "balanced"},
                        "oversupply": {"buyer_lowball": 0.9, "llm_hint": "oversupply"},
                        "undersupply": {"buyer_lowball": 0.9, "llm_hint": "undersupply"},
                    },
                }
            }
        )
        buyer = _Buyer(
            id=13,
            preference=SimpleNamespace(max_price=2_000_000.0),
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        seller = _Buyer(
            id=7,
            story=SimpleNamespace(negotiation_style="balanced"),
        )
        listing = {"property_id": 240, "listed_price": 1_630_420.75, "min_price": 1_434_770.26, "zone": "B"}
        market = SimpleNamespace(properties=[{"property_id": 240, "status": "for_sale", "zone": "B"}])

        responses = iter(
            [
                {"action": "OFFER", "offer_price": 978_643.91, "reason": "initial low offer"},
                {"action": "COUNTER", "counter_price": 1_631_073.0, "reason": "hold firm"},
                {"action": "OFFER", "offer_price": 1_471_391.0, "reason": "near floor move"},
                {"action": "REJECT", "counter_price": 0, "reason": "still not ideal"},
                {"action": "ACCEPT", "reason": "final accept after counter"},
            ]
        )

        original_async = transaction_engine.safe_call_llm_async
        original_randint = transaction_engine.random.randint
        original_build_macro = transaction_engine.build_macro_context
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return next(responses)

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            transaction_engine.random.randint = lambda a, b: 2
            transaction_engine.build_macro_context = lambda month, config=None: ""

            result = asyncio.run(
                transaction_engine.negotiate_async(
                    buyer=buyer,
                    seller=seller,
                    listing=listing,
                    market=market,
                    potential_buyers_count=1,
                    config=cfg,
                    llm_model_type="fast",
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async
            transaction_engine.random.randint = original_randint
            transaction_engine.build_macro_context = original_build_macro

        self.assertEqual(result["outcome"], "success")
        self.assertGreaterEqual(result["final_price"], 1_434_770.26)
        seller_events = [e for e in result["history"] if isinstance(e, dict) and e.get("party") == "seller"]
        self.assertEqual(seller_events[-1]["action"], "COUNTER")
        self.assertIn("RegimeV1终局收敛护栏触发", seller_events[-1]["content"])
        buyer_final_events = [e for e in result["history"] if isinstance(e, dict) and e.get("party") == "buyer_final"]
        self.assertEqual(len(buyer_final_events), 1)
        self.assertEqual(buyer_final_events[0]["action"], "ACCEPT")

    def test_run_batch_bidding_async_normalizes_truncated_price_scale(self):
        cfg = _Cfg(
            {
                "smart_agent.effective_bid_floor_ratio": 0.98,
                "smart_agent.min_offer_ratio_to_list": 0.75,
                "smart_agent.max_overbid_ratio_to_list": 0.15,
                "smart_agent.batch_rebid_enabled": False,
            }
        )
        buyer = _Buyer(
            id=31,
            cash=800_000.0,
            monthly_income=35_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_200_000.0,
            total_debt=0.0,
            preference=SimpleNamespace(max_price=2_000_000.0),
            story=SimpleNamespace(investment_style="balanced"),
        )
        seller = _Buyer(id=41, story=SimpleNamespace(investment_style="balanced"))
        listing = {
            "property_id": 9001,
            "listed_price": 665_321.0,
            "min_price": 650_000.0,
            "initial_value": 640_000.0,
            "zone": "B",
            "building_area": 60.0,
        }
        market = SimpleNamespace()

        original_async = transaction_engine.safe_call_llm_async
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return {"bid_price": 698.0, "reason": "出价69.8万"}

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            result = asyncio.run(
                transaction_engine.run_batch_bidding_async(
                    seller=seller,
                    buyers=[buyer],
                    listing=listing,
                    market=market,
                    month=1,
                    config=cfg,
                    db_conn=None,
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async

        self.assertEqual(result["outcome"], "success")
        self.assertEqual(result["buyer_id"], buyer.id)
        self.assertGreaterEqual(float(result["final_price"]), 650_000.0)

    def test_run_batch_bidding_async_updates_existing_match_row_instead_of_duplicate_insert(self):
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
        cur.execute(
            """
            INSERT INTO property_buyer_matches (
                month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid,
                proceeded_to_negotiation, order_id, selection_reason, selected_in_shortlist,
                final_outcome, failure_stage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 9003, 31, 1_000_000.0, None, 1, 0, 7001, "pre-logged", 1, "ORDER_CREATED", "NEGOTIATION_PENDING"),
        )
        self.conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.effective_bid_floor_ratio": 0.98,
                "smart_agent.min_offer_ratio_to_list": 0.75,
                "smart_agent.max_overbid_ratio_to_list": 0.15,
                "smart_agent.batch_rebid_enabled": False,
            }
        )
        buyer = _Buyer(
            id=31,
            cash=800_000.0,
            monthly_income=35_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_200_000.0,
            total_debt=0.0,
            preference=SimpleNamespace(max_price=2_000_000.0),
            story=SimpleNamespace(investment_style="balanced"),
        )
        seller = _Buyer(id=41, story=SimpleNamespace(investment_style="balanced"))
        listing = {
            "property_id": 9003,
            "listed_price": 1_000_000.0,
            "min_price": 900_000.0,
            "initial_value": 980_000.0,
            "zone": "B",
            "building_area": 60.0,
        }
        market = SimpleNamespace()

        original_async = transaction_engine.safe_call_llm_async
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return {"bid_price": 980_000.0, "reason": "愿意出价"}

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            result = asyncio.run(
                transaction_engine.run_batch_bidding_async(
                    seller=seller,
                    buyers=[buyer],
                    listing=listing,
                    market=market,
                    month=1,
                    config=cfg,
                    db_conn=self.conn,
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async

        self.assertEqual(result["outcome"], "success")
        row_count = cur.execute(
            "SELECT COUNT(*) FROM property_buyer_matches WHERE month=1 AND property_id=9003 AND buyer_id=31"
        ).fetchone()[0]
        self.assertEqual(row_count, 1)
        row = cur.execute(
            """
            SELECT buyer_bid, proceeded_to_negotiation, order_id, selection_reason
            FROM property_buyer_matches
            WHERE month=1 AND property_id=9003 AND buyer_id=31
            """
        ).fetchone()
        self.assertEqual(row[0], 980_000.0)
        self.assertEqual(row[1], 1)
        self.assertEqual(row[2], 7001)
        self.assertEqual(row[3], "pre-logged")

    def test_run_negotiation_session_async_falls_back_to_classic_after_invalid_batch_interest(self):
        cfg = _Cfg(
            {
                "negotiation": {
                    "rounds_range": [2, 2],
                    "heuristic_gap_threshold": 0.2,
                    "market_conditions": {
                        "balanced": {"buyer_lowball": 0.9, "llm_hint": "balanced"},
                        "oversupply": {"buyer_lowball": 0.9, "llm_hint": "oversupply"},
                        "undersupply": {"buyer_lowball": 0.9, "llm_hint": "undersupply"},
                    },
                },
                "smart_agent.effective_bid_floor_ratio": 0.98,
                "smart_agent.min_offer_ratio_to_list": 0.75,
                "smart_agent.max_overbid_ratio_to_list": 0.15,
                "smart_agent.batch_rebid_enabled": False,
                "smart_agent.classic_competitive_enabled": False,
            }
        )
        buyer1 = _Buyer(
            id=51,
            cash=900_000.0,
            monthly_income=30_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_500_000.0,
            total_debt=0.0,
            preference=SimpleNamespace(max_price=2_000_000.0),
            story=SimpleNamespace(investment_style="balanced", negotiation_style="balanced"),
        )
        buyer2 = _Buyer(
            id=52,
            cash=900_000.0,
            monthly_income=28_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_400_000.0,
            total_debt=0.0,
            preference=SimpleNamespace(max_price=2_000_000.0),
            story=SimpleNamespace(investment_style="balanced", negotiation_style="balanced"),
        )
        seller = _Buyer(
            id=61,
            cash=500_000.0,
            monthly_income=20_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_000_000.0,
            total_debt=0.0,
            story=SimpleNamespace(investment_style="balanced", negotiation_style="balanced"),
        )
        listing = {
            "property_id": 9002,
            "listed_price": 665_321.0,
            "min_price": 650_000.0,
            "initial_value": 640_000.0,
            "zone": "B",
            "building_area": 60.0,
        }
        market = SimpleNamespace(properties=[{"property_id": 9002, "status": "for_sale", "zone": "B"}])

        responses = iter(
            [
                {"bid_price": 600_000.0, "reason": "batch lowball 1"},
                {"bid_price": 576_063.0, "reason": "batch lowball 2"},
                {"action": "OFFER", "offer_price": 650_000.0, "reason": "classic retry at floor"},
                {"action": "ACCEPT", "reason": "deal"},
            ]
        )

        original_async = transaction_engine.safe_call_llm_async
        original_decide = transaction_engine.decide_negotiation_format
        original_randint = transaction_engine.random.randint
        original_build_macro = transaction_engine.build_macro_context
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return next(responses)

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            transaction_engine.decide_negotiation_format = lambda seller, buyers, market_hint: "BATCH"
            transaction_engine.random.randint = lambda a, b: 2
            transaction_engine.build_macro_context = lambda month, config=None: ""

            result = asyncio.run(
                transaction_engine.run_negotiation_session_async(
                    seller=seller,
                    buyers=[buyer1, buyer2],
                    listing=listing,
                    market=market,
                    month=1,
                    config=cfg,
                    db_conn=None,
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async
            transaction_engine.decide_negotiation_format = original_decide
            transaction_engine.random.randint = original_randint
            transaction_engine.build_macro_context = original_build_macro

        self.assertEqual(result["outcome"], "success")
        self.assertEqual(result["buyer_id"], buyer1.id)
        self.assertEqual(result["mode"], "classic")
        actions = [e.get("action") for e in result["history"] if isinstance(e, dict)]
        self.assertIn("BATCH_FALLBACK_CLASSIC", actions)

    def test_negotiate_async_uses_current_month_macro_context(self):
        cfg = _Cfg(
            {
                "negotiation": {
                    "rounds_range": [2, 2],
                    "heuristic_gap_threshold": 0.2,
                    "market_conditions": {
                        "balanced": {"buyer_lowball": 0.95, "llm_hint": "balanced"},
                    },
                },
                "smart_agent.effective_bid_floor_ratio": 0.98,
            }
        )
        buyer = _Buyer(
            id=71,
            cash=900_000.0,
            monthly_income=30_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_500_000.0,
            total_debt=0.0,
            preference=SimpleNamespace(max_price=2_000_000.0),
            story=SimpleNamespace(investment_style="balanced", negotiation_style="balanced"),
        )
        seller = _Buyer(
            id=72,
            cash=500_000.0,
            monthly_income=20_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_000_000.0,
            total_debt=0.0,
            story=SimpleNamespace(investment_style="balanced", negotiation_style="balanced"),
        )
        listing = {
            "property_id": 9100,
            "listed_price": 665_321.0,
            "min_price": 650_000.0,
            "initial_value": 640_000.0,
            "zone": "B",
            "building_area": 60.0,
        }
        market = SimpleNamespace(properties=[{"property_id": 9100, "status": "for_sale", "zone": "B"}])

        seen_months = []
        responses = iter(
            [
                {"action": "OFFER", "offer_price": 650_000.0, "reason": "buyer opens at floor"},
                {"action": "ACCEPT", "reason": "seller agrees"},
            ]
        )
        original_async = transaction_engine.safe_call_llm_async
        original_build_macro = transaction_engine.build_macro_context
        original_randint = transaction_engine.random.randint
        try:
            async def _fake_safe_call_llm_async(*args, **kwargs):
                return next(responses)

            def _capture_build_macro(month, config=None):
                seen_months.append(int(month))
                return f"month={int(month)}"

            transaction_engine.safe_call_llm_async = _fake_safe_call_llm_async
            transaction_engine.build_macro_context = _capture_build_macro
            transaction_engine.random.randint = lambda a, b: 2

            result = asyncio.run(
                transaction_engine.negotiate_async(
                    buyer=buyer,
                    seller=seller,
                    listing=listing,
                    market=market,
                    potential_buyers_count=1,
                    config=cfg,
                    month=3,
                )
            )
        finally:
            transaction_engine.safe_call_llm_async = original_async
            transaction_engine.build_macro_context = original_build_macro
            transaction_engine.random.randint = original_randint

        self.assertEqual(result["outcome"], "success")
        self.assertEqual(seen_months, [3])

    def test_recovery_refill_relaxes_filters_for_near_equal_tiebreak(self):
        cfg = _Cfg(
            {
                "smart_agent.precheck_include_tax_and_fee": False,
                "smart_agent.precheck_liquidity_buffer_months": 0,
                "smart_agent.deposit_ratio": 0.1,
            }
        )
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()
        buyer = _Buyer(
            id=77,
            cash=600_000.0,
            monthly_income=20_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=900_000.0,
            total_debt=0.0,
            wait_months=0,
            max_wait_months=6,
            preference=SimpleNamespace(
                target_zone="B",
                need_school_district=True,
                min_bedrooms=3,
                max_price=1_000_000.0,
                max_wait_months=6,
            ),
            story=SimpleNamespace(
                education_path="public_school_priority",
                purchase_motive_primary="education_driven",
            ),
            agent_type="smart",
        )
        active_listings = [
            {"property_id": 501, "owner_id": 88, "zone": "A", "listed_price": 1_250_000.0, "is_school_district": False},
        ]
        cur.execute(
            "INSERT INTO properties_market(property_id, owner_id, status, last_transaction_month, current_valuation) VALUES (?, ?, ?, ?, ?)",
            (501, 88, "for_sale", None, 1_250_000.0),
        )
        props_map = {
            501: {
                "property_id": 501,
                "owner_id": 88,
                "zone": "A",
                "building_area": 95.0,
                "bedrooms": 2,
                "is_school_district": False,
            }
        }

        normal = svc._build_recovery_refill_candidates(
            cursor=cur,
            month=1,
            buyer=buyer,
            active_listings=active_listings,
            props_map=props_map,
            blocked_property_ids=set(),
            attempted_property_ids=set(),
            combined_pressure={},
            monthly_candidate_quota_used={},
            candidate_quota_cfg={},
            limit=6,
            emit_log=False,
            recovery_reason="Outbid: lost in negotiation",
        )
        tiebreak = svc._build_recovery_refill_candidates(
            cursor=cur,
            month=1,
            buyer=buyer,
            active_listings=active_listings,
            props_map=props_map,
            blocked_property_ids=set(),
            attempted_property_ids=set(),
            combined_pressure={},
            monthly_candidate_quota_used={},
            candidate_quota_cfg={},
            limit=6,
            emit_log=False,
            recovery_reason="Outbid: batch tie-break after near-equal final bids",
        )

        self.assertEqual(normal, [])
        self.assertEqual([item["property_id"] for item in tiebreak], [501])

    def test_due_settlement_skips_duplicate_affordability_check_after_replay(self):
        cfg = _Cfg(
            {
                "smart_agent.precheck_include_tax_and_fee": True,
                "smart_agent.precheck_liquidity_buffer_months": 0,
                "smart_agent.settlement_affordability_recheck_enabled": True,
                "smart_agent.deposit_ratio": 0.1,
                "smart_agent.order_expiry_months": 2,
                "smart_agent.settlement_lag_months": 1,
            }
        )
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()

        buyer = _Buyer(
            id=31,
            cash=320_000.0,
            monthly_income=30_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_000_000.0,
            total_debt=0.0,
            owned_properties=[],
            agent_type="smart",
        )
        seller = _Buyer(
            id=41,
            cash=100_000.0,
            monthly_income=20_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=2_000_000.0,
            total_debt=0.0,
            owned_properties=[{"property_id": 501, "status": "pending_settlement"}],
            agent_type="smart",
        )
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (buyer.id, buyer.mortgage_monthly_payment, buyer.cash, buyer.total_assets, buyer.total_debt, buyer.monthly_income),
        )
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (seller.id, seller.mortgage_monthly_payment, seller.cash, seller.total_assets, seller.total_debt, seller.monthly_income),
        )
        cur.execute(
            """
            INSERT INTO transaction_orders (
                order_id, created_month, expires_month, settlement_due_month,
                buyer_id, seller_id, property_id, offer_price, agreed_price,
                negotiation_rounds, deposit_amount, penalty_amount, status,
                close_month, close_reason, agent_type, prequal_cash, prequal_total_debt,
                prequal_owned_property_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                9001, 1, 3, 1,
                buyer.id, seller.id, 501, 1_000_000.0, 1_000_000.0,
                2, 100_000.0, 0.0, "pending_settlement",
                None, None, "smart", 420_000.0, 0.0, 0,
            ),
        )
        cur.execute(
            "INSERT INTO properties_market(property_id, owner_id, status, last_transaction_month, current_valuation) VALUES (?, ?, ?, ?, ?)",
            (501, seller.id, "pending_settlement", None, 1_000_000.0),
        )
        cur.execute(
            "INSERT INTO properties_static(property_id, zone, building_area, is_school_district, property_type) VALUES (?, ?, ?, ?, ?)",
            (501, "B", 92.0, 0, "residential"),
        )
        self.conn.commit()

        captured = {}
        original_execute = transaction_engine.execute_transaction

        def _fake_execute(
            buyer_arg,
            seller_arg,
            property_arg,
            final_price_arg,
            market_arg,
            config=None,
            skip_affordability_check=False,
            transaction_month=None,
        ):
            captured["skip_affordability_check"] = bool(skip_affordability_check)
            captured["transaction_month"] = transaction_month
            return {
                "price": float(final_price_arg),
                "down_payment": 300_000.0,
                "loan_amount": 700_000.0,
                "buyer_transaction_cost": 20_000.0,
                "seller_transaction_cost": 10_000.0,
            }

        transaction_engine.execute_transaction = _fake_execute
        try:
            settled = svc._process_due_settlements(
                cur,
                month=1,
                agent_map={buyer.id: buyer, seller.id: seller},
                props_map={501: {"property_id": 501, "owner_id": seller.id, "status": "pending_settlement"}},
                market=None,
            )
            self.conn.commit()
        finally:
            transaction_engine.execute_transaction = original_execute

        self.assertEqual(settled, 1)
        self.assertTrue(captured.get("skip_affordability_check"))
        self.assertEqual(captured.get("transaction_month"), 1)
        cur.execute("SELECT status, close_reason FROM transaction_orders WHERE order_id=9001")
        row = cur.fetchone()
        self.assertEqual(row, ("filled", "Settlement completed"))

    def test_create_order_prefers_listing_selection_snapshot_over_buyer_tail_context(self):
        cfg = _Cfg(
            {
                "smart_agent.precheck_include_tax_and_fee": True,
                "smart_agent.precheck_liquidity_buffer_months": 0,
                "smart_agent.deposit_ratio": 0.1,
            }
        )
        svc = TransactionService(cfg, self.conn)
        cur = self.conn.cursor()
        buyer = _Buyer(
            id=51,
            cash=800_000.0,
            monthly_income=40_000.0,
            mortgage_monthly_payment=0.0,
            total_assets=1_500_000.0,
            total_debt=0.0,
            agent_type="smart",
        )
        cur.execute(
            "INSERT INTO agents_finance(agent_id, mortgage_monthly_payment, cash, total_assets, total_debt, net_cashflow) VALUES (?, ?, ?, ?, ?, ?)",
            (buyer.id, buyer.mortgage_monthly_payment, buyer.cash, buyer.total_assets, buyer.total_debt, buyer.monthly_income),
        )
        cur.execute(
            "INSERT INTO properties_market(property_id, owner_id, status, last_transaction_month, current_valuation) VALUES (?, ?, ?, ?, ?)",
            (601, 71, "for_sale", None, 1_000_000.0),
        )
        cur.execute(
            "INSERT INTO properties_static(property_id, zone, building_area, is_school_district, property_type) VALUES (?, ?, ?, ?, ?)",
            (601, "B", 99.0, 0, "residential"),
        )
        self.conn.commit()

        buyer._last_buyer_match_context = {
            "selection_reason": "no_candidates_after_soft_relax",
            "selected_in_shortlist": False,
            "llm_monthly_intent": "NO_CANDIDATE",
        }
        buyer._last_buyer_match_retry_trace = [{"result": "no_selection"}]
        buyer._last_buyer_match_retry_budget = {"attempts_spent": 20}

        listing = {
            "property_id": 601,
            "seller_id": 71,
            "owner_id": 71,
            "listed_price": 1_000_000.0,
            "min_price": 900_000.0,
            "_selection_snapshot": {
                "selection_reason": "picked_this_property",
                "selected_in_shortlist": True,
                "llm_monthly_intent": "CONTINUE",
            },
            "_selection_retry_trace": [{"result": "selected", "property_id": 601}],
            "_selection_retry_budget": {"attempts_spent": 3},
        }

        entries = svc._create_orders_for_matches(
            cur,
            month=1,
            buyer_matches=[{"buyer": buyer, "listing": listing}],
            agent_map={buyer.id: buyer},
        )
        self.conn.commit()

        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["selection_reason"], "picked_this_property")
        self.assertTrue(entry["selected_in_shortlist"])
        self.assertEqual(entry["match_context"]["selection_reason"], "picked_this_property")
        self.assertEqual(entry["match_context"]["retry_trace"][0]["result"], "selected")
        self.assertEqual(entry["match_context"]["retry_budget"]["attempts_spent"], 3)


if __name__ == "__main__":
    unittest.main()
