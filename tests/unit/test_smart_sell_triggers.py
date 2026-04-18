import asyncio
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from database import init_db
from models import Agent
from services.agent_service import AgentService


class _Cfg:
    def __init__(self, data):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


class _Market:
    def __init__(self, prices, properties):
        self._prices = prices
        self.properties = properties

    def get_avg_price(self, zone):
        return float(self._prices.get(zone, 0.0))


def _build_conn():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE properties_market (
            property_id INTEGER PRIMARY KEY,
            status TEXT,
            listed_price REAL,
            min_price REAL,
            listing_month INTEGER,
            last_price_update_month INTEGER,
            last_price_update_reason TEXT
        )
        """
    )
    conn.commit()
    return conn


class TestSmartSellTriggers(unittest.TestCase):
    def test_stop_loss_trigger_priority(self):
        conn = _build_conn()
        cfg = _Cfg({"smart_agent.panic_sell_drawdown": 0.15, "smart_agent.take_profit_threshold": 0.20})
        svc = AgentService(cfg, conn)

        agent = Agent(id=1, cash=500_000, monthly_income=20_000)
        agent.agent_type = "smart"
        agent.owned_properties = [
            {"property_id": 101, "zone": "A", "base_value": 1_000_000},
            {"property_id": 102, "zone": "B", "base_value": 1_000_000},
        ]
        payload = svc._evaluate_smart_sell_triggers(agent, {"A": 700_000, "B": 1_300_000})
        self.assertEqual(payload["trigger_mode"], "STOP_LOSS")
        self.assertIn(101, payload["trigger_property_ids"])

    def test_take_profit_trigger_when_no_stop_loss(self):
        conn = _build_conn()
        cfg = _Cfg({"smart_agent.panic_sell_drawdown": 0.15, "smart_agent.take_profit_threshold": 0.20})
        svc = AgentService(cfg, conn)

        agent = Agent(id=2, cash=500_000, monthly_income=20_000)
        agent.agent_type = "smart"
        agent.owned_properties = [{"property_id": 201, "zone": "A", "base_value": 1_000_000}]
        payload = svc._evaluate_smart_sell_triggers(agent, {"A": 1_300_000})
        self.assertEqual(payload["trigger_mode"], "TAKE_PROFIT")
        self.assertEqual(payload["trigger_property_ids"], [201])

    @patch("services.agent_service.determine_listing_strategy")
    @patch("transaction_engine.generate_seller_listing")
    def test_stop_loss_guardrail_overrides_hold(self, mock_generate_listing, mock_determine_strategy):
        conn = _build_conn()
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO properties_market(property_id, status) VALUES(?, 'off_market')",
            [(301,), (302,)],
        )
        conn.commit()

        cfg = _Cfg({"smart_agent.panic_sell_drawdown": 0.10, "smart_agent.take_profit_threshold": 0.20})
        svc = AgentService(cfg, conn)

        agent = Agent(id=3, cash=500_000, monthly_income=20_000)
        agent.agent_type = "smart"
        agent.owned_properties = [
            {"property_id": 301, "zone": "A", "base_value": 1_000_000},
            {"property_id": 302, "zone": "B", "base_value": 1_000_000},
        ]

        market = _Market(
            prices={"A": 700_000, "B": 1_100_000},
            properties=[
                {"property_id": 301, "zone": "A"},
                {"property_id": 302, "zone": "B"},
            ],
        )

        mock_determine_strategy.return_value = (
            {
                "strategy": "D",
                "pricing_coefficient": 1.05,
                "properties_to_sell": [302],
                "reasoning": "hold for now",
            },
            {},
        )
        mock_generate_listing.side_effect = lambda _agent, prop, _market, _hint, pricing_coefficient=1.0: {
            "property_id": prop["property_id"],
            "listed_price": prop["base_value"] * pricing_coefficient,
            "min_price": prop["base_value"] * 0.9,
        }

        decision, metrics = svc._create_seller_listing(agent, market, month=1, market_trend="DOWN")
        self.assertEqual(decision.get("m11_trigger_mode"), "STOP_LOSS")
        self.assertTrue(decision.get("m11_guardrail_applied"))
        self.assertEqual(decision.get("strategy"), "C")
        self.assertLessEqual(float(decision.get("pricing_coefficient", 1.0)), 0.97)
        self.assertIn(301, decision.get("properties_to_sell", []))
        self.assertEqual(metrics.get("m11_trigger_mode"), "STOP_LOSS")

    @patch.object(AgentService, "_build_rule_listing_decision")
    @patch("transaction_engine.generate_seller_listing")
    def test_recent_purchase_is_blocked_from_resale_before_12_months(
        self,
        mock_generate_listing,
        mock_rule_listing_decision,
    ):
        conn = _build_conn()
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO properties_market(property_id, status) VALUES(?, 'off_market')",
            [(401,), (402,)],
        )
        conn.commit()

        cfg = _Cfg({"smart_agent.min_holding_months_before_resale": 12})
        svc = AgentService(cfg, conn)

        agent = Agent(id=4, cash=800_000, monthly_income=30_000)
        agent.agent_type = "normal"
        agent.owned_properties = [
            {"property_id": 401, "zone": "A", "base_value": 1_200_000.0, "acquired_month": 1},
            {"property_id": 402, "zone": "B", "base_value": 1_100_000.0},
        ]
        market = _Market(
            prices={"A": 20_000.0, "B": 18_000.0},
            properties=[{"property_id": 401, "zone": "A"}, {"property_id": 402, "zone": "B"}],
        )

        mock_rule_listing_decision.return_value = (
            {
                "strategy": "B",
                "pricing_coefficient": 1.0,
                "properties_to_sell": [401, 402],
                "reasoning": "rule decision",
            },
            {},
        )
        mock_generate_listing.side_effect = lambda _agent, prop, _market, _hint, pricing_coefficient=1.0: {
            "property_id": prop["property_id"],
            "listed_price": prop["base_value"] * pricing_coefficient,
            "min_price": prop["base_value"] * 0.9,
        }

        decision, metrics = svc._create_seller_listing(agent, market, month=6, market_trend="STABLE")

        self.assertEqual(decision.get("properties_to_sell"), [402])
        self.assertEqual(decision.get("recent_purchase_sell_blocked_property_ids"), [401])
        self.assertEqual(metrics.get("recent_purchase_sell_blocked_count"), 1)
        self.assertEqual(metrics.get("min_holding_months_before_resale"), 12)

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.batched_determine_role_async")
    @patch("transaction_engine.generate_seller_listing")
    def test_activate_new_seller_emits_listing_action_with_persona_metrics(
        self,
        mock_generate_listing,
        mock_batched_role,
        _mock_activation_score,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}\\seller_persona_test.db"
            init_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO properties_static (
                        property_id, zone, quality, building_area, property_type,
                        is_school_district, school_tier, price_per_sqm, zone_price_tier,
                        initial_value, build_year
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (401, "B", 3, 88.0, "residential", 0, 0, 18000.0, "mid", 1_584_000.0, 2018),
                )
                cur.execute(
                    """
                    INSERT INTO properties_market (
                        property_id, owner_id, status, current_valuation,
                        listed_price, min_price, listing_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (401, 41, "off_market", 1_584_000.0, None, None, None),
                )
                conn.commit()

                cfg = _Cfg(
                    {
                        "smart_agent.normal_seller_rule_pricing_enabled": True,
                        "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                    }
                )
                svc = AgentService(cfg, conn)

                agent = Agent(id=41, name="张海川", age=35, marital_status="married", cash=650_000, monthly_income=28_000)
                agent.story.purchase_motive_primary = "upgrade_living"
                agent.story.housing_stage = "owner_upgrade"
                agent.story.family_stage = "young_family"
                agent.story.education_path = "public_school"
                agent.story.financial_profile = "payment_sensitive"
                agent.story.seller_profile = "upgrade_swap"
                agent.owned_properties = [
                    {
                        "property_id": 401,
                        "zone": "B",
                        "base_value": 1_584_000.0,
                        "building_area": 88.0,
                        "status": "off_market",
                    }
                ]

                svc.agents = [agent]
                svc.agent_map = {agent.id: agent}

                market = _Market(
                    prices={"A": 40_000.0, "B": 18_000.0},
                    properties=[
                        {
                            "property_id": 401,
                            "zone": "B",
                            "base_value": 1_584_000.0,
                            "building_area": 88.0,
                            "status": "off_market",
                        }
                    ],
                )

                async def _fake_batch_role(*args, **kwargs):
                    return [
                        {
                            "id": agent.id,
                            "role": "SELLER",
                            "trigger": "upgrade_swap",
                            "reason": "Need to list current home before moving",
                            "life_pressure": "balanced",
                            "price_expectation": 1.0,
                            "risk_mode": "balanced",
                        }
                    ]

                mock_batched_role.side_effect = _fake_batch_role
                mock_generate_listing.side_effect = lambda _agent, prop, _market, _hint, pricing_coefficient=1.0: {
                    "property_id": prop["property_id"],
                    "listed_price": prop["base_value"] * pricing_coefficient,
                    "min_price": prop["base_value"] * 0.92,
                }

                batch_decision_logs = []
                buyers, decisions, _bulletin_exposure = asyncio.run(
                    svc.activate_new_agents(
                        month=1,
                        market=market,
                        macro_desc="市场平稳",
                        batch_decision_logs=batch_decision_logs,
                        market_trend="STABLE",
                        market_bulletin="",
                        recent_bulletins=[],
                    )
                )

                self.assertEqual(buyers, [])
                self.assertEqual(len(decisions), 1)

                listing_logs = [
                    entry for entry in batch_decision_logs
                    if entry[2] == "LISTING_ACTION" and entry[3] == "LIST"
                ]
                self.assertEqual(len(listing_logs), 1)

                _, log_month, event_type, decision, reason, thought_json, metrics_json, llm_called = listing_logs[0]
                self.assertEqual(log_month, 1)
                self.assertEqual(event_type, "LISTING_ACTION")
                self.assertEqual(decision, "LIST")
                self.assertFalse(llm_called)
                self.assertIn("Rule-based normal seller", reason)

                thought = json.loads(thought_json)
                metrics = json.loads(metrics_json)

                self.assertEqual(thought["properties_to_sell"], [401])
                self.assertEqual(metrics["seller_persona_snapshot"]["purchase_motive_primary"], "upgrade_living")
                self.assertEqual(metrics["seller_persona_snapshot"]["seller_profile"], "upgrade_swap")
                self.assertEqual(metrics["seller_persona_snapshot"]["financial_profile"], "payment_sensitive")
                self.assertEqual(metrics["properties_to_sell"], [401])

                cur.execute(
                    "SELECT status, listed_price, min_price, listing_month FROM properties_market WHERE property_id = 401"
                )
                status, listed_price, min_price, listing_month = cur.fetchone()
                self.assertEqual(status, "for_sale")
                self.assertGreater(float(listed_price), 0.0)
                self.assertGreater(float(min_price), 0.0)
                self.assertEqual(listing_month, 1)
            finally:
                conn.close()

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.batched_determine_role_async")
    @patch("services.agent_service.determine_listing_strategy")
    @patch("transaction_engine.generate_seller_listing")
    def test_activate_new_normal_seller_logs_llm_listing_when_rule_pricing_disabled(
        self,
        mock_generate_listing,
        mock_determine_strategy,
        mock_batched_role,
        _mock_activation_score,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}\\seller_persona_llm_test.db"
            init_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO properties_static (
                        property_id, zone, quality, building_area, property_type,
                        is_school_district, school_tier, price_per_sqm, zone_price_tier,
                        initial_value, build_year
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (451, "B", 3, 90.0, "residential", 0, 0, 18500.0, "mid", 1_665_000.0, 2019),
                )
                cur.execute(
                    """
                    INSERT INTO properties_market (
                        property_id, owner_id, status, current_valuation,
                        listed_price, min_price, listing_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (451, 45, "off_market", 1_665_000.0, None, None, None),
                )
                conn.commit()

                cfg = _Cfg(
                    {
                        "smart_agent.normal_seller_rule_pricing_enabled": False,
                        "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                    }
                )
                svc = AgentService(cfg, conn)

                agent = Agent(id=45, name="李青", age=36, marital_status="married", cash=720_000, monthly_income=31_000)
                agent.story.purchase_motive_primary = "upgrade_living"
                agent.story.housing_stage = "owner_upgrade"
                agent.story.family_stage = "young_family"
                agent.story.education_path = "public_school"
                agent.story.financial_profile = "payment_sensitive"
                agent.story.seller_profile = "upgrade_swap"
                agent.owned_properties = [
                    {
                        "property_id": 451,
                        "zone": "B",
                        "base_value": 1_665_000.0,
                        "building_area": 90.0,
                        "status": "off_market",
                    }
                ]

                svc.agents = [agent]
                svc.agent_map = {agent.id: agent}

                market = _Market(
                    prices={"A": 41_000.0, "B": 18_500.0},
                    properties=[
                        {
                            "property_id": 451,
                            "zone": "B",
                            "base_value": 1_665_000.0,
                            "building_area": 90.0,
                            "status": "off_market",
                        }
                    ],
                )

                async def _fake_batch_role(*args, **kwargs):
                    return [
                        {
                            "id": agent.id,
                            "role": "SELLER",
                            "trigger": "upgrade_swap",
                            "reason": "Need to list current home before moving",
                            "life_pressure": "balanced",
                            "price_expectation": 1.03,
                            "risk_mode": "balanced",
                        }
                    ]

                mock_batched_role.side_effect = _fake_batch_role
                mock_determine_strategy.return_value = (
                    {
                        "properties_to_sell": [451],
                        "pricing_coefficient": 1.03,
                        "strategy": "E",
                        "reasoning": "Nearby owners seem to be nudging prices up, so I will follow the crowd a bit.",
                    },
                    {
                        "strategy_source": "llm_listing",
                    },
                )
                mock_generate_listing.side_effect = lambda _agent, prop, _market, _hint, pricing_coefficient=1.0: {
                    "property_id": prop["property_id"],
                    "listed_price": prop["base_value"] * pricing_coefficient,
                    "min_price": prop["base_value"] * 0.92,
                }

                batch_decision_logs = []
                buyers, decisions, _bulletin_exposure = asyncio.run(
                    svc.activate_new_agents(
                        month=1,
                        market=market,
                        macro_desc="市场平稳",
                        batch_decision_logs=batch_decision_logs,
                        market_trend="UP",
                        market_bulletin="",
                        recent_bulletins=[],
                    )
                )

                self.assertEqual(buyers, [])
                self.assertEqual(len(decisions), 1)

                listing_logs = [
                    entry for entry in batch_decision_logs
                    if entry[2] == "LISTING_ACTION" and entry[3] == "LIST"
                ]
                self.assertEqual(len(listing_logs), 1)

                _, log_month, event_type, decision, _reason, thought_json, metrics_json, llm_called = listing_logs[0]
                self.assertEqual(log_month, 1)
                self.assertEqual(event_type, "LISTING_ACTION")
                self.assertEqual(decision, "LIST")
                self.assertTrue(llm_called)

                thought = json.loads(thought_json)
                metrics = json.loads(metrics_json)

                self.assertEqual(thought["strategy"], "E")
                self.assertEqual(metrics["listing_decision_mode"], "llm")
                self.assertTrue(metrics["listing_llm_called"])
                self.assertEqual(metrics["strategy_source"], "llm_listing")

                cur.execute(
                    "SELECT status, listed_price, min_price, listing_month FROM properties_market WHERE property_id = 451"
                )
                status, listed_price, min_price, listing_month = cur.fetchone()
                self.assertEqual(status, "for_sale")
                self.assertGreater(float(listed_price), 0.0)
                self.assertGreater(float(min_price), 0.0)
                self.assertEqual(listing_month, 1)
            finally:
                conn.close()

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.generate_buyer_preference")
    @patch("services.agent_service.batched_determine_role_async")
    def test_role_change_withdraw_emits_persona_metrics(
        self,
        mock_batched_role,
        mock_generate_buyer_preference,
        _mock_activation_score,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}\\seller_withdraw_test.db"
            init_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO properties_static (
                        property_id, zone, quality, building_area, property_type,
                        is_school_district, school_tier, price_per_sqm, zone_price_tier,
                        initial_value, build_year
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (402, "A", 4, 102.0, "residential", 1, 2, 38_000.0, "high", 3_876_000.0, 2017),
                )
                cur.execute(
                    """
                    INSERT INTO properties_market (
                        property_id, owner_id, status, current_valuation,
                        listed_price, min_price, listing_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (402, 42, "for_sale", 3_876_000.0, 4_020_000.0, 3_780_000.0, 1),
                )
                conn.commit()

                cfg = _Cfg({
                    "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                    "smart_agent.role_change_auto_withdraw_enabled": True,
                })
                svc = AgentService(cfg, conn)

                agent = Agent(id=42, name="王嘉衡", age=39, marital_status="married", cash=700_000, monthly_income=32_000)
                agent.story.purchase_motive_primary = "upgrade_living"
                agent.story.housing_stage = "owner_upgrade"
                agent.story.family_stage = "young_family"
                agent.story.education_path = "public_school"
                agent.story.financial_profile = "payment_sensitive"
                agent.story.seller_profile = "upgrade_swap"
                agent.owned_properties = [
                    {
                        "property_id": 402,
                        "zone": "A",
                        "base_value": 3_876_000.0,
                        "building_area": 102.0,
                        "status": "for_sale",
                        "listed_price": 4_020_000.0,
                        "min_price": 3_780_000.0,
                        "is_school_district": 1,
                    }
                ]

                svc.agents = [agent]
                svc.agent_map = {agent.id: agent}

                market = _Market(
                    prices={"A": 39_000.0, "B": 18_000.0},
                    properties=[
                        {
                            "property_id": 402,
                            "zone": "A",
                            "base_value": 3_876_000.0,
                            "building_area": 102.0,
                            "status": "for_sale",
                            "listed_price": 4_020_000.0,
                            "min_price": 3_780_000.0,
                            "is_school_district": 1,
                        }
                    ],
                )

                async def _fake_batch_role(*args, **kwargs):
                    return [
                        {
                            "id": agent.id,
                            "role": "BUYER",
                            "trigger": "starter_home",
                            "reason": "Wants to buy but should withdraw old listing first",
                            "life_pressure": "balanced",
                            "price_expectation": 1.0,
                            "risk_mode": "balanced",
                        }
                    ]

                mock_batched_role.side_effect = _fake_batch_role
                async def _fake_pref(*args, **kwargs):
                    pref = type(
                        "Pref",
                        (),
                        {
                            "max_price": 4_500_000.0,
                            "zones": ["A", "B"],
                            "target_zone": "A",
                            "max_wait_months": 3,
                        },
                    )()
                    return pref, "Need a replacement home", {"x": 1}

                mock_generate_buyer_preference.side_effect = _fake_pref

                batch_decision_logs = []
                buyers, decisions, _bulletin_exposure = asyncio.run(
                    svc.activate_new_agents(
                        month=2,
                        market=market,
                        macro_desc="市场平稳",
                        batch_decision_logs=batch_decision_logs,
                        market_trend="STABLE",
                        market_bulletin="",
                        recent_bulletins=[],
                    )
                )

                self.assertEqual(len(buyers), 1)
                self.assertEqual(len(decisions), 1)

                withdraw_logs = [
                    entry for entry in batch_decision_logs
                    if entry[2] == "LISTING_ACTION" and entry[3] == "WITHDRAW"
                ]
                self.assertEqual(len(withdraw_logs), 1)
                metrics = json.loads(withdraw_logs[0][6])
                self.assertEqual(metrics["seller_persona_snapshot"]["seller_profile"], "upgrade_swap")
                self.assertEqual(metrics["property_snapshot"]["property_id"], 402)
                self.assertEqual(metrics["property_snapshot"]["zone"], "A")
                self.assertIn("listing:role_auto_withdraw", metrics["withdraw_reason_tags"])

                row = conn.execute(
                    "SELECT status FROM properties_market WHERE property_id=402"
                ).fetchone()
                self.assertEqual(row[0], "off_market")
            finally:
                conn.close()

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.determine_buyer_seller_chain_mode_async")
    @patch("services.agent_service.batched_determine_role_async")
    def test_buyer_seller_split_wait_falls_back_to_chain_mode(
        self,
        mock_batched_role,
        mock_chain_split,
        _mock_activation_score,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}\\buyer_seller_wait_fallback_test.db"
            init_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                cfg = _Cfg(
                    {
                        "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                        "smart_agent.buyer_seller_intent_split.enabled": True,
                    }
                )
                svc = AgentService(cfg, conn)

                agent = Agent(id=51, name="陈朗", age=34, marital_status="married", cash=900_000, monthly_income=30_000)
                agent.agent_type = "normal"
                agent.story.purchase_motive_primary = "upgrade_living"
                agent.story.housing_stage = "owner_upgrade"
                agent.story.family_stage = "young_family"
                agent.story.education_path = "public_school"
                agent.story.financial_profile = "balanced"
                agent.owned_properties = [
                    {
                        "property_id": 5101,
                        "zone": "B",
                        "base_value": 1_800_000.0,
                        "status": "off_market",
                    }
                ]
                svc.agents = [agent]
                svc.agent_map = {agent.id: agent}

                market = _Market(prices={"A": 40_000.0, "B": 18_000.0}, properties=[])

                async def _fake_batch_role(*args, **kwargs):
                    return [
                        {
                            "id": agent.id,
                            "role": "BUYER_SELLER",
                            "trigger": "upgrade_cycle",
                            "reason": "Need replacement decision",
                            "life_pressure": "balanced",
                            "price_expectation": 1.0,
                            "risk_mode": "balanced",
                        }
                    ]

                async def _fake_chain_split(*args, **kwargs):
                    return {
                        "chain_mode": "wait",
                        "reason": "Market is hot and family can wait one more month",
                        "llm_called": True,
                    }

                mock_batched_role.side_effect = _fake_batch_role
                mock_chain_split.side_effect = _fake_chain_split

                batch_decision_logs = []
                buyers, decisions, _bulletin_exposure = asyncio.run(
                    svc.activate_new_agents(
                        month=1,
                        market=market,
                        macro_desc="市场偏热",
                        batch_decision_logs=batch_decision_logs,
                        market_trend="HOT",
                        market_bulletin="",
                        recent_bulletins=[],
                    )
                )

                self.assertEqual(len(buyers), 1)
                self.assertEqual(len(decisions), 1)
                self.assertEqual(mock_chain_split.call_count, 1)

                role_logs = [entry for entry in batch_decision_logs if entry[2] == "ROLE_DECISION"]
                self.assertEqual(len(role_logs), 1)
                self.assertEqual(role_logs[0][3], "BUYER_SELLER")
                role_metrics = json.loads(role_logs[0][6])
                self.assertEqual(role_metrics["buyer_seller_split_choice"], "buy_first")
                self.assertTrue(role_metrics["buyer_seller_split_llm_called"])

                chain_logs = [entry for entry in batch_decision_logs if entry[2] == "CHAIN_MODE"]
                self.assertEqual(len(chain_logs), 1)
                self.assertEqual(chain_logs[0][3], "BUY_FIRST")

                row = conn.execute(
                    "SELECT role, chain_mode FROM active_participants WHERE agent_id = ?",
                    (agent.id,),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row[0], "BUYER_SELLER")
                self.assertEqual(row[1], "buy_first")
            finally:
                conn.close()

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.generate_buyer_preference")
    @patch("services.agent_service.AgentService._create_seller_listing")
    @patch("services.agent_service.determine_buyer_seller_chain_mode_async")
    @patch("services.agent_service.batched_determine_role_async")
    def test_buyer_seller_split_buy_first_persists_chain_mode(
        self,
        mock_batched_role,
        mock_chain_split,
        mock_create_listing,
        mock_generate_buyer_preference,
        _mock_activation_score,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}\\buyer_seller_buy_first_test.db"
            init_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                cfg = _Cfg(
                    {
                        "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                        "smart_agent.buyer_seller_intent_split.enabled": True,
                    }
                )
                svc = AgentService(cfg, conn)

                agent = Agent(id=52, name="赵宁", age=36, marital_status="married", cash=1_500_000, monthly_income=45_000)
                agent.agent_type = "normal"
                agent.story.purchase_motive_primary = "upgrade_living"
                agent.story.housing_stage = "owner_upgrade"
                agent.story.family_stage = "junior_school_transition"
                agent.story.education_path = "public_school"
                agent.story.financial_profile = "payment_sensitive"
                agent.owned_properties = [
                    {
                        "property_id": 5201,
                        "zone": "A",
                        "base_value": 3_600_000.0,
                        "status": "off_market",
                        "is_school_district": 1,
                    }
                ]
                svc.agents = [agent]
                svc.agent_map = {agent.id: agent}

                market = _Market(
                    prices={"A": 40_000.0, "B": 18_000.0},
                    properties=[
                        {
                            "property_id": 5201,
                            "zone": "A",
                            "base_value": 3_600_000.0,
                            "status": "off_market",
                            "is_school_district": 1,
                        }
                    ],
                )

                async def _fake_batch_role(*args, **kwargs):
                    return [
                        {
                            "id": agent.id,
                            "role": "BUYER_SELLER",
                            "trigger": "upgrade_cycle",
                            "reason": "Family wants to upgrade near school",
                            "life_pressure": "urgent",
                            "price_expectation": 1.02,
                            "risk_mode": "balanced",
                        }
                    ]

                async def _fake_chain_split(*args, **kwargs):
                    return {
                        "chain_mode": "buy_first",
                        "reason": "School window is approaching, continue buying first",
                        "llm_called": True,
                    }

                async def _fake_pref(*args, **kwargs):
                    pref = type(
                        "Pref",
                        (),
                        {
                            "max_price": 4_800_000.0,
                            "zones": ["A", "B"],
                            "target_zone": "A",
                            "max_wait_months": 4,
                            "target_buy_price": 4_500_000.0,
                            "risk_mode": "balanced",
                        },
                    )()
                    return pref, "Need better school fit", {"pref_source": "test"}

                mock_batched_role.side_effect = _fake_batch_role
                mock_chain_split.side_effect = _fake_chain_split
                mock_generate_buyer_preference.side_effect = _fake_pref
                mock_create_listing.return_value = (
                    {
                        "properties_to_sell": [5201],
                        "pricing_coefficient": 1.0,
                        "strategy": "B",
                        "reasoning": "list old home while shopping",
                    },
                    {"listing_llm_called": False},
                )

                batch_decision_logs = []
                buyers, decisions, _bulletin_exposure = asyncio.run(
                    svc.activate_new_agents(
                        month=1,
                        market=market,
                        macro_desc="市场平稳",
                        batch_decision_logs=batch_decision_logs,
                        market_trend="STABLE",
                        market_bulletin="",
                        recent_bulletins=[],
                    )
                )

                self.assertEqual(len(decisions), 1)
                self.assertEqual(len(buyers), 1)
                self.assertEqual(getattr(buyers[0], "chain_mode", None), "buy_first")
                self.assertEqual(mock_chain_split.call_count, 1)

                role_logs = [entry for entry in batch_decision_logs if entry[2] == "ROLE_DECISION"]
                self.assertEqual(len(role_logs), 1)
                self.assertEqual(role_logs[0][3], "BUYER_SELLER")
                role_metrics = json.loads(role_logs[0][6])
                self.assertEqual(role_metrics["buyer_seller_split_choice"], "buy_first")

                row = conn.execute(
                    "SELECT role, chain_mode FROM active_participants WHERE agent_id = ?",
                    (agent.id,),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row[0], "BUYER_SELLER")
                self.assertEqual(row[1], "buy_first")
            finally:
                conn.close()

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.generate_buyer_preference")
    @patch("services.agent_service.AgentService._create_seller_listing")
    @patch("services.agent_service.determine_buyer_seller_chain_mode_async")
    @patch("services.agent_service.batched_determine_role_async")
    def test_buyer_seller_split_skips_when_initial_chain_mode_exists(
        self,
        mock_batched_role,
        mock_chain_split,
        mock_create_listing,
        mock_generate_buyer_preference,
        _mock_activation_score,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = f"{tmpdir}\\buyer_seller_skip_explicit_chain_mode.db"
            init_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                cfg = _Cfg(
                    {
                        "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                        "smart_agent.buyer_seller_intent_split.enabled": True,
                    }
                )
                svc = AgentService(cfg, conn)

                agent = Agent(id=53, name="孙越", age=38, marital_status="married", cash=520_000, monthly_income=28_000)
                agent.agent_type = "normal"
                agent.story.purchase_motive_primary = "education_driven"
                agent.story.housing_stage = "owner_upgrade"
                agent.story.family_stage = "primary_school_before_transition"
                agent.story.education_path = "public_school"
                agent.story.financial_profile = "payment_sensitive"
                agent.owned_properties = [
                    {
                        "property_id": 5301,
                        "zone": "B",
                        "base_value": 1_650_000.0,
                        "status": "off_market",
                        "is_school_district": 0,
                    }
                ]
                svc.agents = [agent]
                svc.agent_map = {agent.id: agent}

                market = _Market(
                    prices={"A": 40_000.0, "B": 18_000.0},
                    properties=[
                        {
                            "property_id": 5301,
                            "zone": "B",
                            "base_value": 1_650_000.0,
                            "status": "off_market",
                            "is_school_district": 0,
                        }
                    ],
                )

                async def _fake_batch_role(*args, **kwargs):
                    return [
                        {
                            "id": agent.id,
                            "role": "BUYER_SELLER",
                            "trigger": "school_upgrade",
                            "reason": "Need to replace current home before school transition",
                            "life_pressure": "urgent",
                            "price_expectation": 1.03,
                            "chain_mode": "sell_first",
                            "risk_mode": "balanced",
                        }
                    ]

                async def _fake_pref(*args, **kwargs):
                    pref = type(
                        "Pref",
                        (),
                        {
                            "max_price": 4_200_000.0,
                            "zones": ["A", "B"],
                            "target_zone": "A",
                            "max_wait_months": 3,
                            "target_buy_price": 3_900_000.0,
                            "risk_mode": "balanced",
                        },
                    )()
                    return pref, "Need better school fit", {"pref_source": "test"}

                mock_batched_role.side_effect = _fake_batch_role
                mock_generate_buyer_preference.side_effect = _fake_pref
                mock_create_listing.return_value = (
                    {
                        "properties_to_sell": [5301],
                        "pricing_coefficient": 1.0,
                        "strategy": "B",
                        "reasoning": "sell current home first",
                    },
                    {"listing_llm_called": False},
                )

                batch_decision_logs = []
                buyers, decisions, _bulletin_exposure = asyncio.run(
                    svc.activate_new_agents(
                        month=1,
                        market=market,
                        macro_desc="市场平稳",
                        batch_decision_logs=batch_decision_logs,
                        market_trend="STABLE",
                        market_bulletin="",
                        recent_bulletins=[],
                    )
                )

                self.assertEqual(len(decisions), 1)
                self.assertEqual(len(buyers), 0)
                self.assertEqual(mock_chain_split.call_count, 0)

                role_logs = [entry for entry in batch_decision_logs if entry[2] == "ROLE_DECISION"]
                self.assertEqual(len(role_logs), 1)
                self.assertEqual(role_logs[0][3], "BUYER_SELLER")

                chain_logs = [entry for entry in batch_decision_logs if entry[2] == "CHAIN_MODE"]
                self.assertEqual(len(chain_logs), 0)

                row = conn.execute(
                    "SELECT role, chain_mode FROM active_participants WHERE agent_id = ?",
                    (agent.id,),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row[0], "BUYER_SELLER")
                self.assertEqual(row[1], "sell_first")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
