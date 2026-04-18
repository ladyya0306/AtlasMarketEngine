import asyncio
import json
import os
import sqlite3
import unittest
from unittest.mock import patch

from agent_behavior import (
    TIMING_ROLE_BUY_NOW,
    TIMING_ROLE_SELL_THEN_BUY,
    build_activation_lifecycle_packet,
    batched_determine_role_async,
)
from database import init_db
from models import Agent, AgentPreference, Market
from services.agent_service import AgentService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}
        self.life_events = {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class _Market:
    def __init__(self):
        self.properties = []

    def get_avg_price(self, zone):
        return 0.0


class TestActivationTimingRoles(unittest.TestCase):
    def test_lifecycle_packet_captures_window_labels(self):
        agent = Agent(id=1, age=36, marital_status="married", cash=200_000, monthly_income=25_000)
        agent.children_ages = [6, 2]
        agent.story.housing_stage = "owner_upgrade"
        agent.story.family_stage = "primary_school_before_transition"
        agent.story.education_path = "public_school_district_priority"
        agent.story.purchase_motive_primary = "upgrade_living"
        agent.story.housing_need = "改善换房，老人同住"
        agent.owned_properties = [
            {"property_id": 11, "building_area": 68.0, "acquired_month": 4},
        ]

        packet = build_activation_lifecycle_packet(
            agent,
            month=10,
            min_cash_observer=500_000,
            holding_lock_months=12,
        )

        self.assertIn("SPACE_SQUEEZE", packet["labels"])
        self.assertIn("DEADLINE_PRESSURE", packet["labels"])
        self.assertIn("CHAIN_BLOCKED", packet["labels"])
        self.assertIn("RECENTLY_PURCHASED_LOCKED", packet["labels"])
        self.assertEqual(packet["entry_window"], "immediate_window")

    def test_lifecycle_packet_captures_seller_market_signal_labels(self):
        agent = Agent(id=7, age=39, marital_status="married", cash=680_000, monthly_income=28_000)
        agent.story.housing_stage = "owner_upgrade"
        agent.story.housing_need = "置换老房，等待更合适窗口"
        agent.owned_properties = [
            {"property_id": 71, "building_area": 92.0, "acquired_month": 3},
        ]

        packet = build_activation_lifecycle_packet(
            agent,
            month=9,
            min_cash_observer=500_000,
            holding_lock_months=12,
            market_signal_packet={
                "local_price_push_window": True,
                "local_price_push_detail": "A区/非学区/改善 最近两回合真实出价抬升",
                "replacement_old_home_release": True,
                "replacement_release_detail": "旧房已满足释放窗口，可由LLM判断是否挂牌",
            },
        )

        self.assertIn("LOCAL_PRICE_PUSH_WINDOW", packet["labels"])
        self.assertIn("REPLACEMENT_OLD_HOME_RELEASE", packet["labels"])
        self.assertEqual(packet["entry_window"], "immediate_window")
        self.assertIn("真实出价抬升", packet["summary"])
        self.assertIn("挂牌", packet["market_signal_summary"])

    def test_mock_activation_emits_structured_timing_role(self):
        buyer = Agent(id=2, age=34, marital_status="married", cash=900_000, monthly_income=30_000)
        buyer.children_ages = [6]
        buyer.story.family_stage = "primary_school_before_transition"
        buyer.story.education_path = "public_school_district_priority"
        buyer.story.housing_need = "学区改善"

        owner = Agent(id=3, age=35, marital_status="married", cash=120_000, monthly_income=20_000)
        owner.children_ages = [4, 1]
        owner.story.housing_stage = "owner_upgrade"
        owner.story.family_stage = "young_family"
        owner.story.purchase_motive_primary = "upgrade_living"
        owner.story.housing_need = "改善换房"
        owner.owned_properties = [{"property_id": 31, "building_area": 72.0}]

        with patch.dict(os.environ, {"LLM_MOCK_MODE": "true"}):
            results = asyncio.run(
                batched_determine_role_async(
                    [buyer, owner],
                    month=6,
                    market=Market(),
                    macro_summary="平稳",
                    market_trend="STABLE",
                    recent_bulletins=[],
                    min_cash_observer=500_000,
                    decision_profile="smart",
                )
            )

        by_id = {row["id"]: row for row in results}
        self.assertEqual(by_id[2]["timing_role"], TIMING_ROLE_BUY_NOW)
        self.assertEqual(by_id[2]["urgency_level"], "high")
        self.assertEqual(by_id[3]["timing_role"], TIMING_ROLE_SELL_THEN_BUY)
        self.assertEqual(by_id[3]["chain_mode"], "sell_first")
        self.assertIn("behavior_modifier", by_id[3])

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.generate_buyer_preference")
    @patch("services.agent_service.batched_determine_role_async")
    def test_activate_new_agents_persists_timing_role_and_lifecycle_audit(
        self,
        mock_batched_role,
        mock_generate_buyer_preference,
        _mock_activation_score,
    ):
        db_path = os.path.join(os.getcwd(), "test_activation_timing_role_runtime.db")
        if os.path.exists(db_path):
            os.remove(db_path)
        init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            cfg = _Cfg(
                {
                    "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                    "smart_agent.role_decision_optimization.enable_signature_cache": False,
                    "smart_agent.role_decision_optimization.enable_observer_freeze": False,
                }
            )
            svc = AgentService(cfg, conn)

            agent = Agent(id=11, age=33, marital_status="married", cash=800_000, monthly_income=28_000)
            agent.children_ages = [6]
            agent.agent_type = "normal"
            agent.story.family_stage = "primary_school_before_transition"
            agent.story.education_path = "public_school_district_priority"
            agent.story.housing_need = "为了学区准备购房"
            svc.agents = [agent]
            svc.agent_map = {agent.id: agent}

            async def _fake_batch_role(*args, **kwargs):
                return [
                    {
                        "id": 11,
                        "role": "BUYER",
                        "timing_role": "buy_now",
                        "trigger": "life_window",
                        "reason": "本月窗口打开，准备立即买入。",
                        "urgency_level": "high",
                        "life_pressure": "urgent",
                        "price_expectation": 1.0,
                        "risk_mode": "balanced",
                        "lifecycle_labels": ["DEADLINE_PRESSURE", "LIQUIDITY_READY"],
                        "lifecycle_summary": "DEADLINE_PRESSURE:primary_school_before_transition | LIQUIDITY_READY:cash=800,000",
                    }
                ]

            async def _fake_buyer_preference(*args, **kwargs):
                pref = AgentPreference(
                    target_zone="A",
                    max_price=2_000_000.0,
                    min_bedrooms=2,
                    need_school_district=True,
                    max_affordable_price=2_000_000.0,
                    psychological_price=1_800_000.0,
                )
                return pref, "pref ready", {"buyer_pref_snapshot": {"zone": "A"}}

            mock_batched_role.side_effect = _fake_batch_role
            mock_generate_buyer_preference.side_effect = _fake_buyer_preference

            batch_decision_logs = []
            asyncio.run(
                svc.activate_new_agents(
                    month=3,
                    market=_Market(),
                    macro_desc="平稳",
                    batch_decision_logs=batch_decision_logs,
                    market_trend="STABLE",
                    market_bulletin="",
                    recent_bulletins=[],
                )
            )

            row = conn.execute(
                """
                SELECT timing_role, decision_urgency, lifecycle_labels, lifecycle_summary
                FROM active_participants
                WHERE agent_id = ?
                """,
                (11,),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "buy_now")
            self.assertEqual(row[1], "high")
            self.assertEqual(json.loads(row[2]), ["DEADLINE_PRESSURE", "LIQUIDITY_READY"])
            self.assertIn("DEADLINE_PRESSURE", row[3])

            role_logs = [entry for entry in batch_decision_logs if entry[2] == "ROLE_DECISION"]
            self.assertTrue(role_logs)
            metrics = json.loads(role_logs[0][6])
            self.assertEqual(metrics["timing_role"], "buy_now")
            self.assertEqual(metrics["decision_urgency"], "high")
            self.assertEqual(metrics["lifecycle_labels"], ["DEADLINE_PRESSURE", "LIQUIDITY_READY"])
        finally:
            conn.close()
            if os.path.exists(db_path):
                os.remove(db_path)

    @patch("services.agent_service.calculate_activation_probability", return_value=0.5)
    @patch("services.agent_service.generate_buyer_preference")
    @patch("services.agent_service.batched_determine_role_async")
    def test_outbid_buy_lane_override_rewrites_role_payload_for_audit(
        self,
        mock_batched_role,
        mock_generate_buyer_preference,
        _mock_activation_score,
    ):
        db_path = os.path.join(os.getcwd(), "test_activation_buy_lane_override_runtime.db")
        if os.path.exists(db_path):
            os.remove(db_path)
        init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            cfg = _Cfg(
                {
                    "smart_agent.role_decision_optimization.enable_uncertainty_router": False,
                    "smart_agent.role_decision_optimization.enable_signature_cache": False,
                    "smart_agent.role_decision_optimization.enable_observer_freeze": False,
                }
            )
            svc = AgentService(cfg, conn)

            agent = Agent(id=21, age=31, marital_status="married", cash=900_000, monthly_income=26_000)
            agent.agent_type = "normal"
            agent.story.family_stage = "young_family"
            agent.story.housing_need = "上轮竞败后继续找房"
            agent._buy_task_locked = True
            agent._search_exhausted = False
            agent.buy_completed = 0
            svc.agents = [agent]
            svc.agent_map = {agent.id: agent}

            async def _fake_batch_role(*args, **kwargs):
                return [
                    {
                        "id": 21,
                        "role": "OBSERVER",
                        "timing_role": "observe_wait",
                        "trigger": "cooldown",
                        "reason": "本月继续观望。",
                        "urgency_level": "low",
                        "life_pressure": "patient",
                        "price_expectation": 1.0,
                        "risk_mode": "balanced",
                    }
                ]

            async def _fake_buyer_preference(*args, **kwargs):
                pref = AgentPreference(
                    target_zone="B",
                    max_price=1_800_000.0,
                    min_bedrooms=2,
                    need_school_district=False,
                    max_affordable_price=1_800_000.0,
                    psychological_price=1_600_000.0,
                )
                return pref, "pref ready", {"buyer_pref_snapshot": {"zone": "B"}}

            mock_batched_role.side_effect = _fake_batch_role
            mock_generate_buyer_preference.side_effect = _fake_buyer_preference

            batch_decision_logs = []
            asyncio.run(
                svc.activate_new_agents(
                    month=2,
                    market=_Market(),
                    macro_desc="平稳",
                    batch_decision_logs=batch_decision_logs,
                    market_trend="STABLE",
                    market_bulletin="",
                    recent_bulletins=[],
                )
            )

            row = conn.execute(
                """
                SELECT role, timing_role
                FROM active_participants
                WHERE agent_id = ?
                """,
                (21,),
            ).fetchone()
            self.assertEqual(row, ("BUYER", "buy_now"))

            role_logs = [entry for entry in batch_decision_logs if entry[2] == "ROLE_DECISION"]
            self.assertTrue(role_logs)
            self.assertEqual(role_logs[0][3], "BUYER")
            payload = json.loads(role_logs[0][5])
            metrics = json.loads(role_logs[0][6])
            self.assertEqual(payload["role"], "BUYER")
            self.assertEqual(payload["timing_role"], "buy_now")
            self.assertEqual(metrics["timing_role"], "buy_now")
            self.assertIn("forced keep buy lane after outbid", role_logs[0][4])
        finally:
            conn.close()
            if os.path.exists(db_path):
                os.remove(db_path)


if __name__ == "__main__":
    unittest.main()
