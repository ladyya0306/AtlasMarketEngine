import sqlite3
import unittest

from agent_behavior import calculate_activation_probability
from models import Agent
from services.agent_service import AgentService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestNaturalActivationSharedMechanisms(unittest.TestCase):
    def test_cash_poor_owner_keeps_activation_lane(self):
        owner = Agent(id=1, cash=30_000, monthly_income=12_000)
        owner.owned_properties = [{"property_id": 11}]
        owner.story.financial_profile = "income_stressed"

        renter = Agent(id=2, cash=30_000, monthly_income=12_000)
        renter.story.financial_profile = "income_stressed"

        owner_score = calculate_activation_probability(owner)
        renter_score = calculate_activation_probability(renter)

        self.assertGreater(owner_score, 0.01)
        self.assertEqual(renter_score, 0.0)
        self.assertGreater(owner_score, renter_score)

    def test_role_signature_separates_agent_type_and_persona(self):
        svc = AgentService(_Cfg(), sqlite3.connect(":memory:"))

        normal_agent = Agent(id=1, cash=800_000, monthly_income=25_000, marital_status="married")
        normal_agent.agent_type = "normal"
        normal_agent.story.housing_stage = "owner_upgrade"
        normal_agent.story.financial_profile = "payment_sensitive"
        normal_agent.info_delay_months = 1

        smart_agent = Agent(id=2, cash=800_000, monthly_income=25_000, marital_status="married")
        smart_agent.agent_type = "smart"
        smart_agent.story.housing_stage = "owner_upgrade"
        smart_agent.story.financial_profile = "payment_sensitive"
        smart_agent.info_delay_months = 1

        sig_normal = svc._build_role_signature(normal_agent, market_trend="STABLE", recent_bulletins=[{"month": 1, "trend": "UP"}])
        sig_smart = svc._build_role_signature(smart_agent, market_trend="STABLE", recent_bulletins=[{"month": 1, "trend": "UP"}])

        self.assertNotEqual(sig_normal, sig_smart)

    def test_active_role_decisions_are_not_cross_agent_cached(self):
        svc = AgentService(_Cfg(), sqlite3.connect(":memory:"))
        signature = "sig-1"

        svc._cache_store_role_decision(signature, month=1, decision={"role": "BUYER", "trigger": "upgrade"})
        self.assertIsNone(svc._cache_lookup_role_decision(signature, month=1, ttl_months=1))

        svc._cache_store_role_decision(signature, month=1, decision={"role": "OBSERVER", "trigger": "wait"})
        cached = svc._cache_lookup_role_decision(signature, month=1, ttl_months=1)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["role"], "OBSERVER")


if __name__ == "__main__":
    unittest.main()
