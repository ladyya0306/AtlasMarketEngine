import sqlite3
import unittest

from models import Agent
from services.agent_service import AgentService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestForcedRoleMode(unittest.TestCase):
    def test_forced_role_mode_respects_month_gate(self):
        svc = AgentService(
            _Cfg(
                {
                    "smart_agent.forced_role_mode": {
                        "enabled": True,
                        "apply_months": [1, 3],
                        "quota": {"buyer": 1, "seller": 1, "buyer_seller": 1},
                    }
                }
            ),
            sqlite3.connect(":memory:"),
        )

        cfg_m2 = svc._forced_role_mode_cfg(month=2)
        cfg_m3 = svc._forced_role_mode_cfg(month=3)

        self.assertFalse(cfg_m2["enabled"])
        self.assertTrue(cfg_m3["enabled"])
        self.assertEqual(cfg_m3["quota"]["BUYER"], 1)
        self.assertEqual(cfg_m3["quota"]["SELLER"], 1)
        self.assertEqual(cfg_m3["quota"]["BUYER_SELLER"], 1)

    def test_build_forced_role_decisions_respects_property_constraints(self):
        svc = AgentService(
            _Cfg(
                {
                    "smart_agent.forced_role_mode": {
                        "enabled": True,
                        "selection_policy": "affordability_inventory_balanced",
                        "allow_force_locked_buyers": True,
                        "quota": {"buyer": 1, "seller": 1, "buyer_seller": 2},
                    }
                }
            ),
            sqlite3.connect(":memory:"),
        )

        renter = Agent(id=1, cash=900_000, monthly_income=32_000)
        owner_a = Agent(id=2, cash=120_000, monthly_income=18_000)
        owner_a.owned_properties = [{"property_id": 21}]
        owner_b = Agent(id=3, cash=600_000, monthly_income=26_000)
        owner_b.owned_properties = [{"property_id": 31}]
        owner_c = Agent(id=4, cash=780_000, monthly_income=24_000)
        owner_c.owned_properties = [{"property_id": 41}, {"property_id": 42}]
        renter_locked = Agent(id=5, cash=500_000, monthly_income=20_000)
        renter_locked._buy_task_locked = True
        renter_locked._search_exhausted = False
        renter_locked.buy_completed = 0

        decisions, summary = svc._build_forced_role_decisions(
            candidates=[renter, owner_a, owner_b, owner_c, renter_locked],
            month=1,
            forced_cfg=svc._forced_role_mode_cfg(month=1),
        )

        by_role = {}
        for item in decisions:
            by_role.setdefault(item["role"], []).append(item["id"])
            self.assertEqual(item["trigger"], "forced_role_mode")
            self.assertFalse(item["_llm_called"])
            self.assertTrue(item["_skip_signature_cache"])
            self.assertEqual(item["_decision_origin"], "forced_role_mode")

        self.assertEqual(len(by_role["BUYER"]), 1)
        self.assertEqual(len(by_role["SELLER"]), 1)
        self.assertEqual(len(by_role["BUYER_SELLER"]), 2)
        self.assertIn(5, by_role["BUYER"])

        property_owner_ids = {2, 3, 4}
        self.assertTrue(set(by_role["SELLER"]).issubset(property_owner_ids))
        self.assertTrue(set(by_role["BUYER_SELLER"]).issubset(property_owner_ids))

        self.assertEqual(summary["allocated_quota"]["BUYER"], 1)
        self.assertEqual(summary["allocated_quota"]["SELLER"], 1)
        self.assertEqual(summary["allocated_quota"]["BUYER_SELLER"], 2)
        self.assertEqual(svc._last_forced_role_summary["month"], 1)


if __name__ == "__main__":
    unittest.main()
