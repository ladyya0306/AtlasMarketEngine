import sqlite3
import unittest

from services.agent_service import AgentService
from scripts.run_market_state_matrix import _market_state_plan


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestInfoDelayContext(unittest.TestCase):
    def test_v2_plan_prefers_delay_for_normal_agents(self):
        plan = _market_state_plan("V2", seed=606, months=1)
        smart_cfg = plan.get("smart_agent", {})
        self.assertTrue(smart_cfg.get("info_delay_enabled"))
        self.assertTrue(smart_cfg.get("info_delay_apply_to_normal"))
        self.assertGreater(
            float(smart_cfg.get("info_delay_ratio_normal", 0.0) or 0.0),
            float(smart_cfg.get("info_delay_ratio", 0.0) or 0.0),
        )

    def test_deterministic_delay_can_prefer_normal_when_configured(self):
        cfg = _Cfg(
            {
                "smart_agent.info_delay_enabled": True,
                "smart_agent.info_delay_apply_to_normal": True,
                "smart_agent.info_delay_ratio": 0.0,
                "smart_agent.info_delay_ratio_normal": 1.0,
                "smart_agent.info_delay_min_months": 1,
                "smart_agent.info_delay_max_months": 1,
                "simulation.random_seed": 606,
            }
        )
        svc = AgentService(cfg, sqlite3.connect(":memory:"))
        self.assertEqual(svc._deterministic_info_delay_months(agent_id=1, is_smart_agent=True), 0)
        self.assertEqual(svc._deterministic_info_delay_months(agent_id=1, is_smart_agent=False), 1)

    def test_no_delay_keeps_context(self):
        svc = AgentService(_Cfg(), sqlite3.connect(":memory:"))
        recent = [{"month": 1, "trend": "UP"}, {"month": 2, "trend": "DOWN"}]
        b, trend, delay = svc._build_delayed_market_context(recent, "STABLE", 0)
        self.assertEqual(len(b), 2)
        self.assertEqual(trend, "STABLE")
        self.assertEqual(delay, 0)

    def test_delay_hides_latest_bulletins(self):
        svc = AgentService(_Cfg(), sqlite3.connect(":memory:"))
        recent = [
            {"month": 1, "trend": "UP"},
            {"month": 2, "trend": "STABLE"},
            {"month": 3, "trend": "DOWN"},
        ]
        b, trend, delay = svc._build_delayed_market_context(recent, "PANIC", 1)
        self.assertEqual(len(b), 2)
        self.assertEqual(trend, "STABLE")
        self.assertEqual(delay, 1)

    def test_delay_more_than_history_gives_empty_visible(self):
        svc = AgentService(_Cfg(), sqlite3.connect(":memory:"))
        recent = [{"month": 1, "trend": "UP"}]
        b, trend, delay = svc._build_delayed_market_context(recent, "DOWN", 3)
        self.assertEqual(b, [])
        self.assertEqual(trend, "STABLE")
        self.assertEqual(delay, 3)

    def test_delayed_bulletin_text_no_leak(self):
        svc = AgentService(_Cfg(), sqlite3.connect(":memory:"))
        recent = [{"month": 1, "trend": "UP", "avg_price": 1000000, "volume": 3}]
        text = svc._build_delayed_bulletin_text("最新公报: PANIC", recent, 2)
        self.assertIn("信息延迟视角", text)
        self.assertIn("STABLE", text)
        self.assertNotIn("PANIC", text)


if __name__ == "__main__":
    unittest.main()
