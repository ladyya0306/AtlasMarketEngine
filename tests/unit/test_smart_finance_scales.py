import sqlite3
import unittest

from services.agent_service import AgentService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestSmartFinanceScales(unittest.TestCase):
    def test_scale_reads_top_level(self):
        svc = AgentService(_Cfg({"smart_income_scale": 1.5}), sqlite3.connect(":memory:"))
        self.assertAlmostEqual(svc._resolve_smart_finance_scale("smart_income_scale", 1.0), 1.5)

    def test_scale_reads_nested_key(self):
        svc = AgentService(_Cfg({"smart_agent.smart_cash_scale": 0.8}), sqlite3.connect(":memory:"))
        self.assertAlmostEqual(svc._resolve_smart_finance_scale("smart_cash_scale", 1.0), 0.8)

    def test_scale_is_bounded(self):
        svc = AgentService(_Cfg({"smart_cash_scale": 99}), sqlite3.connect(":memory:"))
        self.assertAlmostEqual(svc._resolve_smart_finance_scale("smart_cash_scale", 1.0), 5.0)
        svc2 = AgentService(_Cfg({"smart_cash_scale": -1}), sqlite3.connect(":memory:"))
        self.assertAlmostEqual(svc2._resolve_smart_finance_scale("smart_cash_scale", 1.0), 0.1)


if __name__ == "__main__":
    unittest.main()
