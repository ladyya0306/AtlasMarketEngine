import sqlite3
import unittest
from types import SimpleNamespace

from services.transaction_service import TransactionService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}
        self.mortgage = {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestCandidateTwoStageAndDynamicQuota(unittest.TestCase):
    def test_resolve_two_stage_controls_defaults(self):
        svc = TransactionService(_Cfg({}), sqlite3.connect(":memory:"))
        cfg = svc._resolve_two_stage_candidate_controls()
        self.assertTrue(cfg["enabled"])
        self.assertEqual(cfg["min_stage1_pool"], 4)
        self.assertEqual(cfg["max_stage2_fill"], 6)

    def test_dynamic_quota_applies_pressure_and_stage_bonus(self):
        conn = sqlite3.connect(":memory:")
        try:
            cfg = _Cfg(
                {
                    "smart_agent.candidate_quota_per_property_base": 3,
                    "smart_agent.candidate_quota_per_property_b_zone_bonus": 1,
                    "smart_agent.candidate_quota_per_property_school_bonus": 1,
                    "smart_agent.candidate_quota_buy_task_locked_extra": 1,
                    "smart_agent.candidate_quota_dynamic_enabled": True,
                    "smart_agent.candidate_quota_dynamic_pressure_step": 1.0,
                    "smart_agent.candidate_quota_dynamic_max_bonus": 4,
                    "smart_agent.candidate_quota_stage2_bonus": 2,
                    "smart_agent.candidate_quota_blocked_recovery_bonus": 1,
                }
            )
            svc = TransactionService(cfg, conn)
            buyer = SimpleNamespace(_buy_task_locked=True)
            quota_cfg = svc._resolve_candidate_quota_controls()
            quota = svc._resolve_property_candidate_quota(
                listing={"property_id": 1001},
                prop={"zone": "B", "is_school_district": True},
                buyer=buyer,
                quota_cfg=quota_cfg,
                pressure_score=3.4,
                stage="stage2",
                blocked_recovery=True,
            )
            # 3(base)+1(B)+1(school)+1(locked)+2(stage2)+1(blocked)+3(dynamic)=12
            self.assertEqual(quota, 12)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
