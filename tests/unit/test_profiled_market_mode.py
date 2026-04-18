import sqlite3
import unittest

from models import Agent
from services.agent_service import AgentService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestProfiledMarketMode(unittest.TestCase):
    def test_profiled_market_mode_cfg_defaults(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        cfg = svc._profiled_market_mode_cfg()
        self.assertFalse(cfg["enabled"])
        self.assertEqual(cfg["experiment_mode"], "abundant")
        self.assertEqual(cfg["persona_generation_mode"], "code_only")

    def test_build_profiled_bucket_assignments_respects_count(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        pack = {
            "agent_profile_buckets": {
                "A_HIGH": {"count": 2},
                "B_ENTRY": {"count": 1},
            }
        }
        assignments, defs = svc._build_profiled_agent_bucket_assignments(
            agent_count=5,
            profile_pack=pack,
        )
        self.assertEqual(len(assignments), 5)
        self.assertEqual(sum(1 for x in assignments if x), 3)
        self.assertIn("A_HIGH", defs)
        self.assertIn("B_ENTRY", defs)

    def test_apply_profiled_bucket_overrides_agent_fields(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        agent = Agent(id=1, cash=100_000, monthly_income=8_000)
        bucket_defs = {
            "B_SCHOOL_JUST_ENTRY": {
                "role_side": "buyer",
                "runtime_profile": {"agent_type": "normal", "info_delay_months": 0},
                "story_profile": {
                    "purchase_motive_primary": "starter_home",
                    "financial_profile": "cashflow_sensitive",
                },
                "preference_profile": {
                    "target_zone": "B",
                    "need_school_district": True,
                    "min_bedrooms": 2,
                },
                "budget_profile": {
                    "cash_range": [220000, 220000],
                    "income_range": [18000, 18000],
                    "max_price_range": [1200000, 1200000],
                    "target_buy_price_range": [1000000, 1000000],
                    "payment_tolerance_ratio": 0.4,
                    "down_payment_tolerance_ratio": 0.22,
                },
            }
        }

        meta = svc._apply_profiled_agent_bucket(
            agent=agent,
            bucket_id="B_SCHOOL_JUST_ENTRY",
            bucket_defs=bucket_defs,
            persona_generation_mode="code_only",
        )
        self.assertIsNotNone(meta)
        self.assertEqual(agent.cash, 220000.0)
        self.assertEqual(agent.monthly_income, 18000.0)
        self.assertEqual(agent.preference.target_zone, "B")
        self.assertTrue(agent.preference.need_school_district)
        self.assertEqual(agent.preference.min_bedrooms, 2)
        self.assertEqual(agent.preference.max_price, 1200000.0)
        self.assertEqual(agent.preference.psychological_price, 1000000.0)
        self.assertEqual(agent.story.purchase_motive_primary, "starter_home")
        self.assertEqual(agent.story.financial_profile, "cashflow_sensitive")
        self.assertEqual(agent.profile_bucket_id, "B_SCHOOL_JUST_ENTRY")

    def test_apply_profiled_bucket_respects_income_adjustment_rate(self):
        svc = AgentService(
            _Cfg({"simulation.agent.income_adjustment_rate": 0.5}),
            sqlite3.connect(":memory:"),
        )
        agent = Agent(id=1, cash=100_000, monthly_income=8_000)
        agent.preference.max_price = 1_200_000.0
        agent.preference.max_affordable_price = 1_200_000.0
        agent.preference.psychological_price = 1_000_000.0
        bucket_defs = {
            "B_SCHOOL_JUST_ENTRY": {
                "role_side": "buyer",
                "budget_profile": {
                    "cash_range": [220000, 220000],
                    "income_range": [18000, 18000],
                    "max_price_range": [1200000, 1200000],
                    "target_buy_price_range": [1000000, 1000000],
                    "payment_tolerance_ratio": 0.4,
                    "down_payment_tolerance_ratio": 0.22,
                },
            }
        }

        svc._apply_profiled_agent_bucket(
            agent=agent,
            bucket_id="B_SCHOOL_JUST_ENTRY",
            bucket_defs=bucket_defs,
            persona_generation_mode="code_only",
        )

        self.assertEqual(agent.cash, 110000.0)
        self.assertEqual(agent.monthly_income, 9000.0)
        self.assertEqual(agent.preference.max_price, 600000.0)
        self.assertEqual(agent.preference.max_affordable_price, 600000.0)
        self.assertEqual(agent.preference.psychological_price, 500000.0)
        self.assertEqual(agent.payment_tolerance_ratio, 0.2)
        self.assertEqual(agent.down_payment_tolerance_ratio, 0.11)

    def test_profiled_assignment_roundtrip_table(self):
        conn = sqlite3.connect(":memory:")
        svc = AgentService(_Cfg({}), conn)
        cur = conn.cursor()
        svc._flush_profiled_agent_assignments(
            cur,
            [
                (1, "A_HIGH", "buyer", "profiled_market_mode", "{}"),
                (2, "B_ENTRY", "seller", "profiled_market_mode", "{}"),
            ],
        )
        mapping = svc._load_profiled_agent_bucket_map(cur)
        self.assertEqual(mapping[1], "A_HIGH")
        self.assertEqual(mapping[2], "B_ENTRY")

    def test_profiled_property_supply_mode_abundant_vs_scarce(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        props = [
            {
                "property_id": 1,
                "owner_id": 10,
                "zone": "B",
                "is_school_district": True,
                "property_type": "small",
                "base_value": 1100000,
                "building_area": 70,
                "quality": 1,
                "status": "off_market",
            },
            {
                "property_id": 2,
                "owner_id": 11,
                "zone": "B",
                "is_school_district": True,
                "property_type": "small",
                "base_value": 1150000,
                "building_area": 72,
                "quality": 1,
                "status": "off_market",
            },
            {
                "property_id": 3,
                "owner_id": 12,
                "zone": "B",
                "is_school_district": True,
                "property_type": "small",
                "base_value": 1180000,
                "building_area": 75,
                "quality": 1,
                "status": "off_market",
            },
        ]
        pack = {
            "property_profile_buckets": {
                "B_SCHOOL_JUST": {
                    "zone": "B",
                    "is_school_district": True,
                    "property_type_bucket": "JUST",
                    "building_area_range": [60, 90],
                    "quality_range": [1, 2],
                    "price_range": [900000, 1300000],
                    "count_by_supply_mode": {
                        "abundant": 3,
                        "scarce": 1,
                    },
                }
            }
        }

        rows_abundant = svc._apply_profiled_property_supply_mode(props, pack, "abundant")
        abundant_for_sale = sum(1 for p in props if p["status"] == "for_sale")
        self.assertEqual(len(rows_abundant), 3)
        self.assertEqual(abundant_for_sale, 3)

        for p in props:
            p["status"] = "off_market"
        rows_scarce = svc._apply_profiled_property_supply_mode(props, pack, "scarce")
        scarce_for_sale = sum(1 for p in props if p["status"] == "for_sale")
        self.assertEqual(len(rows_scarce), 1)
        self.assertEqual(scarce_for_sale, 1)

    def test_profiled_property_supply_mode_hard_fills_target_when_strict_pool_missing(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        props = [
            {
                "property_id": 1,
                "owner_id": 10,
                "zone": "A",
                "is_school_district": False,
                "property_type": "small",
                "base_value": 900000,
                "building_area": 60,
                "quality": 1,
                "status": "off_market",
            },
            {
                "property_id": 2,
                "owner_id": 11,
                "zone": "A",
                "is_school_district": False,
                "property_type": "small",
                "base_value": 920000,
                "building_area": 62,
                "quality": 1,
                "status": "off_market",
            },
        ]
        pack = {
            "property_profile_buckets": {
                "B_SCHOOL_JUST": {
                    "zone": "B",
                    "is_school_district": True,
                    "property_type_bucket": "JUST",
                    "bedroom_range": [2, 2],
                    "building_area_range": [68, 72],
                    "quality_range": [2, 2],
                    "price_range": [1_100_000, 1_100_000],
                    "count_by_supply_mode": {"abundant": 1},
                }
            }
        }

        rows = svc._apply_profiled_property_supply_mode(props, pack, "abundant")
        self.assertEqual(len(rows), 1)
        selected = [p for p in props if p.get("status") == "for_sale"]
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["zone"], "B")
        self.assertTrue(bool(selected[0]["is_school_district"]))
        self.assertEqual(selected[0]["base_value"], 1_100_000.0)

    def test_runtime_profiled_property_assignments_split_fallback_and_preserve_canonical_target(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        props = [
            {
                "property_id": 1,
                "owner_id": 10,
                "zone": "A",
                "is_school_district": False,
                "property_type": "豪宅",
                "base_value": 18_500_000,
                "building_area": 210,
                "quality": 5,
                "bedrooms": 5,
                "status": "for_sale",
            },
            {
                "property_id": 2,
                "owner_id": 11,
                "zone": "A",
                "is_school_district": False,
                "property_type": "改善型大户型",
                "base_value": 6_300_000,
                "building_area": 155,
                "quality": 3,
                "bedrooms": 4,
                "status": "for_sale",
            },
        ]
        pack = {
            "property_profile_buckets": {
                "CORE_NONSCHOOL_IMPROVE": {
                    "zone": "A",
                    "is_school_district": False,
                    "property_type_bucket": "IMPROVE",
                    "bedroom_range": [2, 4],
                    "building_area_range": [85, 125],
                    "quality_range": [2, 3],
                    "price_range": [3_600_000, 5_600_000],
                },
                "CORE_LUXURY_PRESERVATION": {
                    "zone": "A",
                    "is_school_district": False,
                    "property_type_bucket": "LUXURY",
                    "bedroom_range": [4, 6],
                    "building_area_range": [160, 260],
                    "quality_range": [4, 5],
                    "price_range": [12_000_000, 24_000_000],
                },
            },
            "canonical_bucket_policy": {
                "fallback_bucket_prefix": "FALLBACK_SUPPLY_",
            },
        }

        rows = svc._build_runtime_profiled_property_assignments(props, pack, "scarce")
        row_map = {int(row[0]): row for row in rows}
        self.assertEqual(row_map[1][1], "CORE_LUXURY_PRESERVATION")
        meta2 = __import__("json").loads(row_map[2][4])
        self.assertTrue(str(row_map[2][1]).startswith("FALLBACK_SUPPLY_CORE_NONSCHOOL_IMPROVE_"))
        self.assertEqual(meta2["canonical_target_bucket_id"], "CORE_NONSCHOOL_IMPROVE")
        self.assertEqual(meta2["bucket_class"], "fallback")


if __name__ == "__main__":
    unittest.main()
