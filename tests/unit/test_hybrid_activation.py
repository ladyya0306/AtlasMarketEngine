import sqlite3
import unittest

from models import Agent
from services.agent_service import AgentService


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestHybridActivation(unittest.TestCase):
    def test_normalize_profiled_property_bucket_definition_flattens_market_profile(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))

        bucket = svc._normalize_profiled_property_bucket_definition(
            {
                "property_profile": {
                    "zone_tier": "sub_core",
                    "school_tier": "general_school",
                    "product_segment": "starter",
                },
                "market_profile": {
                    "zone": "B",
                    "is_school_district": True,
                    "property_type_bucket": "JUST",
                    "price_range": [900000, 1800000],
                },
                "count_by_supply_mode": {"abundant": 12},
            }
        )

        self.assertEqual(bucket["zone"], "B")
        self.assertEqual(bucket["property_type_bucket"], "JUST")
        self.assertEqual(bucket["is_school_district"], True)
        self.assertEqual(bucket["price_range"], [900000, 1800000])

    def test_profiled_no_home_bucket_keeps_zero_initial_properties(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))

        target = svc._resolve_profiled_initial_property_target(
            default_target_props=2,
            bucket_id="B_SCHOOL_JUST_ENTRY",
            bucket_defs={
                "B_SCHOOL_JUST_ENTRY": {
                    "role_side": "buyer",
                    "story_profile": {
                        "housing_stage": "starter_no_home",
                        "purchase_motive_primary": "starter_home",
                    },
                }
            },
        )

        self.assertEqual(target, 0)

    def test_profiled_no_home_constraint_keeps_zero_initial_properties(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))

        target = svc._resolve_profiled_initial_property_target(
            default_target_props=2,
            bucket_id="Y_ENTRY_FAMILY_SCHOOL",
            bucket_defs={
                "Y_ENTRY_FAMILY_SCHOOL": {
                    "role_side": "buyer",
                    "story_profile": {
                        "housing_stage": "owner_first_upgrade",
                        "purchase_motive_primary": "starter_home",
                    },
                    "initialization_constraints": {
                        "preserve_no_home": True,
                    },
                }
            },
        )

        self.assertEqual(target, 0)

    def test_runtime_profiled_property_assignments_follow_final_market_state(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))

        rows = svc._build_runtime_profiled_property_assignments(
            market_properties=[
                {
                    "property_id": 201,
                    "owner_id": 7,
                    "status": "for_sale",
                    "zone": "B",
                    "is_school_district": True,
                    "property_type": "just_small",
                    "building_area": 72,
                    "quality": 1,
                    "base_value": 1200000,
                },
                {
                    "property_id": 202,
                    "owner_id": -1,
                    "status": "for_sale",
                    "zone": "B",
                    "is_school_district": False,
                    "property_type": "just_small",
                    "building_area": 68,
                    "quality": 1,
                    "base_value": 950000,
                },
            ],
            profile_pack={
                "property_profile_buckets": {
                    "B_SCHOOL_JUST": {
                        "zone": "B",
                        "is_school_district": True,
                        "property_type_bucket": "JUST",
                        "building_area_range": [60, 90],
                        "quality_range": [1, 2],
                        "price_range": [850000, 1500000],
                    }
                }
            },
            experiment_mode="abundant",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 201)
        self.assertEqual(rows[0][1], "B_SCHOOL_JUST")
        self.assertEqual(rows[0][2], "abundant")

    def test_runtime_profiled_property_assignments_emit_fallback_bucket_for_unmatched(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))

        rows = svc._build_runtime_profiled_property_assignments(
            market_properties=[
                {
                    "property_id": 301,
                    "owner_id": 7,
                    "status": "for_sale",
                    "zone": "A",
                    "is_school_district": True,
                    "property_type": "豪宅",
                    "building_area": 180,
                    "quality": 5,
                    "base_value": 12000000,
                }
            ],
            profile_pack={
                "canonical_bucket_policy": {
                    "fallback_bucket_prefix": "FALLBACK_SUPPLY_",
                },
                "property_profile_buckets": {
                    "B_SCHOOL_JUST": {
                        "zone": "B",
                        "is_school_district": True,
                        "property_type_bucket": "JUST",
                        "building_area_range": [60, 90],
                        "quality_range": [1, 2],
                        "price_range": [850000, 1500000],
                    }
                }
            },
            experiment_mode="abundant",
        )

        self.assertEqual(len(rows), 1)
        self.assertTrue(str(rows[0][1]).startswith("FALLBACK_SUPPLY_"))
        self.assertIn('"bucket_class": "fallback"', rows[0][4])
        self.assertIn('"runtime_bucket_id": "FALLBACK_SUPPLY_', rows[0][4])
        self.assertIn('"canonical_target_bucket_id": ""', rows[0][4])

    def test_init_supply_coverage_does_not_seed_no_home_bucket(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        starter = Agent(id=1, cash=300000, monthly_income=18000)
        starter.story.housing_stage = "starter_no_home"
        starter.story.purchase_motive_primary = "starter_home"
        keeper = Agent(id=2, cash=1200000, monthly_income=42000)
        keeper.story.housing_stage = "owner_upgrade"
        keeper.story.purchase_motive_primary = "upgrade_living"
        svc.agents = [starter, keeper]
        svc._resolve_init_supply_targets = lambda market_properties, planned_agent_count: {
            "zone_a_owner_target": 0,
            "zone_b_owner_target": 1,
            "zone_a_for_sale_target": 0,
            "zone_b_for_sale_target": 0,
            "tradable_total_target": 0,
        }
        market_properties = [
            {
                "property_id": 301,
                "zone": "B",
                "owner_id": -1,
                "status": "off_market",
                "base_value": 1000000,
            }
        ]

        svc._enforce_init_supply_coverage(
            market_properties=market_properties,
            property_updates=[],
            planned_agent_count=2,
        )

        self.assertEqual(market_properties[0]["owner_id"], 2)

    def test_hybrid_target_budget_floor_uses_bucket_budget_when_no_home(self):
        svc = AgentService(_Cfg({}), sqlite3.connect(":memory:"))
        buyer = Agent(id=1, cash=260000, monthly_income=17000)

        self.assertTrue(
            svc._hybrid_target_budget_floor_pass(
                agent=buyer,
                bucket_id="B_NOSCHOOL_JUST_ENTRY",
                profile_pack={
                    "agent_profile_buckets": {
                        "B_NOSCHOOL_JUST_ENTRY": {
                            "budget_profile": {
                                "cash_range": [220000, 480000],
                                "income_range": [15000, 24000],
                            }
                        }
                    }
                },
                min_cash_observer=500000,
            )
        )

    def test_activation_persona_packet_includes_agent_profile_fields(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE profiled_market_agent_buckets (
                agent_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                role_side TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE profiled_market_property_buckets (
                property_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                supply_mode TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                status TEXT
            )
            """
        )
        cur.execute(
            "INSERT INTO profiled_market_agent_buckets(agent_id,bucket_id,role_side,source,metadata_json,assigned_at) VALUES (1,'Y_ENTRY_FAMILY_SCHOOL','buyer','profiled_market_mode','{}',CURRENT_TIMESTAMP)"
        )
        cur.execute(
            "INSERT INTO profiled_market_property_buckets(property_id,bucket_id,supply_mode,source,metadata_json,assigned_at) VALUES (101,'SUBCORE_GENERAL_SCHOOL_STARTER','abundant','profiled_market_mode','{}',CURRENT_TIMESTAMP)"
        )
        cur.execute(
            "INSERT INTO properties_market(property_id,status) VALUES (101,'for_sale')"
        )
        conn.commit()

        svc = AgentService(_Cfg({}), conn)
        agent = Agent(id=1, cash=320000, monthly_income=22000)
        agent.profile_bucket_id = "Y_ENTRY_FAMILY_SCHOOL"
        svc.agents = [agent]

        packet = svc._build_activation_persona_packet(
            agent,
            profile_pack={
                "agent_profile_buckets": {
                    "Y_ENTRY_FAMILY_SCHOOL": {
                        "role_side": "buyer",
                        "agent_profile": {
                            "life_stage": "young_family_with_child",
                            "education_strategy": "public_school_strong",
                            "asset_state": "cashflow_sensitive",
                        },
                        "preference_profile": {
                            "target_zone": "B",
                            "need_school_district": True,
                            "property_type_target": "STARTER_UPGRADE",
                        },
                        "budget_profile": {
                            "target_buy_price_range": [1200000, 1800000],
                        },
                    }
                },
                "bucket_alignment_rules": [
                    {
                        "agent_bucket_id": "Y_ENTRY_FAMILY_SCHOOL",
                        "eligible_property_buckets": ["SUBCORE_GENERAL_SCHOOL_STARTER"],
                    }
                ],
            },
            bucket_pressure_map=svc._build_profiled_bucket_pressure_map(
                cur,
                {
                    "agent_profile_buckets": {
                        "Y_ENTRY_FAMILY_SCHOOL": {
                            "role_side": "buyer",
                            "count": 1,
                        }
                    },
                    "bucket_alignment_rules": [
                        {
                            "agent_bucket_id": "Y_ENTRY_FAMILY_SCHOOL",
                            "eligible_property_buckets": ["SUBCORE_GENERAL_SCHOOL_STARTER"],
                        }
                    ],
                },
            ),
        )

        self.assertEqual(packet["life_stage"], "young_family_with_child")
        self.assertEqual(packet["education_strategy"], "public_school_strong")
        self.assertEqual(packet["asset_state"], "cashflow_sensitive")

    def test_select_hybrid_targeted_candidates_uses_viable_buckets(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE profiled_market_agent_buckets (
                agent_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                role_side TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE profiled_market_property_buckets (
                property_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                supply_mode TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                status TEXT
            )
            """
        )
        cur.executemany(
            "INSERT INTO profiled_market_agent_buckets(agent_id,bucket_id,role_side,source,metadata_json,assigned_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
            [
                (1, "B_SCHOOL_JUST_ENTRY", "buyer", "profiled_market_mode", "{}"),
                (2, "B_SCHOOL_JUST_ENTRY", "buyer", "profiled_market_mode", "{}"),
            ],
        )
        cur.executemany(
            "INSERT INTO profiled_market_property_buckets(property_id,bucket_id,supply_mode,source,metadata_json,assigned_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
            [
                (101, "B_SCHOOL_JUST", "abundant", "profiled_market_mode", "{}"),
                (102, "B_SCHOOL_JUST", "abundant", "profiled_market_mode", "{}"),
            ],
        )
        cur.executemany(
            "INSERT INTO properties_market(property_id,status) VALUES (?,?)",
            [
                (101, "for_sale"),
                (102, "for_sale"),
            ],
        )
        conn.commit()

        cfg = _Cfg(
            {
                "smart_agent.activation_governance": {
                    "enabled": True,
                    "activation_mode": "hybrid",
                    "severe_bucket_deficit_ratio": 5.0,
                    "autofill_demand_floor": 2,
                },
                "smart_agent.profiled_market_mode": {
                    "enabled": True,
                    "profile_pack": {
                        "agent_profile_buckets": {
                            "B_SCHOOL_JUST_ENTRY": {
                                "role_side": "buyer",
                                "count": 2,
                                "preference_profile": {
                                    "target_zone": "B",
                                    "need_school_district": True,
                                    "property_type_target": "JUST",
                                },
                                "budget_profile": {
                                    "max_price_range": [1100000, 1600000],
                                    "target_buy_price_range": [900000, 1300000],
                                },
                            }
                        },
                        "property_profile_buckets": {
                            "B_SCHOOL_JUST": {
                                "count_by_supply_mode": {"abundant": 2, "scarce": 1},
                            }
                        },
                        "bucket_alignment_rules": [
                            {
                                "agent_bucket_id": "B_SCHOOL_JUST_ENTRY",
                                "eligible_property_buckets": ["B_SCHOOL_JUST"],
                            }
                        ],
                    },
                },
            }
        )
        svc = AgentService(cfg, conn)
        a1 = Agent(id=1, cash=450000, monthly_income=22000)
        a1.profile_bucket_id = "B_SCHOOL_JUST_ENTRY"
        a1.preference.target_zone = "B"
        a1.preference.need_school_district = True
        a2 = Agent(id=2, cash=430000, monthly_income=21000)
        a2.profile_bucket_id = "B_SCHOOL_JUST_ENTRY"
        a2.preference.target_zone = "B"
        a2.preference.need_school_district = True
        svc.agents = [a1, a2]
        svc.agent_map = {1: a1, 2: a2}

        targeted_agents, targeted_map, bucket_pressure_map = svc._select_hybrid_targeted_candidates(
            conn.cursor(),
            existing_candidates=[],
            profile_pack=cfg.get("smart_agent.profiled_market_mode")["profile_pack"],
            min_cash_observer=100000,
            targeted_score_threshold=0.0,
            severe_bucket_deficit_ratio=5.0,
            max_targeted_total=2,
        )

        self.assertEqual(len(targeted_agents), 2)
        self.assertEqual(set(targeted_map.keys()), {1, 2})
        self.assertEqual(
            bucket_pressure_map["B_SCHOOL_JUST_ENTRY"]["eligible_property_bucket_count_this_month"],
            2,
        )
        self.assertEqual(
            targeted_map[1]["reason"],
            "hybrid_bucket_targeted_llm",
        )
        self.assertEqual(
            getattr(a1, "_activation_persona_packet", {}).get("profile_bucket_id"),
            "B_SCHOOL_JUST_ENTRY",
        )

    def test_select_hybrid_targeted_candidates_groups_by_parent_bucket(self):
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE profiled_market_agent_buckets (
                agent_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                role_side TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE profiled_market_property_buckets (
                property_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                supply_mode TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE properties_market (
                property_id INTEGER PRIMARY KEY,
                status TEXT
            )
            """
        )
        cur.executemany(
            "INSERT INTO profiled_market_agent_buckets(agent_id,bucket_id,role_side,source,metadata_json,assigned_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
            [
                (1, "Y_ENTRY_FAMILY_SCHOOL", "buyer", "profiled_market_mode", "{}"),
                (2, "Y_ENTRY_SOLO_COST", "buyer", "profiled_market_mode", "{}"),
            ],
        )
        cur.executemany(
            "INSERT INTO profiled_market_property_buckets(property_id,bucket_id,supply_mode,source,metadata_json,assigned_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
            [
                (101, "SUBCORE_GENERAL_SCHOOL_STARTER", "abundant", "profiled_market_mode", "{}"),
                (102, "SUBCORE_NONSCHOOL_STARTER", "abundant", "profiled_market_mode", "{}"),
            ],
        )
        cur.executemany(
            "INSERT INTO properties_market(property_id,status) VALUES (?,?)",
            [(101, "for_sale"), (102, "for_sale")],
        )
        conn.commit()

        cfg = _Cfg({})
        svc = AgentService(cfg, conn)
        a1 = Agent(id=1, cash=380000, monthly_income=22000)
        a1.profile_bucket_id = "Y_ENTRY_FAMILY_SCHOOL"
        a2 = Agent(id=2, cash=360000, monthly_income=21000)
        a2.profile_bucket_id = "Y_ENTRY_SOLO_COST"
        svc.agents = [a1, a2]
        svc.agent_map = {1: a1, 2: a2}

        profile_pack = {
            "agent_profile_buckets": {
                "Y_ENTRY_FAMILY_SCHOOL": {
                    "count": 1,
                    "role_side": "buyer",
                    "budget_profile": {"cash_range": [280000, 520000], "income_range": [18000, 30000]},
                },
                "Y_ENTRY_SOLO_COST": {
                    "count": 1,
                    "role_side": "buyer",
                    "budget_profile": {"cash_range": [180000, 420000], "income_range": [14000, 24000]},
                },
            },
            "bucket_alignment_rules": [
                {
                    "agent_bucket_id": "Y_ENTRY_FAMILY_SCHOOL",
                    "eligible_property_buckets": ["SUBCORE_GENERAL_SCHOOL_STARTER"],
                },
                {
                    "agent_bucket_id": "Y_ENTRY_SOLO_COST",
                    "eligible_property_buckets": ["SUBCORE_NONSCHOOL_STARTER"],
                },
            ],
            "runtime_parent_buckets": {
                "ENTRY_PARENT": {
                    "child_demand_buckets": ["Y_ENTRY_FAMILY_SCHOOL", "Y_ENTRY_SOLO_COST"],
                    "child_supply_buckets": ["SUBCORE_GENERAL_SCHOOL_STARTER", "SUBCORE_NONSCHOOL_STARTER"],
                }
            },
        }

        targeted_agents, targeted_map, _ = svc._select_hybrid_targeted_candidates(
            conn.cursor(),
            existing_candidates=[],
            profile_pack=profile_pack,
            min_cash_observer=100000,
            targeted_score_threshold=0.0,
            severe_bucket_deficit_ratio=5.0,
            max_targeted_total=2,
        )

        self.assertEqual(len(targeted_agents), 1)
        only_agent = targeted_agents[0]
        self.assertEqual(targeted_map[int(only_agent.id)]["parent_bucket_id"], "ENTRY_PARENT")
        self.assertIn(targeted_map[int(only_agent.id)]["child_bucket_id"], {"Y_ENTRY_FAMILY_SCHOOL", "Y_ENTRY_SOLO_COST"})
        self.assertEqual(
            getattr(only_agent, "_activation_persona_packet", {}).get("runtime_parent_bucket_id"),
            "ENTRY_PARENT",
        )


if __name__ == "__main__":
    unittest.main()
