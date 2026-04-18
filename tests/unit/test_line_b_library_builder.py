import unittest

from scripts.line_b_library_builder import (
    build_budget_consistency_report,
    build_competition_control_report,
    build_demand_library,
    build_graph_consistency_report,
    build_governance_snapshot,
    build_reverse_coverage_report,
    build_runtime_parent_bucket_report,
    build_supply_library,
)


class TestLineBLibraryBuilder(unittest.TestCase):
    def _pack(self):
        return {
            "agent_profile_buckets": {
                "B1": {
                    "count": 6,
                    "role_side": "buyer",
                    "preference_profile": {"target_zone": "B", "need_school_district": True},
                    "budget_profile": {
                        "max_price_range": [1_100_000, 1_600_000],
                        "target_buy_price_range": [900_000, 1_300_000],
                        "cash_range": [250_000, 500_000],
                        "income_range": [15_000, 25_000],
                    },
                    "runtime_profile": {"agent_type": "normal", "info_delay_months": 0},
                }
            },
            "property_profile_buckets": {
                "P1": {
                    "zone": "B",
                    "is_school_district": True,
                    "property_type_bucket": "JUST",
                    "price_range": [850_000, 1_500_000],
                    "count_by_supply_mode": {"abundant": 8, "scarce": 3},
                }
            },
            "bucket_alignment_rules": [
                {
                    "agent_bucket_id": "B1",
                    "eligible_property_buckets": ["P1"],
                    "soft_property_buckets": [],
                }
            ],
        }

    def test_build_libraries_and_reports(self):
        pack = self._pack()
        demand = build_demand_library(pack)
        supply = build_supply_library(pack, experiment_mode="abundant")
        budget = build_budget_consistency_report(pack)
        competition = build_competition_control_report(pack)
        snap = build_governance_snapshot(
            profile_pack=pack,
            profile_pack_path="config/line_b_profiled_market_template.yaml",
            experiment_mode="abundant",
            seed=606,
            group_id="V3",
            months=1,
            agent_count=50,
        )

        self.assertEqual(demand["total_agents_profiled"], 6)
        self.assertEqual(supply["total_selected_supply"], 8)
        self.assertTrue(budget["ok"])
        self.assertGreater(
            competition["abundant"][0]["buyer_to_supply_ratio"],
            0.0,
        )
        self.assertTrue(str(snap["identity_hash"]))
        self.assertEqual(
            snap["demand_library"]["total_agents_profiled"],
            6,
        )

    def test_build_reports_accept_v2_market_profile_supply(self):
        pack = {
            "agent_profile_buckets": {
                "B1": {
                    "count": 6,
                    "role_side": "buyer",
                    "budget_profile": {"max_price_range": [1_100_000, 1_600_000]},
                }
            },
            "property_profile_buckets": {
                "P1": {
                    "market_profile": {
                        "zone": "B",
                        "is_school_district": True,
                        "property_type_bucket": "JUST",
                        "price_range": [850_000, 1_500_000],
                    },
                    "count_by_supply_mode": {"abundant": 8, "scarce": 3},
                }
            },
            "bucket_alignment_rules": [
                {
                    "agent_bucket_id": "B1",
                    "eligible_property_buckets": ["P1"],
                    "soft_property_buckets": [],
                }
            ],
        }

        supply = build_supply_library(pack, experiment_mode="abundant")
        budget = build_budget_consistency_report(pack)

        self.assertEqual(supply["buckets"][0]["price_range"], [850000.0, 1500000.0])
        self.assertTrue(budget["ok"])

    def test_build_bidirectional_graph_reports(self):
        pack = {
            "agent_profile_buckets": {
                "ENTRY_SCHOOL": {
                    "count": 4,
                    "role_side": "buyer",
                    "budget_profile": {"max_price_range": [900_000, 1_800_000]},
                },
                "UPGRADE": {
                    "count": 2,
                    "role_side": "buyer",
                    "budget_profile": {"max_price_range": [4_000_000, 5_000_000]},
                },
            },
            "property_profile_buckets": {
                "SUBCORE_SCHOOL_STARTER": {
                    "market_profile": {
                        "zone": "B",
                        "is_school_district": True,
                        "property_type_bucket": "JUST",
                        "price_range": [950_000, 1_600_000],
                    },
                    "count_by_supply_mode": {"abundant": 6, "scarce": 2},
                },
                "CORE_IMPROVE": {
                    "market_profile": {
                        "zone": "A",
                        "is_school_district": False,
                        "property_type_bucket": "IMPROVE",
                        "price_range": [4_200_000, 5_200_000],
                    },
                    "count_by_supply_mode": {"abundant": 3, "scarce": 1},
                },
            },
            "runtime_parent_buckets": {
                "ENTRY_PARENT": {
                    "child_demand_buckets": ["ENTRY_SCHOOL"],
                    "child_supply_buckets": ["SUBCORE_SCHOOL_STARTER"],
                }
            },
            "compatibility_graph_v1": {
                "demand_to_supply": [
                    {
                        "demand_bucket_id": "ENTRY_SCHOOL",
                        "supply_bucket_id": "SUBCORE_SCHOOL_STARTER",
                        "relation_type": "primary",
                        "budget_overlap_min": 100_000,
                    },
                    {
                        "demand_bucket_id": "UPGRADE",
                        "supply_bucket_id": "CORE_IMPROVE",
                        "relation_type": "primary",
                        "budget_overlap_min": 100_000,
                    },
                ],
                "supply_to_demand": [
                    {
                        "supply_bucket_id": "SUBCORE_SCHOOL_STARTER",
                        "demand_bucket_id": "ENTRY_SCHOOL",
                        "relation_type": "primary",
                    },
                    {
                        "supply_bucket_id": "CORE_IMPROVE",
                        "demand_bucket_id": "UPGRADE",
                        "relation_type": "primary",
                    },
                ],
            },
        }

        runtime_parent = build_runtime_parent_bucket_report(pack)
        reverse = build_reverse_coverage_report(pack, experiment_mode="abundant")
        graph = build_graph_consistency_report(pack)
        snapshot = build_governance_snapshot(
            profile_pack=pack,
            profile_pack_path="config/line_a_profile_pack_v3_graph_template.yaml",
            experiment_mode="abundant",
            seed=606,
            group_id="V2",
            months=3,
            agent_count=12,
        )

        self.assertTrue(runtime_parent["ok"])
        self.assertEqual(runtime_parent["parent_bucket_count"], 1)
        self.assertTrue(reverse["ok"])
        self.assertEqual(reverse["rows"][0]["primary_demand_bucket_count"], 1)
        self.assertTrue(graph["ok"])
        self.assertEqual(snapshot["compatibility_graph_summary"]["demand_to_supply_edge_count"], 2)
        self.assertIn("reverse_coverage_report", snapshot)
        self.assertIn("graph_consistency_report", snapshot)


if __name__ == "__main__":
    unittest.main()
