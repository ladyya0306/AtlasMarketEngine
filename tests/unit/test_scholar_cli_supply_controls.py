import unittest
import uuid
from pathlib import Path

from config.config_loader import SimulationConfig
from real_estate_demo_v2_1 import (
    _derive_agent_count_from_supply,
    apply_scholar_release_config,
    build_scaled_profile_pack_from_snapshot,
    load_release_supply_snapshot_options,
)


ROOT = Path(__file__).resolve().parents[2]


class TestScholarCliSupplyControls(unittest.TestCase):
    def test_load_release_supply_snapshot_options_returns_both_structure_families(self):
        options = load_release_supply_snapshot_options()

        self.assertEqual(
            [item["snapshot_id"] for item in options],
            [
                "spindle_medium",
                "spindle_minimum",
                "spindle_large",
                "pyramid_minimum",
                "pyramid_medium",
                "pyramid_large",
            ],
        )
        by_id = {item["snapshot_id"]: item for item in options}
        self.assertEqual(by_id["spindle_minimum"]["total_selected_supply"], 39)
        self.assertEqual(by_id["spindle_medium"]["total_selected_supply"], 91)
        self.assertEqual(by_id["spindle_large"]["total_selected_supply"], 164)
        self.assertEqual(by_id["pyramid_minimum"]["total_selected_supply"], 39)
        self.assertEqual(by_id["pyramid_medium"]["total_selected_supply"], 93)
        self.assertEqual(by_id["pyramid_large"]["total_selected_supply"], 165)
        self.assertEqual(by_id["spindle_medium"]["structure_family"], "spindle")
        self.assertEqual(by_id["pyramid_medium"]["structure_family"], "pyramid")
        self.assertIn("梭子型", by_id["spindle_medium"]["family_label"])
        self.assertIn("金字塔", by_id["pyramid_medium"]["family_label"])
        self.assertGreater(by_id["spindle_minimum"]["minimum_demand_multiplier"], 0.30)
        self.assertEqual(by_id["spindle_medium"]["demand_bucket_count"], 12)
        self.assertEqual(by_id["pyramid_medium"]["demand_bucket_count"], 12)

    def test_derive_agent_count_from_supply_uses_half_up_rounding(self):
        self.assertEqual(_derive_agent_count_from_supply(91, 1.50), 137)
        self.assertEqual(_derive_agent_count_from_supply(39, 0.10), 4)

    def test_scaled_profile_pack_clamps_low_multiplier_to_preserve_all_buckets(self):
        options = load_release_supply_snapshot_options()
        by_id = {item["snapshot_id"]: item for item in options}
        minimum = by_id["spindle_minimum"]

        inline_pack, demand_plan = build_scaled_profile_pack_from_snapshot(
            base_profile_pack_path=minimum["profile_pack_path"],
            snapshot_payload=minimum["snapshot_payload"],
            target_agent_total=4,
        )

        self.assertEqual(demand_plan["effective_agent_count"], 12)
        self.assertTrue(demand_plan["scale_meta"]["was_clamped"])
        self.assertEqual(len(demand_plan["scaled_bucket_counts"]), 12)
        self.assertTrue(all(count >= 1 for count in demand_plan["scaled_bucket_counts"].values()))
        self.assertTrue(demand_plan["coverage_summary"]["all_supply_buckets_covered"])
        self.assertEqual(
            sum(
                int(bucket.get("count", 0) or 0)
                for bucket in (inline_pack.get("agent_profile_buckets", {}) or {}).values()
            ),
            12,
        )

    def test_apply_scholar_release_config_persists_supply_snapshot_and_shocks(self):
        cfg_path = ROOT / f"test_scholar_cli_supply_controls_{uuid.uuid4().hex}.yaml"
        try:
            cfg_path.write_text((ROOT / "config" / "baseline.yaml").read_text(encoding="utf-8"), encoding="utf-8")
            config = SimulationConfig(str(cfg_path))

            scholar_inputs = {
                "market_goal": "balanced",
                "months": 3,
                "agent_count": 137,
                "property_count": 91,
                "demand_multiplier": 1.5,
                "effective_demand_multiplier": round(137 / 91, 4),
                "supply_snapshot": {
                    "snapshot_id": "spindle_medium",
                    "display_name": "中样本",
                    "structure_family": "spindle",
                    "family_label": "梭子型固定供应盘",
                    "config_patches": {
                        "market.zones.A.supply_band_ratio.low": 0.25,
                        "market.zones.A.supply_band_ratio.mid": 0.50,
                        "market.zones.A.supply_band_ratio.high": 0.25,
                    },
                    "snapshot_status": "pass",
                },
                "profile_pack_inline": {
                    "agent_profile_buckets": {
                        "Y_ENTRY_SOLO_COST": {"count": 14},
                        "Y_ENTRY_FAMILY_SCHOOL": {"count": 19},
                    }
                },
                "demand_bucket_plan": {
                    "effective_agent_count": 137,
                    "coverage_summary": {
                        "buyer_bucket_count": 12,
                        "buyer_bucket_count_preserved": 12,
                        "supply_bucket_count": 10,
                        "supply_bucket_count_covered": 10,
                    },
                    "scale_meta": {"was_clamped": False},
                },
                "buyer_quota": 22,
                "seller_quota": 22,
                "buyer_seller_quota": 11,
                "target_r_order_hint": 1.0,
                "income_multiplier": 1.0,
                "force_role_months": 3,
                "profiled_market_mode": True,
                "hard_bucket_matcher": True,
                "enable_intervention_panel": False,
                "open_startup_intervention_menu": False,
                "profile_pack_path": "config/line_a_profile_pack_v2_template.yaml",
                "experiment_mode": "abundant",
                "listing_plan": {"desired_l0": 33, "listing_rate": 0.36},
                "preplanned_interventions": [
                    {"action_type": "income_shock", "month": 2, "pct_change": -0.10, "target_tier": "all"},
                    {"action_type": "supply_cut", "month": 3, "zone": "A", "count": 8},
                ],
                "seed": 606,
            }

            apply_scholar_release_config(config, scholar_inputs, start_month=1)

            self.assertEqual(config.get("simulation.months"), 3)
            self.assertEqual(config.get("simulation.agent_count"), 137)
            self.assertEqual(config.get("user_property_count"), 91)
            self.assertEqual(config.get("market.zones.A.supply_band_ratio.low"), 0.25)
            self.assertEqual(len(config.get("simulation.preplanned_interventions")), 2)
            self.assertEqual(
                config.get("simulation.preplanned_interventions")[1]["action_type"],
                "supply_cut",
            )
            self.assertEqual(
                config.get("simulation.scholar_cli.fixed_supply_snapshot.snapshot_id"),
                "spindle_medium",
            )
            self.assertEqual(
                config.get("simulation.scholar_cli.preplanned_intervention_count"),
                2,
            )
            self.assertEqual(
                config.get("simulation.scholar_cli.effective_demand_multiplier"),
                round(137 / 91, 4),
            )
            self.assertEqual(
                config.get("simulation.scholar_cli.demand_bucket_plan.coverage_summary.supply_bucket_count_covered"),
                10,
            )
            self.assertEqual(
                config.get("smart_agent.profiled_market_mode.profile_pack.agent_profile_buckets.Y_ENTRY_SOLO_COST.count"),
                14,
            )
        finally:
            if cfg_path.exists():
                cfg_path.unlink()


if __name__ == "__main__":
    unittest.main()
