import unittest
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.run_line_b_forced_role import (
    _parse_init_supply_snapshot,
    apply_overrides,
    build_plan,
    classify_root_causes,
    evaluate_gate,
)


class TestLineBForcedRoleRunner(unittest.TestCase):
    def test_parse_init_supply_snapshot_prefers_init_log_over_end_state(self):
        with TemporaryDirectory() as tmp_dir:
            run_dir = Path(tmp_dir)
            (run_dir / "simulation_run.log").write_text(
                "2026-04-09 18:22:58,253 - INFO - "
                "Init supply coverage snapshot: "
                "A_owned=40 B_owned=20 A_for_sale=9 B_for_sale=8 tradable=17 | "
                "targets(A_owner=8, B_owner=20, A_for_sale=4, B_for_sale=8, tradable=10)\n",
                encoding="utf-8",
            )

            snapshot = _parse_init_supply_snapshot(run_dir)

        self.assertEqual(snapshot["source"], "init_supply_coverage_snapshot")
        self.assertEqual(snapshot["l0"], 17)
        self.assertEqual(snapshot["actual"]["a_for_sale"], 9)
        self.assertEqual(snapshot["actual"]["b_for_sale"], 8)
        self.assertEqual(snapshot["target"]["tradable"], 10)

    def test_build_plan_expands_apply_months_and_metadata(self):
        plan = build_plan(group_id="V3", seed=606, months=3, agent_count=50)

        forced_cfg = plan["smart_agent"]["forced_role_mode"]
        self.assertTrue(forced_cfg["enabled"])
        self.assertEqual(forced_cfg["apply_months"], [1, 2, 3])
        self.assertEqual(forced_cfg["quota"]["buyer"], 12)
        self.assertEqual(plan["macro_environment"]["override_mode"], "optimistic")
        self.assertEqual(plan["line_b_metadata"]["forced_role_mode"]["quota"]["buyer_seller"], 6)

    def test_apply_overrides_can_set_init_supply_guard_knobs(self):
        plan = build_plan(group_id="V3", seed=606, months=1, agent_count=50)
        args = Namespace(
            income_adjustment_rate=None,
            initial_listing_rate=None,
            quota_buyer=None,
            quota_seller=None,
            quota_buyer_seller=None,
            init_min_for_sale_floor_zone_a=None,
            init_min_for_sale_floor_zone_b=2,
            init_min_tradable_floor_total=6,
            init_min_for_sale_ratio_zone_a=None,
            init_min_for_sale_ratio_zone_b=0.01,
            disable_init_multi_owner_listings=True,
            enable_profiled_market_mode=False,
            profile_pack_path="",
            profile_background_library_path="",
            profile_experiment_mode="abundant",
            enable_hard_bucket_matcher=False,
            hard_bucket_include_soft_buckets=False,
            hard_bucket_require_profiled_buyer=False,
            disable_hard_bucket_strict_unmapped=False,
        )

        updated = apply_overrides(plan, args)

        self.assertEqual(updated["smart_agent"]["init_min_for_sale_floor_zone_b"], 2)
        self.assertEqual(updated["smart_agent"]["init_min_tradable_floor_total"], 6)
        self.assertEqual(updated["smart_agent"]["init_min_for_sale_ratio_zone_b"], 0.01)
        self.assertFalse(updated["smart_agent"]["init_multi_owner_listings_enabled"])
        self.assertEqual(
            updated["line_b_metadata"]["override_summary"]["init_supply_guard"]["init_min_for_sale_floor_zone_b"],
            2,
        )

    def test_apply_overrides_can_enable_profiled_market_mode(self):
        plan = build_plan(group_id="V1", seed=606, months=1, agent_count=50)
        args = Namespace(
            income_adjustment_rate=None,
            initial_listing_rate=None,
            quota_buyer=None,
            quota_seller=None,
            quota_buyer_seller=None,
            init_min_for_sale_floor_zone_a=None,
            init_min_for_sale_floor_zone_b=None,
            init_min_tradable_floor_total=None,
            init_min_for_sale_ratio_zone_a=None,
            init_min_for_sale_ratio_zone_b=None,
            disable_init_multi_owner_listings=False,
            enable_profiled_market_mode=True,
            profile_pack_path="config/line_b_profiled_market_template.yaml",
            profile_background_library_path="config/persona_background_library.json",
            profile_experiment_mode="scarce",
            enable_hard_bucket_matcher=True,
            hard_bucket_include_soft_buckets=False,
            hard_bucket_require_profiled_buyer=False,
            disable_hard_bucket_strict_unmapped=False,
        )

        updated = apply_overrides(plan, args)
        profiled = updated["smart_agent"]["profiled_market_mode"]
        self.assertTrue(profiled["enabled"])
        self.assertEqual(profiled["profile_pack_path"], "config/line_b_profiled_market_template.yaml")
        self.assertEqual(profiled["persona_generation_mode"], "code_only")
        self.assertEqual(profiled["background_library_path"], "config/persona_background_library.json")
        self.assertEqual(profiled["experiment_mode"], "scarce")
        self.assertTrue(profiled["hard_bucket_matcher_enabled"])
        self.assertTrue(
            updated["line_b_metadata"]["override_summary"]["profiled_market_mode"]["enabled"]
        )

    def test_apply_overrides_can_set_income_adjustment_rate(self):
        plan = build_plan(group_id="V1", seed=606, months=3, agent_count=50)
        args = Namespace(
            income_adjustment_rate=1.08,
            initial_listing_rate=None,
            quota_buyer=None,
            quota_seller=None,
            quota_buyer_seller=None,
            init_min_for_sale_floor_zone_a=None,
            init_min_for_sale_floor_zone_b=None,
            init_min_tradable_floor_total=None,
            init_min_for_sale_ratio_zone_a=None,
            init_min_for_sale_ratio_zone_b=None,
            disable_init_multi_owner_listings=False,
            enable_profiled_market_mode=False,
            profile_pack_path="",
            profile_background_library_path="",
            profile_experiment_mode="abundant",
            enable_hard_bucket_matcher=False,
            hard_bucket_include_soft_buckets=False,
            hard_bucket_require_profiled_buyer=False,
            disable_hard_bucket_strict_unmapped=False,
        )

        updated = apply_overrides(plan, args)

        self.assertEqual(updated["simulation"]["agent"]["income_adjustment_rate"], 1.08)
        self.assertTrue(updated["simulation"]["agent"]["external_shock_operator"]["enabled"])
        self.assertEqual(updated["simulation"]["agent"]["external_shock_operator"]["cash_scale"], 1.08)
        self.assertEqual(
            updated["line_b_metadata"]["override_summary"]["income_adjustment_rate"],
            1.08,
        )
        self.assertEqual(
            updated["line_b_metadata"]["override_summary"]["external_shock_operator"]["max_price_scale"],
            1.08,
        )

    def test_apply_overrides_can_enable_buyer_seller_intent_split(self):
        plan = build_plan(group_id="V2", seed=606, months=1, agent_count=20)
        args = Namespace(
            income_adjustment_rate=None,
            initial_listing_rate=None,
            quota_buyer=None,
            quota_seller=None,
            quota_buyer_seller=None,
            init_min_for_sale_floor_zone_a=None,
            init_min_for_sale_floor_zone_b=None,
            init_min_tradable_floor_total=None,
            init_min_for_sale_ratio_zone_a=None,
            init_min_for_sale_ratio_zone_b=None,
            disable_init_multi_owner_listings=False,
            disable_activation_hard_only_prefilter=False,
            activation_prefilter_normal_min_cash=None,
            activation_prefilter_normal_min_income=None,
            enable_profiled_market_mode=False,
            profile_pack_path="",
            profile_background_library_path="",
            profile_experiment_mode="abundant",
            enable_hard_bucket_matcher=False,
            hard_bucket_include_soft_buckets=False,
            hard_bucket_require_profiled_buyer=False,
            disable_hard_bucket_strict_unmapped=False,
            activation_mode="natural",
            governance_gate_mode="warn",
            governance_profiled_required=False,
            governance_hard_bucket_required=False,
            governance_hybrid_floor_enabled=False,
            governance_autofill_supply_floor=0,
            governance_autofill_demand_floor=0,
            governance_severe_bucket_deficit_ratio=5.0,
            enable_buyer_seller_intent_split=True,
            buyer_seller_intent_split_apply_to_forced=False,
            buyer_seller_intent_split_model_type="fast",
        )

        updated = apply_overrides(plan, args)

        split_cfg = updated["smart_agent"]["buyer_seller_intent_split"]
        self.assertTrue(split_cfg["enabled"])
        self.assertEqual(split_cfg["model_type"], "fast")

    def test_gate_and_root_cause_classification(self):
        metrics = {
            "l0": 10,
            "b0_role": 8,
            "b0_order": 2,
            "r_order": 0.2,
            "matches_m1": 4,
            "orders_m1": 2,
            "transactions_m1": 0,
            "forced_role_mode_connected": True,
            "forced_role_mode_pure": True,
            "failure_reasons_m1": {
                "INSUFFICIENT_DOWN_PAYMENT": 3,
                "DTI_EXCEEDED": 1,
            },
            "buyer_seller_chain_modes_m1": {
                "sell_first": 2,
            },
            "normal_seller_price_adjustment_m1": {
                "validation_status": "no_rows_in_sample",
            },
        }
        error_counts = {"total": 0}

        gate = evaluate_gate(group_id="V3", metrics=metrics, error_counts=error_counts)
        root_causes = classify_root_causes(group_id="V3", metrics=metrics, gate=gate)

        self.assertFalse(gate["boundary_pass"])
        self.assertFalse(gate["overall_pass"])
        self.assertIn("seller_market_boundary_not_formed", root_causes)
        self.assertIn("role_labels_not_converted_to_effective_orders", root_causes)
        self.assertIn("funding_constraints_suppressed_orders", root_causes)
        self.assertIn("buyer_seller_sell_first_delayed_b0_order", root_causes)
        self.assertIn("chain_effective_but_settlement_lag_not_expanded", root_causes)
        self.assertIn("normal_price_adjustment_not_observable_in_m1_sample", root_causes)


if __name__ == "__main__":
    unittest.main()
