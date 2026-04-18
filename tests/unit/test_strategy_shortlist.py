import unittest

from models import AgentPreference
from transaction_engine import (
    _build_selection_reason_tags,
    _should_run_buyer_crowd_reselect,
    _should_run_competitive_seller_recheck,
    _resolve_batch_tie_break_route,
    _resolve_cross_month_excluded_ids,
    _resolve_hot_listing_auto_bidding_mode,
    _strategy_score_candidate,
    build_candidate_shortlist,
    compute_dynamic_preference_weights,
    resolve_strategy_profile,
)
from models import Agent, AgentStory


class TestStrategyShortlist(unittest.TestCase):
    def test_buyer_crowd_reselect_only_runs_for_smart_or_extreme_fast_case(self):
        self.assertTrue(
            _should_run_buyer_crowd_reselect(
                route_model="smart",
                selected_crowd_units=3.5,
                crowd_tolerance_units=2.0,
                low_crowd_alternative_count=1,
                retry_attempt=0,
                config=None,
            )
        )
        self.assertFalse(
            _should_run_buyer_crowd_reselect(
                route_model="fast",
                selected_crowd_units=2.3,
                crowd_tolerance_units=2.0,
                low_crowd_alternative_count=2,
                retry_attempt=0,
                config=None,
            )
        )
        self.assertTrue(
            _should_run_buyer_crowd_reselect(
                route_model="fast",
                selected_crowd_units=4.5,
                crowd_tolerance_units=2.0,
                low_crowd_alternative_count=3,
                retry_attempt=0,
                config=None,
            )
        )

    def test_competitive_seller_recheck_only_runs_for_gray_or_tight_fast_cases(self):
        self.assertTrue(
            _should_run_competitive_seller_recheck(
                route_model="smart",
                bid_count=2,
                spread_ratio=0.12,
                round_index=1,
                total_rounds=3,
                config=None,
            )
        )
        self.assertFalse(
            _should_run_competitive_seller_recheck(
                route_model="fast",
                bid_count=3,
                spread_ratio=0.02,
                round_index=1,
                total_rounds=3,
                config=None,
            )
        )
        self.assertTrue(
            _should_run_competitive_seller_recheck(
                route_model="fast",
                bid_count=5,
                spread_ratio=0.02,
                round_index=1,
                total_rounds=3,
                config=None,
            )
        )

    def test_resolve_strategy_profile_by_market(self):
        self.assertEqual(
            resolve_strategy_profile("smart", "balanced", "DOWN"),
            "smart_downturn_defensive",
        )
        self.assertEqual(
            resolve_strategy_profile("smart", "aggressive", "UP"),
            "smart_uptrend_momentum",
        )
        self.assertEqual(
            resolve_strategy_profile("normal", "balanced", "DOWN"),
            "normal_balanced",
        )

    def test_downturn_prefers_discount_candidate(self):
        pref = AgentPreference(
            target_zone="A",
            max_price=2_000_000,
            need_school_district=True,
            education_weight=8,
            comfort_weight=5,
            price_sensitivity=9,
        )
        properties_map = {
            1: {
                "property_id": 1,
                "zone": "A",
                "base_value": 1_500_000,
                "current_valuation": 1_500_000,
                "is_school_district": True,
                "building_area": 95,
            },
            2: {
                "property_id": 2,
                "zone": "A",
                "base_value": 1_500_000,
                "current_valuation": 1_500_000,
                "is_school_district": False,
                "building_area": 95,
            },
        }
        candidates = [
            {"property_id": 1, "listed_price": 1_450_000},  # weak discount + school
            {"property_id": 2, "listed_price": 1_100_000},  # deep discount, non-school
        ]
        shortlist = build_candidate_shortlist(
            candidates=candidates,
            properties_map=properties_map,
            pref=pref,
            strategy_profile="smart_downturn_defensive",
            top_k=2,
        )
        self.assertEqual(shortlist[0]["property_id"], 2)

    def test_dynamic_weights_smart_downtrend_relaxes_education(self):
        props_info = [
            {"price": 800_000, "is_developer": "Yes (新房/特价)"},
            {"price": 1_000_000, "is_developer": "No (二手)"},
        ]
        result = compute_dynamic_preference_weights(
            decision_mode="smart",
            market_trend="DOWN",
            base_edu_weight=8,
            base_price_sensitivity=5,
            props_info=props_info,
        )
        self.assertLess(result["education_weight"], 8)
        self.assertGreater(result["price_sensitivity"], 5)

    def test_dynamic_weights_normal_path_no_change(self):
        result = compute_dynamic_preference_weights(
            decision_mode="normal",
            market_trend="DOWN",
            base_edu_weight=7,
            base_price_sensitivity=4,
            props_info=[],
        )
        self.assertEqual(result["education_weight"], 7)
        self.assertEqual(result["price_sensitivity"], 4)

    def test_bargain_discount_trigger_changes_bonus(self):
        pref = AgentPreference(
            target_zone="A",
            max_price=2_000_000,
            need_school_district=False,
            education_weight=5,
            comfort_weight=5,
            price_sensitivity=8,
        )
        listing = {"property_id": 1, "listed_price": 1_290_000}  # ~14% discount vs 1.5m
        prop = {
            "property_id": 1,
            "zone": "A",
            "base_value": 1_500_000,
            "current_valuation": 1_500_000,
            "is_school_district": False,
            "building_area": 90,
        }
        s_lo = _strategy_score_candidate(
            listing, prop, pref, "smart_downturn_defensive", bargain_discount_trigger=0.12
        )
        s_hi = _strategy_score_candidate(
            listing, prop, pref, "smart_downturn_defensive", bargain_discount_trigger=0.18
        )
        self.assertGreater(s_lo, s_hi)

    def test_selection_reason_tags_capture_persona_and_budget(self):
        buyer = Agent(id=7, name="李楠", age=32, marital_status="married", cash=300_000, monthly_income=18_000)
        buyer.story = AgentStory(
            purchase_motive_primary="starter_home",
            housing_stage="no_home_first_purchase",
            family_stage="young_family",
            education_path="public_school_priority",
            financial_profile="cashflow_sensitive",
        )
        pref = AgentPreference(target_zone="B", max_price=2_000_000, need_school_district=False)
        selected_listing = {"property_id": 11, "listed_price": 1_200_000}
        selected_property = {"property_id": 11, "zone": "B", "is_school_district": False}
        shortlist_context = [
            {
                "property_id": 11,
                "persona_reasons": ["刚需上车：B区作为可承受起点", "现金流敏感：偏好压力更小的房源"],
            }
        ]

        tags = _build_selection_reason_tags(
            buyer=buyer,
            pref=pref,
            selected_listing=selected_listing,
            selected_property=selected_property,
            shortlist_context=shortlist_context,
        )

        self.assertIn("zone:B", tags)
        self.assertIn("school:no", tags)
        self.assertIn("motive:starter_home", tags)
        self.assertIn("housing:no_home_first_purchase", tags)
        self.assertIn("finance:cashflow_sensitive", tags)
        self.assertIn("bias:刚需上车：B区作为可承受起点", tags)
        self.assertIn("budget:comfortable", tags)

    def test_shortlist_boosts_unseen_target_zone_candidates(self):
        pref = AgentPreference(
            target_zone="B",
            max_price=2_000_000,
            need_school_district=False,
            education_weight=5,
            comfort_weight=5,
            price_sensitivity=8,
        )
        buyer = Agent(id=18, name="陈越", age=30, marital_status="single", cash=600_000, monthly_income=30_000)
        buyer._historical_seen_property_ids = [201]
        properties_map = {
            101: {
                "property_id": 101,
                "zone": "A",
                "base_value": 1_400_000,
                "current_valuation": 1_400_000,
                "is_school_district": False,
                "building_area": 96,
            },
            201: {
                "property_id": 201,
                "zone": "B",
                "base_value": 1_380_000,
                "current_valuation": 1_380_000,
                "is_school_district": False,
                "building_area": 95,
            },
            202: {
                "property_id": 202,
                "zone": "B",
                "base_value": 1_390_000,
                "current_valuation": 1_390_000,
                "is_school_district": False,
                "building_area": 95,
            },
        }
        candidates = [
            {"property_id": 101, "listed_price": 1_220_000},
            {"property_id": 201, "listed_price": 1_210_000},
            {"property_id": 202, "listed_price": 1_225_000},
        ]

        shortlist = build_candidate_shortlist(
            candidates=candidates,
            properties_map=properties_map,
            pref=pref,
            strategy_profile="smart_downturn_defensive",
            top_k=2,
            exploration_slots=0,
            config={
                "smart_agent.candidate_score_weight_rule": 0.55,
                "smart_agent.candidate_unseen_bonus": 0.12,
                "smart_agent.target_zone_unseen_bonus": 0.08,
                "smart_agent.b_zone_shortlist_min_slots": 2,
            },
            agent=buyer,
        )

        self.assertEqual({item["property_id"] for item in shortlist[:2]}, {201, 202})
        scoring_map = getattr(buyer, "_last_candidate_scoring_map", {})
        self.assertGreater(scoring_map[202]["unseen_discovery_bonus"], scoring_map[201]["unseen_discovery_bonus"])

    def test_repriced_property_can_escape_previous_month_exclusion(self):
        buyer = Agent(id=28, name="周扬", age=34, marital_status="married", cash=500_000, monthly_income=28_000)
        buyer._current_matching_month = 4
        buyer._attempted_property_ids_by_month = {
            3: [101, 102, 103],
        }
        buyer._repriced_reentry_property_ids = [102]

        excluded_ids = _resolve_cross_month_excluded_ids(buyer)

        self.assertEqual(excluded_ids, {101, 103})

    def test_fake_hot_circuit_prefers_same_bucket_sibling(self):
        pref = AgentPreference(
            target_zone="A",
            max_price=3_000_000,
            need_school_district=False,
            education_weight=5,
            comfort_weight=5,
            price_sensitivity=6,
        )
        buyer = Agent(id=52, name="林川", age=35, marital_status="married", cash=900_000, monthly_income=40_000)
        buyer._candidate_heat_meta_map = {
            11: {
                "recent_match_count": 14,
                "recent_negotiation_count": 0,
                "recent_transaction_count": 0,
                "recent_negotiation_ratio": 0.0,
                "hot_listing_score": 0.9,
                "fake_hot_historical": True,
            },
            12: {
                "recent_match_count": 2,
                "recent_negotiation_count": 1,
                "recent_transaction_count": 0,
                "recent_negotiation_ratio": 0.5,
                "hot_listing_score": 0.2,
                "fake_hot_historical": False,
            },
        }
        buyer._candidate_monthly_quota_used_map = {11: 5, 12: 1}
        properties_map = {
            11: {
                "property_id": 11,
                "zone": "A",
                "base_value": 2_200_000,
                "current_valuation": 2_200_000,
                "is_school_district": False,
                "building_area": 95,
                "property_type": "small",
            },
            12: {
                "property_id": 12,
                "zone": "A",
                "base_value": 2_180_000,
                "current_valuation": 2_180_000,
                "is_school_district": False,
                "building_area": 94,
                "property_type": "small",
            },
            21: {
                "property_id": 21,
                "zone": "B",
                "base_value": 2_100_000,
                "current_valuation": 2_100_000,
                "is_school_district": False,
                "building_area": 92,
                "property_type": "small",
            },
        }
        candidates = [
            {"property_id": 11, "listed_price": 1_980_000},
            {"property_id": 12, "listed_price": 1_990_000},
            {"property_id": 21, "listed_price": 1_970_000},
        ]

        shortlist = build_candidate_shortlist(
            candidates=candidates,
            properties_map=properties_map,
            pref=pref,
            strategy_profile="smart_downturn_defensive",
            top_k=2,
            config={
                "smart_agent.candidate_fake_hot_circuit_enabled": True,
                "smart_agent.candidate_fake_hot_same_month_quota_threshold": 4,
                "smart_agent.candidate_fake_hot_pool_preserve_min": 1,
                "smart_agent.candidate_sibling_rotation_bonus": 0.20,
            },
            agent=buyer,
        )

        shortlist_ids = [int(item["property_id"]) for item in shortlist[:2]]
        self.assertIn(12, shortlist_ids)
        self.assertNotEqual(shortlist_ids[0], 11)
        scoring_map = getattr(buyer, "_last_candidate_scoring_map", {})
        self.assertTrue(bool(scoring_map[11]["heat_state"]["is_fake_hot"]))
        self.assertFalse(bool(scoring_map[12]["heat_state"]["is_fake_hot"]))

    def test_hot_listing_auto_bidding_mode_triggers_for_competitive_listing(self):
        mode = _resolve_hot_listing_auto_bidding_mode(
            {
                "_recent_match_count": 12,
                "_recent_competition_count": 4,
                "_recent_commitment_count": 4,
                "_real_competition_score": 0.72,
                "_current_interest_count": 4,
            },
            buyer_count=4,
            config={
                "smart_agent.hot_listing_auto_bidding_enabled": True,
                "smart_agent.hot_listing_auto_bidding_min_buyers": 3,
                "smart_agent.hot_listing_auto_bidding_min_recent_competitions": 3,
                "smart_agent.hot_listing_auto_bidding_min_heat_score": 0.60,
                "smart_agent.hot_listing_auto_bidding_mode": "BATCH",
            },
        )
        self.assertEqual(mode, "BATCH")

    def test_hot_listing_auto_bidding_mode_blocks_fake_hot_listing(self):
        mode = _resolve_hot_listing_auto_bidding_mode(
            {
                "_recent_match_count": 18,
                "_recent_commitment_count": 1,
                "_recent_competition_count": 3,
                "_real_competition_score": 0.81,
                "_current_interest_count": 4,
                "_fake_hot_historical": True,
            },
            buyer_count=4,
            config={
                "smart_agent.hot_listing_auto_bidding_enabled": True,
                "smart_agent.fake_hot_block_auction_enabled": True,
                "smart_agent.true_competition_force_auction_enabled": True,
            },
        )
        self.assertIsNone(mode)

    def test_true_competition_force_auction_triggers_before_hot_score(self):
        mode = _resolve_hot_listing_auto_bidding_mode(
            {
                "_recent_match_count": 6,
                "_recent_commitment_count": 3,
                "_recent_competition_count": 3,
                "_real_competition_score": 0.22,
                "_current_interest_count": 3,
            },
            buyer_count=3,
            config={
                "smart_agent.hot_listing_auto_bidding_enabled": True,
                "smart_agent.hot_listing_auto_bidding_min_recent_competitions": 8,
                "smart_agent.hot_listing_auto_bidding_min_heat_score": 0.90,
                "smart_agent.true_competition_force_auction_enabled": True,
                "smart_agent.true_competition_force_auction_min_buyers": 2,
                "smart_agent.true_competition_force_auction_min_commitments": 2,
                "smart_agent.true_competition_force_auction_min_competitions": 2,
                "smart_agent.true_competition_force_auction_mode": "CLASSIC",
            },
        )
        self.assertEqual(mode, "CLASSIC")

    def test_hot_listing_auto_bidding_mode_does_not_trigger_on_exposure_only_heat(self):
        mode = _resolve_hot_listing_auto_bidding_mode(
            {
                "_recent_match_count": 18,
                "_recent_exposure_count": 18,
                "_recent_commitment_count": 1,
                "_recent_competition_count": 0,
                "_hot_listing_score": 0.08,
                "_real_competition_score": 0.08,
                "_current_interest_count": 4,
            },
            buyer_count=4,
            config={
                "smart_agent.hot_listing_auto_bidding_enabled": True,
                "smart_agent.hot_listing_auto_bidding_min_buyers": 3,
                "smart_agent.hot_listing_auto_bidding_min_recent_competitions": 2,
                "smart_agent.hot_listing_auto_bidding_min_heat_score": 0.60,
                "smart_agent.hot_listing_auto_bidding_mode": "BATCH",
            },
        )
        self.assertIsNone(mode)

    def test_counterfactual_feedback_penalizes_rejected_listing_cluster(self):
        pref = AgentPreference(
            target_zone="A",
            max_price=3_000_000,
            need_school_district=False,
            education_weight=5,
            comfort_weight=5,
            price_sensitivity=6,
        )
        buyer = Agent(id=88, name="测试买家", age=33, marital_status="married", cash=1_000_000, monthly_income=40_000)
        buyer._last_buyer_match_context = {
            "rejected_property_feedback": [
                {
                    "property_id": 101,
                    "reason_tag": "too_expensive",
                    "reason": "同类里太贵",
                    "cluster_key": "A:NOSCHOOL:JUST:MEDIUM:HIGH",
                    "listed_price": 2_280_000,
                    "building_area": 96.0,
                    "zone": "A",
                    "is_school_district": False,
                }
            ]
        }
        properties_map = {
            101: {"property_id": 101, "zone": "A", "base_value": 2_400_000, "current_valuation": 2_400_000, "is_school_district": False, "building_area": 96, "property_type": "small"},
            102: {"property_id": 102, "zone": "A", "base_value": 2_380_000, "current_valuation": 2_380_000, "is_school_district": False, "building_area": 95, "property_type": "small"},
            201: {"property_id": 201, "zone": "B", "base_value": 2_300_000, "current_valuation": 2_300_000, "is_school_district": False, "building_area": 96, "property_type": "small"},
        }
        shortlist = build_candidate_shortlist(
            candidates=[
                {"property_id": 101, "listed_price": 2_280_000},
                {"property_id": 102, "listed_price": 2_260_000},
                {"property_id": 201, "listed_price": 2_240_000},
            ],
            properties_map=properties_map,
            pref=pref,
            strategy_profile="smart_downturn_defensive",
            top_k=2,
            agent=buyer,
        )
        scoring_map = getattr(buyer, "_last_candidate_scoring_map", {})
        self.assertGreater(scoring_map[101]["counterfactual_penalty"], 0.0)
        self.assertGreater(scoring_map[102]["counterfactual_penalty"], 0.0)
        shortlist_ids = [int(item["property_id"]) for item in shortlist[:2]]
        self.assertIn(201, shortlist_ids)

    def test_shortlist_diversity_cluster_cap_limits_same_cluster_crowding(self):
        pref = AgentPreference(
            target_zone="A",
            max_price=3_000_000,
            need_school_district=False,
            education_weight=5,
            comfort_weight=5,
            price_sensitivity=6,
        )
        properties_map = {
            1: {"property_id": 1, "zone": "A", "base_value": 2_100_000, "current_valuation": 2_100_000, "is_school_district": False, "building_area": 96, "property_type": "small"},
            2: {"property_id": 2, "zone": "A", "base_value": 2_090_000, "current_valuation": 2_090_000, "is_school_district": False, "building_area": 95, "property_type": "small"},
            3: {"property_id": 3, "zone": "A", "base_value": 2_080_000, "current_valuation": 2_080_000, "is_school_district": False, "building_area": 94, "property_type": "small"},
            4: {"property_id": 4, "zone": "B", "base_value": 2_070_000, "current_valuation": 2_070_000, "is_school_district": True, "building_area": 108, "property_type": "improve"},
        }
        shortlist = build_candidate_shortlist(
            candidates=[
                {"property_id": 1, "listed_price": 2_000_000},
                {"property_id": 2, "listed_price": 1_995_000},
                {"property_id": 3, "listed_price": 1_990_000},
                {"property_id": 4, "listed_price": 1_980_000},
            ],
            properties_map=properties_map,
            pref=pref,
            strategy_profile="smart_downturn_defensive",
            top_k=3,
            config={"smart_agent.candidate_diversity_cluster_cap": 2},
        )
        scoring_clusters = []
        for item in shortlist[:3]:
            zone = str(properties_map[int(item["property_id"])]["zone"]).upper()
            school = "SCHOOL" if bool(properties_map[int(item["property_id"])]["is_school_district"]) else "NOSCHOOL"
            scoring_clusters.append((zone, school))
        self.assertLessEqual(scoring_clusters.count(("A", "NOSCHOOL")), 2)

    def test_batch_tie_break_route_prefers_smart_on_gray_case(self):
        seller = Agent(id=801, name="卖家", age=45, marital_status="married", cash=0, monthly_income=0)
        seller.agent_type = "smart"
        buyers = []
        for idx in range(3):
            buyer = Agent(id=900 + idx, name=f"买家{idx}", age=30, marital_status="single", cash=500_000, monthly_income=30_000)
            buyer.agent_type = "smart" if idx < 2 else "normal"
            buyers.append(buyer)
        finalists = [
            {"buyer": buyers[0], "price": 2_000_000},
            {"buyer": buyers[1], "price": 1_999_000},
            {"buyer": buyers[2], "price": 1_998_500},
        ]
        route = _resolve_batch_tie_break_route(
            finalists,
            seller,
            {
                "_hot_listing_score": 0.80,
                "_current_interest_count": 5,
            },
            config={
                "smart_agent.batch_tie_break_dual_routing_enabled": True,
                "smart_agent.batch_tie_break_gray_score_threshold": 0.45,
            },
        )
        self.assertEqual(route["model"], "smart")
        self.assertGreaterEqual(float(route["gray_score"]), 0.45)


if __name__ == "__main__":
    unittest.main()
