import unittest
from unittest.mock import patch

from models import Agent, AgentPreference, AgentStory
from services.transaction_service import TransactionService
from transaction_engine import match_properties_for_buyer, match_property_for_buyer


class _Cfg:
    def __init__(self, values):
        self.values = dict(values)

    def get(self, key, default=None):
        return self.values.get(key, default)


class TestSameMonthGuard(unittest.TestCase):
    def _make_buyer(self) -> Agent:
        buyer = Agent(
            id=901,
            name="GuardBuyer",
            age=32,
            marital_status="married",
            cash=120_000.0,
            monthly_income=12_000.0,
        )
        buyer.agent_type = "normal"
        buyer.story = AgentStory(
            background_story="固定护栏测试样本",
            housing_need="学区刚需",
            purchase_motive_primary="education_driven",
            education_path="public_school",
            financial_profile="cash_rich_high_income",
        )
        buyer.preference = AgentPreference(
            target_zone="B",
            max_price=350_000.0,
            need_school_district=True,
            max_affordable_price=350_000.0,
            psychological_price=320_000.0,
        )
        buyer.school_urgency = 2
        buyer.waited_months = 0
        buyer.max_wait_months = 6
        return buyer

    def _make_listing_bundle(self, count: int = 6, zone: str = "B", school: bool = True, base_price: float = 320_000.0):
        listings = []
        props_map = {}
        for idx in range(count):
            pid = 2001 + idx
            price = float(base_price + idx * 4_000.0)
            listings.append(
                {
                    "property_id": pid,
                    "listed_price": price,
                    "owner_id": 50 + idx,
                    "listing_age_months": 0,
                }
            )
            props_map[pid] = {
                "property_id": pid,
                "zone": zone,
                "building_area": 90.0 + idx,
                "bedrooms": 2,
                "is_school_district": bool(school),
                "property_type": "small",
            }
        return listings, props_map

    def test_no_candidate_cycle_does_not_trigger_same_month_loop_stop(self):
        buyer = self._make_buyer()
        listings = [
            {"property_id": 1001, "listed_price": 2_200_000.0, "owner_id": 1, "listing_age_months": 0},
            {"property_id": 1002, "listed_price": 2_350_000.0, "owner_id": 2, "listing_age_months": 0},
        ]
        props_map = {
            1001: {"property_id": 1001, "zone": "B", "building_area": 88.0, "bedrooms": 2, "is_school_district": True, "property_type": "small"},
            1002: {"property_id": 1002, "zone": "B", "building_area": 92.0, "bedrooms": 2, "is_school_district": False, "property_type": "small"},
        }
        config = _Cfg(
            {
                "smart_agent.regime_engine_v1_enabled": True,
                "smart_agent.regime_v1_buyer_search_rhythm_enabled": True,
                "smart_agent.regime_v1_same_month_max_abandon_cycles": 2,
                "smart_agent.candidate_soft_relax_school_enabled": True,
            }
        )

        result = match_properties_for_buyer(
            buyer,
            listings,
            props_map,
            config=config,
            market_trend="STABLE",
        )

        self.assertEqual(result, [])
        retry_trace = list(getattr(buyer, "_last_buyer_match_retry_trace", []) or [])
        retry_budget = dict(getattr(buyer, "_last_buyer_match_retry_budget", {}) or {})
        self.assertIn("structural_no_candidate_defer_next_month", [item.get("result") for item in retry_trace])
        self.assertNotIn("regime_v1_stop_same_month_loop", [item.get("result") for item in retry_trace])
        self.assertEqual(int(retry_budget.get("same_month_abandon_cycles", -1)), 0)

    def test_structural_no_candidate_reason_code_is_stable(self):
        svc = TransactionService.__new__(TransactionService)
        reason = svc._derive_no_selection_reason_code(
            listings_for_buyer_count=24,
            selected_ids=[],
            match_ctx={"selection_reason": "no_candidates_after_hard_filters"},
            retry_trace=[{"result": "structural_no_candidate_defer_next_month"}],
            retry_budget={"attempts_spent": 11, "attempt_budget": 24},
        )
        self.assertEqual(reason, "STRUCTURAL_NO_CANDIDATE_DEFERRED")

    def test_no_candidate_context_records_primary_blockers(self):
        buyer = self._make_buyer()
        listings = [
            {"property_id": 1001, "listed_price": 2_200_000.0, "owner_id": 1, "listing_age_months": 0},
            {"property_id": 1002, "listed_price": 2_350_000.0, "owner_id": 2, "listing_age_months": 0},
            {"property_id": 1003, "listed_price": 320_000.0, "owner_id": 3, "listing_age_months": 0},
        ]
        props_map = {
            1001: {"property_id": 1001, "zone": "B", "building_area": 88.0, "bedrooms": 2, "is_school_district": True, "property_type": "small"},
            1002: {"property_id": 1002, "zone": "B", "building_area": 92.0, "bedrooms": 2, "is_school_district": False, "property_type": "small"},
            1003: {"property_id": 1003, "zone": "A", "building_area": 80.0, "bedrooms": 1, "is_school_district": False, "property_type": "small"},
        }

        result = match_properties_for_buyer(
            buyer,
            listings,
            props_map,
            config=_Cfg({"smart_agent.candidate_soft_relax_school_enabled": False}),
            market_trend="STABLE",
        )

        self.assertEqual(result, [])
        ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
        counts = dict(ctx.get("no_candidate_filter_counts", {}) or {})
        blockers = list(ctx.get("no_candidate_primary_blockers", []) or [])
        self.assertGreaterEqual(int(counts.get("blocked_price_soft_cap", 0)), 2)
        self.assertGreaterEqual(int(counts.get("blocked_school", 0)), 1)
        self.assertIn("blocked_price_soft_cap", blockers)

    def test_same_month_explicit_stop_reuses_previous_llm_decision(self):
        buyer = self._make_buyer()
        buyer._current_matching_month = 2
        listings, props_map = self._make_listing_bundle(count=5, school=True, base_price=260_000.0)
        buyer._last_buyer_match_context = {
            "matching_month": 2,
            "selected_property_id": None,
            "selection_reason": "这批房子都不够合适，本月先不看了",
            "selection_reason_tags": ["route:llm_stop"],
            "stop_search_this_month": True,
            "llm_called": True,
            "shortlist_property_ids": [2001, 2002, 2003, 2004],
            "shortlist_visible_property_ids": [2001, 2002, 2003, 2004],
            "llm_gray_score": 0.41,
            "thought_bubble": "再等等。",
        }

        with patch("transaction_engine.safe_call_llm") as mocked:
            result = match_property_for_buyer(
                buyer,
                listings,
                props_map,
                decision_mode="smart",
                market_trend="STABLE",
                config=_Cfg({"smart_agent.buyer_match_visible_shortlist_cap": 4}),
                retry_attempt=1,
            )

        self.assertIsNone(result)
        mocked.assert_not_called()
        ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
        self.assertTrue(bool(ctx.get("same_month_stop_reused", False)))
        self.assertTrue(bool(ctx.get("stop_search_this_month", False)))
        self.assertFalse(bool(ctx.get("llm_called", True)))
        self.assertIn("route:same_month_stop_reuse", list(ctx.get("selection_reason_tags", []) or []))

    def test_match_property_limits_visible_shortlist_to_cap(self):
        buyer = self._make_buyer()
        buyer.agent_type = "smart"
        buyer._current_matching_month = 1
        listings, props_map = self._make_listing_bundle(count=9, school=True, base_price=330_000.0)

        def _fake_llm(_prompt, _default, model_type=None):
            self.assertEqual(model_type, "fast")
            return {
                "selected_property_id": 2001,
                "thought_bubble": "先选最稳妥的。",
                "reason": "候选已经足够清楚。",
                "monthly_intent": "CONTINUE",
            }

        with patch("transaction_engine.safe_call_llm", side_effect=_fake_llm):
            result = match_property_for_buyer(
                buyer,
                listings,
                props_map,
                decision_mode="smart",
                market_trend="UP",
                config=_Cfg(
                    {
                        "smart_agent.candidate_top_k": 8,
                        "smart_agent.candidate_exploration_slots": 2,
                        "smart_agent.buyer_match_visible_shortlist_cap": 4,
                    }
                ),
            )

        ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
        self.assertEqual(int(ctx.get("shortlist_visible_size", -1)), 4)
        self.assertLessEqual(len(list(ctx.get("shortlist_visible_property_ids", []) or [])), 4)
        self.assertGreater(int(ctx.get("shortlist_full_size", 0)), int(ctx.get("shortlist_visible_size", 0)))
        self.assertEqual(str(ctx.get("llm_route_model", "")), "fast")

    def test_small_homogeneous_shortlist_prefers_fast_route(self):
        buyer = self._make_buyer()
        buyer.agent_type = "smart"
        buyer._current_matching_month = 1
        listings, props_map = self._make_listing_bundle(count=3, school=True, base_price=330_000.0)

        captured = {}

        def _fake_llm(_prompt, _default, model_type=None):
            captured["model_type"] = model_type
            return {
                "selected_property_id": 2001,
                "thought_bubble": "差异不大，直接定一套。",
                "reason": "这是明显简单题。",
                "monthly_intent": "CONTINUE",
            }

        with patch("transaction_engine.safe_call_llm", side_effect=_fake_llm):
            result = match_property_for_buyer(
                buyer,
                listings,
                props_map,
                decision_mode="smart",
                market_trend="STABLE",
                config=_Cfg({}),
            )

        self.assertIsNotNone(result)
        self.assertEqual(captured.get("model_type"), "fast")
        ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
        self.assertEqual(str(ctx.get("llm_route_model", "")), "fast")
        self.assertEqual(str(ctx.get("llm_route_reason", "")), "small_homogeneous_shortlist")

    def test_match_properties_dedupes_same_month_visible_shortlist_on_retry(self):
        buyer = self._make_buyer()
        buyer._current_matching_month = 1
        listings, props_map = self._make_listing_bundle(count=5, school=True, base_price=260_000.0)
        seen_candidate_sets = []

        def _fake_match(_buyer, remaining, _props_map, **kwargs):
            remaining_ids = [int(item["property_id"]) for item in remaining]
            seen_candidate_sets.append(remaining_ids)
            if len(seen_candidate_sets) == 1:
                buyer._last_buyer_match_context = {
                    "matching_month": 1,
                    "selected_property_id": None,
                    "selection_reason": "这批房先不考虑，换一组新的。",
                    "selection_reason_tags": ["route:test_retry"],
                    "stop_search_this_month": False,
                    "llm_called": True,
                    "shortlist_property_ids": [2001, 2002, 2003],
                    "shortlist_visible_property_ids": [2001, 2002, 2003],
                }
                return None
            return remaining[0]

        with patch("transaction_engine.match_property_for_buyer", side_effect=_fake_match):
            result = match_properties_for_buyer(
                buyer,
                listings,
                props_map,
                config=_Cfg({"smart_agent.monthly_retry_attempts": 1, "smart_agent.normal_buyer_backup_slots": 0}),
                market_trend="STABLE",
            )

        self.assertEqual(seen_candidate_sets[0], [2001, 2002, 2003, 2004, 2005])
        self.assertGreaterEqual(len(seen_candidate_sets), 2)
        self.assertTrue(all(pid not in seen_candidate_sets[-1] for pid in [2001, 2002, 2003]))
        self.assertGreaterEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
