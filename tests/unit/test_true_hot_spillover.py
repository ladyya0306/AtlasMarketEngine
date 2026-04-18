import sqlite3
import unittest

from models import Agent, AgentPreference
from services.transaction_service import TransactionService
from transaction_engine import build_candidate_shortlist


class _Cfg:
    def __init__(self, data=None):
        self._data = data or {}
        self.mortgage = {}

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestTrueHotSpillover(unittest.TestCase):
    def test_shortlist_adds_market_spillover_bonus_to_true_hot_siblings(self):
        pref = AgentPreference(
            target_zone="A",
            max_price=2_100_000,
            need_school_district=False,
            education_weight=5,
            comfort_weight=5,
            price_sensitivity=7,
        )
        buyer = Agent(id=1, cash=900_000, monthly_income=35_000)
        buyer.preference = pref
        buyer._candidate_heat_meta_map = {
            101: {
                "recent_exposure_count": 8,
                "recent_commitment_count": 3,
                "recent_competition_count": 4,
                "recent_negotiation_count": 2,
                "recent_transaction_count": 1,
                "hot_listing_score": 0.82,
                "real_competition_score": 0.82,
                "fake_hot_historical": False,
            },
            102: {
                "recent_exposure_count": 2,
                "recent_commitment_count": 0,
                "recent_competition_count": 0,
                "recent_negotiation_count": 0,
                "recent_transaction_count": 0,
                "hot_listing_score": 0.06,
                "real_competition_score": 0.06,
                "fake_hot_historical": False,
            },
            201: {
                "recent_exposure_count": 1,
                "recent_commitment_count": 0,
                "recent_competition_count": 0,
                "recent_negotiation_count": 0,
                "recent_transaction_count": 0,
                "hot_listing_score": 0.02,
                "real_competition_score": 0.02,
                "fake_hot_historical": False,
            },
        }
        properties_map = {
            101: {"property_id": 101, "zone": "A", "property_type": "apartment", "is_school_district": False, "building_area": 98, "current_valuation": 1_700_000, "base_value": 1_700_000},
            102: {"property_id": 102, "zone": "A", "property_type": "apartment", "is_school_district": False, "building_area": 96, "current_valuation": 1_680_000, "base_value": 1_680_000},
            201: {"property_id": 201, "zone": "B", "property_type": "apartment", "is_school_district": False, "building_area": 96, "current_valuation": 1_650_000, "base_value": 1_650_000},
        }
        candidates = [
            {"property_id": 101, "listed_price": 1_650_000, "owner_id": 9},
            {"property_id": 102, "listed_price": 1_660_000, "owner_id": 8},
            {"property_id": 201, "listed_price": 1_600_000, "owner_id": 7},
        ]

        shortlist = build_candidate_shortlist(
            candidates=candidates,
            properties_map=properties_map,
            pref=pref,
            strategy_profile="normal_balanced",
            top_k=3,
            config={"smart_agent.candidate_tiebreak_noise": 0.0},
            agent=buyer,
        )

        scoring = getattr(buyer, "_last_candidate_scoring_map", {})
        self.assertEqual(len(shortlist), 3)
        self.assertGreater(float(scoring[102].get("market_spillover_bonus", 0.0) or 0.0), 0.0)
        self.assertEqual(float(scoring[101].get("market_spillover_bonus", 0.0) or 0.0), 0.0)

    def test_recovery_refill_prioritizes_same_substitute_ring_after_outbid(self):
        conn = sqlite3.connect(":memory:")
        try:
            svc = TransactionService(_Cfg({}), conn)
            buyer = Agent(id=7, cash=900_000, monthly_income=30_000)
            buyer.preference.target_zone = "A"
            buyer.preference.max_price = 2_100_000
            buyer.preference.need_school_district = True
            buyer.timing_role = "buy_now"
            buyer.decision_urgency = "high"
            buyer.lifecycle_labels = ["DEADLINE_PRESSURE"]

            source_listing = {"property_id": 1001, "listed_price": 1_600_000}
            source_prop = {
                "property_id": 1001,
                "zone": "A",
                "property_type": "apartment",
                "is_school_district": True,
                "building_area": 96,
            }
            svc._set_buyer_substitute_spillover_context(
                buyer,
                listing=source_listing,
                prop=source_prop,
                month=3,
                competition_strength=3,
            )

            active_listings = [
                {"property_id": 2001, "listed_price": 1_590_000, "owner_id": 11, "status": "for_sale"},
                {"property_id": 2002, "listed_price": 1_220_000, "owner_id": 12, "status": "for_sale"},
            ]
            props_map = {
                2001: {
                    "property_id": 2001,
                    "zone": "A",
                    "property_type": "apartment",
                    "is_school_district": True,
                    "building_area": 95,
                },
                2002: {
                    "property_id": 2002,
                    "zone": "A",
                    "property_type": "apartment",
                    "is_school_district": True,
                    "building_area": 72,
                },
            }

            refill = svc._build_recovery_refill_candidates(
                cursor=conn.cursor(),
                month=3,
                buyer=buyer,
                active_listings=active_listings,
                props_map=props_map,
                blocked_property_ids=set(),
                attempted_property_ids=set(),
                combined_pressure={},
                monthly_candidate_quota_used={},
                candidate_quota_cfg=svc._resolve_candidate_quota_controls(),
                limit=2,
                recovery_reason="Outbid: stronger bid",
                emit_log=False,
            )

            self.assertEqual([item["property_id"] for item in refill[:2]], [2001, 2002])
            self.assertGreater(float(refill[0].get("spillover_bonus", 0.0) or 0.0), float(refill[1].get("spillover_bonus", 0.0) or 0.0))
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
