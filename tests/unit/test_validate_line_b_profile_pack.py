import unittest

from scripts.validate_line_b_profile_pack import validate_profile_pack


class TestValidateLineBProfilePack(unittest.TestCase):
    def test_validate_profile_pack_ok(self):
        pack = {
            "agent_profile_buckets": {
                "B1": {
                    "count": 5,
                    "budget_profile": {"max_price_range": [1_000_000, 1_500_000]},
                }
            },
            "property_profile_buckets": {
                "P1": {
                    "price_range": [900_000, 1_600_000],
                    "count_by_supply_mode": {"abundant": 8, "scarce": 3},
                }
            },
            "bucket_alignment_rules": [
                {"agent_bucket_id": "B1", "eligible_property_buckets": ["P1"], "soft_property_buckets": []}
            ],
        }
        report = validate_profile_pack(pack)
        self.assertTrue(report["ok"])
        self.assertEqual(report["errors"], [])
        self.assertGreater(report["competition_pressure"]["abundant"]["B1"], 0)

    def test_validate_profile_pack_catches_budget_mismatch(self):
        pack = {
            "agent_profile_buckets": {
                "B1": {
                    "count": 5,
                    "budget_profile": {"max_price_range": [1_000_000, 1_200_000]},
                }
            },
            "property_profile_buckets": {
                "P1": {
                    "price_range": [3_000_000, 3_500_000],
                    "count_by_supply_mode": {"abundant": 8, "scarce": 3},
                }
            },
            "bucket_alignment_rules": [
                {"agent_bucket_id": "B1", "eligible_property_buckets": ["P1"], "soft_property_buckets": []}
            ],
        }
        report = validate_profile_pack(pack)
        self.assertFalse(report["ok"])
        self.assertIn("budget_mismatch:B1", report["errors"])

    def test_validate_profile_pack_accepts_v2_market_profile_range(self):
        pack = {
            "agent_profile_buckets": {
                "B1": {
                    "count": 5,
                    "budget_profile": {"max_price_range": [1_000_000, 1_500_000]},
                }
            },
            "property_profile_buckets": {
                "P1": {
                    "market_profile": {
                        "price_range": [900_000, 1_600_000],
                    },
                    "count_by_supply_mode": {"abundant": 8, "scarce": 3},
                }
            },
            "bucket_alignment_rules": [
                {"agent_bucket_id": "B1", "eligible_property_buckets": ["P1"], "soft_property_buckets": []}
            ],
        }
        report = validate_profile_pack(pack)
        self.assertTrue(report["ok"])
        self.assertEqual(report["errors"], [])


if __name__ == "__main__":
    unittest.main()
