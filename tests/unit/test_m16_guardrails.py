import unittest

from transaction_engine import _m16_guardrails, clamp_offer_price, normalize_llm_price_scale


class _Cfg:
    def __init__(self, data):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestM16Guardrails(unittest.TestCase):
    def test_guardrail_defaults(self):
        g = _m16_guardrails(None)
        self.assertGreaterEqual(g["min_offer_ratio_to_list"], 0.5)
        self.assertGreaterEqual(g["max_overbid_ratio_to_list"], 0.0)
        self.assertGreaterEqual(g["max_negotiation_rounds_cap"], 1)

    def test_clamp_offer_floor_and_ceiling(self):
        cfg = _Cfg(
            {
                "smart_agent.min_offer_ratio_to_list": 0.8,
                "smart_agent.max_overbid_ratio_to_list": 0.1,
            }
        )
        listed = 1_000_000
        buyer_max = 1_050_000
        # Too low -> clamped to floor (800k)
        self.assertEqual(clamp_offer_price(500_000, listed, buyer_max, cfg), 800_000)
        # Too high -> clamped to min(buyer_max, listed*1.1)=1,050,000
        self.assertEqual(clamp_offer_price(1_300_000, listed, buyer_max, cfg), 1_050_000)
        # In range unchanged
        self.assertEqual(clamp_offer_price(900_000, listed, buyer_max, cfg), 900_000)

    def test_normalize_llm_price_scale_recovers_thousand_unit_bid(self):
        normalized = normalize_llm_price_scale(
            raw_price=698.0,
            reference_price=665_321.0,
            buyer_max=2_150_000.0,
        )
        self.assertEqual(normalized, 698_000.0)

    def test_normalize_llm_price_scale_leaves_normal_price_unchanged(self):
        normalized = normalize_llm_price_scale(
            raw_price=648_000.0,
            reference_price=665_321.0,
            buyer_max=2_150_000.0,
        )
        self.assertEqual(normalized, 648_000.0)


if __name__ == "__main__":
    unittest.main()
