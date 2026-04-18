import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from transaction_engine import (
    _resolve_batch_bid_winner,
    _resolve_batch_bid_winner_async,
    _run_batch_tie_break_rebid,
    _run_batch_tie_break_rebid_async,
)


class _Cfg:
    def __init__(self, data):
        self._data = data

    def get(self, key, default=None):
        return self._data.get(key, default)


def _buyer(buyer_id: int):
    return SimpleNamespace(
        id=buyer_id,
        cash=500_000,
        monthly_income=20_000,
        mortgage_monthly_payment=0,
        properties=[],
        preference=SimpleNamespace(max_price=1_000_000),
        story=SimpleNamespace(investment_style="balanced"),
    )


class TestBatchBiddingTieBreak(unittest.TestCase):
    def test_b_zone_near_equal_bids_use_seller_llm_tiebreak(self):
        cfg = _Cfg({})
        seller = SimpleNamespace(id=99)
        listing = {"zone": "B", "listed_price": 800_000, "min_price": 760_000, "building_area": 88}
        bids = [
            {"buyer": _buyer(1), "price": 800_000},
            {"buyer": _buyer(2), "price": 800_000},
        ]

        with patch("transaction_engine.safe_call_llm", return_value={"buyer_id": 2, "reason": "school fit"}):
            winner, history = _resolve_batch_bid_winner(bids, seller, listing, cfg)

        self.assertEqual(winner["buyer"].id, 2)
        self.assertEqual(history[0]["action"], "SELLER_TIE_BREAK")
        self.assertEqual(history[0]["buyer_id"], 2)

    def test_b_zone_near_equal_bids_use_async_seller_llm_tiebreak(self):
        cfg = _Cfg({})
        seller = SimpleNamespace(id=99)
        listing = {"zone": "B", "listed_price": 800_000, "min_price": 760_000, "building_area": 88}
        bids = [
            {"buyer": _buyer(1), "price": 800_000},
            {"buyer": _buyer(2), "price": 800_000},
        ]

        async def _run():
            with patch("transaction_engine.safe_call_llm_async", return_value={"buyer_id": 2, "reason": "lower risk"}):
                return await _resolve_batch_bid_winner_async(bids, seller, listing, cfg)

        winner, history = asyncio.run(_run())
        self.assertEqual(winner["buyer"].id, 2)
        self.assertEqual(history[0]["action"], "SELLER_TIE_BREAK")

    def test_meaningful_price_gap_skips_tiebreak(self):
        cfg = _Cfg({})
        seller = SimpleNamespace(id=99)
        listing = {"zone": "B", "listed_price": 800_000, "min_price": 760_000, "building_area": 88}
        bids = [
            {"buyer": _buyer(1), "price": 810_000},
            {"buyer": _buyer(2), "price": 800_000},
        ]

        with patch("transaction_engine.safe_call_llm") as mocked:
            winner, history = _resolve_batch_bid_winner(bids, seller, listing, cfg)

        self.assertEqual(winner["buyer"].id, 1)
        self.assertEqual(history, [])
        mocked.assert_not_called()

    def test_b_zone_extra_rebid_breaks_exact_tie_before_seller_tiebreak(self):
        cfg = _Cfg(
            {
                "smart_agent.batch_b_zone_extra_rebid_enabled": True,
                "smart_agent.batch_b_zone_extra_rebid_min_increment_abs": 1000.0,
                "smart_agent.batch_b_zone_extra_rebid_min_increment_ratio": 0.005,
            }
        )
        seller = SimpleNamespace(id=99)
        listing = {"zone": "B", "listed_price": 800_000, "min_price": 760_000, "building_area": 88}
        bids = [
            {"buyer": _buyer(1), "price": 800_000},
            {"buyer": _buyer(2), "price": 800_000},
        ]

        with patch("transaction_engine.calculate_max_affordable_price", return_value=1_000_000):
            with patch("transaction_engine.check_affordability", return_value=(True, 0.0, 0.0)):
                with patch("transaction_engine.safe_call_llm", side_effect=[{"bid_price": 805_000, "reason": "push once more"}, {"bid_price": 0, "reason": "hold"}]):
                    updated, history = _run_batch_tie_break_rebid(bids, seller, listing, cfg)

        prices = {entry["buyer"].id: entry["price"] for entry in updated}
        self.assertGreater(prices[1], prices[2])
        self.assertEqual(history[0]["action"], "TIE_BREAK_FINAL_BID")
        self.assertEqual(history[0]["agent_id"], 1)

    def test_async_b_zone_extra_rebid_breaks_exact_tie_before_seller_tiebreak(self):
        cfg = _Cfg(
            {
                "smart_agent.batch_b_zone_extra_rebid_enabled": True,
                "smart_agent.batch_b_zone_extra_rebid_min_increment_abs": 1000.0,
                "smart_agent.batch_b_zone_extra_rebid_min_increment_ratio": 0.005,
            }
        )
        seller = SimpleNamespace(id=99)
        listing = {"zone": "B", "listed_price": 800_000, "min_price": 760_000, "building_area": 88}
        bids = [
            {"buyer": _buyer(1), "price": 800_000},
            {"buyer": _buyer(2), "price": 800_000},
        ]

        async def _run():
            with patch("transaction_engine.calculate_max_affordable_price", return_value=1_000_000):
                with patch("transaction_engine.check_affordability", return_value=(True, 0.0, 0.0)):
                    with patch("transaction_engine.safe_call_llm_async", side_effect=[{"bid_price": 805_000, "reason": "push once more"}, {"bid_price": 0, "reason": "hold"}]):
                        return await _run_batch_tie_break_rebid_async(bids, seller, listing, cfg)

        updated, history = asyncio.run(_run())
        prices = {entry["buyer"].id: entry["price"] for entry in updated}
        self.assertGreater(prices[1], prices[2])
        self.assertEqual(history[0]["action"], "TIE_BREAK_FINAL_BID")
        self.assertEqual(history[0]["agent_id"], 1)


if __name__ == "__main__":
    unittest.main()
