import unittest
from types import SimpleNamespace

from mortgage_system import check_affordability


class _Cfg:
    def __init__(self, data):
        self._data = data
        self.mortgage = {
            "down_payment_ratio": 0.3,
            "annual_interest_rate": 0.05,
            "loan_term_years": 30,
            "max_dti_ratio": 0.5,
        }

    def get(self, key, default=None):
        return self._data.get(key, default)


def _agent(cash=400_000, income=20_000, debt=0.0, assets=800_000):
    return SimpleNamespace(
        cash=float(cash),
        monthly_income=float(income),
        mortgage_monthly_payment=0.0,
        total_debt=float(debt),
        total_assets=float(assets),
    )


class TestAffordabilityGuards(unittest.TestCase):
    def test_default_path_without_new_guards(self):
        agent = _agent()
        cfg = _Cfg({})
        ok, down, loan = check_affordability(agent, 1_000_000, cfg)
        self.assertTrue(ok)
        self.assertGreater(down, 0)
        self.assertGreater(loan, 0)

    def test_liquidity_floor_blocks_cash_starved_buy(self):
        agent = _agent(cash=350_000, income=20_000, assets=900_000)
        cfg = _Cfg({"smart_agent.liquidity_floor_months": 6})
        # price=1,000,000 -> down payment=300,000 -> remaining=50,000
        # required floor=6*(20,000*0.5)=60,000 => blocked
        ok, _, _ = check_affordability(agent, 1_000_000, cfg)
        self.assertFalse(ok)

    def test_leverage_cap_blocks_high_debt_ratio(self):
        agent = _agent(cash=600_000, income=50_000, debt=300_000, assets=200_000)
        cfg = _Cfg({"smart_agent.leverage_cap": 0.50})
        ok, _, _ = check_affordability(agent, 1_000_000, cfg)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
