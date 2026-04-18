import unittest

from services.agent_service import AgentService


class _Cfg(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class TestInitSupplyTargets(unittest.TestCase):
    def test_b_zone_defaults_raise_initial_supply_targets(self):
        svc = AgentService(_Cfg(), db_conn=None)
        market_properties = []
        for i in range(1, 101):
            market_properties.append({"property_id": i, "zone": "A"})
        for i in range(101, 301):
            market_properties.append({"property_id": i, "zone": "B"})

        targets = svc._resolve_init_supply_targets(market_properties, planned_agent_count=50)

        self.assertEqual(targets["zone_a_for_sale_target"], 4)
        self.assertEqual(targets["zone_b_for_sale_target"], 8)
        self.assertEqual(targets["zone_a_owner_target"], 8)
        self.assertEqual(targets["zone_b_owner_target"], 20)

    def test_initial_liquidity_seed_rate_follows_market_initial_listing_rate(self):
        svc = AgentService(_Cfg({"market.initial_listing_rate": 0.02}), db_conn=None)

        self.assertEqual(svc._initial_liquidity_seed_rate(), 0.02)

    def test_init_multi_owner_listings_enabled_defaults_true_and_can_disable(self):
        enabled = AgentService(_Cfg(), db_conn=None)
        disabled = AgentService(_Cfg({"smart_agent.init_multi_owner_listings_enabled": False}), db_conn=None)

        self.assertTrue(enabled._init_multi_owner_listings_enabled())
        self.assertFalse(disabled._init_multi_owner_listings_enabled())


if __name__ == "__main__":
    unittest.main()
