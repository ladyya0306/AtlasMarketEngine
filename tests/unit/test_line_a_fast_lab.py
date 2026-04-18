import unittest

from scripts.run_line_a_fast_lab import (
    _make_agent,
    fake_buyer_llm,
    run_shortlist_probe,
    surrogate_role_decision,
)


class TestLineAFastLab(unittest.TestCase):
    def test_surrogate_role_decision_adds_normal_herd_activation(self):
        agent = _make_agent(
            1,
            agent_type="normal",
            monthly_income=22000,
            cash=800000,
            props=0,
            motive="starter_entry",
            max_price=2200000,
        )

        stable = surrogate_role_decision(
            agent,
            market_trend="STABLE",
            rumor_heat=0.0,
            cash_stress_event=False,
        )
        hot = surrogate_role_decision(
            agent,
            market_trend="UP",
            rumor_heat=0.9,
            cash_stress_event=False,
        )

        self.assertLessEqual(stable["final_score"], hot["final_score"])
        self.assertTrue(hot["herd_activation"])
        self.assertEqual(hot["role"], "BUYER")

    def test_fake_buyer_llm_prefers_low_crowd_candidate(self):
        prompt = """
        【拥挤修正后Top候选】
        [
          {"id": 101, "price": 2050000, "school": "Yes", "crowd_pressure_units": 3.2},
          {"id": 102, "price": 2080000, "school": "Yes", "crowd_pressure_units": 0.8},
          {"id": 103, "price": 1980000, "school": "No", "crowd_pressure_units": 0.4}
        ]
        """
        result = fake_buyer_llm(prompt, {})
        self.assertEqual(result["selected_property_id"], 102)
        self.assertEqual(result["monthly_intent"], "CONTINUE")

    def test_run_shortlist_probe_crowded_case_is_blocked(self):
        report = run_shortlist_probe()
        crowded = next(item for item in report["scenarios"] if item["name"] == "crowded_gate")
        self.assertTrue(all(row["selected_property_id"] is None for row in crowded["rows"]))
        self.assertTrue(
            all(row["selection_reason"] == "all_shortlist_over_crowd_tolerance" for row in crowded["rows"])
        )


if __name__ == "__main__":
    unittest.main()
