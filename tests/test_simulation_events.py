import os
import shutil
import unittest
import uuid

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner
from utils import llm_client


class TestSimulationEvents(unittest.TestCase):
    def setUp(self):
        self.temp_dir = os.path.join(os.getcwd(), f"simulation_events_test_{uuid.uuid4().hex}")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.db_path = os.path.join(self.temp_dir, "simulation.db")
        llm_client.LLM_MOCK_MODE = True

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _make_runner(self, agent_count=3, months=1):
        config = SimulationConfig("config/baseline.yaml")
        config.update("simulation.enable_intervention_panel", False)
        return SimulationRunner(
            agent_count=agent_count,
            months=months,
            seed=42,
            config=config,
            db_path=self.db_path,
        )

    def test_get_month_events_maps_activation_listing_match_success_and_failure(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            cursor = runner.conn.cursor()

            cursor.execute(
                """
                INSERT OR IGNORE INTO agents_static
                (agent_id, name, birth_year, marital_status, children_ages, occupation, background_story, investment_style)
                VALUES (999, 'Tester', 1990, 'single', '[]', 'engineer', 'bg', 'balanced')
                """
            )
            cursor.execute(
                """
                INSERT INTO decision_logs
                (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
                VALUES (999, 1, 'ROLE_DECISION', 'BUYER', 'Need a home', NULL, NULL, 1)
                """
            )
            cursor.execute(
                """
                UPDATE properties_market
                SET status='for_sale', listed_price=880000, listing_month=1
                WHERE property_id = 1
                """
            )
            cursor.execute(
                """
                INSERT INTO property_buyer_matches
                (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation, order_id, final_outcome)
                VALUES (1, 1, 999, 880000, 860000, 1, 1, 501, 'PENDING_SETTLEMENT')
                """
            )
            cursor.execute(
                """
                INSERT INTO negotiations
                (buyer_id, seller_id, property_id, round_count, final_price, success, reason, log)
                VALUES (999, 888, 1, 3, 980000, 1, 'Deal pending settlement', '[{"speaker":"buyer","message":"Budget is tight"},{"speaker":"seller","message":"School premium still matters"}]')
                """
            )
            cursor.execute(
                """
                INSERT INTO transaction_orders
                (order_id, created_month, expires_month, settlement_due_month, buyer_id, seller_id, property_id,
                 offer_price, agreed_price, negotiation_rounds, deposit_amount, penalty_amount, status, close_month, close_reason, agent_type)
                VALUES
                (501, 1, 2, 2, 999, 888, 1, 1000000, 980000, 2, 50000, 0, 'pending_settlement', NULL, NULL, 'normal'),
                (502, 1, 2, 2, 999, 887, 702, 900000, NULL, 1, 30000, 5000, 'cancelled', 1, 'Outbid: lost in negotiation', 'normal')
                """
            )
            runner.conn.commit()

            events = runner.get_month_events(1)
            event_types = [event["event_type"] for event in events]

            self.assertIn("AGENT_ACTIVATED", event_types)
            self.assertIn("PROPERTY_LISTED", event_types)
            self.assertIn("MATCH_ATTEMPT", event_types)
            self.assertIn("NEGOTIATION_STARTED", event_types)
            self.assertIn("NEGOTIATION_PROGRESS", event_types)
            self.assertIn("NEGOTIATION_CLOSED", event_types)
            self.assertIn("SETTLEMENT_PENDING", event_types)
            self.assertIn("DEAL_FAIL", event_types)
            self.assertNotIn("NEGOTIATION_QUOTE", event_types)

            for event in events:
                for key in ("event_id", "run_id", "month", "phase", "event_type", "ts", "payload", "source", "schema_version"):
                    self.assertIn(key, event)
                self.assertEqual(event["schema_version"], "v1")

            activation = next(event for event in events if event["event_type"] == "AGENT_ACTIVATED")
            self.assertEqual(activation["payload"]["agent_id"], 999)
            self.assertEqual(activation["payload"]["role"], "BUYER")

            listed = next(event for event in events if event["event_type"] == "PROPERTY_LISTED")
            self.assertEqual(listed["payload"]["property_id"], 1)
            self.assertEqual(listed["phase"], "listing")

            attempt = next(event for event in events if event["event_type"] == "MATCH_ATTEMPT")
            self.assertEqual(attempt["payload"]["buyer_id"], 999)
            self.assertTrue(attempt["payload"]["proceeded_to_negotiation"])

            negotiation = next(event for event in events if event["event_type"] == "NEGOTIATION_PROGRESS")
            self.assertEqual(negotiation["payload"]["buyer_id"], 999)
            self.assertIn("Budget", negotiation["payload"]["summary"])

            settlement_pending = next(event for event in events if event["event_type"] == "SETTLEMENT_PENDING")
            self.assertEqual(settlement_pending["payload"]["order_id"], 501)
            self.assertEqual(settlement_pending["payload"]["deal_stage"], "pending_settlement")
            self.assertEqual(settlement_pending["payload"]["settlement_due_month"], 2)

            failure = next(event for event in events if event["event_type"] == "DEAL_FAIL")
            self.assertEqual(failure["payload"]["order_id"], 502)
            self.assertIn("Outbid", failure["payload"]["reason"])
        finally:
            runner.close()

    def test_quote_stream_emits_limited_negotiation_quotes_when_enabled(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            runner.apply_runtime_controls(negotiation_quote_stream_enabled=True)
            cursor = runner.conn.cursor()
            cursor.execute(
                """
                INSERT INTO property_buyer_matches
                (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation, order_id, final_outcome)
                VALUES (1, 1, 999, 880000, 860000, 1, 1, 501, 'PENDING_SETTLEMENT')
                """
            )
            cursor.execute(
                """
                INSERT INTO negotiations
                (buyer_id, seller_id, property_id, round_count, final_price, success, reason, log)
                VALUES (999, 888, 1, 5, 980000, 1, 'Deal pending settlement',
                '[{"speaker":"buyer","message":"Budget is tight and we need to be careful with cash flow."},{"speaker":"seller","message":"School premium still matters in this district."},{"speaker":"buyer","message":"Could you move a bit closer to our comfort zone?"},{"speaker":"seller","message":"I can move slightly, but not that far."},{"speaker":"buyer","message":"Let us meet in the middle and close this month."}]')
                """
            )
            runner.conn.commit()

            events = runner.get_month_events(1)
            quote_events = [event for event in events if event["event_type"] == "NEGOTIATION_QUOTE"]

            self.assertTrue(quote_events)
            self.assertLessEqual(len(quote_events), 4)
            self.assertEqual(quote_events[0]["payload"]["speaker"], "buyer")
            self.assertLessEqual(len(quote_events[0]["payload"]["quote"]), 87)
        finally:
            runner.close()

    def test_quote_stream_focused_mode_only_emits_for_high_signal_negotiations(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            runner.apply_runtime_controls(
                negotiation_quote_stream_enabled=True,
                negotiation_quote_filter_mode="focused",
            )
            cursor = runner.conn.cursor()
            cursor.execute(
                """
                INSERT INTO property_buyer_matches
                (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation, order_id, final_outcome)
                VALUES
                (1, 1, 901, 880000, 870000, 1, 1, 801, 'PENDING_SETTLEMENT'),
                (1, 2, 902, 900000, 890000, 1, 1, 802, 'FAILED')
                """
            )
            cursor.execute(
                """
                INSERT INTO negotiations
                (negotiation_id, buyer_id, seller_id, property_id, round_count, final_price, success, reason, log)
                VALUES
                (801, 901, 700, 1, 2, 880000, 1, 'Smooth close',
                 '[{"speaker":"buyer","message":"We can close quickly."},{"speaker":"seller","message":"That works."}]'),
                (802, 902, 701, 2, 4, 890000, 0, 'Stalled after multiple rounds',
                 '[{"speaker":"buyer","message":"This is already near our limit."},{"speaker":"seller","message":"I still need a stronger number."},{"speaker":"buyer","message":"We may have to walk away."}]')
                """
            )
            runner.conn.commit()

            events = runner.get_month_events(1)
            quote_events = [event for event in events if event["event_type"] == "NEGOTIATION_QUOTE"]

            self.assertTrue(quote_events)
            self.assertTrue(all(event["payload"]["negotiation_id"] == 802 for event in quote_events))
        finally:
            runner.close()

    def test_quote_stream_high_value_only_emits_for_price_premium_negotiations(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            runner.apply_runtime_controls(
                negotiation_quote_stream_enabled=True,
                negotiation_quote_filter_mode="high_value_only",
            )
            cursor = runner.conn.cursor()
            cursor.execute(
                """
                INSERT INTO property_buyer_matches
                (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation, order_id, final_outcome)
                VALUES
                (1, 1, 911, 880000, 870000, 1, 1, 811, 'PENDING_SETTLEMENT'),
                (1, 2, 912, 900000, 890000, 1, 1, 812, 'PENDING_SETTLEMENT')
                """
            )
            cursor.execute(
                """
                INSERT INTO negotiations
                (negotiation_id, buyer_id, seller_id, property_id, round_count, final_price, success, reason, log)
                VALUES
                (811, 911, 710, 1, 2, 880000, 1, 'Flat close',
                 '[{"speaker":"buyer","message":"This is fair."},{"speaker":"seller","message":"Accepted."}]'),
                (812, 912, 711, 2, 2, 960000, 1, 'Premium close',
                 '[{"speaker":"buyer","message":"We can pay for the location."},{"speaker":"seller","message":"Then let us close."}]')
                """
            )
            runner.conn.commit()

            events = runner.get_month_events(1)
            quote_events = [event for event in events if event["event_type"] == "NEGOTIATION_QUOTE"]

            self.assertTrue(quote_events)
            self.assertTrue(all(event["payload"]["negotiation_id"] == 812 for event in quote_events))
        finally:
            runner.close()


    def test_full_quote_mode_emits_turn_events_and_batch_end(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            runner.apply_runtime_controls(
                negotiation_quote_stream_enabled=True,
                negotiation_quote_mode="full_quotes",
                negotiation_quote_turn_limit=3,
                negotiation_quote_char_limit=60,
            )
            cursor = runner.conn.cursor()
            cursor.execute(
                """
                INSERT INTO property_buyer_matches
                (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation, order_id, final_outcome)
                VALUES (1, 1, 999, 880000, 860000, 1, 1, 901, 'PENDING_SETTLEMENT')
                """
            )
            cursor.execute(
                """
                INSERT INTO negotiations
                (buyer_id, seller_id, property_id, round_count, final_price, success, reason, log)
                VALUES (999, 888, 1, 4, 980000, 1, 'Deal pending settlement',
                '[{"speaker":"buyer","message":"We need flexibility on closing timeline and pricing."},{"speaker":"seller","message":"Price still reflects the school premium and recent repairs."},{"speaker":"buyer","message":"If you move a little we can close this month."},{"speaker":"seller","message":"Let us finalize near the midpoint."}]')
                """
            )
            runner.conn.commit()

            events = runner.get_month_events(1)
            turn_events = [event for event in events if event["event_type"] == "NEGOTIATION_TURN"]
            batch_end = next(event for event in events if event["event_type"] == "NEGOTIATION_TURN_BATCH_END")

            self.assertEqual(len(turn_events), 3)
            self.assertEqual(turn_events[0]["payload"]["turn_index"], 1)
            self.assertLessEqual(len(turn_events[0]["payload"]["turn_text"]), 63)
            self.assertEqual(batch_end["payload"]["emitted_turns"], 3)
            self.assertEqual(batch_end["payload"]["quote_mode"], "full_quotes")
        finally:
            runner.close()

    def test_preplanned_interventions_execute_during_noninteractive_run(self):
        config = SimulationConfig("config/baseline.yaml")
        config.update("simulation.enable_intervention_panel", False)
        config.update(
            "simulation.preplanned_interventions",
            [
                {
                    "month": 1,
                    "action_type": "population_add",
                    "count": 2,
                    "tier": "middle",
                    "template": "middle_upgrade",
                },
                {
                    "month": 1,
                    "action_type": "income_shock",
                    "tier_adjustments": [
                        {"tier": "middle", "pct_change": 0.05},
                        {"tier": "high", "pct_change": -0.02},
                    ],
                },
                {
                    "month": 1,
                    "action_type": "developer_supply",
                    "zone": "B",
                    "count": 2,
                    "template": "b_entry_level",
                },
                {
                    "month": 1,
                    "action_type": "supply_cut",
                    "zone": "B",
                    "count": 1,
                },
            ],
        )
        runner = SimulationRunner(
            agent_count=3,
            months=1,
            seed=42,
            config=config,
            db_path=self.db_path,
        )
        try:
            initial_agent_count = runner.agent_count
            month_result = runner.run_one_month()

            self.assertEqual(month_result["month"], 1)

            history_types = [item["event_type"] for item in runner.intervention_history]
            self.assertIn("POPULATION_ADDED", history_types)
            self.assertIn("INCOME_SHOCK_APPLIED", history_types)
            self.assertIn("DEVELOPER_SUPPLY_INJECTED", history_types)
            self.assertIn("SUPPLY_CUT_APPLIED", history_types)
            self.assertTrue(all(item["month"] == 1 for item in runner.intervention_history))

            month_review = month_result["month_review"]
            intervention_types = [item["event_type"] for item in month_review["interventions"]]
            self.assertIn("POPULATION_ADDED", intervention_types)
            self.assertIn("INCOME_SHOCK_APPLIED", intervention_types)
            self.assertIn("DEVELOPER_SUPPLY_INJECTED", intervention_types)
            self.assertIn("SUPPLY_CUT_APPLIED", intervention_types)
            self.assertGreater(len(runner.agent_service.agents), initial_agent_count)
        finally:
            runner.close()

    def test_get_generation_events_returns_agent_and_property_events(self):
        runner = self._make_runner(agent_count=3, months=1)
        try:
            runner.initialize()
            events = runner.get_generation_events(property_display_limit=5)
            event_types = [event["event_type"] for event in events]

            self.assertIn("AGENT_GENERATED", event_types)
            self.assertIn("PROPERTY_GENERATED", event_types)

            property_events = [event for event in events if event["event_type"] == "PROPERTY_GENERATED"]
            self.assertLessEqual(len(property_events), 5)
            for event in events:
                self.assertEqual(event["month"], 0)
                self.assertEqual(event["schema_version"], "v1")
        finally:
            runner.close()

    def test_run_one_month_summary_event_count_matches_extracted_events(self):
        runner = self._make_runner(agent_count=3, months=1)
        try:
            runner.initialize()
            summary = runner.run_one_month()
            month_events = runner.get_month_events(1)

            self.assertEqual(summary["month"], 1)
            self.assertEqual(summary["event_count"], len(month_events))
            self.assertEqual(runner.last_month_summary["event_count"], len(month_events))
            self.assertIn("avg_transaction_price", summary)
            self.assertIn("controls_snapshot", summary)
            self.assertIn("max_dti_ratio", summary["controls_snapshot"])
            self.assertIn("market_pulse_enabled", summary["controls_snapshot"])
            self.assertIn("month_review", summary)
            self.assertEqual(summary["month_review"]["month"], 1)
            self.assertIn("interventions", summary["month_review"])
            self.assertIn("round_supply_review", summary)
            self.assertEqual(summary["round_supply_review"]["round"], 1)
            self.assertIn("hot_buckets", summary["round_supply_review"])
            self.assertIn("shortage_buckets", summary["round_supply_review"])
        finally:
            runner.close()

    def test_get_month_events_marks_filled_orders_as_settlement_completed(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            cursor = runner.conn.cursor()
            cursor.execute(
                """
                INSERT INTO transaction_orders
                (order_id, created_month, expires_month, settlement_due_month, buyer_id, seller_id, property_id,
                 offer_price, agreed_price, negotiation_rounds, deposit_amount, penalty_amount, status, close_month, close_reason, agent_type)
                VALUES
                (601, 1, 2, 1, 101, 201, 801, 1200000, 1180000, 3, 60000, 0, 'filled', 1, 'Settlement completed', 'normal')
                """
            )
            runner.conn.commit()

            events = runner.get_month_events(1)
            success = next(event for event in events if event["event_type"] == "DEAL_SUCCESS" and event["payload"]["order_id"] == 601)

            self.assertEqual(success["phase"], "settlement")
            self.assertEqual(success["payload"]["status"], "filled")
            self.assertEqual(success["payload"]["deal_stage"], "settlement_completed")
        finally:
            runner.close()

    def test_get_month_average_transaction_price_uses_completed_transactions_only(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            cursor = runner.conn.cursor()
            cursor.execute(
                """
                INSERT INTO transaction_orders
                (order_id, created_month, expires_month, settlement_due_month, buyer_id, seller_id, property_id,
                 offer_price, agreed_price, negotiation_rounds, deposit_amount, penalty_amount, status, close_month, close_reason, agent_type)
                VALUES
                (701, 1, 2, 2, 101, 201, 801, 1200000, 1180000, 3, 60000, 0, 'pending_settlement', NULL, NULL, 'normal')
                """
            )
            runner.conn.commit()

            self.assertEqual(runner.get_month_average_transaction_price(1), 0.0)

            cursor.execute(
                """
                INSERT INTO transactions
                (month, order_id, buyer_id, seller_id, property_id, final_price, down_payment, loan_amount, negotiation_rounds, negotiation_mode, transaction_type)
                VALUES (1, 701, 101, 201, 801, 1180000, 354000, 826000, 3, 'standard', 'resale')
                """
            )
            runner.conn.commit()

            self.assertEqual(runner.get_month_average_transaction_price(1), 1180000.0)
        finally:
            runner.close()

    def test_get_final_summary_aggregates_agents_properties_and_failures(self):
        runner = self._make_runner(agent_count=1, months=1)
        try:
            runner.initialize()
            cursor = runner.conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO agents_static
                (agent_id, name, birth_year, marital_status, children_ages, occupation, background_story, investment_style, agent_type)
                VALUES (777, 'Summary Agent', 1991, 'single', '[]', 'planner', 'bg', 'balanced', 'smart')
                """
            )
            cursor.execute(
                """
                INSERT INTO decision_logs
                (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
                VALUES (777, 1, 'ROLE_DECISION', 'BUYER', 'Need upgrade', NULL, NULL, 1)
                """
            )
            cursor.execute("SELECT owner_id FROM properties_market WHERE property_id = 1")
            seller_id = int((cursor.fetchone() or [801])[0] or 801)
            cursor.execute(
                """
                UPDATE properties_market
                SET owner_id = ?, listed_price = 1250000, min_price = 1180000, status = 'for_sale', listing_month = 1
                WHERE property_id = 1
                """
                ,
                (seller_id,),
            )
            cursor.execute(
                """
                INSERT INTO property_buyer_matches
                (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation, order_id, final_outcome)
                VALUES (1, 1, 777, 1250000, 1210000, 1, 1, 7771, 'FAILED')
                """
            )
            cursor.execute(
                """
                INSERT INTO transaction_orders
                (order_id, created_month, expires_month, settlement_due_month, buyer_id, seller_id, property_id,
                 offer_price, agreed_price, negotiation_rounds, deposit_amount, penalty_amount, status, close_month, close_reason, agent_type)
                VALUES
                (7771, 1, 2, 2, 777, ?, 1, 1210000, NULL, 2, 40000, 3000, 'cancelled', 1, 'Outbid in final round', 'smart')
                """
                ,
                (seller_id,),
            )
            runner.conn.commit()
            runner.current_month = 1

            summary = runner.get_final_summary()

            self.assertEqual(summary["completed_month"], 1)
            self.assertTrue(summary["top_agents"])
            self.assertTrue(summary["key_properties"])
            self.assertTrue(summary["failure_reasons"])
            self.assertEqual(summary["top_agents"][0]["agent_id"], 777)
            self.assertEqual(summary["top_agents"][0]["agent_type"], "smart")
            self.assertEqual(summary["key_properties"][0]["property_id"], 1)
            self.assertIn("Outbid", summary["failure_reasons"][0]["reason"])
        finally:
            runner.close()

    def test_macro_override_mode_changes_month_bulletin_context(self):
        runner = self._make_runner(agent_count=2, months=1)
        try:
            runner.initialize()
            runner.apply_runtime_controls(macro_override_mode="optimistic", max_dti_ratio=0.42, market_pulse_enabled=True)
            summary = runner.run_one_month()

            self.assertEqual(runner.get_runtime_controls()["macro_override_mode"], "optimistic")
            self.assertEqual(runner.get_runtime_controls()["max_dti_ratio"], 0.42)
            self.assertEqual(runner.get_runtime_controls()["market_pulse_enabled"], True)
            self.assertIn("month_review", summary)
            self.assertIsInstance(runner.last_bulletin, str)
            self.assertTrue(runner.last_bulletin.strip())
            bulletin_event = runner.get_bulletin_event(1)
            self.assertEqual(bulletin_event["event_type"], "MARKET_BULLETIN_READY")
            self.assertEqual(bulletin_event["month"], 1)
            self.assertTrue(bulletin_event["payload"]["bulletin_excerpt"])
        finally:
            runner.close()

    def test_intervention_helpers_return_generation_and_listing_events(self):
        runner = self._make_runner(agent_count=2, months=2)
        try:
            runner.initialize()

            population_result = runner.add_population_intervention(
                count=2,
                tier="middle",
                template="young_first_home",
            )
            self.assertEqual(population_result["added_count"], 2)
            self.assertEqual(population_result["tier"], "lower_middle")
            self.assertEqual(population_result["template"], "young_first_home")
            self.assertEqual(len(population_result["generated_events"]), 2)
            self.assertEqual(population_result["income_multiplier_range"]["min"], 0.85)
            self.assertEqual(population_result["income_multiplier_range"]["max"], 1.05)
            self.assertTrue(all(event["event_type"] == "AGENT_GENERATED" for event in population_result["generated_events"]))

            income_result = runner.apply_income_intervention(
                tier_adjustments=[
                    {"tier": "lower_middle", "pct_change": 0.08},
                    {"tier": "high", "pct_change": 0.02},
                ]
            )
            self.assertGreater(income_result["updated_count"], 0)
            self.assertEqual(len(income_result["tier_adjustments"]), 2)

            supply_result = runner.inject_developer_supply_intervention(
                count=2,
                zone="A",
                template="a_district_premium",
                build_year=2026,
            )
            self.assertEqual(supply_result["count"], 2)
            self.assertEqual(supply_result["template"], "a_district_premium")
            self.assertEqual(len(supply_result["generated_events"]), 2)
            self.assertEqual(len(supply_result["listed_events"]), 2)
            self.assertTrue(all(event["event_type"] == "PROPERTY_GENERATED" for event in supply_result["generated_events"]))
            self.assertTrue(all(event["event_type"] == "PROPERTY_LISTED" for event in supply_result["listed_events"]))
            self.assertTrue(runner.intervention_history)
            self.assertEqual(runner.intervention_history[-1]["event_type"], "DEVELOPER_SUPPLY_INJECTED")

            cursor = runner.conn.cursor()
            cursor.execute(
                """
                UPDATE properties_market
                SET status='off_market',
                    owner_id=1,
                    current_valuation=1600000,
                    listing_month=NULL,
                    listed_price=NULL,
                    min_price=NULL
                WHERE property_id = 1
                """
            )
            runner.conn.commit()

            force_listing_result = runner.force_listing_intervention(
                count=1,
                bucket_id="A_NOSCHOOL_IMPROVE",
                target_month_override=2,
            )
            self.assertEqual(force_listing_result["listed_count"], 1)
            self.assertIn("listed_events", force_listing_result)
            if force_listing_result["listed_events"]:
                self.assertEqual(force_listing_result["listed_events"][0]["month"], 2)

            month_review = runner.get_month_review(1)
            self.assertTrue(month_review["interventions"])

            final_summary = runner.get_final_summary()
            self.assertTrue(final_summary["interventions"])
        finally:
            runner.close()

    def test_get_export_report_contains_run_months_and_final_summary(self):
        runner = self._make_runner(agent_count=2, months=1)
        try:
            runner.initialize()
            runner.run_one_month()
            report = runner.get_export_report()

            self.assertIn("run", report)
            self.assertIn("month_reviews", report)
            self.assertIn("final_summary", report)
            self.assertEqual(report["run"]["completed_month"], 1)
            self.assertEqual(len(report["month_reviews"]), 1)
            self.assertEqual(report["month_reviews"][0]["month"], 1)
            self.assertIn("top_agents", report["final_summary"])
        finally:
            runner.close()


if __name__ == "__main__":
    unittest.main()
