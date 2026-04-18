import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import api_server
from utils import llm_client


class TestApiServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="api_server_test_")
        self.db_path = os.path.join(self.temp_dir, "simulation.db")
        llm_client.LLM_MOCK_MODE = True
        if api_server.runtime.runner is not None:
            api_server.runtime.runner.close()
        api_server.runtime.runner = None
        self.client = TestClient(api_server.app)

    def tearDown(self):
        self.client.close()
        night_thread = getattr(api_server.runtime, "night_run_thread", None)
        if night_thread is not None and night_thread.is_alive():
            night_thread.join(timeout=2.0)
        api_server.runtime.night_run_thread = None
        if api_server.runtime.runner is not None:
            api_server.runtime.runner.close()
        api_server.runtime.runner = None
        api_server.ws_manager._clients = []
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_start_step_and_status_flow(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")

        resp = self.client.get("/status")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "idle")

        resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(resp.status_code, 200)
        start_payload = resp.json()
        self.assertEqual(start_payload["status"], "initialized")
        self.assertEqual(start_payload["current_month"], 0)
        self.assertTrue(os.path.exists(self.db_path))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, "parameter_assumption_report.md")))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, "parameter_assumption_report.json")))

        resp = self.client.post("/step")
        self.assertEqual(resp.status_code, 200)
        step_payload = resp.json()
        self.assertEqual(step_payload["month_result"]["month"], 1)
        self.assertEqual(step_payload["status"]["status"], "completed")
        self.assertEqual(step_payload["status"]["current_month"], 1)
        self.assertIn("avg_transaction_price", step_payload["month_result"])
        self.assertIn("month_review", step_payload["month_result"])
        self.assertIn("controls_snapshot", step_payload["month_result"])

        resp = self.client.get("/status")
        self.assertEqual(resp.status_code, 200)
        final_status = resp.json()
        self.assertEqual(final_status["status"], "completed")
        self.assertEqual(final_status["remaining_months"], 0)
        self.assertIsNotNone(final_status["last_month_summary"])
        self.assertIsNotNone(final_status["final_summary"])
        self.assertIn("top_agents", final_status["final_summary"])
        self.assertIn("interventions", final_status["final_summary"])

    def test_start_accepts_cli_style_startup_overrides(self):
        resp = self.client.post(
            "/start",
            json={
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
                "startup_overrides": {
                    "property_count": 6,
                    "min_cash_observer_threshold": 600000,
                    "base_year": 2026,
                    "income_adjustment_rate": 1.15,
                    "down_payment_ratio": 0.26,
                    "max_dti_ratio": 0.46,
                    "annual_interest_rate": 0.041,
                    "enable_intervention_panel": True,
                    "market_pulse_enabled": True,
                    "market_pulse_seed_ratio": 0.62,
                    "effective_bid_floor_ratio": 1.01,
                    "precheck_liquidity_buffer_months": 4,
                    "precheck_include_tax_and_fee": False,
                    "zones": [
                        {"zone": "A", "price_min": 35000, "price_max": 42000, "rent_per_sqm": 120},
                        {"zone": "B", "price_min": 12000, "price_max": 18000, "rent_per_sqm": 72},
                    ],
                    "agent_tiers": [
                        {"tier": "ultra_high", "count": 1, "income_min": 150000, "income_max": 300000, "property_min": 2, "property_max": 5},
                        {"tier": "high", "count": 1, "income_min": 80000, "income_max": 150000, "property_min": 1, "property_max": 3},
                        {"tier": "middle", "count": 1, "income_min": 40000, "income_max": 80000, "property_min": 0, "property_max": 1},
                        {"tier": "lower_middle", "count": 1, "income_min": 20000, "income_max": 40000, "property_min": 0, "property_max": 1},
                        {"tier": "low", "count": 1, "income_min": 8000, "income_max": 20000, "property_min": 0, "property_max": 0},
                    ],
                },
            },
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "initialized")
        runner = api_server.runtime.runner
        self.assertIsNotNone(runner)
        self.assertEqual(runner.agent_count, 5)
        self.assertEqual(runner.config.get("simulation.base_year"), 2026)
        self.assertEqual(runner.config.get("simulation.agent.income_adjustment_rate"), 1.15)
        self.assertEqual(runner.config.get("mortgage.down_payment_ratio"), 0.26)
        self.assertEqual(runner.config.get("mortgage.max_dti_ratio"), 0.46)
        self.assertEqual(runner.config.get("mortgage.annual_interest_rate"), 0.041)
        self.assertEqual(runner.config.get("simulation.enable_intervention_panel"), True)
        self.assertEqual(runner.config.get("market_pulse.enabled"), True)
        self.assertEqual(runner.config.get("market_pulse.seed_existing_mortgage_ratio"), 0.62)
        self.assertEqual(runner.config.get("market.zones.A.price_per_sqm_range.min"), 35000)
        self.assertEqual(runner.config.get("market.zones.B.price_per_sqm_range.max"), 18000)
        self.assertEqual(runner.config.get("market.rental.zone_a_rent_per_sqm"), 120)
        self.assertEqual(runner.config.get("decision_factors.activation.min_cash_observer_no_property"), 600000)
        self.assertEqual(runner.config.get("smart_agent.effective_bid_floor_ratio"), 1.01)
        self.assertEqual(runner.config.get("smart_agent.precheck_liquidity_buffer_months"), 4)
        self.assertEqual(runner.config.get("smart_agent.precheck_include_tax_and_fee"), False)
        self.assertEqual(runner.config._config.get("user_property_count"), 6)
        self.assertIn("user_agent_config", runner.config._config)

    def test_start_accepts_release_supply_startup_overrides(self):
        resp = self.client.post(
            "/start",
            json={
                "months": 3,
                "seed": 606,
                "db_path": self.db_path,
                "preplanned_interventions": [
                    {"month": 2, "action_type": "income_shock", "target_tier": "all", "pct_change": -0.10},
                    {"month": 2, "action_type": "developer_supply", "zone": "A", "count": 3, "template": "mixed_balanced"},
                    {"month": 3, "action_type": "supply_cut", "zone": "A", "count": 2},
                ],
                "startup_overrides": {
                    "use_release_supply_controls": True,
                    "fixed_supply_snapshot_id": "spindle_minimum",
                    "market_goal": "balanced",
                    "demand_multiplier": 0.10,
                    "income_adjustment_rate": 1.0,
                    "down_payment_ratio": 0.26,
                    "max_dti_ratio": 0.46,
                    "annual_interest_rate": 0.041,
                    "enable_intervention_panel": False,
                },
            },
        )
        self.assertEqual(resp.status_code, 200)
        runner = api_server.runtime.runner
        self.assertIsNotNone(runner)
        self.assertEqual(runner.agent_count, 12)
        self.assertEqual(runner.config._config.get("user_property_count"), 39)
        self.assertEqual(
            runner.config.get("simulation.scholar_cli.fixed_supply_snapshot.snapshot_id"),
            "spindle_minimum",
        )
        self.assertEqual(
            runner.config.get("simulation.scholar_cli.market_goal"),
            "balanced",
        )
        self.assertAlmostEqual(
            runner.config.get("simulation.scholar_cli.effective_demand_multiplier"),
            round(12 / 39, 4),
            places=4,
        )
        self.assertTrue(runner.config.get("smart_agent.profiled_market_mode.enforce_bucket_alignment"))
        self.assertEqual(len(runner.config.get("simulation.preplanned_interventions")), 3)

    def test_step_without_start_returns_conflict(self):
        resp = self.client.post("/step")
        self.assertEqual(resp.status_code, 409)
        self.assertIn("No simulation has been started", resp.json()["detail"])

    def test_final_report_endpoints_work_after_run(self):
        resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(resp.status_code, 200)
        resp = self.client.post("/step")
        self.assertEqual(resp.status_code, 200)

        report = self.client.get("/report/final")
        self.assertEqual(report.status_code, 200)
        report_payload = report.json()
        self.assertIn("run", report_payload)
        self.assertIn("artifacts", report_payload)
        self.assertIn("month_reviews", report_payload)
        self.assertIn("final_summary", report_payload)
        self.assertTrue(Path(report_payload["artifacts"]["parameter_assumption_markdown"]).exists())
        self.assertTrue(Path(report_payload["artifacts"]["parameter_assumption_json"]).exists())

        report_view = self.client.get("/report/final/view")
        self.assertEqual(report_view.status_code, 200)
        self.assertIn("Simulation Final Report", report_view.text)
        self.assertIn("Preset Timeline", report_view.text)
        self.assertIn("Market Bulletin", report_view.text)
        self.assertIn("Round Market Chart", report_view.text)
        self.assertIn("Avg Transaction Price", report_view.text)
        self.assertIn("Summary Highlights", report_view.text)
        self.assertIn("Top Agents", report_view.text)
        self.assertIn("Key Properties", report_view.text)
        self.assertIn("Failure Reasons", report_view.text)
        self.assertIn("Controls Timeline", report_view.text)
        self.assertIn("Preset Impact", report_view.text)

        parameter_report = self.client.get("/report/parameter-assumption")
        self.assertEqual(parameter_report.status_code, 200)
        parameter_payload = parameter_report.json()
        self.assertIn("experiment_info", parameter_payload)
        self.assertIn("parameter_rows", parameter_payload)
        self.assertIn("role_structure", parameter_payload)
        self.assertIn("latest_results", parameter_payload)

        parameter_view = self.client.get("/report/parameter-assumption/view")
        self.assertEqual(parameter_view.status_code, 200)
        self.assertIn("参数与假设说明表", parameter_view.text)
        self.assertIn("参数总表", parameter_view.text)
        self.assertIn("收入结构", parameter_view.text)

        parameter_download = self.client.get("/report/parameter-assumption/download", params={"format": "json"})
        self.assertEqual(parameter_download.status_code, 200)
        self.assertEqual(parameter_download.headers["content-type"], "application/json")

    def test_db_observer_endpoints_work_after_run(self):
        resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(resp.status_code, 200)

        observer = self.client.get("/db-observer", params={"db_path": self.db_path})
        self.assertEqual(observer.status_code, 200)
        payload = observer.json()
        self.assertIn("run", payload)
        self.assertIn("table_counts", payload)
        self.assertIn("latest_records", payload)
        self.assertEqual(payload["run"]["db_path"], self.db_path.replace("\\", "/"))

        observer_view = self.client.get("/db-observer/view", params={"db_path": self.db_path})
        self.assertEqual(observer_view.status_code, 200)
        self.assertIn("Research DB Observer", observer_view.text)
        self.assertIn("simulation.db", observer_view.text)
        self.assertIn("最近决策日志", observer_view.text)

    def test_zero_tx_forensics_endpoint_writes_report(self):
        resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(resp.status_code, 200)
        report = self.client.post("/forensics/zero-tx", json={"db_path": self.db_path})
        self.assertEqual(report.status_code, 200)
        payload = report.json()
        self.assertIn("report", payload)
        self.assertIn("artifacts", payload)
        self.assertTrue(Path(payload["artifacts"]["markdown_path"]).exists())
        self.assertTrue(Path(payload["artifacts"]["json_path"]).exists())
        self.assertIn("transactions_total", payload["report"])
        view = self.client.get("/forensics/zero-tx/view", params={"db_path": self.db_path})
        self.assertEqual(view.status_code, 200)
        self.assertIn("0成交诊断报告", view.text)
        download = self.client.get("/forensics/zero-tx/download", params={"db_path": self.db_path, "format": "json"})
        self.assertEqual(download.status_code, 200)

    def test_night_run_start_completes_automatically(self):
        resp = self.client.post(
            "/night-run/start",
            json={
                "agent_count": 3,
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
                "night_plan_path": "config/night_run_example.yaml",
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["run_mode"], "night_run")
        self.assertEqual(payload["night_plan_path"], "config/night_run_example.yaml")

        final_status = None
        for _ in range(50):
            status_resp = self.client.get("/status")
            self.assertEqual(status_resp.status_code, 200)
            final_status = status_resp.json()
            if final_status["status"] == "completed":
                break
            time.sleep(0.1)

        self.assertIsNotNone(final_status)
        self.assertEqual(final_status["status"], "completed")
        self.assertEqual(final_status["run_mode"], "manual")
        self.assertIsNotNone(final_status["final_summary"])

    def test_night_run_accepts_inline_preplanned_interventions(self):
        resp = self.client.post(
            "/night-run/start",
            json={
                "agent_count": 3,
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
                "preplanned_interventions": [
                    {"month": 1, "action_type": "population_add", "tier": "middle", "count": 2},
                    {"month": 1, "action_type": "income_shock", "target_tier": "middle", "pct_change": 0.06},
                    {"month": 1, "action_type": "developer_supply", "zone": "B", "count": 2},
                    {"month": 1, "action_type": "supply_cut", "zone": "B", "count": 1},
                ],
            },
        )
        self.assertEqual(resp.status_code, 200)

        final_status = None
        for _ in range(50):
            status_resp = self.client.get("/status")
            self.assertEqual(status_resp.status_code, 200)
            final_status = status_resp.json()
            if final_status["status"] == "completed":
                break
            time.sleep(0.1)

        self.assertIsNotNone(final_status)
        self.assertEqual(final_status["status"], "completed")
        interventions = final_status["final_summary"]["interventions"]
        event_types = {item["event_type"] for item in interventions}
        self.assertIn("POPULATION_ADDED", event_types)
        self.assertIn("INCOME_SHOCK_APPLIED", event_types)
        self.assertIn("DEVELOPER_SUPPLY_INJECTED", event_types)
        self.assertIn("SUPPLY_CUT_APPLIED", event_types)

    def test_controls_can_be_read_and_updated_between_steps(self):
        resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 2,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(resp.status_code, 200)

        resp = self.client.get("/controls")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("down_payment_ratio", resp.json())

        resp = self.client.post(
            "/controls",
            json={
                "down_payment_ratio": 0.25,
                "annual_interest_rate": 0.041,
                "max_dti_ratio": 0.44,
                "market_pulse_enabled": True,
                "macro_override_mode": "optimistic",
                "negotiation_quote_filter_mode": "focused",
                "negotiation_quote_mode": "full_quotes",
                "negotiation_quote_turn_limit": 6,
                "negotiation_quote_char_limit": 120,
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["controls"]["down_payment_ratio"], 0.25)
        self.assertEqual(payload["controls"]["annual_interest_rate"], 0.041)
        self.assertEqual(payload["controls"]["max_dti_ratio"], 0.44)
        self.assertEqual(payload["controls"]["market_pulse_enabled"], True)
        self.assertEqual(payload["controls"]["macro_override_mode"], "optimistic")
        self.assertEqual(payload["controls"]["negotiation_quote_stream_enabled"], False)
        self.assertEqual(payload["controls"]["negotiation_quote_filter_mode"], "focused")
        self.assertEqual(payload["controls"]["negotiation_quote_mode"], "full_quotes")
        self.assertEqual(payload["controls"]["negotiation_quote_turn_limit"], 6)
        self.assertEqual(payload["controls"]["negotiation_quote_char_limit"], 120)
        self.assertEqual(payload["status"]["runtime_controls"]["macro_override_mode"], "optimistic")

        resp = self.client.post("/step")
        self.assertEqual(resp.status_code, 200)
        step_payload = resp.json()
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["down_payment_ratio"], 0.25)
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["annual_interest_rate"], 0.041)
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["max_dti_ratio"], 0.44)
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["market_pulse_enabled"], True)
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["macro_override_mode"], "optimistic")
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["negotiation_quote_filter_mode"], "focused")
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["negotiation_quote_mode"], "full_quotes")
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["negotiation_quote_turn_limit"], 6)
        self.assertEqual(step_payload["month_result"]["controls_snapshot"]["negotiation_quote_char_limit"], 120)

    def test_config_schema_lists_whitelisted_parameters(self):
        resp = self.client.get("/config/schema")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertIn("groups", payload)
        self.assertIn("parameters", payload)
        self.assertIn("startup_defaults", payload)
        self.assertTrue(any(item["key"] == "mortgage.down_payment_ratio" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "negotiation.quote_filter_mode" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "negotiation.quote_mode" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "simulation.agent.savings_rate" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "life_events.monthly_event_trigger_prob" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "property_allocation.strategy" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "transaction_costs.buyer.brokerage_ratio" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "system.llm.max_calls_per_month" for item in payload["parameters"]))
        self.assertTrue(any(item["key"] == "market_pulse.seed_existing_mortgage_ratio" for item in payload["parameters"]))
        self.assertTrue(any(group["id"] == "life_events" for group in payload["groups"]))
        self.assertTrue(any(group["id"] == "allocation" for group in payload["groups"]))
        self.assertTrue(any(group["id"] == "transaction_costs" for group in payload["groups"]))
        self.assertTrue(any(group["id"] == "system" for group in payload["groups"]))
        self.assertTrue(any(group["id"] == "pulse" for group in payload["groups"]))
        dti = next(item for item in payload["parameters"] if item["key"] == "mortgage.max_dti_ratio")
        self.assertEqual(dti["editable_phase"], "between_steps")
        self.assertEqual(dti["group"], "financing")
        self.assertEqual(dti["default"], 0.5)
        savings = next(item for item in payload["parameters"] if item["key"] == "simulation.agent.savings_rate")
        self.assertEqual(savings["editable_phase"], "readonly")
        self.assertEqual(savings["default"], 0.4)
        llm_budget = next(item for item in payload["parameters"] if item["key"] == "system.llm.max_calls_per_month")
        self.assertEqual(llm_budget["editable_phase"], "readonly")
        self.assertEqual(llm_budget["default"], 200)
        self.assertIn("agent_tiers", payload["startup_defaults"])
        self.assertIn("zones", payload["startup_defaults"])
        self.assertIn("down_payment_ratio", payload["startup_defaults"])
        self.assertIn("max_dti_ratio", payload["startup_defaults"])
        self.assertIn("annual_interest_rate", payload["startup_defaults"])
        self.assertIn("release_startup", payload["startup_defaults"])
        self.assertEqual(
            payload["startup_defaults"]["release_startup"]["recommended_snapshot_id"],
            "spindle_medium",
        )
        self.assertTrue(
            any(
                item["snapshot_id"] == "spindle_minimum"
                for item in payload["startup_defaults"]["release_startup"]["supply_snapshots"]
            )
        )
        self.assertTrue(
            any(
                item["snapshot_id"] == "pyramid_medium"
                for item in payload["startup_defaults"]["release_startup"]["supply_snapshots"]
            )
        )

    def test_websocket_receives_controls_updated_event(self):
        with self.client.websocket_connect("/ws") as websocket:
            websocket.receive_json()  # STATUS_SNAPSHOT

            resp = self.client.post(
                "/start",
                json={
                    "agent_count": 3,
                    "months": 2,
                    "seed": 42,
                    "db_path": self.db_path,
                },
            )
            self.assertEqual(resp.status_code, 200)
            websocket.receive_json()  # RUN_STARTED

            resp = self.client.post(
                "/controls",
                json={
                    "down_payment_ratio": 0.22,
                    "annual_interest_rate": 0.039,
                    "max_dti_ratio": 0.41,
                    "market_pulse_enabled": True,
                    "macro_override_mode": "stable",
                    "negotiation_quote_filter_mode": "focused",
                },
            )
            self.assertEqual(resp.status_code, 200)

            while True:
                event = websocket.receive_json()
                if event["event_type"] == "CONTROLS_UPDATED":
                    break
            self.assertEqual(event["payload"]["controls"]["down_payment_ratio"], 0.22)
            self.assertEqual(event["payload"]["controls"]["annual_interest_rate"], 0.039)
            self.assertEqual(event["payload"]["controls"]["max_dti_ratio"], 0.41)
            self.assertEqual(event["payload"]["controls"]["market_pulse_enabled"], True)
            self.assertEqual(event["payload"]["controls"]["macro_override_mode"], "stable")
            self.assertEqual(event["payload"]["controls"]["negotiation_quote_filter_mode"], "focused")
            self._assert_event_envelope(event)

    def test_interventions_can_be_applied_between_steps(self):
        resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 2,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(resp.status_code, 200)

        resp = self.client.post(
            "/interventions/population/add",
            json={
                "count": 2,
                "tier": "middle",
                "template": "young_first_home",
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["result"]["added_count"], 2)
        self.assertEqual(payload["result"]["tier"], "lower_middle")
        self.assertEqual(payload["result"]["template"], "young_first_home")
        self.assertEqual(payload["result"]["income_multiplier"], None)
        self.assertEqual(payload["result"]["income_multiplier_range"]["min"], 0.85)
        self.assertEqual(payload["result"]["income_multiplier_range"]["max"], 1.05)
        self.assertEqual(len(payload["result"]["generated_events"]), 2)

        resp = self.client.post(
            "/interventions/income",
            json={
                "tier_adjustments": [
                    {"tier": "lower_middle", "pct_change": 0.08},
                    {"tier": "high", "pct_change": 0.02},
                ],
            },
        )
        self.assertEqual(resp.status_code, 200)
        self.assertGreater(resp.json()["result"]["updated_count"], 0)
        self.assertEqual(len(resp.json()["result"]["tier_adjustments"]), 2)

        resp = self.client.post(
            "/interventions/developer-supply",
            json={
                "count": 2,
                "zone": "A",
                "template": "a_district_premium",
                "build_year": 2026,
            },
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["result"]["count"], 2)
        self.assertEqual(payload["result"]["zone"], "A")
        self.assertEqual(payload["result"]["template"], "a_district_premium")
        self.assertEqual(len(payload["result"]["generated_events"]), 2)
        self.assertEqual(len(payload["result"]["listed_events"]), 2)
        self.assertTrue(payload["status"]["intervention_history"])

        resp = self.client.post("/step")
        self.assertEqual(resp.status_code, 200)
        step_payload = resp.json()
        self.assertTrue(step_payload["month_result"]["month_review"]["interventions"])

    def test_websocket_receives_intervention_events(self):
        with self.client.websocket_connect("/ws") as websocket:
            websocket.receive_json()  # STATUS_SNAPSHOT

            resp = self.client.post(
                "/start",
                json={
                    "agent_count": 3,
                    "months": 2,
                    "seed": 42,
                    "db_path": self.db_path,
                },
            )
            self.assertEqual(resp.status_code, 200)
            websocket.receive_json()  # RUN_STARTED

            resp = self.client.post(
                "/interventions/population/add",
                json={"count": 1, "tier": "middle", "income_multiplier": 1.0},
            )
            self.assertEqual(resp.status_code, 200)

            population_added = None
            generated = None
            while population_added is None or generated is None:
                event = websocket.receive_json()
                if event["event_type"] == "POPULATION_ADDED":
                    population_added = event
                if event["event_type"] == "AGENT_GENERATED" and event["month"] == 0:
                    generated = event

            self._assert_event_envelope(population_added)
            self._assert_event_envelope(generated)

            resp = self.client.post(
                "/interventions/developer-supply",
                json={"count": 1, "zone": "A", "price_per_sqm": 40000, "size": 100, "school_units": 1, "build_year": 2026},
            )
            self.assertEqual(resp.status_code, 200)

            supply_event = None
            listed_event = None
            while supply_event is None or listed_event is None:
                event = websocket.receive_json()
                if event["event_type"] == "DEVELOPER_SUPPLY_INJECTED":
                    supply_event = event
                if event["event_type"] == "PROPERTY_LISTED" and event["month"] == 0:
                    listed_event = event

            self._assert_event_envelope(supply_event)
            self._assert_event_envelope(listed_event)

    def test_scenario_preset_applies_bundled_interventions(self):
        resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 2,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(resp.status_code, 200)

        resp = self.client.post("/presets/apply", json={"preset": "starter_demand_push"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["preset"], "starter_demand_push")
        self.assertIn("controls", payload)
        self.assertIn("population_result", payload)
        self.assertIn("income_result", payload)
        self.assertIn("developer_result", payload)
        self.assertEqual(payload["population_result"]["template"], "young_first_home")
        self.assertEqual(payload["developer_result"]["template"], "b_entry_level")
        self.assertEqual(payload["controls"]["negotiation_quote_stream_enabled"], True)
        self.assertEqual(payload["controls"]["negotiation_quote_filter_mode"], "heated_only")
        self.assertTrue(payload["status"]["intervention_history"])

        resp = self.client.post("/step")
        self.assertEqual(resp.status_code, 200)
        step_payload = resp.json()
        month_interventions = step_payload["month_result"]["month_review"]["interventions"]
        self.assertTrue(any(item["event_type"] == "SCENARIO_PRESET_APPLIED" for item in month_interventions))

    def test_presets_endpoint_lists_available_presets(self):
        resp = self.client.get("/presets")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertIn("presets", payload)
        preset_ids = [item["id"] for item in payload["presets"]]
        self.assertIn("starter_demand_push", preset_ids)
        self.assertIn("upgrade_cycle", preset_ids)
        self.assertIn("investor_cooldown", preset_ids)
        starter = next(item for item in payload["presets"] if item["id"] == "starter_demand_push")
        self.assertIn("heated", starter["description"])
        self.assertEqual(starter["population_template"], "young_first_home")
        self.assertEqual(starter["developer_template"], "b_entry_level")
        self.assertEqual(starter["income_strategy"], "tier_adjustments")
        self.assertEqual(starter["controls_preview"]["down_payment_ratio"], 0.22)
        self.assertEqual(starter["controls_preview"]["macro_override_mode"], "optimistic")
        self.assertEqual(starter["negotiation_quote_stream_enabled"], True)
        self.assertEqual(starter["negotiation_quote_filter_mode"], "heated_only")

    def test_websocket_receives_scenario_preset_event(self):
        with self.client.websocket_connect("/ws") as websocket:
            websocket.receive_json()  # STATUS_SNAPSHOT

            resp = self.client.post(
                "/start",
                json={
                    "agent_count": 3,
                    "months": 2,
                    "seed": 42,
                    "db_path": self.db_path,
                },
            )
            self.assertEqual(resp.status_code, 200)
            websocket.receive_json()  # RUN_STARTED

            resp = self.client.post("/presets/apply", json={"preset": "upgrade_cycle"})
            self.assertEqual(resp.status_code, 200)

            preset_event = None
            while preset_event is None:
                event = websocket.receive_json()
                if event["event_type"] == "SCENARIO_PRESET_APPLIED":
                    preset_event = event
            self.assertEqual(preset_event["payload"]["preset"], "upgrade_cycle")
            self._assert_event_envelope(preset_event)

    def test_websocket_receives_month_end_event(self):
        with self.client.websocket_connect("/ws") as websocket:
            snapshot = websocket.receive_json()
            self.assertEqual(snapshot["event_type"], "STATUS_SNAPSHOT")
            self.assertEqual(snapshot["payload"]["status"]["status"], "idle")
            self.assertIn("stage_snapshot", snapshot["payload"]["status"])
            self.assertIsInstance(snapshot["payload"]["status"]["stage_snapshot"], dict)
            self.assertIn("stage_replay_events", snapshot["payload"]["status"])
            self.assertIsInstance(snapshot["payload"]["status"]["stage_replay_events"], list)
            self.assertEqual(snapshot["schema_version"], "v1")
            self._assert_event_envelope(snapshot)

            resp = self.client.post(
                "/start",
                json={
                    "agent_count": 3,
                    "months": 1,
                    "seed": 42,
                    "db_path": self.db_path,
                },
            )
            self.assertEqual(resp.status_code, 200)

            started = websocket.receive_json()
            self.assertEqual(started["event_type"], "RUN_STARTED")
            self.assertEqual(started["payload"]["status"]["status"], "initialized")
            self.assertEqual(started["schema_version"], "v1")
            self._assert_event_envelope(started)

            resp = self.client.post("/step")
            self.assertEqual(resp.status_code, 200)

            events = []
            while True:
                event = websocket.receive_json()
                events.append(event)
                if event["event_type"] == "RUN_FINISHED":
                    break

            self.assertTrue(any(event["event_type"] == "AGENT_GENERATED" for event in events))
            self.assertTrue(any(event["event_type"] == "PROPERTY_GENERATED" for event in events))
            self.assertTrue(any(event["event_type"] == "MARKET_BULLETIN_READY" for event in events))
            self.assertTrue(any(event["event_type"] == "MONTH_END" for event in events))
            self.assertTrue(any(event["event_type"] == "RUN_FINISHED" for event in events))
            for event in events:
                self._assert_event_envelope(event)
            month_end = next(event for event in events if event["event_type"] == "MONTH_END")
            self.assertEqual(month_end["payload"]["month_result"]["month"], 1)
            self.assertEqual(month_end["payload"]["status"]["status"], "completed")
            self.assertEqual(month_end["schema_version"], "v1")
            self.assertIn("month_review", month_end["payload"]["month_result"])
            run_finished = next(event for event in events if event["event_type"] == "RUN_FINISHED")
            self.assertEqual(run_finished["payload"]["status"]["status"], "completed")
            self.assertIsNotNone(run_finished["payload"]["final_summary"])
            self.assertIn("top_agents", run_finished["payload"]["final_summary"])

    def test_runs_endpoint_exposes_resume_status_fields(self):
        start_resp = self.client.post(
            "/start",
            json={
                "agent_count": 3,
                "months": 1,
                "seed": 42,
                "db_path": self.db_path,
            },
        )
        self.assertEqual(start_resp.status_code, 200)
        runs_resp = self.client.get("/runs")
        self.assertEqual(runs_resp.status_code, 200)
        payload = runs_resp.json()
        self.assertTrue(payload["runs"])
        target = payload["runs"][0]

        self.assertIn("status", target)
        self.assertIn("current_month", target)
        self.assertIn("completed_months", target)
        self.assertIn("transactions_total", target)
        self.assertIn("can_resume", target)
        self.assertIsInstance(target["current_month"], int)
        self.assertIsInstance(target["completed_months"], int)
        self.assertIsInstance(target["transactions_total"], int)
        self.assertIsInstance(target["can_resume"], bool)

    def test_frontend_uses_module_entry(self):
        index_path = Path("web/index.html")
        self.assertTrue(index_path.exists())
        html = index_path.read_text(encoding="utf-8")
        self.assertIn('script type="module" src="/web/js/main.js"', html)
        self.assertNotIn('/web/app.js', html)
        self.assertIn('id="screen-canvas"', html)
        self.assertIn('id="demo-mode-toggle"', html)
        self.assertIn('id="runtime-controls-card"', html)
        self.assertIn('id="config-catalog-card"', html)
        self.assertIn('id="screen-stage-toggle"', html)
        self.assertIn('id="screen-stage-fullscreen"', html)
        self.assertIn('id="screen-stage-shell"', html)
        self.assertIn('id="screen-stage-demo-hint"', html)
        self.assertIn("screen-stage-legend", html)
        self.assertIn('id="negotiation-list"', html)
        self.assertIn('id="population-form"', html)
        self.assertIn('id="population-template"', html)
        self.assertIn('id="developer-form"', html)
        self.assertIn('id="developer-template"', html)
        self.assertIn('id="preset-form"', html)
        self.assertIn('id="scenario-preset"', html)
        self.assertIn('id="scenario-preset-hint"', html)
        self.assertIn('id="scenario-preset-confirm"', html)
        self.assertIn('id="scenario-preset-confirm-apply"', html)
        self.assertIn('id="scenario-preset-confirm-cancel"', html)
        self.assertIn('id="scenario-preset-history"', html)
        self.assertIn('id="scenario-preset-history-count"', html)
        self.assertIn('id="scenario-preset-impact"', html)
        self.assertIn('id="review-preset-timeline"', html)
        self.assertIn('id="open-report-view"', html)
        self.assertIn('id="download-report-json"', html)
        self.assertIn('id="open-parameter-report-view"', html)
        self.assertIn('id="download-parameter-report-json"', html)
        self.assertIn('id="open-db-observer"', html)
        self.assertIn('id="config-schema-count"', html)
        self.assertIn('id="config-schema-summary"', html)
        self.assertIn('id="config-schema-form"', html)
        self.assertIn('id="config-schema-search"', html)
        self.assertIn('id="config-schema-phase-filter"', html)
        self.assertIn('id="config-schema-group-filter"', html)
        self.assertIn('id="config-schema-only-editable"', html)
        self.assertIn('id="config-schema-reset-filters"', html)
        self.assertIn('id="config-schema-startup-list"', html)
        self.assertIn('id="config-schema-edit-list"', html)
        self.assertIn('id="config-schema-readonly-list"', html)
        self.assertIn('id="config-schema-list"', html)
        self.assertIn('id="controls-summary"', html)
        self.assertIn('id="property-total-count"', html)
        self.assertIn('id="startup-supply-snapshot"', html)
        self.assertIn('id="startup-market-goal"', html)
        self.assertIn('id="startup-demand-multiplier"', html)
        self.assertIn('id="startup-demand-coverage"', html)
        self.assertIn('id="start-mode"', html)
        self.assertIn('id="night-plan-path"', html)
        self.assertIn('id="night-plan-editor"', html)
        self.assertIn('id="night-plan-summary"', html)
        self.assertIn('id="night-plan-list"', html)
        self.assertIn('id="night-plan-add-population"', html)
        self.assertIn('id="night-plan-add-developer"', html)
        self.assertIn('id="night-plan-add-supply-cut"', html)
        self.assertIn('id="night-plan-add-income"', html)
        self.assertIn('id="night-plan-reset"', html)
        self.assertIn('id="night-plan-export"', html)
        self.assertIn('id="night-plan-import"', html)
        self.assertIn('id="night-plan-import-file"', html)
        self.assertIn('id="resume-run-select"', html)
        self.assertIn('id="resume-run-summary"', html)
        self.assertIn('id="forensic-summary"', html)
        self.assertIn('id="start-submit-btn"', html)
        self.assertIn('id="startup-overview"', html)
        self.assertIn('id="startup-confirm"', html)
        self.assertIn('id="startup-confirm-apply"', html)
        self.assertIn('id="startup-confirm-cancel"', html)
        self.assertIn('id="zone-a-price-min"', html)
        self.assertIn('id="zone-b-price-max"', html)
        self.assertIn('id="min-cash-threshold"', html)
        self.assertIn('id="startup-bid-floor-ratio"', html)
        self.assertIn('id="startup-down-payment-ratio"', html)
        self.assertIn('id="startup-max-dti-ratio"', html)
        self.assertIn('id="startup-annual-interest-rate"', html)
        self.assertIn('id="startup-precheck-buffer"', html)
        self.assertIn('id="startup-precheck-tax-fee"', html)
        self.assertIn('id="tier-lower-middle-count"', html)
        self.assertIn('运行逻辑体检', html)
        self.assertIn('夜跑模拟', html)
        self.assertIn('应用 schema 参数', html)
        self.assertIn('id="negotiation-density-mode"', html)
        self.assertIn('id="negotiation-quote-focus-limit"', html)
        self.assertIn('Quote 焦点', html)

        main_js = Path("web/js/main.js")
        self.assertTrue(main_js.exists())
        main_text = main_js.read_text(encoding="utf-8")
        self.assertIn('from "./api.js"', main_text)
        self.assertIn("event.payload.final_summary", main_text)
        self.assertIn('DEMO_MODE_KEY = "vre.demoMode"', main_text)
        self.assertIn('document.body.classList.toggle("demo-mode", enabled)', main_text)
        self.assertIn("populationForm.addEventListener", main_text)
        self.assertIn("presetForm.addEventListener", main_text)
        self.assertIn("openDbObserverBtn?.addEventListener", main_text)
        self.assertIn('from "./screen.js"', main_text)
        self.assertIn("initScreenStage()", main_text)
        self.assertIn("ingestScreenEvent(event)", main_text)

    def test_screen_stage_module_exposes_hooks(self):
        screen_js = Path("web/js/screen.js")
        screen_state_js = Path("web/js/screen_state.js")
        screen_render_js = Path("web/js/screen_render.js")
        self.assertTrue(screen_js.exists())
        self.assertTrue(screen_state_js.exists())
        self.assertTrue(screen_render_js.exists())
        screen_text = screen_js.read_text(encoding="utf-8")
        screen_state_text = screen_state_js.read_text(encoding="utf-8")
        screen_render_text = screen_render_js.read_text(encoding="utf-8")
        main_text = Path("web/js/main.js").read_text(encoding="utf-8")
        self.assertIn("window.render_game_to_text", screen_text)
        self.assertIn("window.advanceTime", screen_text)
        self.assertIn("export function initScreenStage", screen_text)
        self.assertIn("export function ingestScreenEvent", screen_text)
        self.assertIn("export async function toggleScreenStageFullscreen", screen_text)
        self.assertIn("requestFullscreen", screen_text)
        self.assertIn("fullscreenchange", screen_text)
        self.assertIn('event.code === "Space"', screen_text)
        self.assertIn('./screen_state.js', screen_text)
        self.assertIn('./screen_render.js', screen_text)
        self.assertIn("/web/assets/icons/agent-smart.svg", screen_state_text)
        self.assertIn("/web/assets/icons/agent-normal.svg", screen_state_text)
        self.assertIn("/web/assets/icons/property.svg", screen_state_text)
        self.assertIn("drawImage", screen_render_text)
        self.assertIn("export function renderScreen", screen_render_text)
        self.assertIn("export function stepScreen", screen_render_text)
        self.assertIn("fetchPresets()", main_text)
        self.assertIn("fetchConfigSchema()", main_text)
        self.assertIn("bindConfigSchemaFilters()", main_text)
        self.assertIn("bindNightRunPlanEditor()", main_text)
        self.assertIn("configSchemaForm?.addEventListener", main_text)
        self.assertIn("renderScenarioPresetHint", main_text)
        self.assertIn("setNegotiationDensityMode", main_text)
        self.assertIn("setNegotiationQuoteFocusLimit", main_text)
        self.assertIn("localStorage", main_text)
        self.assertIn("tier_adjustments", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/presets/apply", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/presets", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/report/final", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/report/final/view", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/report/parameter-assumption", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/report/parameter-assumption/view", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/config/schema", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("renderConfigSchema", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("matchesSchemaFilters", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("bindConfigSchemaFilters", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("resetSchemaFilters", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("onlyEditable", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("collapsedSchemaGroups", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/db-observer/view", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("collectNightRunPlans", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("preplanned_interventions", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("exportNightRunPlans", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("importNightRunPlansFromFile", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("renderNightRunPlanSummary", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("data-schema-toggle", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("countSchemaPhases", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("applySchemaControls", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertNotIn("applyControls", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("SCHEMA_CONTROL_KEY_MAP", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation_quote_mode", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertNotIn("avg-price-line", Path("web/index.html").read_text(encoding="utf-8"))
        self.assertNotIn("tx-bars", Path("web/index.html").read_text(encoding="utf-8"))
        self.assertIn("bulletin-panel", Path("web/index.html").read_text(encoding="utf-8"))
        self.assertNotIn("failed-line", Path("web/index.html").read_text(encoding="utf-8"))
        self.assertIn("avgPriceLine", Path("web/js/chart.js").read_text(encoding="utf-8"))
        self.assertIn("failedLine", Path("web/js/chart.js").read_text(encoding="utf-8"))
        self.assertIn("avg_transaction_price", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("failedNegotiations", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("renderBulletin", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("bulletin-panel", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiation_quote_turn_limit", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation_quote_char_limit", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("getStartupPayloadFromSchema", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("fetchRuns()", main_text)
        self.assertIn('"/runs"', Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn('"/forensics/zero-tx"', Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("renderSelectedRunSummary", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn('data-forensic-action="view"', Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn('data-forensic-action="json"', Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/forensics/zero-tx/view", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/forensics/zero-tx/download", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("/night-run/start", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("night_plan_path", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("runForensicAnalysis", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("controls_preview", Path("api_server.py").read_text(encoding="utf-8"))
        self.assertIn("CONFIG_SCHEMA_FIELDS", Path("api_server.py").read_text(encoding="utf-8"))
        self.assertIn("quoteFilterMode", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("populationTemplate", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("downPaymentRatio", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("preset-hint-grid", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("Delta vs Current", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("buildScenarioPresetConfirmMessage", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("openScenarioPresetConfirm", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("showScenarioPresetAppliedFeedback", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("bindScenarioPresetConfirmActions", main_text)
        self.assertIn("openFinalReportView", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("downloadFinalReportJson", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("openParameterAssumptionReportView", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("downloadParameterAssumptionReportJson", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("renderPresetHistory", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("highlightArchiveMonth", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("renderPresetImpactFromArchiveCard", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("formatSignedDelta", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("data-review-preset-month", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("/web/assets/icons/agent-smart.svg", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("/web/assets/icons/agent-normal.svg", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("/web/assets/icons/property.svg", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("reviewPresetTimeline", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaList", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaSearchInput", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaPhaseFilterInput", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaGroupFilterInput", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaOnlyEditableInput", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaResetFiltersBtn", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaForm", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaStartupList", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("configSchemaReadonlyList", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("controlsSummary", Path("web/js/dom.js").read_text(encoding="utf-8"))
        self.assertIn("archive-card-highlight", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("config-schema-list", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("config-schema-form", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("config-schema-toolbar", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("config-schema-toolbar-actions", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("checkbox-row.compact", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("config-schema-group-head", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("config-schema-group-count", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn('data-collapsed="true"', Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("config-schema-readonly-grid", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("preset-history-item-highlight", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("review-rank-item-highlight", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("event-avatar-glyph", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("presetHistory", Path("web/js/state.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation_quote_stream_enabled", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation_quote_filter_mode", Path("web/js/api.js").read_text(encoding="utf-8"))
        self.assertIn("NEGOTIATION_TURN", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("NEGOTIATION_TURN_BATCH_END", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("toggleNegotiationReplay", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("getOrCreateNegotiationThread", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("buildNegotiationThreadSummary", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation-thread-head", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiation-thread-summary", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("data-negotiation-outcome", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiation-thread-bar", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("getNegotiationOutcomeState", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation-thread-density", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("data-density-level", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("buildNegotiationGapHint", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation-thread-gap", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("buildNegotiationPriceTrail", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation-thread-trail", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("buildNegotiationReplayChart", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation-replay-chart", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiation-replay-chart-legend", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiation-replay-outcome", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiation-replay-break-line", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("failedBreakpoint", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("highlightNegotiationReplayTurn", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("bindNegotiationReplayChart", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("turn-highlight", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiationGroupNodesById", Path("web/js/state.js").read_text(encoding="utf-8"))
        self.assertIn("negotiation-turn-rail", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("data-turn-visibility", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiation-turn-label", Path("web/style.css").read_text(encoding="utf-8"))
        self.assertIn("negotiationReplayExpanded", Path("web/js/state.js").read_text(encoding="utf-8"))
        self.assertIn("densityMode", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("quoteFocus", Path("web/js/render.js").read_text(encoding="utf-8"))
        self.assertIn("negotiationQuoteFocusLimit", Path("web/js/state.js").read_text(encoding="utf-8"))

    def test_websocket_receives_run_failed_event_when_step_crashes(self):
        with self.client.websocket_connect("/ws") as websocket:
            websocket.receive_json()  # STATUS_SNAPSHOT

            resp = self.client.post(
                "/start",
                json={
                    "agent_count": 3,
                    "months": 1,
                    "seed": 42,
                    "db_path": self.db_path,
                },
            )
            self.assertEqual(resp.status_code, 200)
            websocket.receive_json()  # RUN_STARTED

            with patch.object(api_server.runtime.runner, "run_one_month", side_effect=Exception("forced failure")):
                resp = self.client.post("/step")

            self.assertEqual(resp.status_code, 500)
            self.assertIn("forced failure", resp.json()["detail"])

            while True:
                failed = websocket.receive_json()
                if failed["event_type"] == "RUN_FAILED":
                    break
            self.assertEqual(failed["event_type"], "RUN_FAILED")
            self.assertEqual(failed["payload"]["error_code"], "SIMULATION_RUN_FAILED")
            self.assertIn("forced failure", failed["payload"]["message"])
            self._assert_event_envelope(failed)

    def _assert_event_envelope(self, event):
        for key in ("event_id", "run_id", "month", "phase", "event_type", "ts", "payload", "source", "schema_version"):
            self.assertIn(key, event)
        self.assertEqual(event["schema_version"], "v1")
        self.assertIsInstance(event["payload"], dict)
        self.assertIsInstance(event["event_type"], str)
        self.assertTrue(str(event["event_id"]).strip())
        self.assertTrue(str(event["run_id"]).strip())


if __name__ == "__main__":
    unittest.main()
