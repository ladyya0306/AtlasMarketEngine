import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.activation_governance import (
    build_activation_governance_config,
    evaluate_mismatch_gate,
    export_monthly_activation_funnel,
)


class TestActivationGovernance(unittest.TestCase):
    def test_evaluate_mismatch_gate_blocks_school_gap(self):
        snapshot = {
            "demand_library": {
                "buckets": [
                    {
                        "bucket_id": "B_SCHOOL_JUST_ENTRY",
                        "count": 4,
                        "role_side": "buyer",
                        "need_school_district": True,
                    }
                ]
            },
            "supply_library": {
                "experiment_mode": "abundant",
            },
            "budget_consistency_report": {
                "failures": [],
            },
            "competition_control_report": {
                "abundant": [
                    {
                        "agent_bucket_id": "B_SCHOOL_JUST_ENTRY",
                        "buyer_count": 4,
                        "eligible_supply_count": 0,
                        "buyer_to_supply_ratio": 999.0,
                    }
                ]
            },
        }
        cfg = build_activation_governance_config(
            activation_mode="hybrid",
            gate_mode="pause",
            profiled_market_required=True,
            hard_bucket_matcher_required=True,
        )

        gate = evaluate_mismatch_gate(
            governance_snapshot=snapshot,
            activation_governance=cfg,
            profiled_market_enabled=True,
            hard_bucket_matcher_enabled=True,
        )

        self.assertEqual(gate["activation_mode"], "hybrid")
        self.assertEqual(gate["gate_mode"], "pause")
        self.assertEqual(gate["governance_status"], "block")
        self.assertIn("school_supply_gap", gate["mismatch_reasons"])
        self.assertEqual(gate["recommended_action"], "autofill_supply")

    def test_evaluate_mismatch_gate_blocks_affordable_overlap_zero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pack_path = Path(tmpdir) / "profile_pack.yaml"
            pack_path.write_text(
                "\n".join(
                    [
                        "profiled_market_mode:",
                        "  bucket_alignment_rules:",
                        "    - agent_bucket_id: B_SCHOOL_JUST_ENTRY",
                        "      eligible_property_buckets: [B_SCHOOL_JUST]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            snapshot = {
                "identity": {
                    "profile_pack_path": str(pack_path),
                },
                "demand_library": {
                    "buckets": [
                        {
                            "bucket_id": "B_SCHOOL_JUST_ENTRY",
                            "count": 4,
                            "role_side": "buyer",
                            "need_school_district": True,
                            "max_price_range": [800000, 1000000],
                        }
                    ]
                },
                "supply_library": {
                    "experiment_mode": "abundant",
                    "buckets": [
                        {
                            "bucket_id": "B_SCHOOL_JUST",
                            "price_range": [2000000, 3000000],
                            "count_selected_mode": 5,
                        }
                    ],
                },
                "budget_consistency_report": {
                    "failures": [],
                },
                "competition_control_report": {
                    "abundant": [
                        {
                            "agent_bucket_id": "B_SCHOOL_JUST_ENTRY",
                            "buyer_count": 4,
                            "eligible_supply_count": 5,
                            "buyer_to_supply_ratio": 0.8,
                        }
                    ]
                },
            }
            cfg = build_activation_governance_config(activation_mode="hybrid", gate_mode="warn")
            gate = evaluate_mismatch_gate(
                governance_snapshot=snapshot,
                activation_governance=cfg,
                profiled_market_enabled=True,
                hard_bucket_matcher_enabled=True,
            )
            self.assertEqual(gate["governance_status"], "block")
            self.assertIn("affordable_supply_zero", gate["mismatch_reasons"])

    def test_export_monthly_activation_funnel_writes_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sample.db"
            out_path = Path(tmpdir) / "monthly_activation_funnel.csv"
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE decision_logs (
                    agent_id INTEGER,
                    month INTEGER,
                    event_type TEXT,
                    decision TEXT,
                    rationale TEXT,
                    thought_process TEXT,
                    context_metrics TEXT,
                    llm_called INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE agents_static (
                    agent_id INTEGER PRIMARY KEY,
                    agent_type TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE property_buyer_matches (
                    month INTEGER,
                    buyer_id INTEGER,
                    proceeded_to_negotiation INTEGER,
                    failure_stage TEXT,
                    failure_reason TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE transaction_orders (
                    created_month INTEGER,
                    buyer_id INTEGER,
                    property_id INTEGER
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE transactions (
                    month INTEGER,
                    buyer_id INTEGER
                )
                """
            )
            cur.executemany(
                "INSERT INTO agents_static(agent_id, agent_type) VALUES (?, ?)",
                [(1, "normal"), (2, "smart")],
            )
            cur.execute(
                """
                INSERT INTO decision_logs(agent_id, month, event_type, decision, rationale, thought_process, context_metrics, llm_called)
                VALUES (0, 1, 'ROLE_ACTIVATION_ROUTING_SUMMARY', 'SUMMARY', '', NULL, ?, 0)
                """,
                ('{"stage1_prefilter_candidates": 5, "llm_candidate_count": 2, "synthetic_decision_count": 0}',),
            )
            cur.executemany(
                """
                INSERT INTO decision_logs(agent_id, month, event_type, decision, rationale, thought_process, context_metrics, llm_called)
                VALUES (?, 1, 'ROLE_DECISION', ?, '', ?, ?, ?)
                """,
                [
                    (1, "BUYER", '{"trigger":"natural_activation"}', '{"m14_info_delay_months": 1}', 1),
                    (2, "BUYER_SELLER", '{"trigger":"natural_activation"}', '{"m14_info_delay_months": 0}', 1),
                ],
            )
            cur.execute(
                """
                INSERT INTO decision_logs(agent_id, month, event_type, decision, rationale, thought_process, context_metrics, llm_called)
                VALUES (1, 1, 'BUYER_MATCH_SUMMARY', 'normal', '', NULL, ?, 0)
                """,
                (
                    json.dumps(
                        {
                            "shortlist_property_ids": [101, 102],
                            "no_selection_reason_code": "HAS_SELECTION",
                            "no_buy_class": "HAS_SELECTION",
                        }
                    ),
                ),
            )
            cur.execute(
                """
                INSERT INTO decision_logs(agent_id, month, event_type, decision, rationale, thought_process, context_metrics, llm_called)
                VALUES (2, 1, 'BUYER_MATCH_SUMMARY', 'normal', '', NULL, ?, 0)
                """,
                (
                    json.dumps(
                        {
                            "shortlist_property_ids": [],
                            "no_selection_reason_code": "SHORTLIST_EMPTY",
                            "no_buy_class": "NO_SUITABLE_LISTED",
                            "no_buy_branch": "HAS_UNLISTED_SUITABLE",
                        }
                    ),
                ),
            )
            cur.execute(
                "INSERT INTO property_buyer_matches(month, buyer_id, proceeded_to_negotiation, failure_stage, failure_reason) VALUES (1, 1, 1, '', '')"
            )
            cur.execute(
                "INSERT INTO transaction_orders(created_month, buyer_id, property_id) VALUES (1, 1, 101)"
            )
            cur.execute(
                "INSERT INTO transactions(month, buyer_id) VALUES (1, 1)"
            )
            conn.commit()
            conn.close()

            export_monthly_activation_funnel(
                db_path=db_path,
                months=1,
                output_path=out_path,
                run_id="demo",
                group_id="V2",
                shock_id="none",
                activation_mode="hybrid",
                governance_status="warn",
            )

            with out_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["activation_mode"], "hybrid")
            self.assertEqual(row["governance_status"], "warn")
            self.assertEqual(int(row["candidate_count"]), 5)
            self.assertEqual(int(row["llm_candidate_count"]), 2)
            self.assertEqual(int(row["activated_count"]), 2)
            self.assertEqual(int(row["shortlist_entered_count"]), 1)
            self.assertEqual(int(row["negotiation_entered_count"]), 1)
            self.assertEqual(int(row["order_placed_count"]), 1)
            self.assertEqual(int(row["transaction_completed_count"]), 1)
            self.assertEqual(int(row["direct_transaction_without_order_count"]), 1)
            self.assertEqual(int(row["targeted_llm_activated_count"]), 0)
            self.assertEqual(int(row["targeted_llm_observer_count"]), 0)
            self.assertEqual(int(row["has_selection_count"]), 1)
            self.assertEqual(int(row["no_buy_choose_to_wait_count"]), 0)
            self.assertEqual(int(row["no_buy_mechanism_blocked_count"]), 0)
            self.assertEqual(int(row["no_buy_no_suitable_listed_count"]), 1)
            self.assertEqual(int(row["no_buy_no_suitable_listed_has_unlisted_suitable_count"]), 1)
            self.assertEqual(int(row["no_buy_no_suitable_listed_no_suitable_exists_count"]), 0)
            self.assertEqual(int(row["no_buy_no_suitable_listed_unknown_count"]), 0)


if __name__ == "__main__":
    unittest.main()
