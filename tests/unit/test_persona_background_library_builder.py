import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.build_persona_background_library import build_library, infer_income_tier


class TestPersonaBackgroundLibraryBuilder(unittest.TestCase):
    def test_infer_income_tier(self):
        self.assertEqual(infer_income_tier(60000), "ultra_high")
        self.assertEqual(infer_income_tier(30000), "high")
        self.assertEqual(infer_income_tier(15000), "middle")
        self.assertEqual(infer_income_tier(8000), "lower_middle")
        self.assertEqual(infer_income_tier(3000), "low")

    def test_build_library_from_results_dbs(self):
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            run_dir = root / "run_a"
            run_dir.mkdir(parents=True, exist_ok=True)
            db_path = run_dir / "simulation.db"
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE agents_static (
                    agent_id INTEGER PRIMARY KEY,
                    occupation TEXT,
                    background_story TEXT,
                    purchase_motive_primary TEXT,
                    investment_style TEXT,
                    housing_stage TEXT,
                    family_stage TEXT,
                    education_path TEXT,
                    financial_profile TEXT,
                    seller_profile TEXT
                )
                """
            )
            cur.execute(
                "CREATE TABLE agents_finance (agent_id INTEGER PRIMARY KEY, monthly_income REAL)"
            )
            cur.execute(
                """
                INSERT INTO agents_static(
                    agent_id, occupation, background_story, purchase_motive_primary,
                    investment_style, housing_stage, family_stage, education_path,
                    financial_profile, seller_profile
                ) VALUES (1, '工程师', '已有一套房准备改善', 'upgrade', 'balanced',
                          'owner_upgrade', 'young_family', 'public_school',
                          'balanced_finance', 'owner_upgrade')
                """
            )
            cur.execute("INSERT INTO agents_finance(agent_id, monthly_income) VALUES (1, 18000)")
            conn.commit()
            conn.close()

            payload = build_library(results_root=root, max_dbs=5, row_limit_per_db=100)
            self.assertGreaterEqual(int(payload["source_db_count_scanned"]), 1)
            self.assertEqual(len(payload["entries"]), 1)
            self.assertEqual(payload["entries"][0]["occupation"], "工程师")
            self.assertIn("middle", payload["by_tier"])
            self.assertEqual(len(payload["by_tier"]["middle"]), 1)

            # JSON serializable
            json.dumps(payload, ensure_ascii=False)


if __name__ == "__main__":
    unittest.main()
