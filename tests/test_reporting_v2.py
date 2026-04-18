import os
import sqlite3
import unittest

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner


class TestReporting(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_reporting.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        # Create a dummy config
        self.config = SimulationConfig()
        # Force enable LLM portraits (though default is True in my code)
        self.config._config['enable_llm_portraits'] = True

    def tearDown(self):
        # Keep DB for inspection if needed, or remove
        pass

    def test_reporting_generation(self):
        print("\n--- Starting Reporting Test (5 Agents, 2 Months) ---")
        runner = SimulationRunner(
            agent_count=5,
            months=2,
            seed=42,
            config=self.config,
            db_path=self.db_path
        )

        # Run simulation
        runner.run()

        # Verify
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 1. Check Table Existence and Count
        cursor.execute("SELECT count(*) FROM agent_end_reports")
        count = cursor.fetchone()[0]
        print(f"Generated Reports: {count}")
        self.assertEqual(count, 5, "Should generate exactly 5 reports")

        # 2. Check Content
        cursor.execute("SELECT agent_id, identity_summary, llm_portrait FROM agent_end_reports LIMIT 1")
        row = cursor.fetchone()

        print("\nSample Report Data:")
        print(f"Agent ID: {row[0]}")
        print(f"Identity: {row[1]}")
        print(f"LLM Portrait: {row[2]}")

        self.assertIsNotNone(row[2], "LLM Portrait should not be None")
        self.assertNotEqual(row[2], "", "LLM Portrait should not be empty")
        # Check if it looks like Chinese
        self.assertTrue(any('\u4e00' <= char <= '\u9fff' for char in row[2]), "Portrait should contain Chinese characters")

        conn.close()


if __name__ == "__main__":
    unittest.main()
