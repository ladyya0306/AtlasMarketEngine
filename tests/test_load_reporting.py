import os
import sqlite3
import unittest
from unittest.mock import patch

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner


class TestLoadReporting(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_load_reporting.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        # 1. Create Initial Data (Simulate Phase 1)
        # Create a dummy run with 5 agents for 1 month
        print("\n--- [Step 1] Initializing New Simulation (Month 1) ---")
        config = SimulationConfig()
        config._config['enable_llm_portraits'] = False  # Disable for phase 1 to save time

        runner = SimulationRunner(
            agent_count=5,
            months=1,
            seed=42,
            config=config,
            db_path=self.db_path
        )
        runner.run()
        runner.close()  # Close DB connection

        # Verify initial state
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Determine if reporting happened (should allow logic to run but we want to test load)
        # We manually delete the report table to simulate an "old" DB if needed,
        # BUT our migration logic should handle "if not exists".
        # Let's Drop the table to simulate an OLD database that needs migration
        cursor.execute("DROP TABLE IF EXISTS agent_end_reports")
        conn.commit()
        conn.close()
        print("--- [Step 1] Initial DB Created & Report Table Dropped (Simulating Old DB) ---")

    def tearDown(self):
        # pass
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    @patch('services.reporting_service.safe_call_llm_async')
    def test_load_and_report(self, mock_llm):
        # Setup mock
        mock_llm.return_value = "This is a mock LLM portrait generated for testing purposes. It is long enough to pass validation."

        print("\n--- [Step 2] Resuming Simulation (Month 2) with Reporting ---")

        # 2. Resume Simulation
        config = SimulationConfig()
        config._config['enable_llm_portraits'] = True  # Enable for phase 2

        # Initialize in RESUME mode
        runner = SimulationRunner(
            months=1,  # Run 1 more month (Month 2)
            resume=True,
            config=config,
            db_path=self.db_path
        )

        # Run
        runner.run()

        # 3. Verify
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check Table Existence (Migration should have recreated it)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='agent_end_reports'")
        self.assertIsNotNone(cursor.fetchone(), "agent_end_reports table should exist after migration")

        # Check Content
        cursor.execute("SELECT count(*) FROM agent_end_reports")
        count = cursor.fetchone()[0]
        print(f"Generated Reports after Resume: {count}")
        self.assertEqual(count, 5, "Should generate reports for all 5 agents")

        cursor.execute("SELECT llm_portrait FROM agent_end_reports LIMIT 1")
        portrait = cursor.fetchone()[0]
        print(f"Sample Portrait: {portrait[:50]}...")
        self.assertTrue(len(portrait) > 10, "Portrait should have content")

        conn.close()


if __name__ == "__main__":
    unittest.main()
