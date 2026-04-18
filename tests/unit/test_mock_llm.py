import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from agent_behavior import AgentRole, batched_determine_role, determine_role
from models import Agent, AgentStory

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


class TestMockLLM(unittest.TestCase):
    def setUp(self):
        # Load fixtures
        fixture_path = os.path.join(os.path.dirname(__file__), '../fixtures/llm_responses.json')
        with open(fixture_path, 'r', encoding='utf-8') as f:
            self.fixtures = json.load(f)

        # Create dummy agent
        self.agent = MagicMock(spec=Agent)
        self.agent.id = 1
        self.agent.age = 30
        self.agent.marital_status = "single"
        self.agent.cash = 1000000
        self.agent.monthly_income = 20000
        self.agent.owned_properties = []

        self.agent.story = MagicMock(spec=AgentStory)
        self.agent.story.background_story = "A young professional."
        self.agent.story.housing_need = "Buying first home"
        self.agent.story.investment_style = "conservative"

        self.agent.monthly_event = "None"

        self.market = MagicMock()

    @patch('agent_behavior.safe_call_llm')
    def test_determine_role_buyer(self, mock_llm):
        # Setup mock return
        mock_llm.return_value = self.fixtures['role_buyer']

        # Call function
        role, reason = determine_role(self.agent, 1, self.market)

        # Verify
        self.assertEqual(role, AgentRole.BUYER)
        self.assertIn("Savings", reason)
        mock_llm.assert_called_once()

    @patch('agent_behavior.safe_call_llm')
    def test_determine_role_seller_constraint(self, mock_llm):
        # Agent has no property, but LLM says SELLER
        mock_llm.return_value = self.fixtures['role_seller']
        self.agent.owned_properties = []

        # Call function
        role, reason = determine_role(self.agent, 1, self.market)

        # Verify Constraint: Should fallback to OBSERVER
        self.assertEqual(role, AgentRole.OBSERVER)
        self.assertIn("System Corrected", reason)

    @patch('agent_behavior.safe_call_llm')
    def test_batched_determine_role(self, mock_llm):
        # Setup mock return
        mock_llm.return_value = self.fixtures['batch_roles']

        agents = [self.agent, self.agent]  # Dummy list

        # Call function
        results = batched_determine_role(agents, 1, self.market)

        # Verify
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['role'], "BUYER")
        mock_llm.assert_called_once()


if __name__ == '__main__':
    unittest.main()
