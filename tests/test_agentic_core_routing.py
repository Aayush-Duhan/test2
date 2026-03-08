import unittest

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.routing.decisions import (
    should_continue,
    should_continue_after_execute,
)


class RoutingTests(unittest.TestCase):
    def test_should_continue_finalizes_when_validation_passes(self):
        state = MigrationContext(
            current_stage=MigrationState.VALIDATE,
            validation_passed=True,
        )
        self.assertEqual(should_continue(state), "finalize")

    def test_should_continue_moves_to_self_heal_before_max_iterations(self):
        state = MigrationContext(
            current_stage=MigrationState.VALIDATE,
            validation_passed=False,
            self_heal_iteration=1,
            max_self_heal_iterations=3,
            validation_issues=[{"message": "bad"}],
        )
        self.assertEqual(should_continue(state), "self_heal")

    def test_should_continue_after_execute_requests_review_when_ddl_needed(self):
        state = MigrationContext(
            current_stage=MigrationState.EXECUTE_SQL,
            execution_passed=False,
            requires_ddl_upload=True,
        )
        self.assertEqual(should_continue_after_execute(state), "human_review")
