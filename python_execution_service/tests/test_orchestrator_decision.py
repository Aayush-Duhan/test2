from __future__ import annotations

import unittest
from unittest.mock import patch

from agentic_core.orchestrator import (
    SnowflakeCortexOrchestrator,
    build_decision_context,
)
from agentic_core.state import MigrationContext


class OrchestratorDecisionTests(unittest.TestCase):
    def _context(self) -> MigrationContext:
        return MigrationContext(
            project_name="demo",
            session_id="run-1",
            current_stage=None,  # type: ignore[arg-type]
            last_step_success=True,
        )

    def test_decide_parses_valid_json(self) -> None:
        state = self._context()
        decision_context = build_decision_context(
            state,
            "execute_sql",
            ["validate", "self_heal", "human_review"],
        )
        orchestrator = SnowflakeCortexOrchestrator(timeout_seconds=15, retries=1)

        with patch.object(
            orchestrator,
            "_invoke_with_timeout",
            return_value='{"next_node":"validate","confidence":0.93,"reason":"execution ok","summary":"Proceed to validation","next_steps":["validate output"]}',
        ):
            decision = orchestrator.decide(state, decision_context)

        self.assertEqual(decision.status, "ok")
        self.assertEqual(decision.selected_step, "validate")
        self.assertGreaterEqual(decision.confidence, 0.9)
        self.assertEqual(decision.attempt, 1)

    def test_decide_retries_once_then_succeeds(self) -> None:
        state = self._context()
        decision_context = build_decision_context(
            state,
            "validate",
            ["finalize", "human_review"],
        )
        orchestrator = SnowflakeCortexOrchestrator(timeout_seconds=15, retries=1)

        with patch.object(
            orchestrator,
            "_invoke_with_timeout",
            side_effect=[
                ValueError("parse failure"),
                '{"next_node":"finalize","confidence":0.88,"reason":"issues resolved","summary":"Finalize run","next_steps":["persist artifacts"]}',
            ],
        ):
            decision = orchestrator.decide(state, decision_context)

        self.assertEqual(decision.status, "ok")
        self.assertEqual(decision.selected_step, "finalize")
        self.assertEqual(decision.attempt, 2)

    def test_decide_fails_after_retry(self) -> None:
        state = self._context()
        decision_context = build_decision_context(
            state,
            "convert_code",
            ["execute_sql"],
        )
        orchestrator = SnowflakeCortexOrchestrator(timeout_seconds=15, retries=1)

        with patch.object(
            orchestrator,
            "_invoke_with_timeout",
            side_effect=[TimeoutError("t1"), TimeoutError("t2")],
        ):
            decision = orchestrator.decide(state, decision_context)

        self.assertEqual(decision.status, "failed")
        self.assertEqual(decision.selected_step, "human_review")
        self.assertEqual(decision.attempt, 2)
        self.assertIn("t2", decision.error or "")


if __name__ == "__main__":
    unittest.main()
