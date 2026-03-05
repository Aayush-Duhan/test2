from __future__ import annotations

import unittest
from unittest.mock import patch

from agentic_core.orchestrator import OrchestratorDecision
from agentic_core.state import MigrationContext
from python_execution_service import main


def _make_run() -> main.RunRecord:
    return main.RunRecord(
        runId="run-guardrails",
        projectId="proj-1",
        projectName="project",
        sourceId="src-1",
        schemaId="schema-1",
        sourceLanguage="teradata",
        sourcePath="source.sql",
        schemaPath="mapping.csv",
        sfAccount=None,
        sfUser=None,
        sfRole=None,
        sfWarehouse=None,
        sfDatabase=None,
        sfSchema=None,
        sfAuthenticator=None,
        status="running",
        createdAt=main.now_iso(),
        updatedAt=main.now_iso(),
        outputDir=".",
        steps=main.get_steps_template(),
    )


class _FakeOrchestrator:
    def __init__(self, decision: OrchestratorDecision) -> None:
        self.decision = decision
        self.contexts = []

    def decide(self, _state: MigrationContext, context):  # noqa: ANN001
        self.contexts.append(context)
        return self.decision


class OrchestratorGuardrailTests(unittest.TestCase):
    def test_low_confidence_routes_to_human_review(self) -> None:
        run = _make_run()
        state = MigrationContext(project_name="demo", session_id=run.runId, last_step_success=True)
        orchestrator = _FakeOrchestrator(
            OrchestratorDecision(
                from_step="execute_sql",
                candidate_steps=["validate", "self_heal", "human_review"],
                selected_step="validate",
                confidence=0.6,
                reason="low confidence",
                summary="unsure",
                next_steps=[],
                attempt=1,
                latency_ms=10,
                model="test",
                status="ok",
                error=None,
            )
        )
        with patch.object(main, "append_event", return_value=None):
            resolved = main._resolve_next_step(
                run,
                state,
                "execute_sql",
                success=True,
                orchestrator=orchestrator,  # type: ignore[arg-type]
            )
        self.assertEqual(resolved, "human_review")

    def test_invalid_route_falls_back_to_human_review(self) -> None:
        run = _make_run()
        state = MigrationContext(project_name="demo", session_id=run.runId, last_step_success=True)
        orchestrator = _FakeOrchestrator(
            OrchestratorDecision(
                from_step="execute_sql",
                candidate_steps=["validate", "self_heal", "human_review"],
                selected_step="finalize",
                confidence=0.99,
                reason="invalid",
                summary="invalid route",
                next_steps=[],
                attempt=1,
                latency_ms=10,
                model="test",
                status="ok",
                error=None,
            )
        )
        with patch.object(main, "append_event", return_value=None):
            resolved = main._resolve_next_step(
                run,
                state,
                "execute_sql",
                success=True,
                orchestrator=orchestrator,  # type: ignore[arg-type]
            )
        self.assertEqual(resolved, "human_review")

    def test_missing_ddl_forces_human_review_candidate(self) -> None:
        run = _make_run()
        state = MigrationContext(
            project_name="demo",
            session_id=run.runId,
            last_step_success=True,
            requires_ddl_upload=True,
        )
        orchestrator = _FakeOrchestrator(
            OrchestratorDecision(
                from_step="execute_sql",
                candidate_steps=["human_review"],
                selected_step="human_review",
                confidence=0.9,
                reason="ddl required",
                summary="pause",
                next_steps=[],
                attempt=1,
                latency_ms=10,
                model="test",
                status="ok",
                error=None,
            )
        )
        with patch.object(main, "append_event", return_value=None):
            resolved = main._resolve_next_step(
                run,
                state,
                "execute_sql",
                success=True,
                orchestrator=orchestrator,  # type: ignore[arg-type]
            )
        self.assertEqual(resolved, "human_review")
        self.assertEqual(orchestrator.contexts[0].candidate_steps, ["human_review"])

    def test_same_node_retry_is_blocked_after_one(self) -> None:
        run = _make_run()
        state = MigrationContext(
            project_name="demo",
            session_id=run.runId,
            last_step_success=False,
            node_retry_counts={"init_project": 1},
        )
        orchestrator = _FakeOrchestrator(
            OrchestratorDecision(
                from_step="init_project",
                candidate_steps=["human_review"],
                selected_step="init_project",
                confidence=0.99,
                reason="retry",
                summary="retry init",
                next_steps=[],
                attempt=1,
                latency_ms=10,
                model="test",
                status="ok",
                error=None,
            )
        )
        with patch.object(main, "append_event", return_value=None):
            resolved = main._resolve_next_step(
                run,
                state,
                "init_project",
                success=False,
                orchestrator=orchestrator,  # type: ignore[arg-type]
            )
        self.assertEqual(resolved, "human_review")
        self.assertNotIn("init_project", orchestrator.contexts[0].candidate_steps)


if __name__ == "__main__":
    unittest.main()
