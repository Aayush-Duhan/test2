from __future__ import annotations

import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from agentic_core.orchestrator import OrchestratorDecision
from agentic_core.state import MigrationContext, MigrationState
from python_execution_service import main


def _make_run(source_path: str, schema_path: str) -> main.RunRecord:
    return main.RunRecord(
        runId="run-orch-loop",
        projectId="proj-1",
        projectName="project",
        sourceId="src-1",
        schemaId="schema-1",
        sourceLanguage="teradata",
        sourcePath=source_path,
        schemaPath=schema_path,
        sfAccount=None,
        sfUser=None,
        sfRole=None,
        sfWarehouse=None,
        sfDatabase=None,
        sfSchema=None,
        sfAuthenticator=None,
        status="queued",
        createdAt=main.now_iso(),
        updatedAt=main.now_iso(),
        outputDir=".",
        steps=main.get_steps_template(),
    )


def _node_setter(target_stage: MigrationState):
    def _impl(state: MigrationContext) -> MigrationContext:
        state.current_stage = target_stage
        return state
    return _impl


class ExecuteRunOrchestrationTests(unittest.TestCase):
    def test_orchestrator_emits_decision_for_each_transition_including_finalize(self) -> None:
        source = Path("source.sql")
        schema = Path("mapping.csv")
        source.write_text("select 1;", encoding="utf-8")
        schema.write_text("a,b", encoding="utf-8")

        run = _make_run(str(source.resolve()), str(schema.resolve()))
        recorded_events: list[tuple[str, dict]] = []

        transition_map = {
            "init_project": "add_source_code",
            "add_source_code": "apply_schema_mapping",
            "apply_schema_mapping": "convert_code",
            "convert_code": "execute_sql",
            "execute_sql": "validate",
            "validate": "finalize",
            "finalize": "END",
        }

        def decide_side_effect(_state: MigrationContext, context):  # noqa: ANN001
            selected = transition_map.get(context.from_step, "human_review")
            return OrchestratorDecision(
                from_step=context.from_step,
                candidate_steps=list(context.candidate_steps),
                selected_step=selected,
                confidence=0.95,
                reason="deterministic test",
                summary=f"{context.from_step} -> {selected}",
                next_steps=[],
                attempt=1,
                latency_ms=3,
                model="test-model",
                status="ok",
                error=None,
            )

        def append_event_side_effect(_run: main.RunRecord, event_type: str, payload: dict) -> None:
            recorded_events.append((event_type, payload))

        original_runs = dict(main.RUNS)
        original_locks = dict(main.PROJECT_LOCKS)
        original_flags = dict(main.CANCEL_FLAGS)
        main.RUNS[run.runId] = run
        main.PROJECT_LOCKS[run.projectId] = run.runId
        main.CANCEL_FLAGS[run.runId] = threading.Event()

        try:
            with patch.object(main, "persist_runs_locked", return_value=None):
                with patch.object(main.sqlite_store, "append_run_event", return_value=None):
                    with patch.object(main.sqlite_store, "append_run_message", return_value=None):
                        with patch.object(main.sqlite_store, "append_run_log", return_value=None):
                            with patch.object(main, "append_event", side_effect=append_event_side_effect):
                                with patch.object(main.SnowflakeCortexOrchestrator, "decide", side_effect=decide_side_effect):
                                    with patch.object(main, "init_project_node", side_effect=_node_setter(MigrationState.INIT_PROJECT)):
                                        with patch.object(main, "add_source_code_node", side_effect=_node_setter(MigrationState.ADD_SOURCE_CODE)):
                                            with patch.object(main, "apply_schema_mapping_node", side_effect=_node_setter(MigrationState.APPLY_SCHEMA_MAPPING)):
                                                with patch.object(main, "convert_code_node", side_effect=_node_setter(MigrationState.CONVERT_CODE)):
                                                    with patch.object(main, "execute_sql_node", side_effect=_node_setter(MigrationState.EXECUTE_SQL)):
                                                        with patch.object(main, "validate_node", side_effect=_node_setter(MigrationState.VALIDATE)):
                                                            with patch.object(main, "self_heal_node", side_effect=_node_setter(MigrationState.SELF_HEAL)):
                                                                with patch.object(main, "human_review_node", side_effect=_node_setter(MigrationState.HUMAN_REVIEW)):
                                                                    with patch.object(main, "finalize_node", side_effect=_node_setter(MigrationState.COMPLETED)):
                                                                        main.execute_run_sync(run.runId)
        finally:
            main.RUNS.clear()
            main.RUNS.update(original_runs)
            main.PROJECT_LOCKS.clear()
            main.PROJECT_LOCKS.update(original_locks)
            main.CANCEL_FLAGS.clear()
            main.CANCEL_FLAGS.update(original_flags)
            try:
                source.unlink()
            except Exception:
                pass
            try:
                schema.unlink()
            except Exception:
                pass

        orchestrator_events = [payload for kind, payload in recorded_events if kind == "orchestrator:decision"]
        self.assertGreaterEqual(len(orchestrator_events), 7)
        self.assertTrue(any(ev.get("from_step") == "finalize" for ev in orchestrator_events))
        self.assertTrue(any(ev.get("resolved_step") == "END" for ev in orchestrator_events))
        self.assertTrue(any(kind == "run:completed" for kind, _ in recorded_events))


if __name__ == "__main__":
    unittest.main()
