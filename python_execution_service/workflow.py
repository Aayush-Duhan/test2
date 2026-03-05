"""LangGraph workflow construction and synchronous run execution."""

import json
import logging
from pathlib import Path
from typing import Any

from langgraph.graph import END, START, StateGraph

from agentic_core.decision import should_continue, should_continue_after_execute
from agentic_core.nodes import (
    add_source_code_node,
    apply_schema_mapping_node,
    convert_code_node,
    execute_sql_node,
    finalize_node,
    human_review_node,
    init_project_node,
    self_heal_node,
    validate_node,
)
from agentic_core.state import MigrationContext, MigrationState
from python_execution_service.config import (
    PROJECT_LOCKS,
    RUN_LOCK,
    RUNS,
    STEP_LABELS,
)
from python_execution_service.helpers import (
    add_log,
    append_chat_message,
    append_event,
    ensure_not_canceled,
    format_activity_log_entry,
    persist_runs_locked,
    set_run_status,
    update_step,
)
from python_execution_service.models import RunRecord, WorkflowState

logger = logging.getLogger(__name__)


def run_node(
    run: RunRecord,
    run_id: str,
    state: MigrationContext,
    step_id: str,
    node_fn,
) -> MigrationContext:
    ensure_not_canceled(run_id)
    update_step(run, step_id, "running")
    append_event(
        run,
        "step:started",
        {"runId": run_id, "stepId": step_id, "label": STEP_LABELS[step_id]},
    )
    append_chat_message(
        run,
        role="system",
        kind="step_started",
        content=f"Starting: {STEP_LABELS[step_id]}",
        step={"id": step_id, "label": STEP_LABELS[step_id]},
    )
    state = node_fn(state)
    if state.current_stage == MigrationState.ERROR:
        update_step(run, step_id, "failed")
        raise RuntimeError(state.errors[-1] if state.errors else f"Step failed: {step_id}")
    update_step(run, step_id, "completed")
    append_event(
        run,
        "step:completed",
        {"runId": run_id, "stepId": step_id, "label": STEP_LABELS[step_id]},
    )
    append_chat_message(
        run,
        role="system",
        kind="step_completed",
        content=f"Completed: {STEP_LABELS[step_id]}",
        step={"id": step_id, "label": STEP_LABELS[step_id]},
    )
    return state


def execute_run_sync(run_id: str) -> None:  # noqa: C901
    with RUN_LOCK:
        run = RUNS[run_id]
        resume_ddl_path = run.ddlUploadPath
        resume_from_stage = run.resumeFromStage or "execute_sql"
        resume_missing_objects = list(run.missingObjects)
        resume_last_executed_file_index = int(run.lastExecutedFileIndex)
    try:
        set_run_status(run, "running")
        append_event(run, "run:started", {"runId": run_id})
        append_chat_message(
            run,
            role="system",
            kind="run_status",
            content="Migration started.",
        )

        def activity_log_sink(entry: dict[str, Any]) -> None:
            formatted = format_activity_log_entry(entry)
            if formatted:
                stage = entry.get("stage")
                step_id = stage if isinstance(stage, str) and stage in STEP_LABELS else None
                add_log(run, formatted, step_id=step_id)

        context = MigrationContext(
            project_name=run.projectName,
            source_language=run.sourceLanguage.lower(),
            source_directory=str(Path(run.sourcePath).resolve().parent),
            source_files=[run.sourcePath],
            mapping_csv_path=run.schemaPath,
            activity_log_sink=activity_log_sink,
            sf_account=run.sfAccount or "",
            sf_user=run.sfUser or "",
            sf_role=run.sfRole or "",
            sf_warehouse=run.sfWarehouse or "",
            sf_database=run.sfDatabase or "",
            sf_schema=run.sfSchema or "",
            sf_authenticator=run.sfAuthenticator or "externalbrowser",
            session_id=run_id,
        )

        if resume_ddl_path:
            context.requires_ddl_upload = True
            context.ddl_upload_path = resume_ddl_path
            context.resume_from_stage = resume_from_stage
            context.last_executed_file_index = max(-1, resume_last_executed_file_index)
            context.missing_objects = resume_missing_objects
            add_log(
                run,
                f"Applying uploaded DDL ({Path(resume_ddl_path).name}) before resuming execute_sql.",
            )

        def sync_execution_state(updated: MigrationContext) -> None:
            with RUN_LOCK:
                run.executionLog = updated.execution_log or []
                run.executionErrors = updated.execution_errors or []
                run.missingObjects = updated.missing_objects or []
                run.requiresDdlUpload = bool(updated.requires_ddl_upload)
                run.resumeFromStage = updated.resume_from_stage or ""
                run.lastExecutedFileIndex = int(updated.last_executed_file_index)
                run.ddlUploadPath = updated.ddl_upload_path or ""
                persist_runs_locked()

        def emit_execute_events(updated: MigrationContext) -> None:
            execution_log = updated.execution_log or []
            with RUN_LOCK:
                start_index = max(0, int(run.executionEventCursor))
            new_entries = execution_log[start_index:]

            for file_entry in new_entries:
                for statement_entry in file_entry.get("statements", []):
                    append_event(
                        run,
                        "execute_sql:statement",
                        {
                            "runId": run_id,
                            "file": file_entry.get("file"),
                            "fileIndex": file_entry.get("index"),
                            "statementIndex": statement_entry.get("statement_index"),
                            "statement": statement_entry.get("statement"),
                            "status": statement_entry.get("status"),
                            "rowCount": statement_entry.get("row_count", 0),
                            "outputPreview": statement_entry.get("output_preview", []),
                        },
                    )
                    statement_index = statement_entry.get("statement_index")
                    label = f"Stmt {int(statement_index) + 1}" if isinstance(statement_index, int) else "Stmt ?"
                    output_preview = statement_entry.get("output_preview", [])
                    output_text = ""
                    if isinstance(output_preview, list) and output_preview:
                        try:
                            output_text = json.dumps(output_preview, ensure_ascii=False, default=str, indent=2)
                        except Exception:
                            output_text = str(output_preview)
                    append_chat_message(
                        run,
                        role="agent",
                        kind="sql_statement",
                        content=label,
                        step={"id": "execute_sql", "label": STEP_LABELS["execute_sql"]},
                        sql={
                            "statement": str(statement_entry.get("statement") or ""),
                            "output": output_text,
                        },
                    )
                if file_entry.get("status") == "failed":
                    append_event(
                        run,
                        "execute_sql:error",
                        {
                            "runId": run_id,
                            "file": file_entry.get("file"),
                            "fileIndex": file_entry.get("index"),
                            "errorType": file_entry.get("error_type"),
                            "errorMessage": file_entry.get("error_message"),
                            "failedStatement": file_entry.get("failed_statement"),
                            "failedStatementIndex": file_entry.get("failed_statement_index"),
                        },
                    )
                    failed_statement_index = file_entry.get("failed_statement_index")
                    label = (
                        f"Stmt {int(failed_statement_index) + 1} ERROR"
                        if isinstance(failed_statement_index, int)
                        else "Stmt ? ERROR"
                    )
                    append_chat_message(
                        run,
                        role="error",
                        kind="sql_error",
                        content=label,
                        step={"id": "execute_sql", "label": STEP_LABELS["execute_sql"]},
                        sql={
                            "error": str(file_entry.get("error_message") or ""),
                            "failedStatement": str(file_entry.get("failed_statement") or ""),
                            "output": f"Error type: {str(file_entry.get('error_type') or 'execution_error')}",
                        },
                    )
            with RUN_LOCK:
                run.executionEventCursor = len(execution_log)
                persist_runs_locked()

        def sync_validation_state(updated: MigrationContext) -> None:
            with RUN_LOCK:
                run.validationIssues = updated.validation_issues or []
                persist_runs_locked()
            for issue in updated.validation_issues:
                append_event(run, "validation:issue", issue if isinstance(issue, dict) else {"message": str(issue)})

        def sync_self_heal_state(updated: MigrationContext) -> None:
            with RUN_LOCK:
                run.selfHealIteration = int(updated.self_heal_iteration)
                persist_runs_locked()
            append_event(
                run,
                "selfheal:iteration",
                {"iteration": updated.self_heal_iteration, "runId": run_id},
            )
            add_log(run, f"Self-heal iteration {updated.self_heal_iteration} applied; re-running execute_sql.")

        def wrap_node(step_id: str, node_fn, *, success_log: str | None = None, post_hook=None):
            def _wrapped(state_obj: WorkflowState) -> WorkflowState:
                updated = run_node(run, run_id, state_obj["context"], step_id, node_fn)
                if success_log:
                    add_log(run, success_log)
                if callable(post_hook):
                    post_hook(updated)
                return {"context": updated}
            return _wrapped

        def route_after_execute(state_obj: WorkflowState) -> str:
            current = state_obj["context"]
            decision = should_continue_after_execute(current)
            if decision == "self_heal" and current.self_heal_iteration >= current.max_self_heal_iterations:
                current.requires_human_intervention = True
                current.human_intervention_reason = (
                    f"Could not resolve execution errors after {current.max_self_heal_iterations} self-heal iteration(s)."
                )
                return "human_review"
            if decision == "finalize":
                return "validate"
            return decision

        def route_after_validate(state_obj: WorkflowState) -> str:
            current = state_obj["context"]
            decision = should_continue(current)
            if decision == "finalize":
                return "finalize"
            if not current.human_intervention_reason:
                current.human_intervention_reason = "Validation failed after execution"
            return "human_review"

        graph_builder = StateGraph(WorkflowState)
        graph_builder.add_node("init_project", wrap_node("init_project", init_project_node, success_log="Project initialized."))
        graph_builder.add_node("add_source_code", wrap_node("add_source_code", add_source_code_node, success_log="Source code added."))
        graph_builder.add_node(
            "apply_schema_mapping",
            wrap_node("apply_schema_mapping", apply_schema_mapping_node, success_log="Schema mapping applied."),
        )
        graph_builder.add_node("convert_code", wrap_node("convert_code", convert_code_node, success_log="Code conversion complete."))
        graph_builder.add_node(
            "execute_sql",
            wrap_node(
                "execute_sql",
                execute_sql_node,
                success_log="Execute SQL stage completed.",
                post_hook=lambda updated: (sync_execution_state(updated), emit_execute_events(updated)),
            ),
        )
        graph_builder.add_node(
            "self_heal",
            wrap_node("self_heal", self_heal_node, post_hook=sync_self_heal_state),
        )
        graph_builder.add_node(
            "validate",
            wrap_node("validate", validate_node, post_hook=sync_validation_state),
        )
        graph_builder.add_node("human_review", wrap_node("human_review", human_review_node))
        graph_builder.add_node("finalize", wrap_node("finalize", finalize_node))

        graph_builder.add_edge(START, "init_project")
        graph_builder.add_edge("init_project", "add_source_code")
        graph_builder.add_edge("add_source_code", "apply_schema_mapping")
        graph_builder.add_edge("apply_schema_mapping", "convert_code")
        graph_builder.add_edge("convert_code", "execute_sql")
        graph_builder.add_conditional_edges(
            "execute_sql",
            route_after_execute,
            {
                "validate": "validate",
                "self_heal": "self_heal",
                "human_review": "human_review",
            },
        )
        graph_builder.add_edge("self_heal", "execute_sql")
        graph_builder.add_conditional_edges(
            "validate",
            route_after_validate,
            {
                "finalize": "finalize",
                "human_review": "human_review",
            },
        )
        graph_builder.add_edge("human_review", END)
        graph_builder.add_edge("finalize", END)

        workflow = graph_builder.compile()
        final_state = workflow.invoke({"context": context})
        final_context = final_state["context"]

        if final_context.current_stage == MigrationState.COMPLETED:
            set_run_status(run, "completed")
            append_event(run, "run:completed", {"runId": run_id})
            append_chat_message(
                run,
                role="system",
                kind="run_status",
                content="Migration completed.",
            )
            return

        reason = final_context.human_intervention_reason or "Migration stopped before completion"
        set_run_status(run, "failed", reason)
        append_event(run, "run:failed", {"runId": run_id, "reason": reason})
        if final_context.current_stage == MigrationState.HUMAN_REVIEW:
            add_log(run, reason, step_id="human_review")
    except Exception as exc:
        message = str(exc)
        canceled = message == "Run canceled"
        status = "canceled" if canceled else "failed"
        set_run_status(run, status, message)
        append_event(run, "run:failed", {"runId": run_id, "reason": message})
        append_chat_message(
            run,
            role="error",
            kind="run_status",
            content=message or "Run failed",
        )
    finally:
        with RUN_LOCK:
            if PROJECT_LOCKS.get(run.projectId) == run_id:
                del PROJECT_LOCKS[run.projectId]
