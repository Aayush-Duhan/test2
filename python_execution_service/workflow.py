"""LangGraph agent-driven workflow execution."""

import logging
import threading
import time
from pathlib import Path
from typing import Any

from agentic_core.agent.graph import run_agent_loop
from agentic_core.agent.tools import get_active_context
from agentic_core.models.context import MigrationContext, MigrationState
from python_execution_service.config import (
    PROJECT_LOCKS,
    RUN_LOCK,
    RUNS,
    STEP_LABELS,
)
from python_execution_service.helpers import (
    add_log,
    append_chat_message,
    append_run_status_part,
    append_sql_error_part,
    append_sql_statement_part,
    append_step_status_part,
    append_terminal_output,
    append_tool_call_part,
    ensure_not_canceled,
    flush_persist_if_dirty,
    format_activity_log_entry,
    persist_runs_locked,
    pop_user_message,
    set_run_status,
    update_step,
)
from python_execution_service.models import RunRecord

logger = logging.getLogger(__name__)


def _perf_log(run: RunRecord, message: str) -> None:
    """Append a timestamped line to the run's performance.txt."""
    try:
        perf_file = Path(run.outputDir) / "performance.txt"
        perf_file.parent.mkdir(parents=True, exist_ok=True)
        with perf_file.open("a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%H:%M:%S')}] {message}\n")
    except Exception:
        pass


def execute_run_sync(run_id: str, *, is_follow_up_chat: bool = False) -> None:  # noqa: C901
    """Execute a migration run using the autonomous agent.

    This replaces the old rigid pipeline with an LLM-driven agent that
    decides which migration tool to call next, handles errors
    autonomously, and can respond to user messages during execution.
    """
    run_t0 = time.monotonic()
    with RUN_LOCK:
        run = RUNS[run_id]
        resume_ddl_path = run.ddlUploadPath
        resume_from_stage = run.resumeFromStage or "execute_sql"
        resume_missing_objects = list(run.missingObjects)
        resume_last_executed_file_index = int(run.lastExecutedFileIndex)
    _perf_log(run, f"RUN_START   run_id={run_id}")



    try:
        set_run_status(run, "running")
        if not is_follow_up_chat:
            append_run_status_part(run, "running")

        # ── Sink callbacks (identical to the old workflow) ──────

        def activity_log_sink(entry: dict[str, Any]) -> None:
            formatted = format_activity_log_entry(entry)
            if formatted:
                stage = entry.get("stage")
                step_id = stage if isinstance(stage, str) and stage in STEP_LABELS else None
                is_progress = bool(entry.get("data", {}).get("is_progress"))
                add_log(run, formatted, step_id=step_id, is_progress=is_progress)

        def terminal_output_sink(text: str, is_progress: bool = False) -> None:
            stage = context.current_stage.value if context.current_stage else None
            step_id = stage if isinstance(stage, str) and stage in STEP_LABELS else None
            append_terminal_output(run, text, is_progress=is_progress, step_id=step_id)



        def sync_execution_state(updated: MigrationContext) -> None:
            previous_error_count = len(run.executionErrors)
            with RUN_LOCK:
                run.executionLog = updated.execution_log or []
                run.executionErrors = updated.execution_errors or []
                run.missingObjects = updated.missing_objects or []
                run.requiresDdlUpload = bool(updated.requires_ddl_upload)
                run.resumeFromStage = updated.resume_from_stage or ""
                run.lastExecutedFileIndex = int(updated.last_executed_file_index)
                run.ddlUploadPath = updated.ddl_upload_path or ""
                persist_runs_locked()
            for error_entry in run.executionErrors[previous_error_count:]:
                if isinstance(error_entry, dict):
                    append_sql_error_part(run, {"runId": run_id, **error_entry})

        _streamed_stmt_lock = threading.Lock()
        _streamed_stmt_count = 0

        def realtime_execution_event_sink(entry: dict[str, Any]) -> None:
            """Called from inside execute_sql_statements for EACH completed statement."""
            nonlocal _streamed_stmt_count
            stmt_index = entry.get("statement_index")
            output_preview = entry.get("output_preview", [])

            statement_payload = {
                "runId": run_id,
                "file": entry.get("file"),
                "fileIndex": entry.get("fileIndex"),
                "statementIndex": stmt_index,
                "statement": entry.get("statement"),
                "status": entry.get("status"),
                "rowCount": entry.get("row_count", 0),
                "outputPreview": output_preview,
            }
            append_sql_statement_part(run, statement_payload)
            with _streamed_stmt_lock:
                _streamed_stmt_count += 1

        # ── Build migration context ───────────────────────────

        if is_follow_up_chat:
            try:
                context = get_active_context(run_id)
            except Exception:
                context = MigrationContext(
                    project_name=run.projectName,
                    source_language=run.sourceLanguage.lower(),
                    source_directory=str(Path(run.sourcePath).resolve().parent),
                    source_files=[run.sourcePath],
                    mapping_csv_path=run.schemaPath,
                    sf_account=run.sfAccount or "",
                    sf_user=run.sfUser or "",
                    sf_role=run.sfRole or "",
                    sf_warehouse=run.sfWarehouse or "",
                    sf_database=run.sfDatabase or "",
                    sf_schema=run.sfSchema or "",
                    sf_authenticator=run.sfAuthenticator or "externalbrowser",
                    session_id=run_id,
                )
        else:
            context = MigrationContext(
                project_name=run.projectName,
                source_language=run.sourceLanguage.lower(),
                source_directory=str(Path(run.sourcePath).resolve().parent),
                source_files=[run.sourcePath],
                mapping_csv_path=run.schemaPath,
                sf_account=run.sfAccount or "",
                sf_user=run.sfUser or "",
                sf_role=run.sfRole or "",
                sf_warehouse=run.sfWarehouse or "",
                sf_database=run.sfDatabase or "",
                sf_schema=run.sfSchema or "",
                sf_authenticator=run.sfAuthenticator or "externalbrowser",
                session_id=run_id,
            )

        context.project_name = run.projectName
        context.source_language = run.sourceLanguage.lower()
        context.source_directory = str(Path(run.sourcePath).resolve().parent)
        context.source_files = [run.sourcePath]
        context.mapping_csv_path = run.schemaPath
        context.activity_log_sink = activity_log_sink
        context.execution_event_sink = realtime_execution_event_sink
        context.terminal_output_sink = terminal_output_sink

        context.sf_account = run.sfAccount or ""
        context.sf_user = run.sfUser or ""
        context.sf_role = run.sfRole or ""
        context.sf_warehouse = run.sfWarehouse or ""
        context.sf_database = run.sfDatabase or ""
        context.sf_schema = run.sfSchema or ""
        context.sf_authenticator = run.sfAuthenticator or "externalbrowser"
        context.session_id = run_id

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

        # ── Agent callbacks ────────────────────────────────────

        def message_callback(event: dict[str, Any]) -> None:
            """Stream agent messages to the frontend in real time."""
            event_type = str(event.get("type", "message"))
            if event_type == "message":
                append_chat_message(
                    run,
                    role=str(event.get("role", "agent")),
                    kind=str(event.get("kind", "agent_response")),
                    content=str(event.get("content", "")),
                )
                return

            if event_type == "tool-call":
                if event.get("output", "") == "":
                    return
                append_tool_call_part(
                    run,
                    tool_name=str(event.get("toolName", "tool")),
                    tool_input=event.get("input", {}) if isinstance(event.get("input"), dict) else {},
                    output=event.get("output", ""),
                    tool_call_id=str(event.get("toolCallId")) if event.get("toolCallId") else None,
                )
                return

        def step_callback(step_id: str, status: str) -> None:
            """Update step progress in the UI."""
            ensure_not_canceled(run_id)
            update_step(run, step_id, status)
            append_step_status_part(run, step_id, status)
            if status in ("completed", "failed"):
                # After execute_sql, sync execution state
                try:
                    updated_ctx = get_active_context(run_id)
                    sync_execution_state(updated_ctx)
                except Exception:
                    pass

        def user_message_getter() -> str | None:
            """Check for pending user messages."""
            ensure_not_canceled(run_id)
            return pop_user_message(run_id)

        def conversation_callback(history: list[dict[str, str]]) -> None:
            with RUN_LOCK:
                run.conversationHistory = history
                persist_runs_locked()

        # ── Run the agent loop ─────────────────────────────────

        _perf_log(run, "AGENT_RUN_START")
        run_agent_loop(
            context,
            message_callback=message_callback,
            step_callback=step_callback,
            user_message_getter=user_message_getter,
            conversation_history=list(run.conversationHistory),
            conversation_callback=conversation_callback,
            consume_user_messages_from_start=is_follow_up_chat,
            start_with_migration_prompt=not is_follow_up_chat,
        )
        _perf_log(run, "AGENT_RUN_END")

        # ── Process final state ────────────────────────────────

        final_context = get_active_context(run_id)

        # Sync final execution state
        sync_execution_state(final_context)

        if final_context.current_stage == MigrationState.COMPLETED:
            set_run_status(run, "completed")
            append_run_status_part(run, "completed")
            return

        if final_context.requires_ddl_upload:
            reason = final_context.human_intervention_reason or "DDL upload required"
            set_run_status(run, "failed", reason)
            with RUN_LOCK:
                run.requiresDdlUpload = True
                run.missingObjects = final_context.missing_objects or []
                run.resumeFromStage = final_context.resume_from_stage or "execute_sql"
                run.lastExecutedFileIndex = int(final_context.last_executed_file_index)
                persist_runs_locked(force=True)
            append_run_status_part(run, "failed", reason)
            return

        reason = final_context.human_intervention_reason or "Migration stopped before completion"
        set_run_status(run, "failed", reason)
        append_run_status_part(run, "failed", reason)

    except Exception as exc:
        message = str(exc)
        canceled = message == "Run canceled"
        status = "canceled" if canceled else "failed"
        set_run_status(run, status, message)
        append_run_status_part(run, status, message)
    finally:

        total_elapsed = time.monotonic() - run_t0
        _perf_log(run, f"RUN_END     run_id={run_id}  total_elapsed={total_elapsed:.3f}s  status={run.status}")
        flush_persist_if_dirty()
        with RUN_LOCK:
            if PROJECT_LOCKS.get(run.projectId) == run_id:
                del PROJECT_LOCKS[run.projectId]
