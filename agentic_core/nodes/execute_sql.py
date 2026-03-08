"""Execute SQL workflow node."""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.runtime.snowflake_execution import (
    build_snowflake_connection,
    classify_snowflake_error,
    close_connection,
    execute_sql_statements,
)
from agentic_core.utils.activity_log import log_event
from agentic_core.utils.sql_files import list_sql_files

logger = logging.getLogger(__name__)


def apply_uploaded_ddl_and_resume(state: MigrationContext) -> MigrationContext:
    """Apply uploaded DDL script and prepare workflow to resume execute_sql."""
    if not state.ddl_upload_path or not os.path.exists(state.ddl_upload_path):
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.human_intervention_reason = "DDL upload is required to resolve missing objects."
        log_event(state, "warning", "DDL upload path missing for resume")
        return state

    try:
        with open(state.ddl_upload_path, "r", encoding="utf-8-sig") as ddl_file:
            ddl_sql = ddl_file.read()

        if not ddl_sql.strip():
            state.current_stage = MigrationState.HUMAN_REVIEW
            state.requires_human_intervention = True
            state.human_intervention_reason = "Uploaded DDL file is empty."
            log_event(state, "warning", "Uploaded DDL file is empty")
            return state

        connection = build_snowflake_connection(state)
        try:
            execute_sql_statements(connection, ddl_sql)
        finally:
            close_connection(connection)

        state.requires_ddl_upload = False
        state.ddl_upload_path = ""
        state.resume_from_stage = "execute_sql"
        state.requires_human_intervention = False
        state.human_intervention_reason = ""
        log_event(state, "info", "Uploaded DDL executed successfully, resuming SQL execution")
        return state
    except Exception as exc:
        error_msg = f"Failed to execute uploaded DDL: {exc}"
        state.errors.append(error_msg)
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.requires_ddl_upload = True
        state.human_intervention_reason = error_msg
        log_event(state, "error", error_msg)
        return state


def execute_sql_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Executing converted SQL for project: %s", state.project_name)
    state.current_stage = MigrationState.EXECUTE_SQL
    state.updated_at = datetime.now()
    log_event(state, "info", "Executing converted SQL")

    if state.requires_ddl_upload:
        state = apply_uploaded_ddl_and_resume(state)
        if state.requires_ddl_upload:
            return state

    converted_dir = os.path.join(state.project_path, "converted")
    sql_files = list_sql_files(converted_dir)
    on_statement = getattr(state, "execution_event_sink", None)

    try:
        connection = build_snowflake_connection(state)
        try:
            if sql_files:
                start_index = max(0, state.last_executed_file_index + 1)
                for index in range(start_index, len(sql_files)):
                    sql_file = sql_files[index]
                    with open(sql_file, "r", encoding="utf-8-sig") as file_handle:
                        sql_text = file_handle.read()
                    if not sql_text.strip():
                        state.execution_log.append({"file": sql_file, "index": index, "status": "skipped_empty"})
                        state.last_executed_file_index = index
                        continue

                    def file_statement_sink(
                        entry: Dict[str, Any],
                        file_path: str = sql_file,
                        file_index: int = index,
                    ) -> None:
                        if callable(on_statement):
                            on_statement({**entry, "file": file_path, "fileIndex": file_index})

                    statement_results = execute_sql_statements(
                        connection,
                        sql_text,
                        on_statement=file_statement_sink,
                    )
                    state.execution_log.append(
                        {
                            "file": sql_file,
                            "index": index,
                            "status": "success",
                            "statements": statement_results,
                        }
                    )
                    state.last_executed_file_index = index
            elif state.converted_code.strip():
                def mem_statement_sink(entry: Dict[str, Any]) -> None:
                    if callable(on_statement):
                        on_statement({**entry, "file": "in_memory_converted_code", "fileIndex": 0})

                statement_results = execute_sql_statements(
                    connection,
                    state.converted_code,
                    on_statement=mem_statement_sink,
                )
                state.execution_log.append(
                    {
                        "file": "in_memory_converted_code",
                        "index": 0,
                        "status": "success",
                        "statements": statement_results,
                    }
                )
                state.last_executed_file_index = 0
            else:
                raise ValueError("No converted SQL files or converted_code found for execution.")
        finally:
            close_connection(connection)

        state.execution_passed = True
        state.execution_errors = []
        state.missing_objects = []
        state.validation_issues = []
        state.updated_at = datetime.now()
        log_event(state, "info", "Converted SQL execution completed successfully")
        return state
    except Exception as exc:
        error_message = str(exc)
        error_type, object_name = ("execution_error", "")
        failed_statement = ""
        failed_statement_index = -1
        partial_results: List[Dict[str, Any]] = []

        if hasattr(exc, "statement"):
            failed_statement = str(getattr(exc, "statement", ""))
            failed_statement_index = int(getattr(exc, "statement_index", -1))
            partial_results = list(getattr(exc, "partial_results", []) or [])
        try:
            error_type, object_name = classify_snowflake_error(error_message)
        except Exception:
            pass

        state.execution_passed = False
        state.execution_errors.append(
            {
                "type": error_type,
                "message": error_message,
                "object_name": object_name,
                "stage": "execute_sql",
                "statement": failed_statement,
                "statement_index": failed_statement_index,
            }
        )
        state.execution_log.append(
            {
                "file": sql_files[state.last_executed_file_index + 1]
                if sql_files and state.last_executed_file_index + 1 < len(sql_files)
                else "unknown",
                "index": state.last_executed_file_index + 1,
                "status": "failed",
                "error_type": error_type,
                "error_message": error_message,
                "missing_object": object_name,
                "statements": partial_results,
                "failed_statement": failed_statement,
                "failed_statement_index": failed_statement_index,
            }
        )

        if error_type == "missing_object":
            if object_name:
                normalized_obj = object_name.strip()
                if normalized_obj and normalized_obj not in state.missing_objects:
                    state.missing_objects.append(normalized_obj)
            state.requires_ddl_upload = True
            state.requires_human_intervention = True
            state.resume_from_stage = "execute_sql"
            state.current_stage = MigrationState.HUMAN_REVIEW
            missing_detail = ", ".join(state.missing_objects) if state.missing_objects else "unresolved object"
            state.human_intervention_reason = (
                f"Missing object detected during execution: {missing_detail}. "
                "Upload DDL script to create required objects, then resume."
            )
            log_event(state, "warning", state.human_intervention_reason)
            state.updated_at = datetime.now()
            return state

        state.validation_issues.append(
            {"type": "execution_error", "severity": "error", "message": error_message}
        )
        log_event(state, "error", f"Execution failed, routing to self-heal: {error_message}")
        state.updated_at = datetime.now()
        return state
