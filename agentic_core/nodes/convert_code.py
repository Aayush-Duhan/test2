"""Convert code workflow node."""

import logging
import os
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.services.report_context import build_report_context_memory
from agentic_core.services.scai_runner import run_scai_command
from agentic_core.utils.activity_log import log_event
from agentic_core.utils.sql_files import list_sql_files, read_sql_files

logger = logging.getLogger(__name__)


def convert_code_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Converting code for project: %s", state.project_name)
    log_event(state, "info", f"Converting code for project: {state.project_name}")

    try:
        cmd = ["scai", "code", "convert"]
        terminal_sink = getattr(state, "terminal_output_sink", None)

        return_code, stdout, stderr = run_scai_command(
            cmd,
            state.project_path,
            terminal_callback=terminal_sink,
        )
        if stderr:
            log_event(state, "warning", "scai code convert stderr", {"stderr": stderr})

        if return_code != 0:
            error_detail = stderr or stdout or "Unknown error"
            error_msg = f"Failed to convert code: {error_detail}"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_converted = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.scai_converted = True
        state.current_stage = MigrationState.CONVERT_CODE
        state.updated_at = datetime.now()
        logger.info("Code conversion completed successfully")
        log_event(state, "info", "Code conversion completed successfully")

        converted_dir = os.path.join(state.project_path, "converted")
        state.converted_files = list_sql_files(converted_dir)
        state.converted_code = read_sql_files(converted_dir)
        if not state.converted_code:
            state.converted_code = state.schema_mapped_code or state.original_code or ""
            if state.converted_code:
                warning_msg = "Converted output files not found; using in-memory SQL content."
                state.warnings.append(warning_msg)
                log_event(state, "warning", warning_msg)

        report_context = build_report_context_memory(state)
        state.report_context = report_context
        state.ignored_report_codes = report_context.get("ignored_codes", [])
        state.report_scan_summary = report_context.get("report_scan_summary", {})
    except Exception as exc:
        error_msg = f"Exception during code conversion: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_converted = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
