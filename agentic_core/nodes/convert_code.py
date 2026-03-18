"""Convert code workflow node."""

import logging
import os
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.services.scai_runner import run_scai_command
from agentic_core.services.ewi_cleanup import clean_ewi_from_file, clean_ewi_markers
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

        # --- Auto-clean EWI markers from converted files ---
        ewi_cleaned_count = 0
        for file_path in state.converted_files:
            if clean_ewi_from_file(file_path):
                ewi_cleaned_count += 1
        if ewi_cleaned_count:
            log_event(state, "info", f"Cleaned EWI markers from {ewi_cleaned_count} file(s)")

        # Read the cleaned files into memory
        state.converted_code = read_sql_files(converted_dir)
        if not state.converted_code:
            state.converted_code = state.schema_mapped_code or state.original_code or ""
            if state.converted_code:
                warning_msg = "Converted output files not found; using in-memory SQL content."
                state.warnings.append(warning_msg)
                log_event(state, "warning", warning_msg)
            # Also clean in-memory code if loaded from fallback
            state.converted_code = clean_ewi_markers(state.converted_code)


    except Exception as exc:
        error_msg = f"Exception during code conversion: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_converted = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
