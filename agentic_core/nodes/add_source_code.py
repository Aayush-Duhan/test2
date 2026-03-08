"""Add source code workflow node."""

import logging
import os
import shutil
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.services.scai_runner import run_scai_command
from agentic_core.utils.activity_log import log_event
from agentic_core.utils.sql_files import read_sql_files

logger = logging.getLogger(__name__)


def add_source_code_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Adding source code for project: %s", state.project_name)
    log_event(state, "info", f"Adding source code for project: {state.project_name}")

    try:
        source_dir = os.path.join(state.project_path, "source")
        source_dir_abs = os.path.abspath(source_dir)

        source_input = state.source_directory or (state.source_files[0] if state.source_files else "")
        if not source_input:
            error_msg = "No source directory provided for code add"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        source_input_abs = os.path.abspath(source_input)
        if os.path.isfile(source_input_abs):
            source_input_abs = os.path.dirname(source_input_abs)

        if not os.path.isdir(source_input_abs):
            fallback_dir = source_dir_abs
            os.makedirs(fallback_dir, exist_ok=True)
            warning_msg = (
                f"Source directory does not exist: {source_input_abs}. "
                f"Using fallback directory: {fallback_dir}"
            )
            logger.warning(warning_msg)
            state.warnings.append(warning_msg)
            log_event(state, "warning", warning_msg)
            source_input_abs = fallback_dir

        if os.path.isdir(source_dir_abs):
            shutil.rmtree(source_dir_abs)

        cmd = ["scai", "code", "add", "-i", source_input_abs]

        def line_sink(line: str, is_progress: bool = False) -> None:
            log_event(state, "info", line, {"is_progress": is_progress} if is_progress else None)

        return_code, stdout, stderr = run_scai_command(cmd, state.project_path, line_callback=line_sink)
        if stderr:
            log_event(state, "warning", "scai code add stderr", {"stderr": stderr})

        if return_code != 0:
            error_detail = stderr or stdout or "Unknown error"
            error_msg = f"Failed to add source code: {error_detail}"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_source_added = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.scai_source_added = True
        state.current_stage = MigrationState.ADD_SOURCE_CODE
        state.updated_at = datetime.now()
        logger.info("Source code added successfully")
        log_event(state, "info", "Source code added successfully")

        if not state.original_code:
            state.original_code = read_sql_files(source_dir)
    except Exception as exc:
        error_msg = f"Exception during source code addition: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_source_added = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
