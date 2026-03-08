"""Initialize project workflow node."""

import logging
import os
import shutil
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.services.scai_runner import run_scai_command
from agentic_core.utils.activity_log import log_event

logger = logging.getLogger(__name__)


def init_project_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Initializing project: %s", state.project_name)
    log_event(state, "info", f"Initializing project: {state.project_name}")

    try:
        project_path = os.path.join("projects", state.project_name)

        if os.path.isdir(project_path):
            entries = [
                entry
                for entry in os.listdir(project_path)
                if entry not in {".DS_Store", "Thumbs.db", "desktop.ini"}
            ]
            if entries:
                warning_msg = (
                    f"Project directory already exists and is not empty. "
                    f"Resetting before init: {project_path}"
                )
                logger.warning(warning_msg)
                state.warnings.append(warning_msg)
                log_event(state, "warning", warning_msg)
                shutil.rmtree(project_path, ignore_errors=True)

        os.makedirs(project_path, exist_ok=True)
        cmd = ["scai", "init", "-l", state.source_language, "-n", state.project_name]
        terminal_sink = getattr(state, "terminal_output_sink", None)

        return_code, stdout, stderr = run_scai_command(
            cmd,
            project_path,
            terminal_callback=terminal_sink,
        )
        if stderr:
            log_event(state, "warning", "scai init stderr", {"stderr": stderr})

        if return_code != 0:
            error_detail = stderr or stdout or f"Exit code {return_code}"
            error_msg = f"Failed to initialize project: {error_detail}"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_project_initialized = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.project_path = project_path
        state.scai_project_initialized = True
        state.current_stage = MigrationState.INIT_PROJECT
        state.updated_at = datetime.now()
        logger.info("Project initialized successfully at: %s", project_path)
        log_event(state, "info", f"Project initialized at: {project_path}")
    except Exception as exc:
        error_msg = f"Exception during project initialization: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_project_initialized = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
