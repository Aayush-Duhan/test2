"""Finalize workflow node."""

import logging
import os
import shutil
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.utils.activity_log import log_event

logger = logging.getLogger(__name__)


def finalize_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Finalizing migration for project: %s", state.project_name)
    log_event(state, "info", f"Finalizing migration for project: {state.project_name}")

    try:
        output_dir = os.path.join("outputs", state.project_name)
        os.makedirs(output_dir, exist_ok=True)
        converted_dir = os.path.join(state.project_path, "converted")

        if os.path.exists(converted_dir):
            for root, _, files in os.walk(converted_dir):
                for file_name in files:
                    src_path = os.path.join(root, file_name)
                    rel_path = os.path.relpath(src_path, converted_dir)
                    dst_path = os.path.join(output_dir, "converted", rel_path)
                    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                    shutil.copy2(src_path, dst_path)
                    state.output_files.append(dst_path)

        state.summary_report = {
            "project_name": state.project_name,
            "source_language": state.source_language,
            "target_platform": state.target_platform,
            "scai_project_initialized": state.scai_project_initialized,
            "scai_source_added": state.scai_source_added,
            "scai_converted": state.scai_converted,
            "self_heal_iterations": state.self_heal_iteration,
            "validation_passed": state.validation_passed,
            "validation_issues_count": len(state.validation_issues),
            "errors_count": len(state.errors),
            "warnings_count": len(state.warnings),
            "output_files_count": len(state.output_files),
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
        }

        state.output_path = output_dir
        state.validation_passed = True
        state.current_stage = MigrationState.COMPLETED
        state.updated_at = datetime.now()
        logger.info("Migration finalized. Output at: %s", output_dir)
        log_event(state, "info", f"Migration finalized. Output at: {output_dir}")
    except Exception as exc:
        error_msg = f"Exception during finalization: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
