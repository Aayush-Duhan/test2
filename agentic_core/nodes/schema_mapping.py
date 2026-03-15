"""Apply schema mapping workflow node."""

import logging
import os
import shutil
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.services.schema_mapping import process_sql_with_pandas_replace
from agentic_core.utils.activity_log import log_event
from agentic_core.utils.sql_files import read_sql_files

logger = logging.getLogger(__name__)


def apply_schema_mapping_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Applying schema mapping for project: %s", state.project_name)
    log_event(state, "info", f"Applying schema mapping for project: {state.project_name}")

    try:
        source_dir = os.path.join(state.project_path, "source")
        mapping_path = (state.mapping_csv_path or "").strip()

        if not mapping_path:
            msg = "No schema mapping file provided; skipping schema mapping step."
            logger.info(msg)
            log_event(state, "info", msg)
            state.current_stage = MigrationState.APPLY_SCHEMA_MAPPING
            state.updated_at = datetime.now()
            state.schema_mapped_code = read_sql_files(source_dir)
            return state

        mapped_dir = os.path.join(state.project_path, "source_mapped")
        os.makedirs(mapped_dir, exist_ok=True)

        def log_callback(msg: str) -> None:
            logger.info("Schema mapping: %s", msg)
            log_event(state, "info", f"Schema mapping: {msg}")

        process_sql_with_pandas_replace(
            csv_file_path=mapping_path,
            sql_file_path=source_dir,
            output_dir=mapped_dir,
            logg=log_callback,
        )

        if os.path.isdir(source_dir):
            shutil.rmtree(source_dir)
        if os.path.isdir(mapped_dir):
            shutil.move(mapped_dir, source_dir)
        else:
            os.makedirs(source_dir, exist_ok=True)
            warning_msg = f"Mapped output directory not found after schema mapping: {mapped_dir}"
            state.warnings.append(warning_msg)
            log_event(state, "warning", warning_msg)

        state.current_stage = MigrationState.APPLY_SCHEMA_MAPPING
        state.updated_at = datetime.now()
        logger.info("Schema mapping applied successfully")
        log_event(state, "info", "Schema mapping applied successfully")
        state.schema_mapped_code = read_sql_files(source_dir)
    except Exception as exc:
        error_msg = f"Exception during schema mapping: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
