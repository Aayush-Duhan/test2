"""Validate workflow node."""

import logging
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.services.validation import format_validation_report, validate_code
from agentic_core.utils.activity_log import log_event

logger = logging.getLogger(__name__)


def validate_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Validating converted code for project: %s", state.project_name)
    log_event(state, "info", f"Validating converted code for project: {state.project_name}")

    try:
        state.current_stage = MigrationState.VALIDATE
        state.validation_issues = []
        code_to_validate = state.converted_code

        if not code_to_validate:
            error_msg = "No code available for validation"
            logger.warning(error_msg)
            state.warnings.append(error_msg)
            state.validation_passed = False
            state.validation_issues.append(
                {"type": "validation_error", "severity": "error", "message": error_msg}
            )
            state.updated_at = datetime.now()
            log_event(state, "warning", error_msg)
            return state

        def log_callback(msg: str) -> None:
            state.warnings.append(f"[Validation] {msg}")
            logger.info("Validation: %s", msg)
            log_event(state, "info", f"Validation: {msg}")

        validation_result = validate_code(
            code=code_to_validate,
            original_code=state.original_code if state.original_code else None,
            state=state,
            logger_callback=log_callback,
        )

        log_callback(format_validation_report(validation_result))

        state.validation_passed = validation_result.passed
        state.validation_issues = validation_result.issues
        state.validation_results = validation_result.results

        if validation_result.passed:
            state.final_code = code_to_validate
            logger.info("Validation passed - code is ready for finalization")
            log_event(state, "info", "Validation passed")
        else:
            logger.warning("Validation failed with %s issues", len(validation_result.issues))
            log_event(state, "warning", f"Validation failed with {len(validation_result.issues)} issues")

        state.updated_at = datetime.now()
    except Exception as exc:
        error_msg = f"Exception during validation: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        state.validation_passed = False
        state.validation_issues.append(
            {"type": "validation_error", "severity": "error", "message": error_msg}
        )
        log_event(state, "error", error_msg)

    return state
