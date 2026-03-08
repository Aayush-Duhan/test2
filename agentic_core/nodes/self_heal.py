"""Self-heal workflow node."""

import logging
from datetime import datetime
from pathlib import Path

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.services.report_context import (
    build_report_context_memory,
    load_ignored_report_codes,
)
from agentic_core.services.self_healing import (
    apply_self_healing,
    format_self_heal_report,
)
from agentic_core.utils.activity_log import log_event

logger = logging.getLogger(__name__)


def self_heal_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info(
        "Self-healing iteration %s for project: %s",
        state.self_heal_iteration + 1,
        state.project_name,
    )
    log_event(state, "info", f"Self-healing iteration {state.self_heal_iteration + 1}")

    try:
        state.self_heal_iteration += 1
        state.current_stage = MigrationState.SELF_HEAL

        report_context = build_report_context_memory(state)
        state.report_context = report_context
        state.ignored_report_codes = report_context.get("ignored_codes", load_ignored_report_codes())
        state.report_scan_summary = report_context.get("report_scan_summary", {})

        code_to_heal = state.converted_code
        if not code_to_heal:
            error_msg = "No code available for self-healing"
            logger.warning(error_msg)
            state.warnings.append(error_msg)
            state.updated_at = datetime.now()
            log_event(state, "warning", error_msg)
            return state

        def log_callback(msg: str) -> None:
            state.warnings.append(f"[Self-Heal Iter {state.self_heal_iteration}] {msg}")
            logger.info("Self-healing: %s", msg)
            log_event(state, "info", f"Self-healing: {msg}")

        heal_result = apply_self_healing(
            code=code_to_heal,
            issues=state.validation_issues,
            state=state,
            iteration=state.self_heal_iteration,
            statement_type=state.statement_type,
            logger_callback=log_callback,
        )

        log_callback(format_self_heal_report(heal_result))

        if heal_result.success:
            state.converted_code = heal_result.fixed_code

            if state.converted_files:
                for file_path in state.converted_files:
                    try:
                        path_obj = Path(file_path)
                        path_obj.parent.mkdir(parents=True, exist_ok=True)
                        path_obj.write_text(heal_result.fixed_code, encoding="utf-8")
                    except Exception as file_exc:
                        msg = f"Failed to persist healed code to {file_path}: {file_exc}"
                        state.warnings.append(msg)
                        log_event(state, "warning", msg)

            if heal_result.issues_fixed == 0 or state.self_heal_iteration >= state.max_self_heal_iterations:
                state.final_code = heal_result.fixed_code

            state.self_heal_log.append(
                {
                    "iteration": state.self_heal_iteration,
                    "timestamp": heal_result.timestamp,
                    "success": heal_result.success,
                    "fixes_applied": heal_result.fixes_applied,
                    "issues_fixed": heal_result.issues_fixed,
                    "llm_provider": "snowflake_cortex",
                }
            )

            logger.info("Self-healing iteration %s completed successfully", state.self_heal_iteration)
            log_event(state, "info", f"Self-healing iteration {state.self_heal_iteration} completed")
        else:
            error_msg = heal_result.error_message or "Self-healing failed"
            state.errors.append(f"[Self-Heal Iter {state.self_heal_iteration}] {error_msg}")
            log_event(state, "error", f"Self-heal failed: {error_msg}")
            state.self_heal_log.append(
                {
                    "iteration": state.self_heal_iteration,
                    "timestamp": heal_result.timestamp,
                    "success": heal_result.success,
                    "error": heal_result.error_message,
                    "llm_provider": "snowflake_cortex",
                }
            )
            logger.warning("Self-healing iteration %s failed: %s", state.self_heal_iteration, error_msg)

        state.updated_at = datetime.now()
    except Exception as exc:
        error_msg = f"Exception during self-healing: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)
        state.self_heal_log.append(
            {
                "iteration": state.self_heal_iteration,
                "timestamp": datetime.now().isoformat(),
                "success": False,
                "error": error_msg,
            }
        )

    return state
