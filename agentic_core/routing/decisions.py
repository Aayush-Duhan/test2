"""Workflow routing decisions."""

import logging
from datetime import datetime

from agentic_core.models.context import MigrationContext

logger = logging.getLogger(__name__)


def should_continue(state: MigrationContext) -> str:
    """Route after validation based on validation results."""
    logger.info("Making decision for project: %s", state.project_name)
    logger.info("Validation passed: %s", state.validation_passed)
    logger.info(
        "Self-heal iteration: %s/%s",
        state.self_heal_iteration,
        state.max_self_heal_iterations,
    )

    decision = {
        "timestamp": datetime.now().isoformat(),
        "validation_passed": state.validation_passed,
        "self_heal_iteration": state.self_heal_iteration,
        "max_self_heal_iterations": state.max_self_heal_iterations,
        "validation_issues_count": len(state.validation_issues),
    }

    if str(getattr(state.current_stage, "value", state.current_stage)) == "error":
        decision["action"] = "human_review"
        decision["reason"] = "Error state detected during validate routing"
        state.requires_human_intervention = True
        state.decision_history.append(decision)
        return "human_review"

    if state.validation_passed:
        decision["action"] = "finalize"
        decision["reason"] = "Validation passed"
        state.decision_history.append(decision)
        logger.info("Decision: Finalize (validation passed)")
        return "finalize"

    if state.self_heal_iteration < state.max_self_heal_iterations:
        decision["action"] = "self_heal"
        decision["reason"] = "Issues found; continue self-heal"
        state.decision_history.append(decision)
        logger.info("Decision: Self-heal (iteration %s)", state.self_heal_iteration + 1)
        return "self_heal"

    decision["action"] = "human_review"
    decision["reason"] = "Max iterations reached; human intervention required"
    state.requires_human_intervention = True
    state.human_intervention_reason = (
        f"Could not resolve {len(state.validation_issues)} issue(s) "
        f"after {state.max_self_heal_iterations} iteration(s)."
    )
    state.decision_history.append(decision)
    logger.info("Decision: Human review (max iterations reached)")
    return "human_review"


def should_continue_after_execute(state: MigrationContext) -> str:
    """Route after execute_sql based on execution outcome."""
    logger.info("Making execute_sql decision for project: %s", state.project_name)
    logger.info("Execution passed: %s", state.execution_passed)
    logger.info("Requires DDL upload: %s", state.requires_ddl_upload)

    decision = {
        "timestamp": datetime.now().isoformat(),
        "execution_passed": state.execution_passed,
        "requires_ddl_upload": state.requires_ddl_upload,
        "execution_errors_count": len(state.execution_errors),
    }

    if str(getattr(state.current_stage, "value", state.current_stage)) == "error":
        decision["action"] = "human_review"
        decision["reason"] = "Error state detected during execute routing"
        state.requires_human_intervention = True
        state.decision_history.append(decision)
        return "human_review"

    if state.execution_passed:
        decision["action"] = "finalize"
        decision["reason"] = "Execution succeeded"
        state.decision_history.append(decision)
        return "finalize"

    if state.requires_ddl_upload:
        decision["action"] = "human_review"
        decision["reason"] = "Missing object requires user DDL upload"
        state.decision_history.append(decision)
        return "human_review"

    decision["action"] = "self_heal"
    decision["reason"] = "Execution failed with non-missing-object error"
    state.decision_history.append(decision)
    return "self_heal"
