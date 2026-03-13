"""Workflow routing decisions.

Note: The self-heal loop has been removed. Error recovery is now handled by the
autonomous agent using view_file + edit_file tools, which avoids code truncation
and allows flexible reasoning about fixes.
"""

import logging
from datetime import datetime

from agentic_core.models.context import MigrationContext

logger = logging.getLogger(__name__)


def should_continue(state: MigrationContext) -> str:
    """Route after validation based on validation results."""
    logger.info("Making decision for project: %s", state.project_name)
    logger.info("Validation passed: %s", state.validation_passed)

    decision = {
        "timestamp": datetime.now().isoformat(),
        "validation_passed": state.validation_passed,
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

    # Validation failed — the agent will handle errors via view_file + edit_file
    decision["action"] = "human_review"
    decision["reason"] = "Validation failed; agent will handle via tools"
    state.requires_human_intervention = True
    state.human_intervention_reason = (
        f"{len(state.validation_issues)} validation issue(s) found. "
        "The agent will attempt to fix them using file inspection and editing."
    )
    state.decision_history.append(decision)
    logger.info("Decision: Human review (validation failed, agent handles fixes)")
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

    # Execution failed (non-missing-object error) — agent handles via tools
    decision["action"] = "human_review"
    decision["reason"] = "Execution failed; agent will fix via view_file + edit_file"
    state.requires_human_intervention = True
    state.human_intervention_reason = (
        "Execution failed with errors. The agent will inspect and fix the SQL."
    )
    state.decision_history.append(decision)
    logger.info("Decision: Human review (execution error, agent handles fixes)")
    return "human_review"
