"""Human review workflow node."""

import logging
from datetime import datetime

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.common import is_error_state
from agentic_core.utils.activity_log import log_event

logger = logging.getLogger(__name__)


def human_review_node(state: MigrationContext) -> MigrationContext:
    if is_error_state(state):
        return state

    logger.info("Requesting human review for project: %s", state.project_name)
    log_event(state, "info", "Human review requested")

    try:
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.updated_at = datetime.now()
        logger.info("Human review requested - workflow paused")
    except Exception as exc:
        error_msg = f"Exception during human review setup: {exc}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
