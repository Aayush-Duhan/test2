"""Snowflake Cortex chat model wrapper for the orchestration agent."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from agentic_core.models.context import MigrationContext
from agentic_core.runtime.snowflake_session import get_snowflake_session

logger = logging.getLogger(__name__)

# Default model for the orchestration agent
DEFAULT_AGENT_MODEL = "claude-4-sonnet"


def get_agent_model_name() -> str:
    """Resolve the model name for the agent orchestrator."""
    return (
        os.getenv("SNOWFLAKE_CORTEX_AGENT_MODEL")
        or os.getenv("SNOWFLAKE_CORTEX_MODEL")
        or os.getenv("CORTEX_MODEL")
        or DEFAULT_AGENT_MODEL
    ).strip() or DEFAULT_AGENT_MODEL


def create_cortex_chat_model(
    state: MigrationContext,
    *,
    model: str | None = None,
    temperature: float = 0,
):
    """Create a ChatSnowflakeCortex instance for the agent.

    Returns the chat model and the Snowflake session (caller must close session).
    """
    try:
        from langchain_community.chat_models import ChatSnowflakeCortex
    except ImportError as exc:
        raise RuntimeError(
            "langchain-community is required for the agent. "
            "Install with: pip install langchain-community"
        ) from exc

    session = get_snowflake_session(state)
    if session is None:
        raise RuntimeError(
            "Failed to create Snowflake session for agent. "
            "Check Snowflake credentials (account, user, role, warehouse)."
        )

    model_name = model or get_agent_model_name()
    cortex_function = (
        os.getenv("SNOWFLAKE_CORTEX_FUNCTION") or "complete"
    ).strip() or "complete"

    chat_model = ChatSnowflakeCortex(
        model=model_name,
        cortex_function=cortex_function,
        session=session,
        temperature=temperature,
    )

    logger.info("Created Cortex chat model: model=%s, function=%s", model_name, cortex_function)
    return chat_model, session
