"""Direct Snowflake Cortex API wrapper — bypasses LangChain ChatSnowflakeCortex.

Calls snowflake.cortex.complete() via Snowpark SQL, giving us full control
over message formatting and avoiding the bind_tools/ToolNode incompatibility.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from agentic_core.models.context import MigrationContext
from agentic_core.runtime.snowflake_session import get_snowflake_session

logger = logging.getLogger(__name__)

DEFAULT_AGENT_MODEL = "claude-4-sonnet"


def get_agent_model_name() -> str:
    """Resolve the model name for the agent orchestrator."""
    return (
        os.getenv("SNOWFLAKE_CORTEX_AGENT_MODEL")
        or os.getenv("SNOWFLAKE_CORTEX_MODEL")
        or os.getenv("CORTEX_MODEL")
        or DEFAULT_AGENT_MODEL
    ).strip() or DEFAULT_AGENT_MODEL


def get_cortex_session(state: MigrationContext):
    """Get a Snowpark session for Cortex calls."""
    session = get_snowflake_session(state)
    if session is None:
        raise RuntimeError(
            "Failed to create Snowflake session for agent. "
            "Check Snowflake credentials (account, user, role, warehouse)."
        )
    return session


def call_cortex_complete(
    session: Any,
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    temperature: float = 0,
    max_tokens: int = 4096,
    top_p: float = 0,
) -> str:
    """Call snowflake.cortex.complete() directly and return the response text.

    Args:
        session: Snowpark session.
        messages: List of {"role": "system"|"user"|"assistant", "content": "..."}.
        model: Cortex model name.
        temperature: Sampling temperature.
        max_tokens: Maximum response tokens.
        top_p: Top-p sampling parameter.

    Returns:
        The assistant response text.
    """
    model_name = model or get_agent_model_name()

    # Sanitize messages — Cortex only accepts system/user/assistant
    sanitized = []
    for msg in messages:
        role = msg.get("role", "user")
        if role not in ("system", "user", "assistant"):
            role = "user"
        content = msg.get("content", "")
        sanitized.append({"role": role, "content": content})

    # Ensure proper role alternation: after system, must have user
    # Adjacent messages with same role need to be merged
    merged: list[dict[str, str]] = []
    for msg in sanitized:
        if merged and merged[-1]["role"] == msg["role"]:
            # Merge with previous message of same role
            merged[-1]["content"] += "\n\n" + msg["content"]
        else:
            merged.append(dict(msg))

    # Cortex requires at least a user message after system
    if len(merged) == 1 and merged[0]["role"] == "system":
        merged.append({"role": "user", "content": "Begin the migration process now."})

    # If last message is system, add a user prompt
    if merged and merged[-1]["role"] == "system":
        merged.append({"role": "user", "content": "Please proceed."})

    message_json = json.dumps(merged)
    options_json = json.dumps({
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
    })

    # Dollar-quoting ($$..$$) is the safest way to embed JSON in
    # Snowflake SQL — it treats everything inside as literal text
    # (no backslash or quote escaping).
    #
    # However, if the conversation contains SQL stored procedure code
    # that also uses $$ as body delimiters, it prematurely closes the
    # quoting.  Fix: replace $$ → $ $ in the payload so it can't collide.
    # This only affects what the LLM sees, not any actual files.
    safe_msg = message_json.replace("$$", "$ $")
    safe_opts = options_json.replace("$$", "$ $")

    sql_stmt = f"""
        SELECT snowflake.cortex.complete(
            '{model_name}',
            parse_json($${safe_msg}$$),
            parse_json($${safe_opts}$$)
        ) AS llm_response
    """

    try:
        # Ensure warehouse is active
        warehouse = session.get_current_warehouse()
        if warehouse:
            session.sql(f"USE WAREHOUSE {warehouse};").collect()

        rows = session.sql(sql_stmt).collect()
    except Exception as exc:
        raise RuntimeError(f"Cortex complete() call failed: {exc}") from exc

    response = json.loads(rows[0]["LLM_RESPONSE"])
    return response["choices"][0]["messages"]




