"""Agent context logger — writes the full agent conversation to a text file.

During each agent run, this module maintains a human-readable log file that
captures every LLM call, response, tool execution, and result.  This gives
full visibility into what the agent is doing even when nothing is visible
in the terminal (e.g. during retries or internal reasoning).

The log file is written to: <project_path>/agent_context.txt
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── Module-level state ─────────────────────────────────────────
# Holds the active log file path per session so helpers can append.
_ACTIVE_LOG_PATHS: dict[str, str] = {}


def _separator(char: str = "─", width: int = 80) -> str:
    return char * width


def start_log(session_id: str, project_path: str, **metadata: str) -> str:
    """Create / overwrite the agent context log for a new run.

    Returns the absolute path to the log file.
    """
    log_path = os.path.join(project_path, "agent_context.txt")
    _ACTIVE_LOG_PATHS[session_id] = log_path

    os.makedirs(project_path, exist_ok=True)

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"{'═' * 80}\n")
        f.write(f"  AGENT CONTEXT LOG\n")
        f.write(f"  Session: {session_id}\n")
        f.write(f"  Started: {datetime.now().isoformat()}\n")
        for key, value in metadata.items():
            f.write(f"  {key}: {value}\n")
        f.write(f"{'═' * 80}\n\n")

    logger.info("Agent context log started: %s", log_path)
    return log_path


def _append(session_id: str, text: str) -> None:
    """Append text to the active log file."""
    path = _ACTIVE_LOG_PATHS.get(session_id)
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)
    except Exception as exc:
        logger.debug("Failed to write agent context log: %s", exc)


def log_iteration_start(session_id: str, iteration: int) -> None:
    _append(session_id, (
        f"\n{_separator('─')}\n"
        f"  ITERATION {iteration}  |  {datetime.now().strftime('%H:%M:%S')}\n"
        f"{_separator('─')}\n\n"
    ))


def log_llm_request(session_id: str, conversation_length: int) -> None:
    _append(session_id, (
        f"[LLM REQUEST]  Messages in conversation: {conversation_length}\n"
        f"  Calling Snowflake Cortex...\n\n"
    ))


def log_llm_response(session_id: str, response_text: str) -> None:
    # Truncate very long responses for readability, but keep enough for context
    display_text = response_text if len(response_text) <= 3000 else (
        response_text[:2500] + f"\n\n... (truncated, {len(response_text)} chars total) ...\n\n" + response_text[-500:]
    )
    _append(session_id, (
        f"[LLM RESPONSE]\n"
        f"{display_text}\n\n"
    ))


def log_llm_error(session_id: str, error: str) -> None:
    _append(session_id, (
        f"[LLM ERROR]  ❌\n"
        f"  {error}\n\n"
    ))


def log_parsed_action(
    session_id: str,
    tool_name: Optional[str],
    reasoning: str,
    extra_args: Optional[dict] = None,
) -> None:
    if tool_name:
        args_str = f"  Args: {extra_args}" if extra_args else ""
        _append(session_id, (
            f"[PARSED ACTION]  Tool: {tool_name}{args_str}\n"
            f"  Reasoning: {reasoning[:500] if reasoning else '(none)'}\n\n"
        ))
    else:
        _append(session_id, (
            f"[NO ACTION]  Agent responded without calling a tool\n"
            f"  Response: {reasoning[:500] if reasoning else '(none)'}\n\n"
        ))


def log_tool_start(session_id: str, tool_name: str) -> None:
    _append(session_id, (
        f"[TOOL START]  ▶ {tool_name}\n"
        f"  Time: {datetime.now().strftime('%H:%M:%S')}\n"
    ))


def log_tool_result(
    session_id: str,
    tool_name: str,
    result: str,
    success: bool = True,
    summary: str = "",
) -> None:
    status = "✅ Success" if success else "❌ Failed"
    # Truncate very long results
    display_result = result if len(result) <= 2000 else (
        result[:1500] + f"\n... (truncated, {len(result)} chars total) ...\n" + result[-300:]
    )
    _append(session_id, (
        f"[TOOL RESULT]  {tool_name}  {status}\n"
        f"  Summary: {summary}\n"
        f"  Full Result:\n{display_result}\n\n"
    ))


def log_user_message(session_id: str, message: str) -> None:
    _append(session_id, (
        f"[USER MESSAGE]\n"
        f"  {message}\n\n"
    ))


def log_stopping(session_id: str, reason: str) -> None:
    _append(session_id, (
        f"\n{_separator('═')}\n"
        f"  AGENT STOPPED\n"
        f"  Reason: {reason}\n"
        f"  Time: {datetime.now().isoformat()}\n"
        f"{_separator('═')}\n"
    ))


def close_log(session_id: str) -> None:
    """Mark the log as finished and remove from active tracking."""
    _append(session_id, (
        f"\n{_separator('═')}\n"
        f"  LOG CLOSED  |  {datetime.now().isoformat()}\n"
        f"{_separator('═')}\n"
    ))
    _ACTIVE_LOG_PATHS.pop(session_id, None)
