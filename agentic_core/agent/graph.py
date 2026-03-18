"""Autonomous agent loop — Cortex REST API with native tool calling.

Uses the Snowflake Cortex Chat Completions REST API with:
- Native tool calling (OpenAI function-calling format)
- Streaming SSE for real-time token delivery
- Structured tool dispatch (no text parsing needed)

Flow:
    1. Build messages with conversation history
    2. Call Cortex REST API with tools schema
    3. If response has tool_calls → execute tool → add result → goto 1
    4. If response has content only → agent is done → stop
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Optional

from agentic_core.agent.context_logger import (
    start_log,
    close_log,
    log_iteration_start,
    log_llm_request,
    log_llm_response,
    log_llm_error,
    log_parsed_action,
    log_tool_start,
    log_tool_result,
    log_user_message,
    log_stopping,
)

from agentic_core.agent.cortex_chat import (
    get_cortex_session,
    stream_cortex_to_response,
)
from agentic_core.agent.tools import (
    ALL_TOOLS,
    get_active_context,
    set_active_context,
    set_step_callback,
    tools_to_openai_schema,
)
from agentic_core.models.context import MigrationContext, MigrationState

logger = logging.getLogger(__name__)

MAX_AGENT_ITERATIONS = 30
MAX_TOOL_RESULT_CHARS = 12000

# Only surface detailed tool results for file-oriented tools in the chat UI.
TOOL_RESULT_DISPLAY = {
    "view_file",
    "edit_file",
    "edit_file_batch",
    "get_converted_file_info",
    "list_files",
    "search_file",
    "read_file",
    "write_file",
    "make_directory",
    "execute_sql_range",
}

# ── System prompt ──────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an autonomous Snowflake migration agent. You execute migration steps \
using tools and communicate with the user.

## Available Tools

You have access to the following tools via function calling. The system will \
automatically provide the tool schemas — call them by name with the required arguments.

### Pipeline Tools
These accept only `session_id` (value: {session_id}):
- **init_project** — Initialize the SCAI project (always first)
- **add_source_code** — Ingest source SQL files
- **apply_schema_mapping** — Apply schema mapping CSV (skips if no CSV)
- **convert_code** — Convert source SQL to Snowflake SQL
- **execute_sql** — Execute converted SQL on Snowflake
- **validate_output** — Validate the conversion quality
- **finalize_migration** — Generate final report (only after success)

### File Tools
These accept `session_id` plus additional arguments:
- **get_converted_file_info** — Get metadata (paths, lines, size)
- **view_file** — View a section of a file with line numbers
- **edit_file** — Replace a range of lines with new content
- **edit_file_batch** — Apply multiple line edits in one call
- **list_files** — List files/directories under project root
- **search_file** — Search within a file
- **read_file** — Read file contents
- **write_file** — Write full file contents
- **make_directory** — Create a directory
- **execute_sql_range** — Execute SQL from a specific line range

### Control
- **pause** — Stop the agent loop (reason + status: completed/blocked/error)

## Important Notes

- Always pass `session_id` = "{session_id}" to every tool call.
- Call ONE tool at a time. Wait for results before deciding next action.
- If you want to communicate without calling a tool, just write your message.

## Execution Strategy

1. Start with init_project → add_source_code → apply_schema_mapping → convert_code → execute_sql
2. After execute_sql:
   - Success → validate_output → finalize_migration
   - Errors (NOT missing objects) → use get_converted_file_info, view_file, \
and edit_file to diagnose and fix, then execute_sql again (retry up to 5 times)
   - Missing objects / DDL needed → tell the user, call pause with status "blocked"
3. After validate_output:
   - Passed → finalize_migration
   - Failed → use view_file + edit_file to fix, then execute_sql again

## Error Recovery Strategy

When you encounter execution or validation errors:
1. Call get_converted_file_info to see the file paths and sizes
2. Use the error message to identify the problematic area
3. Call view_file to examine the relevant section
4. Call edit_file to apply a targeted fix to ONLY the affected lines
5. After editing, call execute_sql to retry
6. If the same error persists after 3 attempts, explain the issue and call pause

## Response Format

All human-readable text must be in GitHub-flavored Markdown.
Use short headings, bullets, and fenced code blocks for SQL/JSON/logs.

## Project Info
Source language: {source_language}
Project name: {project_name}
Has schema mapping: {has_schema_mapping}
"""

# ── Tool dispatch ──────────────────────────────────────────────

_TOOL_MAP = {t.name: t for t in ALL_TOOLS}

# Tools that accept extra arguments beyond session_id
_MULTI_ARG_TOOLS = {
    "view_file",
    "edit_file",
    "edit_file_batch",
    "list_files",
    "search_file",
    "read_file",
    "write_file",
    "make_directory",
    "execute_sql_range",
    "pause",
}


def _format_tool_result_for_chat(tool_name: str, tool_result: str) -> str:
    """Prepare a concise JSON payload for chat display."""
    try:
        data = json.loads(tool_result)
    except Exception:
        data = {"tool": tool_name, "raw": tool_result}

    if isinstance(data, dict) and "tool" not in data:
        data["tool"] = tool_name

    payload = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    if len(payload) <= MAX_TOOL_RESULT_CHARS:
        return payload

    wrapper = {
        "tool": tool_name,
        "truncated": True,
        "total_chars": len(payload),
        "preview": payload[:MAX_TOOL_RESULT_CHARS],
    }
    return json.dumps(wrapper, indent=2, ensure_ascii=False, default=str)


def execute_tool(tool_name: str, session_id: str, extra_args: dict | None = None) -> str:
    """Execute a tool by name and return the result string."""
    tool_fn = _TOOL_MAP.get(tool_name)
    if tool_fn is None:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})

    try:
        invoke_args = {"session_id": session_id}
        if extra_args and tool_name in _MULTI_ARG_TOOLS:
            invoke_args.update(extra_args)
        result = tool_fn.invoke(invoke_args)
        return result if isinstance(result, str) else json.dumps(result, default=str)
    except Exception as exc:
        return json.dumps({"error": f"Tool {tool_name} failed: {exc}"})


# ── Agent loop ─────────────────────────────────────────────────


def run_agent_loop(
    context: MigrationContext,
    *,
    message_callback: Optional[Callable[[dict[str, Any]], None]] = None,
    step_callback: Optional[Callable[[str, str], None]] = None,
    user_message_getter: Optional[Callable[[], Optional[str]]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
    conversation_callback: Optional[Callable[[list[dict[str, str]]], None]] = None,
    consume_user_messages_from_start: bool = False,
    start_with_migration_prompt: bool = True,
) -> MigrationContext:
    """Run the autonomous agent loop with native tool calling.

    Args:
        context: MigrationContext with all configuration and callbacks.
        message_callback: Called with (role, kind, content) for each message
            to stream to the frontend.
        step_callback: Called with (step_id, status) to update step progress.
        user_message_getter: Returns pending user message or None.

    Returns:
        The final MigrationContext after the agent completes.
    """
    session_id = context.session_id
    set_active_context(session_id, context)
    set_step_callback(session_id, step_callback)

    # Start the persistent context log
    log_file = start_log(
        session_id,
        context.project_path or os.path.join(os.getcwd(), "agent_logs"),
        project_name=context.project_name or "unknown",
        source_language=context.source_language or "teradata",
    )
    logger.info("Agent context log: %s", log_file)

    # Get Snowpark session for Cortex REST API calls
    sf_session = get_cortex_session(context)

    # Build OpenAI-format tools schema
    openai_tools = tools_to_openai_schema()

    def emit(role: str, kind: str, content: str) -> None:
        if message_callback and content.strip():
            try:
                message_callback({"type": "message", "role": role, "kind": kind, "content": content})
            except Exception:
                pass

    def sync_conversation(conversation: list[dict]) -> None:
        if not conversation_callback:
            return
        try:
            # Only sync serializable messages (filter out tool_calls objects)
            serializable = []
            for msg in conversation:
                entry = {
                    "role": str(msg.get("role", "user")),
                    "content": str(msg.get("content", "") or ""),
                }
                serializable.append(entry)
            conversation_callback(serializable)
        except Exception:
            pass

    # Build system prompt
    system_prompt = SYSTEM_PROMPT.format(
        session_id=session_id,
        source_language=context.source_language or "teradata",
        project_name=context.project_name or "migration_project",
        has_schema_mapping="Yes" if context.mapping_csv_path else "No",
    )

    if conversation_history:
        conversation: list[dict] = [
            {
                "role": str(message.get("role", "user")),
                "content": str(message.get("content", "")),
            }
            for message in conversation_history
            if isinstance(message, dict)
        ]

        if conversation and conversation[0]["role"] == "system":
            conversation[0]["content"] = system_prompt
        else:
            conversation.insert(0, {"role": "system", "content": system_prompt})
    else:
        conversation = [{"role": "system", "content": system_prompt}]

        if start_with_migration_prompt:
            conversation.append(
                {
                    "role": "user",
                    "content": "Begin the migration process. Execute the tools in order.",
                }
            )

    sync_conversation(conversation)

    try:
        for iteration in range(1, MAX_AGENT_ITERATIONS + 1):
            logger.info("Agent iteration %d", iteration)
            log_iteration_start(session_id, iteration)

            # ── Check for user messages ────────────────────────
            if user_message_getter and (iteration > 1 or consume_user_messages_from_start):
                user_msg = user_message_getter()
                if user_msg and user_msg.strip():
                    log_user_message(session_id, user_msg)
                    if conversation[-1]["role"] == "user":
                        conversation[-1]["content"] += f"\n\nUser message: {user_msg}"
                    else:
                        conversation.append({"role": "user", "content": user_msg})
                    sync_conversation(conversation)

            # ── Call Cortex REST API with tools ────────────────
            log_llm_request(session_id, len(conversation))
            try:
                response = stream_cortex_to_response(
                    sf_session,
                    conversation,
                    tools=openai_tools,
                    max_tokens=4096,
                )
            except Exception as exc:
                error_msg = f"Agent LLM call failed: {exc}"
                logger.error(error_msg)
                log_llm_error(session_id, error_msg)
                emit("error", "run_status", error_msg)
                log_stopping(session_id, error_msg)
                break

            content = response.get("content") or ""
            tool_calls = response.get("tool_calls")
            finish_reason = response.get("finish_reason", "stop")

            log_llm_response(session_id, content or json.dumps(tool_calls or [], default=str))

            # ── Build assistant message for conversation ───────
            assistant_msg: dict[str, Any] = {"role": "assistant"}
            if content:
                assistant_msg["content"] = content
            if tool_calls:
                assistant_msg["tool_calls"] = tool_calls
                if not content:
                    assistant_msg["content"] = None

            conversation.append(assistant_msg)
            sync_conversation(conversation)

            # ── No tool calls → text response ──────────────────
            if not tool_calls:
                if content:
                    emit("agent", "agent_response", content)
                log_parsed_action(session_id, None, content, {})
                continue

            # ── Process tool calls ─────────────────────────────
            for tc in tool_calls:
                tc_id = tc.get("id", "")
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                arguments_str = func.get("arguments", "{}")

                try:
                    extra_args = json.loads(arguments_str) if arguments_str else {}
                except json.JSONDecodeError:
                    extra_args = {}

                log_parsed_action(session_id, tool_name, content or "", extra_args)

                # Emit reasoning text (if the model included content alongside tool call)
                if content:
                    emit("agent", "thinking", content)
                    content = ""  # Only emit once for multi-tool responses

                # ── Handle pause ───────────────────────────────
                if tool_name == "pause":
                    pause_reason = extra_args.get("reason", "Agent paused")
                    pause_status = extra_args.get("status", "blocked")
                    log_stopping(session_id, f"Agent paused ({pause_status}): {pause_reason}")
                    if pause_status == "completed":
                        emit("system", "run_status", "Migration completed successfully!")
                    elif pause_status == "error":
                        emit("error", "run_status", f"Agent stopped: {pause_reason}")
                    else:
                        emit("system", "run_status", f"Agent paused: {pause_reason}")

                    # Add tool result to conversation
                    conversation.append({
                        "role": "tool",
                        "tool_call_id": tc_id,
                        "content": json.dumps({"status": pause_status, "reason": pause_reason}),
                    })
                    sync_conversation(conversation)
                    return get_active_context(session_id)

                # ── Execute tool ───────────────────────────────
                logger.info("Agent calling tool: %s", tool_name)
                if message_callback:
                    try:
                        message_callback({
                            "type": "tool-call",
                            "toolCallId": tc_id,
                            "toolName": tool_name,
                            "input": extra_args,
                            "output": "",
                        })
                    except Exception:
                        pass
                log_tool_start(session_id, tool_name)

                tool_result = execute_tool(
                    tool_name,
                    extra_args.pop("session_id", session_id),
                    extra_args,
                )

                # Parse result for summary
                try:
                    result_data = json.loads(tool_result)
                    success = result_data.get("success", False)
                    summary = result_data.get("summary", "")
                    step_status = "completed" if success else "failed"
                except (json.JSONDecodeError, TypeError):
                    success = True
                    summary = tool_result[:200]
                    step_status = "completed"

                log_tool_result(session_id, tool_name, tool_result, success, summary)

                if tool_name in TOOL_RESULT_DISPLAY:
                    tool_payload = _format_tool_result_for_chat(tool_name, tool_result)
                    emit("agent", "tool_result", tool_payload)
                elif message_callback:
                    try:
                        parsed_output = json.loads(tool_result)
                    except Exception:
                        parsed_output = tool_result
                    try:
                        message_callback({
                            "type": "tool-call",
                            "toolCallId": tc_id,
                            "toolName": tool_name,
                            "input": extra_args,
                            "output": parsed_output,
                        })
                    except Exception:
                        pass

                # Add tool result as role: "tool" message (API format)
                conversation.append({
                    "role": "tool",
                    "tool_call_id": tc_id,
                    "content": tool_result,
                })
                sync_conversation(conversation)

        return get_active_context(session_id)

    finally:
        close_log(session_id)
        try:
            sf_session.close()
        except Exception:
            pass


# ── Legacy compatibility ───────────────────────────────────────
# The workflow.py imports build_agent_graph and cleanup_agent_session.
# We provide thin wrappers so workflow.py doesn't need changes.

def build_agent_graph(
    context: MigrationContext,
    *,
    message_callback: Optional[Callable[[dict[str, Any]], None]] = None,
    step_callback: Optional[Callable[[str, str], None]] = None,
    user_message_getter: Optional[Callable[[], Optional[str]]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
    conversation_callback: Optional[Callable[[list[dict[str, str]]], None]] = None,
    consume_user_messages_from_start: bool = False,
    start_with_migration_prompt: bool = True,
) -> dict:
    """Build an 'agent graph' (really just a config dict for run_agent_loop).

    Returns a dict that workflow.py can pass to invoke().
    """
    return {
        "_run_fn": lambda: run_agent_loop(
            context,
            message_callback=message_callback,
            step_callback=step_callback,
            user_message_getter=user_message_getter,
            conversation_history=conversation_history,
            conversation_callback=conversation_callback,
            consume_user_messages_from_start=consume_user_messages_from_start,
            start_with_migration_prompt=start_with_migration_prompt,
        ),
        "_context": context,
    }


def cleanup_agent_session(graph: Any) -> None:
    """No-op — session cleanup is handled inside run_agent_loop."""
    pass
