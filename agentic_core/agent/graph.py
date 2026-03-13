"""Autonomous agent loop — direct Cortex API calls with manual tool dispatch.

Instead of LangGraph's StateGraph + ToolNode (which is incompatible with
Snowflake Cortex), this module implements a simple ReAct-style loop:

    1. Build prompt with conversation history
    2. Call Cortex complete()
    3. Parse the response for a JSON action block
    4. If action found → execute tool → add result to history → goto 1
    5. If no action → agent is done talking or waiting for user → stop

This is the bolt.new-style approach: direct API, full control, no framework tax.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Callable, Optional

from agentic_core.agent.cortex_chat import call_cortex_complete, get_cortex_session
from agentic_core.agent.tools import (
    ALL_TOOLS,
    get_active_context,
    set_active_context,
    set_step_callback,
)
from agentic_core.models.context import MigrationContext, MigrationState

logger = logging.getLogger(__name__)

MAX_AGENT_ITERATIONS = 30

# ── System prompt ──────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an autonomous Snowflake migration agent. You execute migration steps \
using tools and communicate with the user.

## Available Tools

### Pipeline Tools (session_id only)

The session_id is: {session_id}

| Tool | Description |
|------|-------------|
| init_project | Initialize the SCAI project (always first) |
| add_source_code | Ingest source SQL files |
| apply_schema_mapping | Apply schema mapping CSV (skips if no CSV) |
| convert_code | Convert source SQL to Snowflake SQL |
| execute_sql | Execute converted SQL on Snowflake |
| validate_output | Validate the conversion quality |
| self_heal | Fix issues using LLM-guided repair (legacy, avoid for large files) |
| finalize_migration | Generate final report (only after success) |

### File Tools (multi-argument)

These tools allow you to inspect and surgically edit converted SQL files \
without rewriting the entire file. Use these instead of self_heal for targeted fixes.

| Tool | Arguments | Description |
|------|-----------|-------------|
| get_converted_file_info | session_id | Get metadata (paths, total lines, size) of converted files |
| view_file | session_id, file_path, start_line, end_line | View a section of a file with line numbers |
| edit_file | session_id, file_path, start_line, end_line, new_content | Replace a range of lines with new content |

## How to Call a Tool

For pipeline tools (session_id only):
```json
{{"action": "TOOL_NAME", "session_id": "{session_id}"}}
```

For file tools (with additional arguments):
```json
{{"action": "view_file", "session_id": "{session_id}", "args": {{"file_path": "/path/to/file.sql", "start_line": 1, "end_line": 100}}}}
```

```json
{{"action": "edit_file", "session_id": "{session_id}", "args": {{"file_path": "/path/to/file.sql", "start_line": 45, "end_line": 48, "new_content": "-- fixed SQL here\nSELECT 1;"}}}}
```

Only call ONE tool per response. Include explanation text BEFORE the JSON block.

## How to Send a Final Message (No Tool)

When you want to communicate without calling a tool (e.g., summarizing results \
or responding to the user), just write your message normally WITHOUT any JSON block.

## Execution Strategy

1. Start with init_project → add_source_code → apply_schema_mapping → convert_code → execute_sql
2. After execute_sql:
   - Success → validate_output → finalize_migration
   - Errors (NOT missing objects) → use view_file + edit_file to fix the specific error, then execute_sql again (retry up to 5 times)
   - Missing objects / DDL needed → tell the user, STOP (do not retry)
3. After validate_output:
   - Passed → finalize_migration
   - Failed → use view_file + edit_file to fix, then execute_sql again

## Self-Heal Strategy (IMPORTANT)

When you encounter execution or validation errors:
1. Call get_converted_file_info to see the file paths and sizes
2. Use the error message to identify the problematic area
3. Call view_file to examine the relevant section of the file (around the error)
4. Call edit_file to apply a targeted fix to ONLY the affected lines
5. Do NOT rewrite the entire file. Only change the lines that need fixing.
6. After editing, call execute_sql to retry

This approach prevents code truncation on large files.

## Rules
- Before each tool call, briefly explain what you are doing and why.
- After each tool result, summarize the outcome concisely.
- If you encounter errors, explain them clearly.
- When the user sends a message, respond helpfully.
- Be concise but informative.
- NEVER use self_heal for large files. Use view_file + edit_file instead.

## Project Info
Source language: {source_language}
Project name: {project_name}
Has schema mapping: {has_schema_mapping}
"""

# ── Action parsing ─────────────────────────────────────────────

_ACTION_PATTERN = re.compile(
    r'\{\s*"action"\s*:\s*"(\w+)"\s*,\s*"session_id"\s*:\s*"([^"]+)"\s*(?:,\s*"args"\s*:\s*(\{[^}]*\}))?\s*\}',
    re.DOTALL,
)

# Also match code-fenced JSON blocks — captures the full JSON for re-parsing
_FENCED_JSON_PATTERN = re.compile(
    r'```(?:json)?\s*\n?(\{.*?\})\s*\n?```',
    re.DOTALL,
)

VALID_TOOL_NAMES = {t.name for t in ALL_TOOLS}


def parse_action(response_text: str) -> tuple[str | None, str | None, str, dict]:
    """Parse tool action from LLM response.

    Returns:
        (tool_name, session_id, reasoning_text, extra_args)
        tool_name is None if no action was found.
        extra_args is a dict of additional arguments (from "args" key).
    """
    def _try_parse_json_block(text: str) -> tuple[str | None, str | None, str, dict]:
        """Attempt to parse a JSON action block from text."""
        try:
            data = json.loads(text)
            action = data.get("action")
            sid = data.get("session_id")
            args = data.get("args", {})
            if action and action in VALID_TOOL_NAMES and sid:
                return action, sid, "", args if isinstance(args, dict) else {}
        except (json.JSONDecodeError, TypeError):
            pass
        return None, None, "", {}

    # Try fenced block first
    match = _FENCED_JSON_PATTERN.search(response_text)
    if match:
        tool_name, session_id, _, args = _try_parse_json_block(match.group(1))
        if tool_name:
            reasoning = response_text[:match.start()].strip()
            return tool_name, session_id, reasoning, args

    # Try bare JSON — find the last { that looks like an action
    match = _ACTION_PATTERN.search(response_text)
    if match:
        tool_name, session_id = match.group(1), match.group(2)
        # Try to parse the full JSON block for args
        # Find the full JSON starting from this match
        json_start = match.start()
        # Try to find the complete JSON by parsing from the match start
        remaining = response_text[json_start:]
        _, _, _, args = _try_parse_json_block(remaining.strip())
        if not args and match.group(3):
            try:
                args = json.loads(match.group(3))
            except (json.JSONDecodeError, TypeError):
                args = {}
        reasoning = response_text[:json_start].strip()
        if tool_name in VALID_TOOL_NAMES:
            return tool_name, session_id, reasoning, args

    return None, None, response_text.strip(), {}


# ── Tool dispatch ──────────────────────────────────────────────

_TOOL_MAP = {t.name: t for t in ALL_TOOLS}

# Tools that accept extra arguments beyond session_id
_MULTI_ARG_TOOLS = {"view_file", "edit_file"}


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
    message_callback: Optional[Callable[[str, str, str], None]] = None,
    step_callback: Optional[Callable[[str, str], None]] = None,
    user_message_getter: Optional[Callable[[], Optional[str]]] = None,
) -> MigrationContext:
    """Run the autonomous agent loop.

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

    # Get Snowpark session for Cortex calls
    sf_session = get_cortex_session(context)

    def emit(role: str, kind: str, content: str) -> None:
        if message_callback and content.strip():
            try:
                message_callback(role, kind, content)
            except Exception:
                pass

    # Build system prompt
    system_prompt = SYSTEM_PROMPT.format(
        session_id=session_id,
        source_language=context.source_language or "teradata",
        project_name=context.project_name or "migration_project",
        has_schema_mapping="Yes" if context.mapping_csv_path else "No",
    )

    # Conversation history — only system/user/assistant roles
    conversation: list[dict[str, str]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Begin the migration process. Execute the tools in order."},
    ]

    try:
        for iteration in range(1, MAX_AGENT_ITERATIONS + 1):
            logger.info("Agent iteration %d", iteration)

            # ── Check for user messages ────────────────────────
            if user_message_getter and iteration > 1:
                user_msg = user_message_getter()
                if user_msg and user_msg.strip():
                    emit("user", "user_input", user_msg)
                    # Need to ensure role alternation — if last message is user,
                    # we can't add another user message
                    if conversation[-1]["role"] == "user":
                        conversation[-1]["content"] += f"\n\nUser message: {user_msg}"
                    else:
                        conversation.append({"role": "user", "content": user_msg})

            # ── Call Cortex ────────────────────────────────────
            try:
                response_text = call_cortex_complete(
                    sf_session,
                    conversation,
                    max_tokens=4096,
                )
            except Exception as exc:
                error_msg = f"Agent LLM call failed: {exc}"
                logger.error(error_msg)
                emit("error", "run_status", error_msg)
                break

            # Add assistant response to conversation
            conversation.append({"role": "assistant", "content": response_text})

            # ── Parse action ───────────────────────────────────
            tool_name, tool_session_id, reasoning, extra_args = parse_action(response_text)

            # Emit the reasoning/text part
            if reasoning:
                if tool_name:
                    emit("agent", "thinking", reasoning)
                else:
                    emit("agent", "agent_response", reasoning)

            # ── No tool call → check if done ───────────────────
            if tool_name is None:
                updated_ctx = get_active_context(session_id)
                if updated_ctx.current_stage == MigrationState.COMPLETED:
                    break
                if updated_ctx.requires_ddl_upload:
                    break
                if updated_ctx.current_stage == MigrationState.HUMAN_REVIEW:
                    break

                # Agent responded without a tool call — might be answering a user
                # question. Add a follow-up prompt to continue.
                if iteration < MAX_AGENT_ITERATIONS:
                    conversation.append({
                        "role": "user",
                        "content": "Continue with the migration. If all steps are complete, say 'Migration complete' without any JSON action block.",
                    })
                continue

            # ── Execute tool ───────────────────────────────────
            logger.info("Agent calling tool: %s", tool_name)
            emit("system", "step_started", f"Executing: {tool_name}")

            tool_result = execute_tool(tool_name, tool_session_id or session_id, extra_args)

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

            # Add tool result as a user message (maintaining role alternation)
            tool_result_msg = f"Tool `{tool_name}` result:\n```json\n{tool_result}\n```"
            conversation.append({"role": "user", "content": tool_result_msg})

            # ── Check for completion/stopping conditions ───────
            updated_ctx = get_active_context(session_id)
            if updated_ctx.current_stage == MigrationState.COMPLETED:
                emit("system", "run_status", "Migration completed successfully!")
                break
            if updated_ctx.requires_ddl_upload:
                emit("system", "run_status",
                     "DDL upload required. The agent will resume after you upload the required DDL.")
                break

        return get_active_context(session_id)

    finally:
        # Close Snowpark session
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
    message_callback: Optional[Callable[[str, str, str], None]] = None,
    step_callback: Optional[Callable[[str, str], None]] = None,
    user_message_getter: Optional[Callable[[], Optional[str]]] = None,
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
        ),
        "_context": context,
    }


def cleanup_agent_session(graph: Any) -> None:
    """No-op — session cleanup is handled inside run_agent_loop."""
    pass
