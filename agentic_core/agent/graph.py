"""LangGraph ReAct agent graph for autonomous migration orchestration."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Optional

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from agentic_core.agent.cortex_chat import create_cortex_chat_model
from agentic_core.agent.state import AgentState
from agentic_core.agent.tools import (
    ALL_TOOLS,
    get_active_context,
    set_active_context,
    set_step_callback,
)
from agentic_core.models.context import MigrationContext, MigrationState

logger = logging.getLogger(__name__)

MAX_AGENT_ITERATIONS = 30  # safety limit

SYSTEM_PROMPT = """\
You are an autonomous Snowflake migration agent. Your job is to migrate SQL code \
from a source platform (e.g. Teradata) to Snowflake by executing migration tools \
in the correct order.

## Available Tools

You have the following tools. Each tool takes a single argument `session_id` (string) \
which you must pass through unchanged from the conversation context.

1. **init_project** – Initialize the SCAI project (always first)
2. **add_source_code** – Ingest source SQL files
3. **apply_schema_mapping** – Apply schema mapping CSV (skips if no CSV)
4. **convert_code** – Convert source SQL to Snowflake SQL
5. **execute_sql** – Execute converted SQL on Snowflake
6. **validate_output** – Validate the conversion quality
7. **self_heal** – Fix issues using LLM-guided repair
8. **finalize_migration** – Generate final report (only after success)

## Execution Strategy

Follow this general order: init_project → add_source_code → apply_schema_mapping → \
convert_code → execute_sql.

After execute_sql:
- If execution succeeded: run validate_output, then finalize_migration
- If execution failed with errors (NOT missing objects): run self_heal, then \
  execute_sql again. You may retry self_heal + execute_sql up to 5 times.
- If execution failed with missing_objects / requires_ddl_upload: inform the user \
  that DDL upload is required. Do NOT retry — wait for user input.

After validate_output:
- If validation passed: run finalize_migration
- If validation failed: run self_heal, then execute_sql again

## Communication

- Before each tool call, briefly explain what you are about to do and why.
- After each tool result, summarize the outcome.
- If you encounter errors, explain them clearly and state your plan.
- When the user sends a message, respond helpfully. You can answer questions about \
  the migration, explain errors, or take actions they request.
- Be concise but informative.

## Session Context

The session_id for this migration is: {session_id}

Source language: {source_language}
Project name: {project_name}
Has schema mapping: {has_schema_mapping}
"""


def _build_system_message(context: MigrationContext) -> SystemMessage:
    """Build the system prompt with context details."""
    return SystemMessage(content=SYSTEM_PROMPT.format(
        session_id=context.session_id,
        source_language=context.source_language or "teradata",
        project_name=context.project_name or "migration_project",
        has_schema_mapping="Yes" if context.mapping_csv_path else "No",
    ))


def _extract_model_text(content: Any) -> str:
    """Extract text from potentially complex LLM response content."""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
            elif hasattr(item, "text"):
                parts.append(str(item.text))
        return "\n".join(parts).strip()
    return str(content or "").strip()


# ── Graph builder ──────────────────────────────────────────────


def build_agent_graph(
    context: MigrationContext,
    *,
    message_callback: Optional[Callable[[str, str, str], None]] = None,
    step_callback: Optional[Callable[[str, str], None]] = None,
    user_message_getter: Optional[Callable[[], Optional[str]]] = None,
):
    """Build and return a compiled LangGraph agent.

    Args:
        context: The MigrationContext with all configuration.
        message_callback: Called with (role, kind, content) for each message to
            stream to the frontend.
        step_callback: Called with (step_id, status) to update step progress.
        user_message_getter: Called to check for pending user messages.
            Returns the message string or None.

    Returns:
        A compiled LangGraph that can be invoked with an initial state dict.
    """
    session_id = context.session_id
    set_active_context(session_id, context)
    set_step_callback(session_id, step_callback)

    # Create the LLM
    chat_model, sf_session = create_cortex_chat_model(context)

    # Bind tools to the model
    model_with_tools = chat_model.bind_tools(ALL_TOOLS)

    def emit(role: str, kind: str, content: str) -> None:
        if message_callback and content.strip():
            try:
                message_callback(role, kind, content)
            except Exception:
                pass

    # ── Node: agent (LLM reasoning) ────────────────────────────

    def agent_node(state: AgentState) -> AgentState:
        messages = state.get("messages", [])
        iteration = state.get("iteration_count", 0) + 1

        if iteration > MAX_AGENT_ITERATIONS:
            emit("system", "run_status", "Agent reached maximum iterations. Stopping.")
            return {
                **state,
                "iteration_count": iteration,
                "should_stop": True,
            }

        # Inject system message if not present
        if not messages or not isinstance(messages[0], SystemMessage):
            ctx = get_active_context(session_id)
            messages = [_build_system_message(ctx)] + list(messages)

        try:
            response = model_with_tools.invoke(messages)
        except Exception as exc:
            error_msg = f"Agent LLM call failed: {exc}"
            logger.error(error_msg)
            emit("error", "run_status", error_msg)
            return {
                **state,
                "messages": messages + [AIMessage(content=error_msg)],
                "iteration_count": iteration,
                "should_stop": True,
            }

        # Extract text content for display
        response_text = _extract_model_text(response.content)

        # Strip markdown fences if present
        if response_text.startswith("```"):
            lines = response_text.splitlines()
            if len(lines) >= 2 and lines[-1].startswith("```"):
                response_text = "\n".join(lines[1:-1]).strip()

        # Emit agent message if there's text (not just tool calls)
        has_tool_calls = bool(getattr(response, "tool_calls", None))
        if response_text and not has_tool_calls:
            emit("agent", "agent_response", response_text)
        elif response_text and has_tool_calls:
            emit("agent", "thinking", response_text)

        return {
            **state,
            "messages": messages + [response],
            "iteration_count": iteration,
        }

    # ── Node: tools ────────────────────────────────────────────

    tool_node = ToolNode(ALL_TOOLS)

    def tools_node(state: AgentState) -> AgentState:
        """Execute tools and emit results."""
        messages = state.get("messages", [])
        last_msg = messages[-1] if messages else None

        if not last_msg or not hasattr(last_msg, "tool_calls") or not last_msg.tool_calls:
            return state

        # Emit what tool we're calling
        for tc in last_msg.tool_calls:
            tool_name = tc.get("name", "unknown")
            emit("system", "step_started", f"Executing: {tool_name}")

        # Run through LangGraph ToolNode
        result = tool_node.invoke(state)

        # After tool execution, sync context back
        try:
            updated_ctx = get_active_context(session_id)
            set_active_context(session_id, updated_ctx)
        except Exception:
            pass

        return result

    # ── Node: check user input ─────────────────────────────────

    def check_user_input_node(state: AgentState) -> AgentState:
        """Check for pending user messages and inject them."""
        if user_message_getter is None:
            return state

        user_msg = user_message_getter()
        if user_msg and user_msg.strip():
            messages = list(state.get("messages", []))
            messages.append(HumanMessage(content=user_msg))
            emit("user", "user_input", user_msg)
            return {**state, "messages": messages, "pending_user_message": ""}

        return state

    # ── Routing ────────────────────────────────────────────────

    def should_continue(state: AgentState) -> str:
        """Route after agent node."""
        if state.get("should_stop"):
            return "end"

        messages = state.get("messages", [])
        last_msg = messages[-1] if messages else None

        if last_msg and hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
            return "tools"

        # Check if migration is complete
        try:
            ctx = get_active_context(session_id)
            if ctx.current_stage == MigrationState.COMPLETED:
                return "end"
            if ctx.requires_ddl_upload:
                emit("system", "run_status",
                     "Waiting for DDL upload. The agent will resume after you upload the required DDL.")
                return "end"
        except Exception:
            pass

        return "check_user"

    def after_tools(state: AgentState) -> str:
        """Route after tool execution — always go back to agent."""
        return "agent"

    def after_check_user(state: AgentState) -> str:
        """Route after checking user input."""
        messages = state.get("messages", [])
        if messages and isinstance(messages[-1], HumanMessage):
            return "agent"

        # Check if we should stop
        try:
            ctx = get_active_context(session_id)
            if ctx.current_stage == MigrationState.COMPLETED:
                return "end"
            if ctx.current_stage == MigrationState.HUMAN_REVIEW:
                return "end"
        except Exception:
            pass

        # Continue reasoning
        return "agent"

    # ── Build the graph ────────────────────────────────────────

    graph_builder = StateGraph(AgentState)

    graph_builder.add_node("agent", agent_node)
    graph_builder.add_node("tools", tools_node)
    graph_builder.add_node("check_user", check_user_input_node)

    graph_builder.add_edge(START, "agent")

    graph_builder.add_conditional_edges(
        "agent",
        should_continue,
        {
            "tools": "tools",
            "check_user": "check_user",
            "end": END,
        },
    )

    graph_builder.add_conditional_edges(
        "tools",
        after_tools,
        {"agent": "agent"},
    )

    graph_builder.add_conditional_edges(
        "check_user",
        after_check_user,
        {
            "agent": "agent",
            "end": END,
        },
    )

    compiled = graph_builder.compile()

    # Attach cleanup for the Snowflake session
    compiled._sf_session = sf_session  # type: ignore[attr-defined]

    return compiled


def cleanup_agent_session(graph: Any) -> None:
    """Close the Snowflake session held by the agent graph."""
    session = getattr(graph, "_sf_session", None)
    if session is not None:
        try:
            session.close()
        except Exception:
            pass
