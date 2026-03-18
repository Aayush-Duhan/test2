"""Agent state model with conversation memory."""

from typing import Any, TypedDict

from agentic_core.models.context import MigrationContext


class AgentState(TypedDict, total=False):
    """LangGraph state for the autonomous agent."""

    # ── Migration context (passed through to all node functions) ──
    context: MigrationContext

    # ── Conversation memory ──────────────────────────────────────
    messages: list[dict[str, Any]]        # full chat history (LangChain BaseMessage dicts)
    pending_user_message: str             # queued user message to inject
    agent_plan: str                       # agent's current plan summary

    # ── Execution tracking ───────────────────────────────────────
    tool_call_history: list[dict[str, Any]]  # record of tool calls + results
    current_tool: str                        # tool being executed right now
    iteration_count: int                     # safety: max agent iterations
    is_complete: bool                        # agent decided migration is done
    should_stop: bool                        # external stop signal
