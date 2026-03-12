"""Tool definitions wrapping existing node functions for the agent."""

from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime
from typing import Any, Callable, Optional

from langchain_core.tools import tool

from agentic_core.models.context import MigrationContext, MigrationState
from agentic_core.nodes.init_project import init_project_node
from agentic_core.nodes.add_source_code import add_source_code_node
from agentic_core.nodes.schema_mapping import apply_schema_mapping_node
from agentic_core.nodes.convert_code import convert_code_node
from agentic_core.nodes.execute_sql import execute_sql_node
from agentic_core.nodes.validate import validate_node
from agentic_core.nodes.self_heal import self_heal_node
from agentic_core.nodes.finalize import finalize_node

logger = logging.getLogger(__name__)


# ── Tool result helpers ─────────────────────────────────────────

def _tool_result(
    tool_name: str,
    context: MigrationContext,
    *,
    success: bool | None = None,
    summary: str = "",
) -> dict[str, Any]:
    """Build a structured tool result from the updated MigrationContext."""
    is_error = context.current_stage == MigrationState.ERROR
    if success is None:
        success = not is_error

    result: dict[str, Any] = {
        "tool": tool_name,
        "success": success,
        "current_stage": context.current_stage.value if context.current_stage else "unknown",
        "summary": summary,
    }

    if context.errors:
        result["errors"] = context.errors[-3:]  # last 3 errors

    if context.warnings:
        result["warnings"] = context.warnings[-3:]

    if context.execution_errors:
        result["execution_errors"] = [
            {"type": e.get("type"), "message": e.get("message", "")[:200]}
            for e in context.execution_errors[-3:]
        ]

    if context.missing_objects:
        result["missing_objects"] = context.missing_objects

    if context.requires_ddl_upload:
        result["requires_ddl_upload"] = True
        result["human_intervention_reason"] = context.human_intervention_reason

    if context.validation_issues:
        result["validation_issues"] = [
            {"type": i.get("type"), "severity": i.get("severity"), "message": i.get("message", "")[:200]}
            for i in context.validation_issues[-5:]
        ]

    if context.self_heal_iteration > 0:
        result["self_heal_iteration"] = context.self_heal_iteration
        result["max_self_heal_iterations"] = context.max_self_heal_iterations

    if context.execution_passed is not None:
        result["execution_passed"] = context.execution_passed

    if context.validation_passed is not None:
        result["validation_passed"] = context.validation_passed

    return result


def _run_node_safely(
    tool_name: str,
    node_fn: Callable[[MigrationContext], MigrationContext],
    context: MigrationContext,
    success_summary: str,
) -> tuple[MigrationContext, dict[str, Any]]:
    """Execute a node function with error handling, return updated context + result."""
    try:
        updated = node_fn(context)
        if updated.current_stage == MigrationState.ERROR:
            return updated, _tool_result(
                tool_name, updated,
                success=False,
                summary=f"{tool_name} failed: {updated.errors[-1] if updated.errors else 'Unknown error'}",
            )
        return updated, _tool_result(tool_name, updated, success=True, summary=success_summary)
    except Exception as exc:
        context.errors.append(f"Exception in {tool_name}: {exc}")
        context.current_stage = MigrationState.ERROR
        logger.error("Tool %s raised exception: %s\n%s", tool_name, exc, traceback.format_exc())
        return context, _tool_result(
            tool_name, context,
            success=False,
            summary=f"{tool_name} raised exception: {exc}",
        )


# ── Shared context holder ──────────────────────────────────────
# The agent graph stores context in state; tools access it via this
# thread-local-ish holder set by the graph before each tool execution.

_ACTIVE_CONTEXT: dict[str, MigrationContext] = {}
_STEP_CALLBACK: dict[str, Optional[Callable]] = {}


def set_active_context(session_id: str, ctx: MigrationContext) -> None:
    _ACTIVE_CONTEXT[session_id] = ctx


def get_active_context(session_id: str) -> MigrationContext:
    ctx = _ACTIVE_CONTEXT.get(session_id)
    if ctx is None:
        raise RuntimeError(f"No active context for session {session_id}")
    return ctx


def set_step_callback(session_id: str, cb: Optional[Callable]) -> None:
    _STEP_CALLBACK[session_id] = cb


def get_step_callback(session_id: str) -> Optional[Callable]:
    return _STEP_CALLBACK.get(session_id)


# ── Tool definitions ───────────────────────────────────────────

@tool
def init_project(session_id: str) -> str:
    """Initialize the SCAI project. This must be the first step. Creates the project directory and runs `scai init`."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("init_project", "running")
    updated, result = _run_node_safely("init_project", init_project_node, ctx, "Project initialized successfully.")
    set_active_context(session_id, updated)
    if cb:
        cb("init_project", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


@tool
def add_source_code(session_id: str) -> str:
    """Ingest source SQL files into the project. Must be called after init_project. Runs `scai code add`."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("add_source_code", "running")
    updated, result = _run_node_safely("add_source_code", add_source_code_node, ctx, "Source code ingested successfully.")
    set_active_context(session_id, updated)
    if cb:
        cb("add_source_code", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


@tool
def apply_schema_mapping(session_id: str) -> str:
    """Apply schema mapping CSV to source SQL. Must be called after add_source_code. Skips gracefully if no CSV provided."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("apply_schema_mapping", "running")
    updated, result = _run_node_safely("apply_schema_mapping", apply_schema_mapping_node, ctx, "Schema mapping applied.")
    set_active_context(session_id, updated)
    if cb:
        cb("apply_schema_mapping", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


@tool
def convert_code(session_id: str) -> str:
    """Convert source SQL to Snowflake SQL using `scai code convert`. Must be called after schema mapping."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("convert_code", "running")
    updated, result = _run_node_safely("convert_code", convert_code_node, ctx, "Code converted to Snowflake SQL.")
    set_active_context(session_id, updated)
    if cb:
        cb("convert_code", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


@tool
def execute_sql(session_id: str) -> str:
    """Execute the converted SQL on Snowflake. Returns execution results including any errors or missing objects."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("execute_sql", "running")
    updated, result = _run_node_safely("execute_sql", execute_sql_node, ctx, "SQL executed successfully on Snowflake.")
    set_active_context(session_id, updated)
    if cb:
        cb("execute_sql", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


@tool
def validate_output(session_id: str) -> str:
    """Validate the converted SQL output. Checks line-count regression and other quality metrics."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("validate", "running")
    updated, result = _run_node_safely("validate", validate_node, ctx, "Validation completed.")
    set_active_context(session_id, updated)
    if cb:
        cb("validate", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


@tool
def self_heal(session_id: str) -> str:
    """Apply self-healing to fix issues in converted SQL using Snowflake Cortex LLM. Use after execute_sql or validate reports errors."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("self_heal", "running")
    updated, result = _run_node_safely("self_heal", self_heal_node, ctx, f"Self-healing iteration {ctx.self_heal_iteration + 1} complete.")
    set_active_context(session_id, updated)
    if cb:
        cb("self_heal", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


@tool
def finalize_migration(session_id: str) -> str:
    """Finalize the migration — generate summary report and mark as complete. Call only after successful execution and validation."""
    ctx = get_active_context(session_id)
    cb = get_step_callback(session_id)
    if cb:
        cb("finalize", "running")
    updated, result = _run_node_safely("finalize", finalize_node, ctx, "Migration finalized successfully.")
    set_active_context(session_id, updated)
    if cb:
        cb("finalize", "completed" if result["success"] else "failed")
    return json.dumps(result, default=str)


# ── Tool registry ──────────────────────────────────────────────

ALL_TOOLS = [
    init_project,
    add_source_code,
    apply_schema_mapping,
    convert_code,
    execute_sql,
    validate_output,
    self_heal,
    finalize_migration,
]
