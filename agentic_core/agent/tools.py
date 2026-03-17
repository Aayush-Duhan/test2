"""Tool definitions wrapping existing node functions for the agent."""

from __future__ import annotations

import json
import logging
import os
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
from agentic_core.runtime.snowflake_execution import (
    SQLExecutionError,
    build_snowflake_connection,
    close_connection,
    execute_sql_statements,
)
from agentic_core.nodes.validate import validate_node

from agentic_core.nodes.finalize import finalize_node
from agentic_core.services.file_tools import (
    FileAccessPolicy,
    apply_edit_operations,
    edit_file_section,
    get_file_info,
    list_directory,
    make_directory as make_directory_fs,
    read_file as read_file_content,
    search_in_file,
    view_file_section,
    write_file_content,
)

logger = logging.getLogger(__name__)

_DEFAULT_ALLOWED_EXTENSIONS = {
    ".sql",
    ".ddl",
    ".btq",
    ".txt",
    ".csv",
    ".json",
    ".yaml",
    ".yml",
    ".md",
    ".html",
    ".xml",
    ".log",
    ".ini",
    ".cfg",
}


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


def _get_file_policy(ctx: MigrationContext, *, allow_hidden: Optional[bool] = None) -> Optional[FileAccessPolicy]:
    roots: list[str] = []
    if ctx.project_path:
        roots.append(os.path.abspath(ctx.project_path))
    if ctx.output_path:
        output_root = os.path.abspath(ctx.output_path)
        if output_root not in roots:
            roots.append(output_root)

    if not roots:
        return None

    policy = FileAccessPolicy(
        root_paths=roots,
        allowed_extensions=_DEFAULT_ALLOWED_EXTENSIONS,
    )
    if allow_hidden is not None:
        policy.allow_hidden = allow_hidden
    return policy


def _read_sql_line_range(
    file_path: str,
    start_line: int,
    end_line: int,
    *,
    policy: Optional[FileAccessPolicy] = None,
) -> tuple[str, dict[str, Any] | None]:
    actual_end = end_line if end_line > 0 else None
    view = view_file_section(
        file_path,
        start_line,
        actual_end,
        policy=policy,
    )
    if not isinstance(view, dict) or view.get("error"):
        return "", view
    if view.get("truncated"):
        return "", {
            "error": "Selected range exceeds read limits; reduce the line range.",
            "file_path": view.get("file_path"),
            "start_line": view.get("start_line"),
            "end_line": view.get("end_line"),
            "total_lines": view.get("total_lines"),
        }

    raw_lines: list[str] = []
    for row in str(view.get("content", "")).splitlines():
        sep = row.find(": ")
        raw_lines.append(row[sep + 2:] if sep >= 0 else row)

    sql_text = "\n".join(raw_lines).strip()
    if not sql_text:
        return "", {
            "error": "Selected range is empty.",
            "file_path": view.get("file_path"),
            "start_line": view.get("start_line"),
            "end_line": view.get("end_line"),
            "total_lines": view.get("total_lines"),
        }
    return sql_text, view


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
def execute_sql_range(
    session_id: str,
    file_path: str = "",
    start_line: int = 1,
    end_line: int = 0,
) -> str:
    """Execute SQL statements from a specific line range within a converted SQL file."""
    ctx = get_active_context(session_id)

    if not file_path:
        if ctx.converted_files:
            file_path = ctx.converted_files[0]
        else:
            return json.dumps({"tool": "execute_sql_range", "success": False, "error": "file_path is required"})

    policy = _get_file_policy(ctx)
    sql_text, view = _read_sql_line_range(file_path, start_line, end_line, policy=policy)
    if not sql_text or (view and view.get("error")):
        return json.dumps(
            {
                "tool": "execute_sql_range",
                "success": False,
                "error": (view or {}).get("error", "Unable to read SQL range"),
                "file_path": (view or {}).get("file_path", file_path),
                "start_line": (view or {}).get("start_line", start_line),
                "end_line": (view or {}).get("end_line", end_line),
            },
            default=str,
        )

    on_statement = getattr(ctx, "execution_event_sink", None)
    range_start = int((view or {}).get("start_line", start_line))
    range_end = int((view or {}).get("end_line", end_line))
    resolved_path = (view or {}).get("file_path", file_path)

    def range_statement_sink(entry: dict[str, Any]) -> None:
        if callable(on_statement):
            on_statement(
                {
                    **entry,
                    "file": resolved_path,
                    "fileIndex": -1,
                    "lineRange": {"start": range_start, "end": range_end},
                }
            )

    try:
        connection = build_snowflake_connection(ctx)
        try:
            statement_results = execute_sql_statements(
                connection,
                sql_text,
                on_statement=range_statement_sink,
            )
        finally:
            close_connection(connection)

        ctx.execution_log.append(
            {
                "file": resolved_path,
                "index": -1,
                "status": "success",
                "source": "manual_range",
                "line_range": {"start": range_start, "end": range_end},
                "statements": statement_results,
            }
        )
        set_active_context(session_id, ctx)
        return json.dumps(
            {
                "tool": "execute_sql_range",
                "success": True,
                "file_path": resolved_path,
                "start_line": range_start,
                "end_line": range_end,
                "statements_executed": len(statement_results),
                "statement_results": statement_results,
            },
            default=str,
        )
    except SQLExecutionError as exc:
        ctx.execution_errors.append(
            {
                "type": "execution_error",
                "message": str(exc),
                "stage": "execute_sql_range",
                "statement": getattr(exc, "statement", ""),
                "statement_index": getattr(exc, "statement_index", -1),
            }
        )
        ctx.execution_log.append(
            {
                "file": resolved_path,
                "index": -1,
                "status": "failed",
                "source": "manual_range",
                "line_range": {"start": range_start, "end": range_end},
                "error_type": "execution_error",
                "error_message": str(exc),
                "statements": list(getattr(exc, "partial_results", []) or []),
                "failed_statement": getattr(exc, "statement", ""),
                "failed_statement_index": getattr(exc, "statement_index", -1),
            }
        )
        set_active_context(session_id, ctx)
        return json.dumps(
            {
                "tool": "execute_sql_range",
                "success": False,
                "error": str(exc),
                "file_path": resolved_path,
                "start_line": range_start,
                "end_line": range_end,
                "failed_statement": getattr(exc, "statement", ""),
                "failed_statement_index": getattr(exc, "statement_index", -1),
            },
            default=str,
        )
    except Exception as exc:
        ctx.execution_errors.append(
            {
                "type": "execution_error",
                "message": str(exc),
                "stage": "execute_sql_range",
            }
        )
        ctx.execution_log.append(
            {
                "file": resolved_path,
                "index": -1,
                "status": "failed",
                "source": "manual_range",
                "line_range": {"start": range_start, "end": range_end},
                "error_type": "execution_error",
                "error_message": str(exc),
            }
        )
        set_active_context(session_id, ctx)
        return json.dumps(
            {
                "tool": "execute_sql_range",
                "success": False,
                "error": str(exc),
                "file_path": resolved_path,
                "start_line": range_start,
                "end_line": range_end,
            },
            default=str,
        )


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


@tool
def view_file(session_id: str, file_path: str = "", start_line: int = 1, end_line: int = 0) -> str:
    """View a section of a SQL file with line numbers.

    Args:
        session_id: The active migration session ID.
        file_path: Absolute path to the file to view. If empty, views the
            first converted file for the session.
        start_line: First line to return (1-indexed, inclusive). Default: 1.
        end_line: Last line to return (1-indexed, inclusive). Default: 0
            means start_line + 99 (view 100 lines).

    Returns:
        JSON with numbered file content, total_lines, and file metadata.
    """
    ctx = get_active_context(session_id)

    # Resolve file path if not provided
    if not file_path:
        if ctx.converted_files:
            file_path = ctx.converted_files[0]
        else:
            return json.dumps({"error": "No file_path provided and no converted files on context."})

    policy = _get_file_policy(ctx)
    actual_end = end_line if end_line > 0 else None
    result = view_file_section(file_path, start_line, actual_end, policy=policy)
    return json.dumps(result, default=str)


@tool
def edit_file(
    session_id: str,
    file_path: str = "",
    start_line: int = 0,
    end_line: int = 0,
    new_content: str = "",
) -> str:
    """Replace lines [start_line, end_line] in a file with new_content.

    The agent should use view_file first to inspect the area, then call this
    tool to apply targeted fixes without rewriting the entire file.

    Args:
        session_id: The active migration session ID.
        file_path: Absolute path to the file to edit. If empty, edits the
            first converted file for the session.
        start_line: First line to replace (1-indexed, inclusive).
        end_line: Last line to replace (1-indexed, inclusive).
        new_content: Replacement text. May be more or fewer lines than the
            range being replaced.

    Returns:
        JSON with success status, lines removed/added, and new total lines.
    """
    ctx = get_active_context(session_id)

    # Resolve file path if not provided
    if not file_path:
        if ctx.converted_files:
            file_path = ctx.converted_files[0]
        else:
            return json.dumps({"error": "No file_path provided and no converted files on context."})

    if start_line < 1 or end_line < 1:
        return json.dumps({"error": "start_line and end_line must be >= 1"})

    policy = _get_file_policy(ctx)
    result = edit_file_section(file_path, start_line, end_line, new_content, policy=policy)

    # Sync the in-memory converted_code with the edited file
    if result.get("success") and file_path in (ctx.converted_files or []):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                ctx.converted_code = f.read()
            set_active_context(session_id, ctx)
        except Exception:
            pass

    return json.dumps(result, default=str)


@tool
def get_converted_file_info(session_id: str) -> str:
    """Get metadata about the converted SQL files (total lines, file size, paths).

    Call this before view_file to understand the file structure.
    """
    ctx = get_active_context(session_id)
    files = ctx.converted_files or []

    if not files and ctx.converted_code:
        return json.dumps({
            "source": "in_memory",
            "total_lines": len(ctx.converted_code.splitlines()),
            "size_bytes": len(ctx.converted_code.encode("utf-8")),
        })

    policy = _get_file_policy(ctx)
    info_list = []
    for fp in files:
        info_list.append(get_file_info(fp, policy=policy))

    return json.dumps({"files": info_list}, default=str)


@tool
def list_files(
    session_id: str,
    dir_path: str = "",
    depth: int = 2,
    pattern: str = "",
    include_hidden: bool = False,
) -> str:
    """List files and directories under the project root."""
    ctx = get_active_context(session_id)
    policy = _get_file_policy(ctx, allow_hidden=include_hidden)

    if not dir_path:
        dir_path = ctx.project_path or os.getcwd()

    result = list_directory(
        dir_path,
        policy=policy,
        max_depth=depth,
        pattern=pattern or None,
        include_hidden=include_hidden,
    )
    return json.dumps(result, default=str)


@tool
def search_file(
    session_id: str,
    file_path: str = "",
    query: str = "",
    regex: bool = False,
    case_sensitive: bool = False,
    max_results: int = 0,
) -> str:
    """Search within a file and return matching lines with line numbers."""
    ctx = get_active_context(session_id)
    policy = _get_file_policy(ctx)

    if not file_path:
        if ctx.converted_files:
            file_path = ctx.converted_files[0]
        else:
            return json.dumps({"error": "No file_path provided and no converted files on context."})

    result = search_in_file(
        file_path,
        query,
        policy=policy,
        regex=regex,
        case_sensitive=case_sensitive,
        max_results=max_results if max_results > 0 else None,
    )
    return json.dumps(result, default=str)


@tool
def read_file(
    session_id: str,
    file_path: str = "",
    max_bytes: int = 0,
) -> str:
    """Read a file's contents with size limits."""
    ctx = get_active_context(session_id)
    policy = _get_file_policy(ctx)

    if not file_path:
        if ctx.converted_files:
            file_path = ctx.converted_files[0]
        else:
            return json.dumps({"error": "No file_path provided and no converted files on context."})

    result = read_file_content(
        file_path,
        policy=policy,
        max_bytes=max_bytes if max_bytes > 0 else None,
    )
    return json.dumps(result, default=str)


@tool
def write_file(
    session_id: str,
    file_path: str,
    content: str,
    expected_hash: str = "",
) -> str:
    """Write a full file's contents (use sparingly)."""
    ctx = get_active_context(session_id)
    policy = _get_file_policy(ctx)

    result = write_file_content(
        file_path,
        content,
        policy=policy,
        expected_hash=expected_hash or None,
        create_dirs=True,
    )
    return json.dumps(result, default=str)


@tool
def edit_file_batch(
    session_id: str,
    file_path: str,
    edits: list[dict[str, Any]],
    expected_hash: str = "",
) -> str:
    """Apply multiple line edits to a file in one call."""
    ctx = get_active_context(session_id)
    policy = _get_file_policy(ctx)

    result = apply_edit_operations(
        file_path,
        edits,
        expected_hash=expected_hash or None,
        policy=policy,
    )
    return json.dumps(result, default=str)


@tool
def make_directory(
    session_id: str,
    dir_path: str,
) -> str:
    """Create a directory under the project root."""
    ctx = get_active_context(session_id)
    policy = _get_file_policy(ctx)
    result = make_directory_fs(dir_path, policy=policy)
    return json.dumps(result, default=str)


# ── Pause tool ─────────────────────────────────────────────────

@tool
def pause(session_id: str, reason: str = "Agent paused", status: str = "blocked") -> str:
    """Stop the agent loop. Use when:
    - The migration is complete (status: "completed")
    - You need user action — missing schemas, permissions, etc. (status: "blocked")
    - You hit an unrecoverable error after retrying (status: "error")

    Args:
        session_id: The active migration session ID.
        reason: Brief explanation of why you are stopping.
        status: One of "completed", "blocked", or "error".
    """
    return json.dumps({
        "tool": "pause",
        "success": True,
        "reason": reason,
        "status": status,
    })


# ── Tool registry ──────────────────────────────────────────────

ALL_TOOLS = [
    init_project,
    add_source_code,
    apply_schema_mapping,
    convert_code,
    execute_sql,
    execute_sql_range,
    validate_output,

    finalize_migration,
    view_file,
    edit_file,
    get_converted_file_info,
    list_files,
    search_file,
    read_file,
    write_file,
    edit_file_batch,
    make_directory,
    pause,
]


# ── OpenAI function-calling schema converter ───────────────────

def tools_to_openai_schema(tools: list | None = None) -> list[dict]:
    """Convert LangChain @tool functions to OpenAI function-calling format.

    Returns a list of dicts suitable for the ``tools`` parameter of the
    Cortex REST API Chat Completions endpoint.
    """
    if tools is None:
        tools = ALL_TOOLS

    result: list[dict] = []
    for t in tools:
        # LangChain tools expose args_schema (a Pydantic model)
        if hasattr(t, "args_schema") and t.args_schema is not None:
            try:
                params = t.args_schema.model_json_schema()
            except Exception:
                params = t.args_schema.schema() if hasattr(t.args_schema, "schema") else {"type": "object", "properties": {}}
        else:
            params = {"type": "object", "properties": {}}

        # Clean up schema — remove pydantic metadata keys the API doesn't need
        params.pop("title", None)
        params.pop("description", None)

        result.append({
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description or "",
                "parameters": params,
            },
        })
    return result
