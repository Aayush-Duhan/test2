"""Self-healing service integration."""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from typing import Any, Callable, Dict, List, Optional

from agentic_core.models.context import MigrationContext
from agentic_core.models.results import SelfHealResult
from agentic_core.runtime.snowflake_session import get_snowflake_session

logger = logging.getLogger(__name__)

try:
    from langchain_community.chat_models import ChatSnowflakeCortex
except Exception:
    ChatSnowflakeCortex = None


def _extract_model_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
                else:
                    parts.append(str(item))
            else:
                text = getattr(item, "text", None)
                if isinstance(text, str):
                    parts.append(text)
                else:
                    parts.append(str(item))
        return "\n".join(parts).strip()
    return str(content or "").strip()


def _strip_markdown_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if len(lines) >= 2 and lines[0].startswith("```") and lines[-1].startswith("```"):
        return "\n".join(lines[1:-1]).strip()
    return stripped


def remove_enclosed_strings(text: str) -> str:
    """Remove enclosed strings marked with !!!RESOLVE EWI!!! markers."""
    return re.sub(r"!!!RESOLVE EWI!!!.*?\*\*\*/!!!", "", text, flags=re.DOTALL)


def extract_database_from_code(code: str) -> Optional[str]:
    """Extract database name from CREATE OR REPLACE PROCEDURE statement."""
    pattern = r"^CREATE OR REPLACE PROCEDURE\s+([^.]+)\.[^.]+\.[^(]+\(\)"
    for line in code.split("\n"):
        match = re.match(pattern, line.strip())
        if match:
            return match.group(1)
    return None


def write_code_to_temp_file(code: str, file_name: str = "temp_code.sql") -> str:
    """Write code to a temporary file."""
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file_name)
    with open(temp_file_path, "w", encoding="utf-8") as handle:
        handle.write(code)
    return temp_file_path


def apply_self_healing(
    code: str,
    issues: List[Dict[str, Any]],
    state: MigrationContext,
    iteration: int = 1,
    statement_type: str = "mixed",
    logger_callback: Optional[Callable[[str], None]] = None,
) -> SelfHealResult:
    """Apply self-healing to code using Snowflake Cortex through LangChain."""
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback(f"Starting self-healing iteration {iteration}")

    if ChatSnowflakeCortex is None:
        error_msg = "Snowflake Cortex dependency missing. Install langchain-community."
        logger.error(error_msg)
        return SelfHealResult(
            success=False,
            fixed_code=code,
            fixes_applied=[],
            issues_fixed=0,
            error_message=error_msg,
            iteration=iteration,
        )

    session = get_snowflake_session(state)
    if session is None:
        error_msg = "Snowflake session creation failed for self-heal."
        logger.error(error_msg)
        return SelfHealResult(
            success=False,
            fixed_code=code,
            fixes_applied=[],
            issues_fixed=0,
            error_message=error_msg,
            iteration=iteration,
        )

    model_name = (os.getenv("SNOWFLAKE_CORTEX_MODEL") or os.getenv("CORTEX_MODEL") or "claude-4-sonnet").strip()
    model_name = model_name or "claude-4-sonnet"
    cortex_function = (os.getenv("SNOWFLAKE_CORTEX_FUNCTION") or "complete").strip() or "complete"
    cleaned_code = remove_enclosed_strings(code)
    prompt_code = cleaned_code.replace("$$", "$ $")
    issue_text = "\n".join(
        f"- [{issue.get('severity', 'error')}] {issue.get('message', issue)}"
        for issue in issues
    ) or "- No explicit issues provided"
    fix_strategy = {
        "ddl": "Prioritize object-creation order, dependencies, and Snowflake DDL compatibility.",
        "dml": "Prioritize column mapping, joins, update semantics, and data type compatibility.",
        "procedure": "Prioritize procedure syntax, variable handling, and CALL semantics.",
        "function": "Prioritize return type compatibility and SQL function semantics.",
        "mixed": "Prioritize broad Snowflake compatibility while preserving intent.",
    }.get(statement_type or "mixed", "Prioritize broad Snowflake compatibility.")

    report_context = state.report_context if isinstance(state.report_context, dict) else {}
    actionable_issues = report_context.get("actionable_issues", [])
    ignored_codes = report_context.get("ignored_codes", [])
    failed_statements = report_context.get("failed_statements", [])
    execution_errors = report_context.get("latest_execution_errors", [])
    report_scan_summary = report_context.get("report_scan_summary", {})

    prompt = (
        "You are a Snowflake SQL migration repair assistant.\n"
        "Use only the provided context and do not hallucinate missing requirements.\n"
        "Do not invent missing objects unless explicitly referenced in runtime errors or actionable report issues.\n"
        "Return only corrected SQL code with no commentary, no markdown, and no code fences.\n"
        f"Statement type: {statement_type or 'mixed'}\n"
        f"Repair strategy: {fix_strategy}\n"
        f"Iteration: {iteration}\n\n"
        f"Validation/Runtime Issues:\n{issue_text}\n\n"
        f"Report Scan Summary: {json.dumps(report_scan_summary, ensure_ascii=False)}\n"
        f"Ignored Report Codes (non-actionable unless runtime errors): {json.dumps(ignored_codes, ensure_ascii=False)}\n"
        f"Actionable Report Issues: {json.dumps(actionable_issues, ensure_ascii=False)}\n"
        f"Latest Execution Errors: {json.dumps(execution_errors, ensure_ascii=False)}\n"
        f"Failed Statements: {json.dumps(failed_statements, ensure_ascii=False)}\n\n"
        f"Code to Fix:\n{prompt_code}"
    )

    try:
        chat_model = ChatSnowflakeCortex(
            model=model_name,
            cortex_function=cortex_function,
            session=session,
            temperature=0,
        )
        response = chat_model.invoke(prompt)
        fixed_code = _strip_markdown_fences(
            _extract_model_text(getattr(response, "content", response))
        )
    except Exception as exc:
        raw_error = str(exc)
        error_msg = raw_error
        marker = 'SnowparkSQLException("'
        if marker in raw_error:
            start = raw_error.find(marker) + len(marker)
            end = raw_error.find('",', start)
            if end != -1:
                error_msg = raw_error[start:end]
        error_msg = error_msg.replace("\\n", "\n").replace('\\"', '"').replace("\\'", "'")
        if "select snowflake.cortex.complete" in error_msg:
            error_msg = error_msg.split("select snowflake.cortex.complete", 1)[0].strip()
        error_msg = f"Snowflake Cortex self-heal failed for model '{model_name}': {error_msg}"
        logger.error(error_msg)
        return SelfHealResult(
            success=False,
            fixed_code=code,
            fixes_applied=[],
            issues_fixed=0,
            error_message=error_msg,
            iteration=iteration,
        )
    finally:
        try:
            session.close()
        except Exception:
            pass

    if not fixed_code:
        fixed_code = cleaned_code

    logger_callback(f"LLM response (iteration {iteration}, model {model_name}):\n{fixed_code}")

    return SelfHealResult(
        success=True,
        fixed_code=fixed_code,
        fixes_applied=[f"Applied LLM-guided repair via Snowflake Cortex ({model_name})"],
        issues_fixed=len(issues),
        iteration=iteration,
    )


def apply_simple_code_fixes(
    code: str,
    issues: List[Dict[str, Any]],
    logger_callback: Optional[Callable[[str], None]] = None,
) -> SelfHealResult:
    """Apply simple code fixes without Snowflake connection."""
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Applying simple code fixes (no Snowflake connection)")

    fixed_code = code
    fixes_applied: List[str] = []
    issues_fixed = 0

    try:
        fixed_code = remove_enclosed_strings(fixed_code)
        fixes_applied.append("Removed enclosed strings marked with !!!RESOLVE EWI!!!")

        teradata_to_snowflake = {
            r"\bTRIM\(BOTH\s+FROM\s+": "TRIM(",
            r"\bTRIM\(LEADING\s+FROM\s+": "LTRIM(",
            r"\bTRIM\(TRAILING\s+FROM\s+": "RTRIM(",
            r"\bQUALIFY\s+": "QUALIFY ",
        }

        for pattern, replacement in teradata_to_snowflake.items():
            if re.search(pattern, fixed_code):
                fixed_code = re.sub(pattern, replacement, fixed_code)
                fixes_applied.append(f"Replaced pattern: {pattern}")

        update_pattern = r"UPDATE\s+(\w+)\s+SET\s+(.+?)\s+FROM\s+(.+?)\s+AS\s+\1\s+WHERE"
        if re.search(update_pattern, fixed_code, re.IGNORECASE | re.DOTALL):
            fixes_applied.append("Detected UPDATE with alias (may need manual review)")

        issues_fixed = len(
            [issue for issue in issues if "syntax" in issue.get("type", "").lower()]
        )

        logger_callback(
            f"Simple fixes applied: {len(fixes_applied)}, issues potentially fixed: {issues_fixed}"
        )

        return SelfHealResult(
            success=True,
            fixed_code=fixed_code,
            fixes_applied=fixes_applied,
            issues_fixed=issues_fixed,
        )
    except Exception as exc:
        error_msg = f"Exception during simple code fixes: {exc}"
        logger.error(error_msg)
        return SelfHealResult(
            success=False,
            fixed_code=code,
            fixes_applied=[],
            issues_fixed=0,
            error_message=error_msg,
        )


def format_self_heal_report(self_heal_result: SelfHealResult) -> str:
    """Format a self-heal result as a readable report."""
    lines = [
        "Self-Healing Report",
        f"Timestamp: {self_heal_result.timestamp}",
        f"Iteration: {self_heal_result.iteration}",
        f"Success: {self_heal_result.success}",
        f"Issues Fixed: {self_heal_result.issues_fixed}",
        "",
    ]

    if self_heal_result.fixes_applied:
        lines.append("FIXES APPLIED:")
        for fix in self_heal_result.fixes_applied:
            lines.append(f"  - {fix}")
        lines.append("")

    if self_heal_result.error_message:
        lines.append("ERROR:")
        lines.append(f"  {self_heal_result.error_message}")
        lines.append("")

    return "\n".join(lines)
