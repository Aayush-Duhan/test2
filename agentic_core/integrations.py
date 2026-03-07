"""
Integration helper module for agentic_core workflow nodes.

This module provides wrapper functions for existing self-healing and validation
scripts, adapting them to work with the MigrationContext dataclass used in the
autonomous migration workflow.

The module handles:
- Self-healing integration with scripts/self_healing_script.py
- Validation integration with scripts/validation2.py
- Error handling and logging
- Type hints and docstrings
"""

import os
import re
import tempfile
import logging
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from dataclasses import dataclass, field

from .state import MigrationContext

# Configure logging
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


def _count_lines(text: str) -> int:
    if not text:
        return 0
    return len(text.splitlines())


def _count_lines_from_files(paths: List[str]) -> Optional[int]:
    total = 0
    found = False
    for path in paths:
        try:
            if not path or not os.path.isfile(path):
                continue
            with open(path, "r", encoding="utf-8-sig") as handle:
                total += _count_lines(handle.read())
                found = True
        except Exception:
            continue
    return total if found else None


# ============================================================================
# Result Data Classes
# ============================================================================

@dataclass
class SelfHealResult:
    """Result of a self-healing operation."""
    success: bool
    fixed_code: str
    fixes_applied: List[str]
    issues_fixed: int
    error_message: Optional[str] = None
    iteration: int = 0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    passed: bool
    issues: List[Dict[str, Any]]
    results: Dict[str, Any]
    error_message: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


# ============================================================================
# Self-Healing Integration
# ============================================================================

def get_snowflake_session(state: MigrationContext):
    """
    Create a Snowflake session from MigrationContext.

    Args:
        state: MigrationContext containing Snowflake connection parameters

    Returns:
        Snowflake Session object or None if connection fails
    """
    try:
        from .snowflake_auth import (
            SnowflakeAuthConfig,
            resolve_password_from_sources,
            create_snowpark_session,
        )

        sf_account = (state.sf_account or "").strip()
        sf_user = (state.sf_user or "").strip()

        if not sf_account:
            logger.error("Snowflake account is required but not provided.")
            return None
        if not sf_user:
            logger.error("Snowflake user is required but not provided.")
            return None

        sf_role = (state.sf_role or "").strip() or None
        sf_warehouse = (state.sf_warehouse or "").strip() or None
        sf_database = (state.sf_database or "").strip() or None
        sf_schema = (state.sf_schema or "").strip() or None
        sf_authenticator = (state.sf_authenticator or "").strip() or "externalbrowser"

        config = SnowflakeAuthConfig(
            account=sf_account,
            user=sf_user,
            role=sf_role,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema,
            authenticator=sf_authenticator,
        )

        password = resolve_password_from_sources(
            authenticator=config.authenticator,
            explicit_password=None,
        )

        session = create_snowpark_session(config, password=password)
        logger.info("Snowflake session created successfully")
        return session

    except Exception as e:
        logger.error(f"Failed to create Snowflake session: {e}")
        return None


def remove_enclosed_strings(text: str) -> str:
    """
    Remove enclosed strings marked with !!!RESOLVE EWI!!! markers.

    Args:
        text: Input text containing enclosed strings

    Returns:
        Text with enclosed strings removed
    """
    pattern = r'!!!RESOLVE EWI!!!.*?\*\*\*/!!!'
    cleaned_text = re.sub(pattern, '', text, flags=re.DOTALL)
    return cleaned_text


def extract_database_from_code(code: str) -> Optional[str]:
    """
    Extract database name from CREATE OR REPLACE PROCEDURE statement.

    Args:
        code: SQL code containing procedure definition

    Returns:
        Database name or None if not found
    """
    pattern = r'^CREATE OR REPLACE PROCEDURE\s+([^.]+)\.[^.]+\.[^(]+\(\)'
    for line in code.split('\n'):
        line = line.strip()
        match = re.match(pattern, line)
        if match:
            return match.group(1)
    return None


def write_code_to_temp_file(code: str, file_name: str = "temp_code.sql") -> str:
    """
    Write code to a temporary file.

    Args:
        code: Code content to write
        file_name: Name for the temporary file

    Returns:
        Path to the temporary file
    """
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file_name)
    with open(temp_file_path, 'w', encoding='utf-8') as f:
        f.write(code)
    return temp_file_path


def apply_self_healing(
    code: str,
    issues: List[Dict[str, Any]],
    state: MigrationContext,
    iteration: int = 1,
    statement_type: str = "mixed",
    logger_callback: Optional[Callable[[str], None]] = None
) -> SelfHealResult:
    """
    Apply self-healing to code using Snowflake Cortex through LangChain.

    Args:
        code: The code to be healed
        issues: List of validation issues to address
        state: MigrationContext containing connection parameters
        iteration: Current self-healing iteration number
        logger_callback: Optional callback for logging messages

    Returns:
        SelfHealResult containing the fixed code and metadata
    """
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

    model_name = (
        os.getenv("SNOWFLAKE_CORTEX_MODEL")
        or os.getenv("CORTEX_MODEL")
        or "claude-4-sonnet"
    ).strip() or "claude-4-sonnet"
    cortex_function = (os.getenv("SNOWFLAKE_CORTEX_FUNCTION") or "complete").strip() or "complete"
    cleaned_code = remove_enclosed_strings(code)
    # Snowflake Cortex SQL wrapper uses $$...$$; avoid breaking it by stripping $$ from the prompt.
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
        fixed_code = _strip_markdown_fences(_extract_model_text(getattr(response, "content", response)))
    except Exception as e:
        raw_error = str(e)
        error_msg = raw_error
        marker = 'SnowparkSQLException("'
        if marker in raw_error:
            start = raw_error.find(marker) + len(marker)
            end = raw_error.find('",', start)
            if end != -1:
                error_msg = raw_error[start:end]
        error_msg = (
            error_msg.replace("\\n", "\n")
            .replace("\\\"", "\"")
            .replace("\\'", "'")
        )
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

    logger_callback(
        f"LLM response (iteration {iteration}, model {model_name}):\n{fixed_code}"
    )

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
    logger_callback: Optional[Callable[[str], None]] = None
) -> SelfHealResult:
    """
    Apply simple code fixes without Snowflake connection.

    This function provides a fallback self-healing mechanism that doesn't
    require a Snowflake connection. It applies basic syntax fixes.

    Args:
        code: The code to be healed
        issues: List of validation issues to address
        logger_callback: Optional callback for logging messages

    Returns:
        SelfHealResult containing the fixed code and metadata
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Applying simple code fixes (no Snowflake connection)")

    fixed_code = code
    fixes_applied = []
    issues_fixed = 0

    try:
        # Remove enclosed strings
        fixed_code = remove_enclosed_strings(fixed_code)
        fixes_applied.append("Removed enclosed strings marked with !!!RESOLVE EWI!!!")

        # Fix common Teradata to Snowflake syntax issues
        # 1. Replace Teradata-specific functions
        teradata_to_snowflake = {
            r'\bTRIM\(BOTH\s+FROM\s+': 'TRIM(',
            r'\bTRIM\(LEADING\s+FROM\s+': 'LTRIM(',
            r'\bTRIM\(TRAILING\s+FROM\s+': 'RTRIM(',
            r'\bQUALIFY\s+': 'QUALIFY ',  # Keep QUALIFY but ensure proper syntax
        }

        for pattern, replacement in teradata_to_snowflake.items():
            if re.search(pattern, fixed_code):
                fixed_code = re.sub(pattern, replacement, fixed_code)
                fixes_applied.append(f"Replaced pattern: {pattern}")

        # 2. Fix UPDATE statements with alias issues
        # Pattern: UPDATE tgt SET ... FROM db.schema.tbl1, db.schema.tbl2 as tgt
        update_pattern = r'UPDATE\s+(\w+)\s+SET\s+(.+?)\s+FROM\s+(.+?)\s+AS\s+\1\s+WHERE'
        if re.search(update_pattern, fixed_code, re.IGNORECASE | re.DOTALL):
            # This is a simplified fix - real fix would need more complex parsing
            fixes_applied.append("Detected UPDATE with alias (may need manual review)")

        # Count issues that might be fixed
        issues_fixed = len([issue for issue in issues if "syntax" in issue.get("type", "").lower()])

        logger_callback(f"Simple fixes applied: {len(fixes_applied)}, issues potentially fixed: {issues_fixed}")

        return SelfHealResult(
            success=True,
            fixed_code=fixed_code,
            fixes_applied=fixes_applied,
            issues_fixed=issues_fixed
        )

    except Exception as e:
        error_msg = f"Exception during simple code fixes: {e}"
        logger.error(error_msg)
        return SelfHealResult(
            success=False,
            fixed_code=code,
            fixes_applied=[],
            issues_fixed=0,
            error_message=error_msg
        )


# ============================================================================
# Validation Integration
# ============================================================================

def normalize_sql(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> str:
    """
    Normalize SQL by removing comments and converting to uppercase.

    Args:
        sql: SQL string to normalize
        logger_callback: Optional callback for logging messages

    Returns:
        Normalized SQL string
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Normalizing SQL...")
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    logger_callback("SQL normalized successfully")
    return sql.upper()


def extract_statements(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> Dict[str, int]:
    """
    Extract and count SQL statement types.

    Args:
        sql: SQL string to analyze
        logger_callback: Optional callback for logging messages

    Returns:
        Dictionary with statement type counts
    """
    if logger_callback is None:
        logger_callback = logger.info

    from collections import Counter

    logger_callback("Extracting SQL statements...")
    keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE', 'DROP', 'CALL', 'EXEC', 'TRUNCATE']
    counts = Counter()
    for kw in keywords:
        pattern = r'\b{}\b'.format(kw)
        counts[kw] = len(re.findall(pattern, sql))
    logger_callback("SQL statements extracted successfully")
    return dict(counts)


def extract_tables(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> set:
    """
    Extract table references from SQL.

    Args:
        sql: SQL string to analyze
        logger_callback: Optional callback for logging messages

    Returns:
        Set of table names
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Extracting tables...")
    table_pattern = r'\b(?:FROM|JOIN|INTO|UPDATE|MERGE\s+INTO|DELETE\s+FROM)\s+([A-Z0-9_.]+)'
    tables = set(re.findall(table_pattern, sql))
    logger.info("Tables extracted successfully")
    return tables


def extract_columns(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> set:
    """
    Extract column references from SQL.

    Args:
        sql: SQL string to analyze
        logger_callback: Optional callback for logging messages

    Returns:
        Set of column names
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Extracting columns...")
    col_patterns = [
        r'SELECT\s+(.*?)\s+FROM',
        r'INSERT\s+INTO\s+[A-Z0-9_.]+\s*\((.*?)\)',
        r'UPDATE\s+[A-Z0-9_.]+\s+SET\s+(.*?)\s+(?:WHERE|;)',
        r'ON\s+(.*?)\s+(?:AND|OR|WHERE|;)',
        r'WHERE\s+(.*?)\s+(?:GROUP|ORDER|HAVING|UNION|;)',
    ]
    columns = set()
    for pat in col_patterns:
        for match in re.findall(pat, sql, flags=re.DOTALL):
            for col in re.split(r',|\s|\(|\)|=|<|>!', match):
                col = col.strip()
                if col and not col.upper() in ['AND', 'OR', 'NOT', 'NULL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'DISTINCT', 'COUNT', 'SUM', 'MIN', 'MAX', 'AVG']:
                    if '.' in col:
                        col = col.split('.')[-1]
                    col = re.sub(r'\(.*\)', '', col)
                    if col and re.match(r'^[A-Z0-9_]+$', col):
                        columns.add(col)
    logger_callback("Columns successfully extracted")
    return columns


def extract_procedure_calls(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> set:
    """
    Extract procedure/function calls from SQL.

    Args:
        sql: SQL string to analyze
        logger_callback: Optional callback for logging messages

    Returns:
        Set of procedure names
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Extracting procedure calls...")
    proc_pattern = r'\b(?:CALL|EXEC(?:UTE)?)\s+([A-Z0-9_.]+)'
    procedures = set(re.findall(proc_pattern, sql))
    logger_callback("Extracted procedure calls successfully")
    return procedures


def analyze_code(code: str, logger_callback: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """
    Analyze SQL code and extract metadata.

    Args:
        code: SQL code to analyze
        logger_callback: Optional callback for logging messages

    Returns:
        Dictionary containing analysis results
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Analyzing code...")
    sql = normalize_sql(code, logger_callback)
    return {
        'statements': extract_statements(sql, logger_callback),
        'tables': extract_tables(sql, logger_callback),
        'columns': extract_columns(sql, logger_callback),
        'procedures': extract_procedure_calls(sql, logger_callback)
    }


def compare_code_analysis(
    analysis1: Dict[str, Any],
    analysis2: Dict[str, Any],
    logger_callback: Optional[Callable[[str], None]] = None
) -> Dict[str, Any]:
    """
    Compare two code analyses and identify differences.

    Args:
        analysis1: First analysis result
        analysis2: Second analysis result
        logger_callback: Optional callback for logging messages

    Returns:
        Dictionary containing comparison results
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Comparing code analyses...")

    def fmt_set(s): return ', '.join(sorted(s)) if s else '(none)'

    issues = []

    # Compare statements
    for k in sorted(set(analysis1['statements']) | set(analysis2['statements'])):
        v1 = analysis1['statements'].get(k, 0)
        v2 = analysis2['statements'].get(k, 0)
        if v1 != v2:
            issues.append({
                "type": "statement_count_mismatch",
                "statement": k,
                "count1": v1,
                "count2": v2,
                "message": f"{k} count is {'higher' if v1 > v2 else 'lower'} in analysis 1"
            })

    # Compare tables
    tables1 = analysis1['tables']
    tables2 = analysis2['tables']
    missing_tables = tables1 - tables2
    extra_tables = tables2 - tables1
    if missing_tables:
        issues.append({
            "type": "missing_tables",
            "tables": list(missing_tables),
            "message": f"Tables in analysis 1 but not in analysis 2: {fmt_set(missing_tables)}"
        })
    if extra_tables:
        issues.append({
            "type": "extra_tables",
            "tables": list(extra_tables),
            "message": f"Tables in analysis 2 but not in analysis 1: {fmt_set(extra_tables)}"
        })

    # Compare columns
    columns1 = analysis1['columns']
    columns2 = analysis2['columns']
    missing_columns = columns1 - columns2
    extra_columns = columns2 - columns1
    if missing_columns:
        issues.append({
            "type": "missing_columns",
            "columns": list(missing_columns),
            "message": f"Columns in analysis 1 but not in analysis 2: {fmt_set(missing_columns)}"
        })
    if extra_columns:
        issues.append({
            "type": "extra_columns",
            "columns": list(extra_columns),
            "message": f"Columns in analysis 2 but not in analysis 1: {fmt_set(extra_columns)}"
        })

    # Compare procedures
    procedures1 = analysis1['procedures']
    procedures2 = analysis2['procedures']
    missing_procedures = procedures1 - procedures2
    extra_procedures = procedures2 - procedures1
    if missing_procedures:
        issues.append({
            "type": "missing_procedures",
            "procedures": list(missing_procedures),
            "message": f"Procedures in analysis 1 but not in analysis 2: {fmt_set(missing_procedures)}"
        })
    if extra_procedures:
        issues.append({
            "type": "extra_procedures",
            "procedures": list(extra_procedures),
            "message": f"Procedures in analysis 2 but not in analysis 1: {fmt_set(extra_procedures)}"
        })

    logger_callback(f"Comparison complete. Found {len(issues)} issues.")

    return {
        "issues": issues,
        "analysis1": analysis1,
        "analysis2": analysis2,
        "passed": len(issues) == 0
    }


def validate_code(
    code: str,
    original_code: Optional[str] = None,
    state: Optional[MigrationContext] = None,
    logger_callback: Optional[Callable[[str], None]] = None
) -> ValidationResult:
    """
    Validate converted code using line-count regression rule only.

    Args:
        code: The code to validate
        original_code: Optional original code for comparison
        state: Optional MigrationContext for Snowflake connection
        logger_callback: Optional callback for logging messages

    Returns:
        ValidationResult containing validation results
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Starting line-count validation...")
    issues: List[Dict[str, Any]] = []
    results: Dict[str, Any] = {}

    try:
        input_line_count: Optional[int] = None
        output_line_count: Optional[int] = None

        if state:
            input_line_count = _count_lines_from_files(state.source_files)
            output_line_count = _count_lines_from_files(state.converted_files)

        if input_line_count is None:
            baseline = original_code if original_code is not None else (state.original_code if state else "")
            input_line_count = _count_lines(baseline)

        if output_line_count is None:
            output_line_count = _count_lines(code)

        passed = output_line_count >= input_line_count
        results["line_count_validation"] = {
            "passed": passed,
            "input_line_count": input_line_count,
            "output_line_count": output_line_count,
        }

        if not passed:
            issues.append(
                {
                    "type": "line_count_regression",
                    "severity": "error",
                    "message": (
                        f"Output line count ({output_line_count}) is less than input line count ({input_line_count})."
                    ),
                    "input_line_count": input_line_count,
                    "output_line_count": output_line_count,
                }
            )

        logger_callback(
            f"Line-count validation complete. input={input_line_count}, output={output_line_count}, passed={passed}"
        )
        return ValidationResult(
            passed=passed,
            issues=issues,
            results=results,
        )
    except Exception as e:
        error_msg = f"Exception during validation: {e}"
        logger.error(error_msg)
        return ValidationResult(
            passed=False,
            issues=[{"type": "validation_error", "severity": "error", "message": error_msg}],
            results=results,
            error_message=error_msg,
        )


def validate_syntax(code: str, logger_callback: Optional[Callable[[str], None]] = None) -> List[Dict[str, Any]]:
    """
    Perform basic syntax validation on SQL code.

    Args:
        code: SQL code to validate
        logger_callback: Optional callback for logging messages

    Returns:
        List of syntax issues found
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Performing syntax validation...")
    issues = []

    # Check for balanced parentheses
    open_parens = code.count('(')
    close_parens = code.count(')')
    if open_parens != close_parens:
        issues.append({
            "type": "syntax_error",
            "severity": "error",
            "message": f"Unbalanced parentheses: {open_parens} opening, {close_parens} closing"
        })

    # Check for balanced quotes
    single_quotes = code.count("'")
    if single_quotes % 2 != 0:
        issues.append({
            "type": "syntax_error",
            "severity": "error",
            "message": "Unbalanced single quotes"
        })

    # Check for common Teradata syntax that needs conversion
    teradata_patterns = [
        (r'\bQUALIFY\s+', "QUALIFY clause may need review for Snowflake"),
        (r'\bWITH\s+DATA\b', "WITH DATA clause not supported in Snowflake"),
        (r'\bCREATE\s+MULTISET\s+TABLE\b', "MULTISET TABLE not supported in Snowflake"),
        (r'\bCREATE\s+VOLATILE\s+TABLE\b', "VOLATILE TABLE not supported in Snowflake"),
    ]

    for pattern, message in teradata_patterns:
        if re.search(pattern, code, re.IGNORECASE):
            issues.append({
                "type": "syntax_warning",
                "severity": "warning",
                "message": message
            })

    logger_callback(f"Syntax validation complete. Found {len(issues)} issues.")
    return issues


def validate_snowflake_compilation(
    code: str,
    state: MigrationContext,
    logger_callback: Optional[Callable[[str], None]] = None
) -> List[Dict[str, Any]]:
    """
    Validate code by attempting to compile it in Snowflake.

    Args:
        code: SQL code to validate
        state: MigrationContext containing Snowflake connection parameters
        logger_callback: Optional callback for logging messages

    Returns:
        List of compilation issues found
    """
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Performing Snowflake compilation validation...")
    issues = []

    try:
        from snowflake.snowpark.exceptions import SnowparkSQLException

        sf_session = get_snowflake_session(state)
        if sf_session is None:
            issues.append({
                "type": "connection_error",
                "severity": "error",
                "message": "Failed to create Snowflake session for compilation check"
            })
            return issues

        # Try to execute the code (this will fail if there are syntax errors)
        # Note: We wrap in a transaction and rollback to avoid side effects
        try:
            sf_session.sql("BEGIN").collect()
            sf_session.sql(code).collect()
            sf_session.sql("ROLLBACK").collect()
            logger_callback("Snowflake compilation successful")
        except SnowparkSQLException as e:
            issues.append({
                "type": "compilation_error",
                "severity": "error",
                "message": str(e)
            })
            logger_callback(f"Snowflake compilation error: {e}")

    except ImportError as e:
        logger_callback(f"Snowpark not available for compilation check: {e}")
        issues.append({
            "type": "dependency_error",
            "severity": "warning",
            "message": f"Snowpark not available: {e}"
        })

    except Exception as e:
        logger_callback(f"Exception during Snowflake validation: {e}")
        issues.append({
            "type": "validation_error",
            "severity": "error",
            "message": str(e)
        })

    return issues


# ============================================================================
# Utility Functions
# ============================================================================

def format_validation_report(validation_result: ValidationResult) -> str:
    """
    Format a validation result as a readable report.

    Args:
        validation_result: ValidationResult to format

    Returns:
        Formatted report string
    """
    lines = [
        "Validation Report",
        f"Timestamp: {validation_result.timestamp}",
        f"Passed: {validation_result.passed}",
        f"Issues Found: {len(validation_result.issues)}",
        "",
    ]

    if validation_result.issues:
        lines.append("ISSUES:")
        for i, issue in enumerate(validation_result.issues, 1):
            lines.append(f"{i}. [{issue.get('severity', 'info').upper()}] {issue.get('type', 'unknown')}")
            lines.append(f"   {issue.get('message', 'No message')}")
        lines.append("")

    if validation_result.results:
        lines.append("RESULTS:")
        for key, value in validation_result.results.items():
            if isinstance(value, dict) and "passed" in value:
                status = "PASSED" if value["passed"] else "FAILED"
                lines.append(f"{key}: {status}")
                if value.get("issues"):
                    for issue in value["issues"]:
                        lines.append(f"  - {issue}")
            else:
                lines.append(f"{key}: {value}")

    return "\n".join(lines)


def format_self_heal_report(self_heal_result: SelfHealResult) -> str:
    """
    Format a self-heal result as a readable report.

    Args:
        self_heal_result: SelfHealResult to format

    Returns:
        Formatted report string
    """
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
