"""Validation helpers for converted SQL."""

from __future__ import annotations

import logging
import os
import re
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Set

from agentic_core.models.context import MigrationContext
from agentic_core.models.results import ValidationResult
from agentic_core.runtime.snowflake_session import get_snowflake_session

logger = logging.getLogger(__name__)


def _count_lines(text: str) -> int:
    return len(text.splitlines()) if text else 0


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


def normalize_sql(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> str:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Normalizing SQL...")
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    logger_callback("SQL normalized successfully")
    return sql.upper()


def extract_statements(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> Dict[str, int]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Extracting SQL statements...")
    keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "CALL", "EXEC", "TRUNCATE"]
    counts = Counter()
    for keyword in keywords:
        counts[keyword] = len(re.findall(rf"\b{keyword}\b", sql))
    logger_callback("SQL statements extracted successfully")
    return dict(counts)


def extract_tables(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> Set[str]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Extracting tables...")
    table_pattern = r"\b(?:FROM|JOIN|INTO|UPDATE|MERGE\s+INTO|DELETE\s+FROM)\s+([A-Z0-9_.]+)"
    tables = set(re.findall(table_pattern, sql))
    logger_callback("Tables extracted successfully")
    return tables


def extract_columns(sql: str, logger_callback: Optional[Callable[[str], None]] = None) -> Set[str]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Extracting columns...")
    col_patterns = [
        r"SELECT\s+(.*?)\s+FROM",
        r"INSERT\s+INTO\s+[A-Z0-9_.]+\s*\((.*?)\)",
        r"UPDATE\s+[A-Z0-9_.]+\s+SET\s+(.*?)\s+(?:WHERE|;)",
        r"ON\s+(.*?)\s+(?:AND|OR|WHERE|;)",
        r"WHERE\s+(.*?)\s+(?:GROUP|ORDER|HAVING|UNION|;)",
    ]
    columns: Set[str] = set()
    ignored = {
        "AND",
        "OR",
        "NOT",
        "NULL",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "IN",
        "EXISTS",
        "BETWEEN",
        "LIKE",
        "IS",
        "DISTINCT",
        "COUNT",
        "SUM",
        "MIN",
        "MAX",
        "AVG",
    }
    for pattern in col_patterns:
        for match in re.findall(pattern, sql, flags=re.DOTALL):
            for col in re.split(r",|\s|\(|\)|=|<|>!", match):
                col = col.strip()
                if not col or col.upper() in ignored:
                    continue
                if "." in col:
                    col = col.split(".")[-1]
                col = re.sub(r"\(.*\)", "", col)
                if col and re.match(r"^[A-Z0-9_]+$", col):
                    columns.add(col)
    logger_callback("Columns successfully extracted")
    return columns


def extract_procedure_calls(
    sql: str,
    logger_callback: Optional[Callable[[str], None]] = None,
) -> Set[str]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Extracting procedure calls...")
    procedures = set(re.findall(r"\b(?:CALL|EXEC(?:UTE)?)\s+([A-Z0-9_.]+)", sql))
    logger_callback("Extracted procedure calls successfully")
    return procedures


def analyze_code(code: str, logger_callback: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Analyzing code...")
    sql = normalize_sql(code, logger_callback)
    return {
        "statements": extract_statements(sql, logger_callback),
        "tables": extract_tables(sql, logger_callback),
        "columns": extract_columns(sql, logger_callback),
        "procedures": extract_procedure_calls(sql, logger_callback),
    }


def compare_code_analysis(
    analysis1: Dict[str, Any],
    analysis2: Dict[str, Any],
    logger_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Comparing code analyses...")
    issues: List[Dict[str, Any]] = []

    def fmt_set(values: Set[str]) -> str:
        return ", ".join(sorted(values)) if values else "(none)"

    for key in sorted(set(analysis1["statements"]) | set(analysis2["statements"])):
        value1 = analysis1["statements"].get(key, 0)
        value2 = analysis2["statements"].get(key, 0)
        if value1 != value2:
            issues.append(
                {
                    "type": "statement_count_mismatch",
                    "statement": key,
                    "count1": value1,
                    "count2": value2,
                    "message": f"{key} count is {'higher' if value1 > value2 else 'lower'} in analysis 1",
                }
            )

    tables1 = analysis1["tables"]
    tables2 = analysis2["tables"]
    missing_tables = tables1 - tables2
    extra_tables = tables2 - tables1
    if missing_tables:
        issues.append(
            {
                "type": "missing_tables",
                "tables": list(missing_tables),
                "message": f"Tables in analysis 1 but not in analysis 2: {fmt_set(missing_tables)}",
            }
        )
    if extra_tables:
        issues.append(
            {
                "type": "extra_tables",
                "tables": list(extra_tables),
                "message": f"Tables in analysis 2 but not in analysis 1: {fmt_set(extra_tables)}",
            }
        )

    columns1 = analysis1["columns"]
    columns2 = analysis2["columns"]
    missing_columns = columns1 - columns2
    extra_columns = columns2 - columns1
    if missing_columns:
        issues.append(
            {
                "type": "missing_columns",
                "columns": list(missing_columns),
                "message": f"Columns in analysis 1 but not in analysis 2: {fmt_set(missing_columns)}",
            }
        )
    if extra_columns:
        issues.append(
            {
                "type": "extra_columns",
                "columns": list(extra_columns),
                "message": f"Columns in analysis 2 but not in analysis 1: {fmt_set(extra_columns)}",
            }
        )

    procedures1 = analysis1["procedures"]
    procedures2 = analysis2["procedures"]
    missing_procedures = procedures1 - procedures2
    extra_procedures = procedures2 - procedures1
    if missing_procedures:
        issues.append(
            {
                "type": "missing_procedures",
                "procedures": list(missing_procedures),
                "message": f"Procedures in analysis 1 but not in analysis 2: {fmt_set(missing_procedures)}",
            }
        )
    if extra_procedures:
        issues.append(
            {
                "type": "extra_procedures",
                "procedures": list(extra_procedures),
                "message": f"Procedures in analysis 2 but not in analysis 1: {fmt_set(extra_procedures)}",
            }
        )

    logger_callback(f"Comparison complete. Found {len(issues)} issues.")
    return {
        "issues": issues,
        "analysis1": analysis1,
        "analysis2": analysis2,
        "passed": len(issues) == 0,
    }


def validate_code(
    code: str,
    original_code: Optional[str] = None,
    state: Optional[MigrationContext] = None,
    logger_callback: Optional[Callable[[str], None]] = None,
) -> ValidationResult:
    """Validate converted code using line-count regression rule only."""
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
                    "message": f"Output line count ({output_line_count}) is less than input line count ({input_line_count}).",
                    "input_line_count": input_line_count,
                    "output_line_count": output_line_count,
                }
            )

        logger_callback(
            f"Line-count validation complete. input={input_line_count}, output={output_line_count}, passed={passed}"
        )
        return ValidationResult(passed=passed, issues=issues, results=results)
    except Exception as exc:
        error_msg = f"Exception during validation: {exc}"
        logger.error(error_msg)
        return ValidationResult(
            passed=False,
            issues=[{"type": "validation_error", "severity": "error", "message": error_msg}],
            results=results,
            error_message=error_msg,
        )


def validate_syntax(code: str, logger_callback: Optional[Callable[[str], None]] = None) -> List[Dict[str, Any]]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Performing syntax validation...")
    issues: List[Dict[str, Any]] = []

    open_parens = code.count("(")
    close_parens = code.count(")")
    if open_parens != close_parens:
        issues.append(
            {
                "type": "syntax_error",
                "severity": "error",
                "message": f"Unbalanced parentheses: {open_parens} opening, {close_parens} closing",
            }
        )

    if code.count("'") % 2 != 0:
        issues.append({"type": "syntax_error", "severity": "error", "message": "Unbalanced single quotes"})

    teradata_patterns = [
        (r"\bQUALIFY\s+", "QUALIFY clause may need review for Snowflake"),
        (r"\bWITH\s+DATA\b", "WITH DATA clause not supported in Snowflake"),
        (r"\bCREATE\s+MULTISET\s+TABLE\b", "MULTISET TABLE not supported in Snowflake"),
        (r"\bCREATE\s+VOLATILE\s+TABLE\b", "VOLATILE TABLE not supported in Snowflake"),
    ]
    for pattern, message in teradata_patterns:
        if re.search(pattern, code, re.IGNORECASE):
            issues.append({"type": "syntax_warning", "severity": "warning", "message": message})

    logger_callback(f"Syntax validation complete. Found {len(issues)} issues.")
    return issues


def validate_snowflake_compilation(
    code: str,
    state: MigrationContext,
    logger_callback: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, Any]]:
    if logger_callback is None:
        logger_callback = logger.info

    logger_callback("Performing Snowflake compilation validation...")
    issues: List[Dict[str, Any]] = []

    try:
        from snowflake.snowpark.exceptions import SnowparkSQLException

        sf_session = get_snowflake_session(state)
        if sf_session is None:
            issues.append(
                {
                    "type": "connection_error",
                    "severity": "error",
                    "message": "Failed to create Snowflake session for compilation check",
                }
            )
            return issues

        try:
            sf_session.sql("BEGIN").collect()
            sf_session.sql(code).collect()
            sf_session.sql("ROLLBACK").collect()
            logger_callback("Snowflake compilation successful")
        except SnowparkSQLException as exc:
            issues.append({"type": "compilation_error", "severity": "error", "message": str(exc)})
            logger_callback(f"Snowflake compilation error: {exc}")
    except ImportError as exc:
        logger_callback(f"Snowpark not available for compilation check: {exc}")
        issues.append({"type": "dependency_error", "severity": "warning", "message": f"Snowpark not available: {exc}"})
    except Exception as exc:
        logger_callback(f"Exception during Snowflake validation: {exc}")
        issues.append({"type": "validation_error", "severity": "error", "message": str(exc)})

    return issues


def format_validation_report(validation_result: ValidationResult) -> str:
    """Format a validation result as a readable report."""
    lines = [
        "Validation Report",
        f"Timestamp: {validation_result.timestamp}",
        f"Passed: {validation_result.passed}",
        f"Issues Found: {len(validation_result.issues)}",
        "",
    ]

    if validation_result.issues:
        lines.append("ISSUES:")
        for index, issue in enumerate(validation_result.issues, 1):
            lines.append(f"{index}. [{issue.get('severity', 'info').upper()}] {issue.get('type', 'unknown')}")
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
