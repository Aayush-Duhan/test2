"""Runtime helpers for Snowflake SQL execution."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Tuple

from agentic_core.models.context import MigrationContext

logger = logging.getLogger(__name__)


class SQLExecutionError(Exception):
    """Execution error containing statement-level context."""

    def __init__(
        self,
        message: str,
        statement: str,
        statement_index: int,
        partial_results: List[Dict[str, Any]],
    ) -> None:
        super().__init__(message)
        self.statement = statement
        self.statement_index = statement_index
        self.partial_results = partial_results


def build_snowflake_connection(
    state: MigrationContext,
) -> Any:
    """Build a Snowflake connection from MigrationContext."""
    import snowflake.connector

    sf_account = (state.sf_account or "").strip()
    sf_user = (state.sf_user or "").strip()

    if not sf_account:
        raise ValueError("Snowflake account is required. Please provide it in the connection settings.")
    if not sf_user:
        raise ValueError("Snowflake user is required. Please provide it in the connection settings.")

    sf_role = (state.sf_role or "").strip() or None
    sf_warehouse = (state.sf_warehouse or "").strip() or None
    sf_database = (state.sf_database or "").strip() or None
    sf_schema = (state.sf_schema or "").strip() or None
    sf_authenticator = (state.sf_authenticator or "").strip() or "externalbrowser"

    return snowflake.connector.connect(
        account=sf_account,
        user=sf_user,
        role=sf_role,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        authenticator=sf_authenticator,
        client_store_temporary_credential=True,
    )


def split_sql_statements(sql_text: str) -> List[str]:
    """Split SQL into statements while respecting quoted strings."""
    statements: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    in_dollar = False
    prev = ""
    idx = 0

    while idx < len(sql_text):
        char = sql_text[idx]
        nxt = sql_text[idx + 1] if idx + 1 < len(sql_text) else ""

        if not in_single and not in_double and char == "$" and nxt == "$":
            in_dollar = not in_dollar
            buf.append(char)
            buf.append(nxt)
            idx += 2
            prev = ""
            continue

        if char == "'" and not in_double and prev != "\\":
            in_single = not in_single if not in_dollar else in_single
        elif char == '"' and not in_single and prev != "\\":
            in_double = not in_double if not in_dollar else in_double

        if char == ";" and not in_single and not in_double and not in_dollar:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            idx += 1
            continue

        buf.append(char)
        prev = char
        idx += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def classify_snowflake_error(error_message: str) -> Tuple[str, str]:
    """Classify execution error type and extract likely missing object name."""
    lowered = (error_message or "").lower()
    missing_patterns = [
        "does not exist or not authorized",
        "does not exist",
        "object does not exist",
        "table does not exist",
        "schema does not exist",
    ]
    if any(pattern in lowered for pattern in missing_patterns):
        object_name = ""
        for token in ("Object '", "object '", "Table '", "table '", '"'):
            if token in (error_message or ""):
                try:
                    start = (error_message or "").index(token) + len(token)
                    end = (error_message or "").index("'", start)
                    object_name = (error_message or "")[start:end]
                    break
                except ValueError:
                    continue
        return "missing_object", object_name
    return "execution_error", ""


def execute_sql_statements(
    connection,
    sql_text: str,
    on_statement: Callable[[Dict[str, Any]], None] | None = None,
) -> List[Dict[str, Any]]:
    """Execute SQL text statement-by-statement via snowflake.connector."""
    results: List[Dict[str, Any]] = []
    statements = split_sql_statements(sql_text)
    cursor = connection.cursor()
    try:
        for idx, statement in enumerate(statements):
            try:
                cursor.execute(statement)
                rows = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description] if cursor.description else []
                preview_rows: List[Any] = []
                for row in rows[:5]:
                    if col_names:
                        preview_rows.append(dict(zip(col_names, row)))
                    else:
                        preview_rows.append(str(row))
                result_entry = {
                    "statement_index": idx,
                    "status": "success",
                    "statement": statement,
                    "row_count": cursor.rowcount,
                    "output_preview": preview_rows,
                }
                results.append(result_entry)
                if on_statement is not None:
                    try:
                        on_statement(result_entry)
                    except Exception:
                        pass
            except Exception as exc:
                raise SQLExecutionError(
                    message=str(exc),
                    statement=statement,
                    statement_index=idx,
                    partial_results=results,
                ) from exc
    finally:
        cursor.close()
    return results


def close_connection(connection) -> None:
    """Close Snowflake connection if available."""
    try:
        if connection is not None:
            connection.close()
    except Exception as exc:
        logger.warning("Failed to close Snowflake connection cleanly: %s", exc)
