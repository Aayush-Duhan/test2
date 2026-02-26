"""
Runtime helpers for Snowflake SQL execution and LLM fixes.

This module centralizes all interactions through ChatSnowflakeCortex so both
SQL execution and LLM prompting use the same Snowflake session contract.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple, Any

from langchain_core.messages import HumanMessage

from .snowflake_auth import (
    SnowflakeAuthConfig,
    create_snowpark_session,
)
from .state import MigrationContext

logger = logging.getLogger(__name__)

MVP_SNOWFLAKE_CONNECTION = {
    "account": "EYGDS-LND_DNA_AZ_USE2",
    "user": "AAYUSH@CTPSANDBOX.COM",
    "authenticator": "externalbrowser",
    "role": "EY_DNA_SANDBOX_ROLE_DBMIG_POC_RW",
    "warehouse": "WH_DBMIG_POC_XS",
    "database": "DBMIG_POC",
    "schema": "DBMIG_POC",
}


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


def build_chat_snowflake_from_context(state: MigrationContext):
    """Build ChatSnowflakeCortex with an injected Snowpark session."""
    from langchain_community.chat_models.snowflake import ChatSnowflakeCortex

    sf_account = (state.sf_account or MVP_SNOWFLAKE_CONNECTION["account"]).strip()
    sf_user = (state.sf_user or MVP_SNOWFLAKE_CONNECTION["user"]).strip()
    sf_role = (state.sf_role or MVP_SNOWFLAKE_CONNECTION["role"]).strip()
    sf_warehouse = (state.sf_warehouse or MVP_SNOWFLAKE_CONNECTION["warehouse"]).strip()
    sf_database = (state.sf_database or MVP_SNOWFLAKE_CONNECTION["database"]).strip()
    sf_schema = (state.sf_schema or MVP_SNOWFLAKE_CONNECTION["schema"]).strip()
    sf_authenticator = (
        state.sf_authenticator or MVP_SNOWFLAKE_CONNECTION["authenticator"]
    ).strip() or "externalbrowser"

    config = SnowflakeAuthConfig(
        account=sf_account,
        user=sf_user,
        role=sf_role,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        authenticator=sf_authenticator,
    )
    session = create_snowpark_session(config, password=None)
    return ChatSnowflakeCortex(session=session)


def split_sql_statements(sql_text: str) -> List[str]:
    """
    Split SQL into statements while respecting quoted strings.

    This is a lightweight splitter that handles semicolons outside of single
    and double quotes, and respects $$...$$ blocks used in SQL procedures.
    """
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


def execute_sql_with_chat_runtime(chat_model, sql_text: str) -> List[Dict[str, Any]]:
    """Execute SQL text statement-by-statement via chat_model.session.sql."""
    results: List[Dict[str, Any]] = []
    statements = split_sql_statements(sql_text)
    for idx, statement in enumerate(statements):
        try:
            rows = chat_model.session.sql(statement).collect()
            preview_rows: List[Any] = []
            for row in rows[:5]:
                if hasattr(row, "as_dict"):
                    preview_rows.append(row.as_dict())
                else:
                    preview_rows.append(str(row))
            results.append(
                {
                    "statement_index": idx,
                    "status": "success",
                    "statement": statement,
                    "row_count": len(rows),
                    "output_preview": preview_rows,
                }
            )
        except Exception as exc:
            raise SQLExecutionError(
                message=str(exc),
                statement=statement,
                statement_index=idx,
                partial_results=results,
            ) from exc
    return results


def llm_fix_with_chat_runtime(chat_model, prompt: str) -> str:
    """Generate a fix using ChatSnowflakeCortex invoke path."""
    response = chat_model.invoke([HumanMessage(content=prompt)])
    return str(response.content or "").strip()


def close_runtime(chat_model) -> None:
    """Close underlying Snowflake session if available."""
    try:
        session = getattr(chat_model, "session", None)
        if session is not None:
            session.close()
    except Exception as exc:
        logger.warning("Failed to close runtime session cleanly: %s", exc)
