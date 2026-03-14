"""Shared Snowflake authentication helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class SnowflakeAuthConfig:
    account: str
    user: str
    role: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""
    authenticator: str = "externalbrowser"

    def to_connection_parameters(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "authenticator": self.authenticator,
        }
        if self.role:
            params["role"] = self.role
        if self.warehouse:
            params["warehouse"] = self.warehouse
        if self.database:
            params["database"] = self.database
        if self.schema:
            params["schema"] = self.schema
        return params


def create_snowpark_session(config: SnowflakeAuthConfig) -> "Session":
    """Create a Snowpark Session using the configured Snowflake authenticator."""
    from snowflake.snowpark import Session

    return Session.builder.configs(config.to_connection_parameters()).create()
