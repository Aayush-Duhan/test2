"""
Shared Snowflake authentication helpers.

This module centralizes Snowflake auth configuration and session creation.
It intentionally keeps secrets out of MigrationContext and persisted storage.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Mapping, Dict, Any


@dataclass(frozen=True)
class SnowflakeAuthConfig:
    account: str
    user: str
    role: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""
    authenticator: str = "externalbrowser"  # "externalbrowser" or "snowflake"

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


def resolve_password_from_sources(
    authenticator: str,
    explicit_password: Optional[str] = None,
    secrets: Optional[Mapping[str, Any]] = None,
    env: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """
    Resolve the Snowflake password from allowed sources.

    Priority:
      1) explicit_password (e.g., UI input in-memory)
      2) secrets["SNOWFLAKE_PASSWORD"] or secrets["snowflake"]["password"]
      3) environment variables

    Returns None when authenticator does not require a password.
    """
    if authenticator != "snowflake":
        return None

    if explicit_password:
        return explicit_password

    if secrets:
        # Flat secret
        if isinstance(secrets, Mapping) and "SNOWFLAKE_PASSWORD" in secrets:
            return str(secrets["SNOWFLAKE_PASSWORD"])
        # Nested secret
        snowflake_block = secrets.get("snowflake") if isinstance(secrets, Mapping) else None
        if isinstance(snowflake_block, Mapping) and "password" in snowflake_block:
            return str(snowflake_block["password"])

    env_map = env or os.environ
    for key in ("SNOWFLAKE_PASSWORD", "SNOWFLAKE_PWD"):
        if key in env_map and env_map[key]:
            return env_map[key]

    return None


def create_snowpark_session(
    config: SnowflakeAuthConfig,
    password: Optional[str] = None,
) -> "Session":
    """
    Create a Snowpark Session from the shared auth config.
    """
    from snowflake.snowpark import Session

    params = config.to_connection_parameters()
    if config.authenticator == "snowflake":
        if not password:
            raise ValueError("Password is required for Snowflake authenticator.")
        params["password"] = password

    return Session.builder.configs(params).create()