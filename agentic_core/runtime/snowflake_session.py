"""Snowpark session helpers."""

import logging

from agentic_core.models.context import MigrationContext
from agentic_core.runtime.snowflake_auth import (
    SnowflakeAuthConfig,
    create_snowpark_session,
    resolve_password_from_sources,
)

logger = logging.getLogger(__name__)


def get_snowflake_session(state: MigrationContext):
    """Create a Snowflake session from MigrationContext."""
    try:
        sf_account = (state.sf_account or "").strip()
        sf_user = (state.sf_user or "").strip()

        if not sf_account:
            logger.error("Snowflake account is required but not provided.")
            return None
        if not sf_user:
            logger.error("Snowflake user is required but not provided.")
            return None

        config = SnowflakeAuthConfig(
            account=sf_account,
            user=sf_user,
            role=(state.sf_role or "").strip() or None,
            warehouse=(state.sf_warehouse or "").strip() or None,
            database=(state.sf_database or "").strip() or None,
            schema=(state.sf_schema or "").strip() or None,
            authenticator=(state.sf_authenticator or "").strip() or "externalbrowser",
        )

        password = resolve_password_from_sources(
            authenticator=config.authenticator,
            explicit_password=None,
        )

        session = create_snowpark_session(config, password=password)
        logger.info("Snowflake session created successfully")
        return session
    except Exception as exc:
        logger.error("Failed to create Snowflake session: %s", exc)
        return None
