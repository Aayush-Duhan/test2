"""Schema mapping service wrappers."""


def process_sql_with_pandas_replace(*args, **kwargs):
    """Compatibility wrapper for schema mapping implementation."""
    from scripts.schema_conversion_teradata_to_snowflake import (
        process_sql_with_pandas_replace as implementation,
    )

    return implementation(*args, **kwargs)
