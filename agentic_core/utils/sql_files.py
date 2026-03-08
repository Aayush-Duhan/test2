"""Helpers for reading SQL-like files from project folders."""

import logging
import os
from typing import List

logger = logging.getLogger(__name__)


def read_sql_files(directory: str) -> str:
    """Read SQL-like files from a directory and return concatenated contents."""
    if not directory or not os.path.isdir(directory):
        return ""

    contents: List[str] = []
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.lower().endswith((".sql", ".ddl", ".btq", ".txt")):
                file_path = os.path.join(root, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as handle:
                        contents.append(f"-- FILE: {filename}\n{handle.read()}\n")
                except Exception as exc:
                    logger.warning("Failed to read %s: %s", file_path, exc)
    return "\n".join(contents)


def list_sql_files(directory: str) -> List[str]:
    """Return sorted SQL-like file paths under a directory."""
    if not directory or not os.path.isdir(directory):
        return []

    sql_files: List[str] = []
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.lower().endswith((".sql", ".ddl", ".btq", ".txt")):
                sql_files.append(os.path.join(root, filename))
    sql_files.sort()
    return sql_files
