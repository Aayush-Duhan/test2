"""Global constants, shared mutable state, and configuration."""

import os
import threading
from pathlib import Path

from python_execution_service.models import RunRecord

EXECUTION_TOKEN = os.getenv("EXECUTION_TOKEN", "local-dev-token")
OUTPUT_ROOT = Path(os.getenv("PYTHON_EXEC_OUTPUT_ROOT", "outputs")).resolve()
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# ── Shared mutable state (guarded by RUN_LOCK) ─────────────────

RUN_LOCK = threading.RLock()
RUNS: dict[str, RunRecord] = {}
PROJECT_LOCKS: dict[str, str] = {}
CANCEL_FLAGS: dict[str, threading.Event] = {}

# ── Step / node metadata ────────────────────────────────────────

STEP_LABELS: dict[str, str] = {
    "init_project": "Initialize project",
    "add_source_code": "Ingest source SQL",
    "apply_schema_mapping": "Apply schema mapping",
    "convert_code": "Convert SQL",
    "execute_sql": "Execute SQL",
    "self_heal": "Self-heal fixes",
    "validate": "Validate output",
    "human_review": "Human review",
    "finalize": "Finalize output",
}

THINKING_STEP_IDS: set[str] = {"self_heal", "convert_code", "validate"}

# ── Agent configuration ─────────────────────────────────────────

AGENT_MODEL = os.getenv("SNOWFLAKE_CORTEX_AGENT_MODEL", "claude-4-sonnet")

# Per-run user message queues (thread-safe via RUN_LOCK)
USER_MESSAGE_QUEUES: dict[str, list[str]] = {}
