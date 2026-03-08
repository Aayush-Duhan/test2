"""Structured activity log helpers."""

from datetime import datetime
from typing import Any, Dict, Optional

from agentic_core.models.context import MigrationContext


def log_event(
    state: MigrationContext,
    level: str,
    message: str,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """Append a structured activity log entry to the migration context."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message,
        "stage": state.current_stage.value if state.current_stage else None,
    }
    if data:
        entry["data"] = data
    state.activity_log.append(entry)
    sink = getattr(state, "activity_log_sink", None)
    if callable(sink):
        try:
            sink(entry)
        except Exception:
            pass
