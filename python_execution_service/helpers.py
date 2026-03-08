"""Utility / helper functions: serialization, logging, auth, run-record management."""

import json
import logging
import re
import threading
import time
import uuid
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from python_execution_service import sqlite_store
from python_execution_service.config import (
    CANCEL_FLAGS,
    EXECUTION_TOKEN,
    RUN_LOCK,
    RUNS,
    STEP_LABELS,
    THINKING_STEP_IDS,
)
from python_execution_service.models import RunRecord, RunStep, StartRunRequest

logger = logging.getLogger(__name__)

_PERSIST_INTERVAL = 2.0
_last_persist_time: float = 0.0
_persist_dirty: bool = False


# ── Time helpers ────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.utcnow().isoformat()


# ── Serialization / deserialization ─────────────────────────────

def _serialize_run_record(run: RunRecord) -> dict[str, Any]:
    return asdict(run)


def _deserialize_run_record(payload: dict[str, Any]) -> RunRecord:
    steps = [RunStep(**step) for step in payload.get("steps", [])]
    return RunRecord(
        runId=payload["runId"],
        projectId=payload["projectId"],
        projectName=payload["projectName"],
        sourceId=payload.get("sourceId", ""),
        schemaId=payload.get("schemaId", ""),
        sourceLanguage=payload.get("sourceLanguage", "teradata"),
        sourcePath=payload.get("sourcePath", ""),
        schemaPath=payload.get("schemaPath", ""),
        sfAccount=payload.get("sfAccount"),
        sfUser=payload.get("sfUser"),
        sfRole=payload.get("sfRole"),
        sfWarehouse=payload.get("sfWarehouse"),
        sfDatabase=payload.get("sfDatabase"),
        sfSchema=payload.get("sfSchema"),
        sfAuthenticator=payload.get("sfAuthenticator"),
        status=payload.get("status", "failed"),
        createdAt=payload.get("createdAt", now_iso()),
        updatedAt=payload.get("updatedAt", now_iso()),
        steps=steps,
        logs=payload.get("logs", []),
        validationIssues=payload.get("validationIssues", []),
        executionLog=payload.get("executionLog", []),
        executionErrors=payload.get("executionErrors", []),
        missingObjects=payload.get("missingObjects", []),
        requiresDdlUpload=payload.get("requiresDdlUpload", False),
        resumeFromStage=payload.get("resumeFromStage", ""),
        lastExecutedFileIndex=int(payload.get("lastExecutedFileIndex", -1)),
        selfHealIteration=int(payload.get("selfHealIteration", 0)),
        error=payload.get("error"),
        outputDir=payload.get("outputDir", ""),
        ddlUploadPath=payload.get("ddlUploadPath", ""),
        executionEventCursor=int(payload.get("executionEventCursor", 0)),
        events=payload.get("events", []),
        messages=payload.get("messages", []),
    )


# ── Persistence ─────────────────────────────────────────────────

def persist_runs_locked(*, force: bool = False) -> None:
    """Must be called while holding RUN_LOCK.

    When *force* is False (the default for high-frequency callers), the actual
    sqlite write is throttled to at most once per ``_PERSIST_INTERVAL`` seconds.
    The in-memory state is always authoritative; the SSE stream reads from
    ``run.events`` directly, so throttling persistence does not delay events
    reaching the frontend.
    """
    global _last_persist_time, _persist_dirty
    now = time.monotonic()
    _persist_dirty = True
    if not force and (now - _last_persist_time) < _PERSIST_INTERVAL:
        return
    _persist_dirty = False
    _last_persist_time = now
    for run in RUNS.values():
        try:
            sqlite_store.save_run_snapshot(_serialize_run_record(run))
        except Exception as exc:
            logger.warning("Failed to persist run %s: %s", run.runId, exc)


def flush_persist_if_dirty() -> None:
    """Force a persist if any writes were deferred by throttling."""
    global _persist_dirty
    with RUN_LOCK:
        if _persist_dirty:
            persist_runs_locked(force=True)


def load_persisted_runs() -> None:
    try:
        payload = sqlite_store.list_runs()
    except Exception as exc:
        logger.warning("Failed to load persisted runs from sqlite: %s", exc)
        return
    now = now_iso()
    with RUN_LOCK:
        for item in payload:
            if not isinstance(item, dict) or "runId" not in item:
                continue
            run = _deserialize_run_record(item)
            if run.status in ("queued", "running"):
                run.status = "failed"
                run.error = "service_restarted"
                run.updatedAt = now
                for step in run.steps:
                    if step.status == "running":
                        step.status = "failed"
                        step.endedAt = now
            RUNS[run.runId] = run
            CANCEL_FLAGS[run.runId] = threading.Event()
        if RUNS:
            persist_runs_locked(force=True)


# ── Auth ────────────────────────────────────────────────────────

def require_auth(x_execution_token: str | None) -> None:
    if x_execution_token != EXECUTION_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Event / message / log helpers ───────────────────────────────

def append_event(run: RunRecord, event_type: str, payload: dict[str, Any]) -> None:
    event = {"type": event_type, "payload": payload, "timestamp": now_iso()}
    with RUN_LOCK:
        run.events.append(event)
        run.updatedAt = event["timestamp"]
        persist_runs_locked()
    try:
        sqlite_store.append_run_event(run.runId, event_type, payload, event["timestamp"])
    except Exception as exc:
        logger.warning("Failed to append event for run %s: %s", run.runId, exc)
    events_file = Path(run.outputDir) / "events.jsonl"
    events_file.parent.mkdir(parents=True, exist_ok=True)
    with events_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")


def _strip_log_tags(message: str) -> str:
    return re.sub(r"^\s*(?:\[[^\]]+\]\s*)+", "", message).strip()


def _clean_terminal_output(message: str) -> str:
    ansi_stripped = re.sub(r"\u001b\[[0-?]*[ -/]*[@-~]", "", message)
    lines: list[str] = []
    for raw_line in ansi_stripped.splitlines():
        line = raw_line
        line = re.sub(r"[¿´³]", " ", line)
        line = re.sub(r"[\u2500-\u257f\u2580-\u259f]", " ", line)
        line = re.sub(r"[\u00c0-\u00ff]", " ", line)
        line = re.sub(r"[=]{3,}", " ", line)
        line = re.sub(r"[?]{5,}", " ", line)
        line = re.sub(r"\s{2,}", " ", line).strip()
        if not line:
            continue
        if re.fullmatch(r"[=\-_*~.#|:+`^]+", line):
            continue
        lines.append(line)
    return "\n".join(lines)


def _sanitize_content(message: str, *, strip_prefix: bool = True) -> str:
    text = str(message)
    if strip_prefix:
        text = _strip_log_tags(text)
    return _clean_terminal_output(text)


def append_chat_message(
    run: RunRecord,
    *,
    role: str,
    kind: str,
    content: str,
    step: dict[str, str] | None = None,
    sql: dict[str, str] | None = None,
    ts: str | None = None,
) -> dict[str, Any]:
    timestamp = ts or now_iso()
    cleaned_content = _sanitize_content(content)
    message: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "ts": timestamp,
        "role": role,
        "kind": kind,
        "content": cleaned_content,
    }

    if step:
        message["step"] = step

    if sql:
        cleaned_sql = {
            key: _sanitize_content(value, strip_prefix=False)
            for key, value in sql.items()
            if isinstance(value, str) and _sanitize_content(value, strip_prefix=False)
        }
        if cleaned_sql:
            message["sql"] = cleaned_sql

    with RUN_LOCK:
        run.messages.append(message)
        run.updatedAt = timestamp
        persist_runs_locked()
    try:
        sqlite_store.append_run_message(run.runId, message)
    except Exception as exc:
        logger.warning("Failed to append message for run %s: %s", run.runId, exc)
    append_event(run, "chat:message", message)
    return message


def format_activity_log_entry(entry: dict[str, Any]) -> str:
    message = entry.get("message") or ""
    header = str(message).strip()
    data = entry.get("data")
    if not data:
        return header

    def stringify_value(value: Any) -> str:
        if isinstance(value, str):
            return value.rstrip()
        try:
            return json.dumps(value, indent=2, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    if isinstance(data, dict):
        lines: list[str] = []
        for key, value in data.items():
            text = stringify_value(value)
            if not text:
                lines.append(f"{key}:")
            elif "\n" in text:
                lines.append(f"{key}:\n{text}")
            else:
                lines.append(f"{key}: {text}")
        body = "\n".join(lines).rstrip()
        return f"{header}\n{body}" if body else header

    if isinstance(data, str):
        body = data.rstrip()
        return f"{header}\n{body}" if body else header

    body = stringify_value(data)
    return f"{header}\n{body}" if body else header


def update_step(run: RunRecord, step_id: str, status: str) -> None:
    current_time = now_iso()
    with RUN_LOCK:
        for step in run.steps:
            if step.id == step_id:
                step.status = status
                if status == "running":
                    step.startedAt = current_time
                if status in ("completed", "failed"):
                    step.endedAt = current_time
                run.updatedAt = current_time
                persist_runs_locked(force=True)
                return


def add_log(
    run: RunRecord,
    message: str,
    step_id: str | None = None,
    is_progress: bool = False,
) -> None:
    line = _sanitize_content(str(message), strip_prefix=False).strip()
    if not line:
        return
    created_at = now_iso()
    resolved_step_id = step_id if step_id in STEP_LABELS else None

    if is_progress:
        kind = "terminal_progress"
        append_event(run, "log", {"message": line, "is_progress": True})
        append_chat_message(
            run,
            role="agent",
            kind=kind,
            content=line,
            step=(
                {"id": resolved_step_id, "label": STEP_LABELS[resolved_step_id]}
                if resolved_step_id
                else None
            ),
        )
        return

    with RUN_LOCK:
        run.logs.append(line)
        run.updatedAt = created_at
        persist_runs_locked()
    try:
        sqlite_store.append_run_log(run.runId, line, created_at)
    except Exception as exc:
        logger.warning("Failed to append log for run %s: %s", run.runId, exc)
    append_event(run, "log", {"message": line})
    append_chat_message(
        run,
        role="agent",
        kind="thinking" if resolved_step_id in THINKING_STEP_IDS else "log",
        content=line,
        step=(
            {"id": resolved_step_id, "label": STEP_LABELS[resolved_step_id]}
            if resolved_step_id
            else None
        ),
    )


def set_run_status(run: RunRecord, status: str, error: str | None = None) -> None:
    with RUN_LOCK:
        run.status = status
        run.error = error
        run.updatedAt = now_iso()
        persist_runs_locked(force=True)


def get_steps_template() -> list[RunStep]:
    return [RunStep(id=step_id, label=label) for step_id, label in STEP_LABELS.items()]


def ensure_not_canceled(run_id: str) -> None:
    cancel_flag = CANCEL_FLAGS.get(run_id)
    if cancel_flag and cancel_flag.is_set():
        raise RuntimeError("Run canceled")


# ── Run-record factory helpers ──────────────────────────────────

def _sanitize_upload_filename(name: str) -> str:
    base = Path(name or "uploaded.ddl.sql").name
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in base)
    return safe or "uploaded.ddl.sql"


def _request_from_run(existing: RunRecord) -> StartRunRequest:
    return StartRunRequest(
        projectId=existing.projectId,
        projectName=existing.projectName,
        sourceId=existing.sourceId,
        schemaId=existing.schemaId,
        sourceLanguage=existing.sourceLanguage,
        sourcePath=existing.sourcePath,
        schemaPath=existing.schemaPath,
        sfAccount=existing.sfAccount,
        sfUser=existing.sfUser,
        sfRole=existing.sfRole,
        sfWarehouse=existing.sfWarehouse,
        sfDatabase=existing.sfDatabase,
        sfSchema=existing.sfSchema,
        sfAuthenticator=existing.sfAuthenticator,
    )
