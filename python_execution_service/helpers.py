"""Utility / helper functions: serialization, logging, auth, run-record management."""

from __future__ import annotations

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
    USER_MESSAGE_QUEUES,
)
from python_execution_service.data_stream import (
    create_data_part,
    create_finish_part,
    create_reasoning_delta_part,
    create_reasoning_end_part,
    create_reasoning_start_part,
    create_start_part,
    create_text_delta_part,
    create_text_end_part,
    create_text_start_part,
    create_tool_input_available_part,
    create_tool_input_delta_part,
    create_tool_input_start_part,
    create_tool_output_available_part,
    generate_message_id,
    generate_reasoning_id,
    generate_text_id,
    generate_tool_call_id,
)
from python_execution_service.models import RunRecord, RunStep, StartRunRequest

logger = logging.getLogger(__name__)

_PERSIST_INTERVAL = 2.0
_last_persist_time: float = 0.0
_persist_dirty: bool = False


def now_iso() -> str:
    return datetime.utcnow().isoformat()


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
        streamParts=payload.get("streamParts", []),
        messages=payload.get("messages", []),
        outputDir=payload.get("outputDir", ""),
        ddlUploadPath=payload.get("ddlUploadPath", ""),
        userMessageQueue=payload.get("userMessageQueue", []),
        conversationHistory=payload.get("conversationHistory", []),
    )


def persist_runs_locked(*, force: bool = False) -> None:
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


def require_auth(x_execution_token: str | None) -> None:
    if x_execution_token != EXECUTION_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _append_stream_part_file(run: RunRecord, part: dict[str, Any]) -> None:
    stream_file = Path(run.outputDir) / "stream_parts.jsonl"
    stream_file.parent.mkdir(parents=True, exist_ok=True)
    with stream_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(part, ensure_ascii=False) + "\n")


def append_stream_part(run: RunRecord, part: dict[str, Any]) -> None:
    timestamp = now_iso()
    payload = dict(part)
    with RUN_LOCK:
        run.streamParts.append(payload)
        run.updatedAt = timestamp
        persist_runs_locked()
    _append_stream_part_file(run, payload)


def append_snapshot_message(
    run: RunRecord,
    *,
    role: str,
    kind: str,
    content: str,
    step: dict[str, str] | None = None,
    sql: dict[str, str] | None = None,
    ts: str | None = None,
    message_id: str | None = None,
) -> dict[str, Any]:
    timestamp = ts or now_iso()
    message: dict[str, Any] = {
        "id": message_id or str(uuid.uuid4()),
        "ts": timestamp,
        "role": role,
        "kind": kind,
        "content": content,
    }
    if step:
        message["step"] = step
    if sql:
        message["sql"] = sql

    with RUN_LOCK:
        run.messages.append(message)
        run.updatedAt = timestamp
        persist_runs_locked()
    try:
        sqlite_store.append_run_message(run.runId, message)
    except Exception as exc:
        logger.warning("Failed to append message for run %s: %s", run.runId, exc)
    return message


def _strip_log_tags(message: str) -> str:
    return re.sub(r"^\s*(?:\[[^\]]+\]\s*)+", "", message).strip()


def _clean_terminal_output(message: str) -> str:
    ansi_stripped = re.sub(r"\u001b\[[0-?]*[ -/]*[@-~]", "", message)
    lines: list[str] = []
    for raw_line in ansi_stripped.splitlines():
        line = raw_line
        line = re.sub(r"[Â¿Â´Â³]", " ", line)
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


def append_terminal_output(
    run: RunRecord,
    text: str,
    *,
    is_progress: bool = False,
    step_id: str | None = None,
) -> None:
    cleaned = _sanitize_content(str(text), strip_prefix=False).strip()
    if not cleaned:
        return
    payload: dict[str, Any] = {
        "runId": run.runId,
        "text": cleaned,
        "isProgress": is_progress,
    }
    if step_id in STEP_LABELS:
        payload["stepId"] = step_id
        payload["stepLabel"] = STEP_LABELS[step_id]
    append_stream_part(run, create_data_part("terminal-progress", payload, transient=True))


def send_terminal_data(run: RunRecord, raw_chunk: str) -> None:
    cleaned = raw_chunk.replace("\x00", "")
    if not cleaned:
        return


def append_text_message(
    run: RunRecord,
    *,
    role: str,
    kind: str,
    content: str,
) -> dict[str, Any] | None:
    cleaned = content if kind == "tool_result" else _sanitize_content(content)
    if not cleaned.strip():
        return None
    message_id = generate_message_id()
    append_snapshot_message(
        run,
        role=role,
        kind=kind,
        content=cleaned,
        message_id=message_id,
    )
    text_id = generate_text_id()
    append_stream_part(run, create_start_part(message_id))
    append_stream_part(run, create_text_start_part(text_id))
    append_stream_part(run, create_text_delta_part(text_id, cleaned))
    append_stream_part(run, create_text_end_part(text_id))
    append_stream_part(run, create_finish_part({"role": role, "kind": kind}))
    return {"messageId": message_id, "textId": text_id}


def append_reasoning_message(run: RunRecord, content: str) -> None:
    cleaned = _sanitize_content(content)
    if not cleaned.strip():
        return
    reasoning_id = generate_reasoning_id()
    append_stream_part(run, create_reasoning_start_part(reasoning_id))
    append_stream_part(run, create_reasoning_delta_part(reasoning_id, cleaned))
    append_stream_part(run, create_reasoning_end_part(reasoning_id))


def append_tool_call_part(
    run: RunRecord,
    *,
    tool_name: str,
    tool_input: dict[str, Any],
    output: dict[str, Any] | str,
    tool_call_id: str | None = None,
) -> str:
    resolved_tool_call_id = tool_call_id or generate_tool_call_id()
    input_text = json.dumps(tool_input, ensure_ascii=False, default=str)
    append_stream_part(run, create_tool_input_start_part(resolved_tool_call_id, tool_name, dynamic=True))
    if input_text:
        append_stream_part(run, create_tool_input_delta_part(resolved_tool_call_id, input_text))
    append_stream_part(run, create_tool_input_available_part(resolved_tool_call_id, tool_name, tool_input, dynamic=True))
    append_stream_part(run, create_tool_output_available_part(resolved_tool_call_id, output))
    return resolved_tool_call_id


def append_chat_message(
    run: RunRecord,
    *,
    role: str,
    kind: str,
    content: str,
    step: dict[str, str] | None = None,
    sql: dict[str, str] | None = None,
    ts: str | None = None,
) -> dict[str, Any] | None:
    timestamp = ts or now_iso()
    if role == "user":
        cleaned = _sanitize_content(content)
        if not cleaned:
            return None
        return append_snapshot_message(run, role=role, kind=kind, content=cleaned, step=step, sql=sql, ts=timestamp)

    if kind == "thinking":
        append_reasoning_message(run, content)
        return None

    if kind == "tool_result":
        cleaned = content.strip()
        if not cleaned:
            return None
        snapshot = append_snapshot_message(run, role=role, kind=kind, content=cleaned, ts=timestamp)
        try:
            parsed = json.loads(cleaned)
        except Exception:
            parsed = cleaned
        tool_name = parsed.get("tool", "tool") if isinstance(parsed, dict) else "tool"
        append_tool_call_part(run, tool_name=tool_name, tool_input={}, output=parsed)
        return snapshot

    cleaned = _sanitize_content(content)
    if not cleaned:
        return None
    snapshot = append_snapshot_message(run, role=role, kind=kind, content=cleaned, step=step, sql=sql, ts=timestamp)
    text_id = generate_text_id()
    append_stream_part(run, create_start_part(snapshot["id"]))
    append_stream_part(run, create_text_start_part(text_id))
    append_stream_part(run, create_text_delta_part(text_id, cleaned))
    append_stream_part(run, create_text_end_part(text_id))
    append_stream_part(run, create_finish_part({"role": role, "kind": kind}))
    return snapshot


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
                    step.endedAt = None
                if status in ("completed", "failed"):
                    step.endedAt = current_time
                run.updatedAt = current_time
                persist_runs_locked(force=True)
                return


def append_step_status_part(run: RunRecord, step_id: str, status: str) -> None:
    append_stream_part(
        run,
        create_data_part(
            "step-status",
            {
                "runId": run.runId,
                "stepId": step_id,
                "label": STEP_LABELS.get(step_id, step_id),
                "status": status,
            },
            transient=True,
        ),
    )


def append_run_status_part(run: RunRecord, status: str, error: str | None = None) -> None:
    append_stream_part(
        run,
        create_data_part(
            "run-status",
            {
                "runId": run.runId,
                "status": status,
                "error": error,
                "requiresDdlUpload": run.requiresDdlUpload,
                "resumeFromStage": run.resumeFromStage,
                "lastExecutedFileIndex": run.lastExecutedFileIndex,
                "missingObjects": list(run.missingObjects),
            },
            transient=True,
        ),
    )


def append_sql_statement_part(run: RunRecord, payload: dict[str, Any]) -> None:
    append_stream_part(run, create_data_part("sql-statement", payload))


def append_sql_error_part(run: RunRecord, payload: dict[str, Any]) -> None:
    append_stream_part(run, create_data_part("sql-error", payload))


def append_run_sync_part(run: RunRecord) -> None:
    append_stream_part(
        run,
        create_data_part(
            "run-sync",
            {
                "runId": run.runId,
                "status": run.status,
                "steps": [asdict(step) for step in run.steps],
                "requiresDdlUpload": run.requiresDdlUpload,
                "resumeFromStage": run.resumeFromStage,
                "lastExecutedFileIndex": run.lastExecutedFileIndex,
                "missingObjects": list(run.missingObjects),
                "executionErrors": list(run.executionErrors),
            },
            transient=True,
        ),
    )


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
    with RUN_LOCK:
        run.logs.append(line)
        run.updatedAt = created_at
        persist_runs_locked()
    try:
        sqlite_store.append_run_log(run.runId, line, created_at)
    except Exception as exc:
        logger.warning("Failed to append log for run %s: %s", run.runId, exc)
    append_terminal_output(run, line, is_progress=is_progress, step_id=step_id)


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


def push_user_message(run_id: str, message: str) -> None:
    with RUN_LOCK:
        if run_id not in USER_MESSAGE_QUEUES:
            USER_MESSAGE_QUEUES[run_id] = []
        USER_MESSAGE_QUEUES[run_id].append(message)
        run = RUNS.get(run_id)
        if run:
            run.userMessageQueue.append(message)
            persist_runs_locked()


def pop_user_message(run_id: str) -> str | None:
    with RUN_LOCK:
        queue = USER_MESSAGE_QUEUES.get(run_id, [])
        if queue:
            msg = queue.pop(0)
            run = RUNS.get(run_id)
            if run and run.userMessageQueue:
                run.userMessageQueue.pop(0)
            return msg
    return None
