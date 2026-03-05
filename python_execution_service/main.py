from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from fastapi import FastAPI, File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from agentic_core.nodes import (
    add_source_code_node,
    apply_schema_mapping_node,
    convert_code_node,
    execute_sql_node,
    finalize_node,
    human_review_node,
    init_project_node,
    self_heal_node,
    validate_node,
)
from agentic_core.orchestrator import (
    OrchestratorDecision,
    SnowflakeCortexOrchestrator,
    build_decision_context,
)
from agentic_core.state import MigrationContext, MigrationState
from python_execution_service import sqlite_store


EXECUTION_TOKEN = os.getenv("EXECUTION_TOKEN", "local-dev-token")
OUTPUT_ROOT = Path(os.getenv("PYTHON_EXEC_OUTPUT_ROOT", "outputs")).resolve()
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


class StartRunRequest(BaseModel):
    projectId: str
    projectName: str
    sourceId: str
    schemaId: Optional[str] = None
    sourceLanguage: str = "teradata"
    sourcePath: str
    schemaPath: Optional[str] = None
    sfAccount: Optional[str] = None
    sfUser: Optional[str] = None
    sfRole: Optional[str] = None
    sfWarehouse: Optional[str] = None
    sfDatabase: Optional[str] = None
    sfSchema: Optional[str] = None
    sfAuthenticator: Optional[str] = None


class StartRunResponse(BaseModel):
    runId: str


@dataclass
class RunStep:
    id: str
    label: str
    status: str = "pending"
    startedAt: Optional[str] = None
    endedAt: Optional[str] = None


@dataclass
class RunRecord:
    runId: str
    projectId: str
    projectName: str
    sourceId: str
    schemaId: str
    sourceLanguage: str
    sourcePath: str
    schemaPath: str
    sfAccount: Optional[str]
    sfUser: Optional[str]
    sfRole: Optional[str]
    sfWarehouse: Optional[str]
    sfDatabase: Optional[str]
    sfSchema: Optional[str]
    sfAuthenticator: Optional[str]
    status: str
    createdAt: str
    updatedAt: str
    steps: List[RunStep] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)
    terminalEvents: List[Dict[str, Any]] = field(default_factory=list)
    validationIssues: List[Dict[str, Any]] = field(default_factory=list)
    executionLog: List[Dict[str, Any]] = field(default_factory=list)
    executionErrors: List[Dict[str, Any]] = field(default_factory=list)
    missingObjects: List[str] = field(default_factory=list)
    requiresDdlUpload: bool = False
    resumeFromStage: str = ""
    lastExecutedFileIndex: int = -1
    selfHealIteration: int = 0
    error: Optional[str] = None
    events: List[Dict[str, Any]] = field(default_factory=list)
    messages: List[Dict[str, Any]] = field(default_factory=list)
    outputDir: str = ""
    ddlUploadPath: str = ""
    executionEventCursor: int = 0


@dataclass
class ResumeRunConfig:
    ddl_content: bytes
    ddl_filename: str
    missing_objects: List[str] = field(default_factory=list)
    resume_from_stage: str = "execute_sql"
    last_executed_file_index: int = -1


RUN_LOCK = threading.RLock()
RUNS: Dict[str, RunRecord] = {}
PROJECT_LOCKS: Dict[str, str] = {}
CANCEL_FLAGS: Dict[str, threading.Event] = {}
STEP_LABELS = {
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

app = FastAPI(title="Python Execution Service", version="0.1.0")
logger = logging.getLogger(__name__)
THINKING_STEP_IDS = {"self_heal", "convert_code", "validate"}
ORCHESTRATOR_CONFIDENCE_THRESHOLD = 0.75
ORCHESTRATOR_TIMEOUT_SECONDS = 15
ORCHESTRATOR_RETRIES = 1
ORCHESTRATOR_HUMAN_REVIEW_STEP = "human_review"
WORKFLOW_END_STEP = "END"

SUCCESS_TRANSITIONS: Dict[str, List[str]] = {
    "init_project": ["add_source_code"],
    "add_source_code": ["apply_schema_mapping"],
    "apply_schema_mapping": ["convert_code"],
    "convert_code": ["execute_sql"],
    "execute_sql": ["validate", "self_heal", "human_review"],
    "self_heal": ["execute_sql"],
    "validate": ["finalize", "human_review"],
    "human_review": [WORKFLOW_END_STEP],
    "finalize": [WORKFLOW_END_STEP],
}

FAILURE_TRANSITIONS: Dict[str, List[str]] = {
    "init_project": ["init_project", "human_review"],
    "add_source_code": ["add_source_code", "human_review"],
    "apply_schema_mapping": ["apply_schema_mapping", "human_review"],
    "convert_code": ["convert_code", "human_review"],
    "execute_sql": ["execute_sql", "human_review"],
    "self_heal": ["execute_sql", "human_review"],
    "validate": ["validate", "human_review"],
    "human_review": [WORKFLOW_END_STEP],
    "finalize": [WORKFLOW_END_STEP],
}


@dataclass
class NodeExecutionResult:
    step_id: str
    success: bool
    context: MigrationContext
    error: str = ""

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if request.url.path == "/v1/runs/start":
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8", errors="replace")
        logger.error(
            "Validation error on /v1/runs/start. body=%s errors=%s",
            body_text,
            exc.errors(),
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def _serialize_run_record(run: RunRecord) -> Dict[str, Any]:
    return asdict(run)


def _deserialize_run_record(payload: Dict[str, Any]) -> RunRecord:
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
        terminalEvents=payload.get("terminalEvents", []),
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


def persist_runs_locked() -> None:
    for run in RUNS.values():
        try:
            sqlite_store.save_run_snapshot(_serialize_run_record(run))
        except Exception as exc:
            logger.warning("Failed to persist run %s: %s", run.runId, exc)


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
            persist_runs_locked()


def require_auth(x_execution_token: Optional[str]) -> None:
    if x_execution_token != EXECUTION_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


sqlite_store.init_schema()
load_persisted_runs()


def append_event(run: RunRecord, event_type: str, payload: Dict[str, Any]) -> None:
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
    lines: List[str] = []
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
    step: Optional[Dict[str, str]] = None,
    sql: Optional[Dict[str, str]] = None,
    ts: Optional[str] = None,
) -> Dict[str, Any]:
    timestamp = ts or now_iso()
    cleaned_content = _sanitize_content(content)
    message: Dict[str, Any] = {
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


def append_terminal_event(
    run: RunRecord,
    *,
    event_type: str,
    step_id: Optional[str] = None,
    command: Optional[str] = None,
    cwd: Optional[str] = None,
    attempt: Optional[int] = None,
    stream: Optional[str] = None,
    text: Optional[str] = None,
    ts: Optional[str] = None,
) -> Dict[str, Any]:
    if event_type not in {"terminal:command", "terminal:line"}:
        raise ValueError(f"Unsupported terminal event type: {event_type}")

    timestamp = ts or now_iso()
    payload: Dict[str, Any] = {"type": event_type, "runId": run.runId, "ts": timestamp}

    if isinstance(step_id, str) and step_id.strip():
        payload["stepId"] = step_id.strip()

    if event_type == "terminal:command":
        command_text = _sanitize_content(command or "", strip_prefix=False).strip()
        if not command_text:
            raise ValueError("terminal:command requires command")
        payload["command"] = command_text
        if isinstance(cwd, str) and cwd.strip():
            payload["cwd"] = cwd.strip()
        if isinstance(attempt, int):
            payload["attempt"] = attempt
    else:
        stream_name = str(stream or "stdout").strip().lower()
        if stream_name not in {"stdout", "stderr"}:
            stream_name = "stdout"
        text_value = _sanitize_content(text or "", strip_prefix=False).strip()
        if not text_value:
            raise ValueError("terminal:line requires text")
        payload["stream"] = stream_name
        payload["text"] = text_value

    with RUN_LOCK:
        run.terminalEvents.append(payload)
        run.updatedAt = timestamp
        persist_runs_locked()
    append_event(run, event_type, payload)
    return payload


def format_activity_log_entry(entry: Dict[str, Any]) -> str:
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
        lines: List[str] = []
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


def _extract_terminal_event_from_activity(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, dict):
        return None
    data = entry.get("data")
    if not isinstance(data, dict):
        return None
    terminal = data.get("terminal")
    if not isinstance(terminal, dict):
        return None

    terminal_type = str(terminal.get("type") or "").strip().lower()
    step_id = terminal.get("stepId")
    resolved_step_id = step_id if isinstance(step_id, str) and step_id in STEP_LABELS else None
    timestamp = str(entry.get("timestamp") or now_iso())

    if terminal_type == "command":
        command = terminal.get("command")
        if not isinstance(command, str):
            return None
        attempt = terminal.get("attempt")
        attempt_int = int(attempt) if isinstance(attempt, int) else None
        return {
            "event_type": "terminal:command",
            "step_id": resolved_step_id,
            "command": command,
            "cwd": terminal.get("cwd") if isinstance(terminal.get("cwd"), str) else None,
            "attempt": attempt_int,
            "ts": timestamp,
        }

    if terminal_type == "line":
        text = terminal.get("text")
        if not isinstance(text, str):
            return None
        stream = terminal.get("stream")
        stream_name = stream if isinstance(stream, str) else "stdout"
        return {
            "event_type": "terminal:line",
            "step_id": resolved_step_id,
            "stream": stream_name,
            "text": text,
            "ts": timestamp,
        }

    return None


def route_activity_log_entry(run: RunRecord, entry: Dict[str, Any]) -> None:
    terminal_payload = _extract_terminal_event_from_activity(entry)
    if terminal_payload:
        try:
            append_terminal_event(run, **terminal_payload)
        except Exception as exc:
            logger.warning("Failed to append terminal event for run %s: %s", run.runId, exc)
        return

    formatted = format_activity_log_entry(entry)
    if not formatted:
        return
    stage = entry.get("stage")
    step_id = stage if isinstance(stage, str) and stage in STEP_LABELS else None
    add_log(run, formatted, step_id=step_id)


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
                persist_runs_locked()
                return


def add_log(run: RunRecord, message: str, step_id: Optional[str] = None) -> None:
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
    append_event(run, "log", {"message": line})
    resolved_step_id = step_id if step_id in STEP_LABELS else None
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


def set_run_status(run: RunRecord, status: str, error: Optional[str] = None) -> None:
    with RUN_LOCK:
        run.status = status
        run.error = error
        run.updatedAt = now_iso()
        persist_runs_locked()


def get_steps_template() -> List[RunStep]:
    return [RunStep(id=step_id, label=label) for step_id, label in STEP_LABELS.items()]


def ensure_not_canceled(run_id: str) -> None:
    cancel_flag = CANCEL_FLAGS.get(run_id)
    if cancel_flag and cancel_flag.is_set():
        raise RuntimeError("Run canceled")


def _emit_step_started(run: RunRecord, run_id: str, step_id: str) -> None:
    update_step(run, step_id, "running")
    append_event(run, "step:started", {"runId": run_id, "stepId": step_id, "label": STEP_LABELS[step_id]})
    append_chat_message(
        run,
        role="system",
        kind="step_started",
        content=f"Starting: {STEP_LABELS[step_id]}",
        step={"id": step_id, "label": STEP_LABELS[step_id]},
    )


def _emit_step_completed(run: RunRecord, run_id: str, step_id: str) -> None:
    update_step(run, step_id, "completed")
    append_event(run, "step:completed", {"runId": run_id, "stepId": step_id, "label": STEP_LABELS[step_id]})
    append_chat_message(
        run,
        role="system",
        kind="step_completed",
        content=f"Completed: {STEP_LABELS[step_id]}",
        step={"id": step_id, "label": STEP_LABELS[step_id]},
    )


def _emit_step_failed(run: RunRecord, run_id: str, step_id: str, reason: str) -> None:
    update_step(run, step_id, "failed")
    append_event(
        run,
        "step:failed",
        {
            "runId": run_id,
            "stepId": step_id,
            "label": STEP_LABELS[step_id],
            "reason": reason,
        },
    )
    append_chat_message(
        run,
        role="error",
        kind="run_status",
        content=f"Failed: {STEP_LABELS[step_id]} ({reason})",
        step={"id": step_id, "label": STEP_LABELS[step_id]},
    )


def _get_allowed_transitions(step_id: str, *, success: bool) -> List[str]:
    table = SUCCESS_TRANSITIONS if success else FAILURE_TRANSITIONS
    candidates = table.get(step_id, [ORCHESTRATOR_HUMAN_REVIEW_STEP])
    return [candidate for candidate in candidates if isinstance(candidate, str) and candidate]


def _apply_transition_guardrails(
    state: MigrationContext,
    step_id: str,
    candidates: List[str],
) -> List[str]:
    allowed = [item for item in candidates]

    # DDL upload blocker always forces explicit pause for human review.
    if state.requires_ddl_upload:
        if ORCHESTRATOR_HUMAN_REVIEW_STEP in allowed:
            return [ORCHESTRATOR_HUMAN_REVIEW_STEP]
        return [WORKFLOW_END_STEP] if WORKFLOW_END_STEP in allowed else allowed[:1]

    # Same-step retry is allowed at most once per node.
    retry_counts = state.node_retry_counts if isinstance(state.node_retry_counts, dict) else {}
    if retry_counts.get(step_id, 0) >= 1:
        allowed = [candidate for candidate in allowed if candidate != step_id]

    if not allowed:
        if ORCHESTRATOR_HUMAN_REVIEW_STEP in candidates:
            return [ORCHESTRATOR_HUMAN_REVIEW_STEP]
        if WORKFLOW_END_STEP in candidates:
            return [WORKFLOW_END_STEP]
        return candidates[:1]
    return allowed


def _emit_orchestrator_decision(
    run: RunRecord,
    decision: OrchestratorDecision,
    *,
    resolved_step: str,
    guarded_candidates: List[str],
) -> None:
    payload = decision.to_payload()
    payload["resolved_step"] = resolved_step
    payload["guarded_candidates"] = guarded_candidates
    append_event(run, "orchestrator:decision", payload)


def _resolve_next_step(
    run: RunRecord,
    state: MigrationContext,
    step_id: str,
    *,
    success: bool,
    orchestrator: SnowflakeCortexOrchestrator,
) -> str:
    allowed = _get_allowed_transitions(step_id, success=success)
    guarded_candidates = _apply_transition_guardrails(state, step_id, allowed)
    context = build_decision_context(state, step_id, guarded_candidates)
    decision = orchestrator.decide(state, context)

    resolved_step = decision.selected_step
    if resolved_step not in guarded_candidates:
        resolved_step = (
            ORCHESTRATOR_HUMAN_REVIEW_STEP
            if ORCHESTRATOR_HUMAN_REVIEW_STEP in guarded_candidates
            else guarded_candidates[0]
        )
    elif decision.confidence < ORCHESTRATOR_CONFIDENCE_THRESHOLD:
        resolved_step = (
            ORCHESTRATOR_HUMAN_REVIEW_STEP
            if ORCHESTRATOR_HUMAN_REVIEW_STEP in guarded_candidates
            else guarded_candidates[0]
        )

    if state.requires_ddl_upload and ORCHESTRATOR_HUMAN_REVIEW_STEP in guarded_candidates:
        resolved_step = ORCHESTRATOR_HUMAN_REVIEW_STEP

    if decision.status != "ok":
        resolved_step = (
            ORCHESTRATOR_HUMAN_REVIEW_STEP
            if ORCHESTRATOR_HUMAN_REVIEW_STEP in guarded_candidates
            else guarded_candidates[0]
        )

    _emit_orchestrator_decision(
        run,
        decision,
        resolved_step=resolved_step,
        guarded_candidates=guarded_candidates,
    )

    state.orchestrator_history.append(
        {
            **decision.to_payload(),
            "resolved_step": resolved_step,
            "guarded_candidates": guarded_candidates,
            "node_success": success,
        }
    )
    return resolved_step


def run_node_safe(
    run: RunRecord,
    run_id: str,
    state: MigrationContext,
    step_id: str,
    node_fn: Callable[[MigrationContext], MigrationContext],
    *,
    success_log: Optional[str] = None,
    post_hook: Optional[Callable[[MigrationContext], None]] = None,
) -> NodeExecutionResult:
    ensure_not_canceled(run_id)
    _emit_step_started(run, run_id, step_id)
    try:
        updated = node_fn(state)
    except Exception as exc:
        if str(exc) == "Run canceled":
            raise
        message = str(exc) or f"Step failed: {step_id}"
        state.last_step_success = False
        state.last_step_error = message
        state.current_stage = MigrationState.ERROR
        if message not in state.errors:
            state.errors.append(message)
        _emit_step_failed(run, run_id, step_id, message)
        return NodeExecutionResult(step_id=step_id, success=False, context=state, error=message)

    if updated.current_stage == MigrationState.ERROR:
        message = updated.errors[-1] if updated.errors else f"Step failed: {step_id}"
        updated.last_step_success = False
        updated.last_step_error = message
        _emit_step_failed(run, run_id, step_id, message)
        return NodeExecutionResult(step_id=step_id, success=False, context=updated, error=message)

    _emit_step_completed(run, run_id, step_id)
    if success_log:
        add_log(run, success_log)
    if callable(post_hook):
        post_hook(updated)
    updated.last_step_success = True
    updated.last_step_error = ""
    return NodeExecutionResult(step_id=step_id, success=True, context=updated)


def execute_run_sync(run_id: str) -> None:
    with RUN_LOCK:
        run = RUNS[run_id]
        resume_ddl_path = run.ddlUploadPath
        resume_from_stage = run.resumeFromStage or "execute_sql"
        resume_missing_objects = list(run.missingObjects)
        resume_last_executed_file_index = int(run.lastExecutedFileIndex)
    try:
        set_run_status(run, "running")
        append_event(run, "run:started", {"runId": run_id})
        append_chat_message(
            run,
            role="system",
            kind="run_status",
            content="Migration started.",
        )

        def activity_log_sink(entry: Dict[str, Any]) -> None:
            route_activity_log_entry(run, entry)

        context = MigrationContext(
            project_name=run.projectName,
            source_language=run.sourceLanguage.lower(),
            source_directory=str(Path(run.sourcePath).resolve().parent),
            source_files=[run.sourcePath],
            mapping_csv_path=run.schemaPath,
            activity_log_sink=activity_log_sink,
            sf_account=run.sfAccount or "",
            sf_user=run.sfUser or "",
            sf_role=run.sfRole or "",
            sf_warehouse=run.sfWarehouse or "",
            sf_database=run.sfDatabase or "",
            sf_schema=run.sfSchema or "",
            sf_authenticator=run.sfAuthenticator or "externalbrowser",
            session_id=run_id,
        )

        if resume_ddl_path:
            context.requires_ddl_upload = True
            context.ddl_upload_path = resume_ddl_path
            context.resume_from_stage = resume_from_stage
            context.last_executed_file_index = max(-1, resume_last_executed_file_index)
            context.missing_objects = resume_missing_objects
            add_log(
                run,
                f"Applying uploaded DDL ({Path(resume_ddl_path).name}) before resuming execute_sql.",
            )

        def sync_execution_state(updated: MigrationContext) -> None:
            with RUN_LOCK:
                run.executionLog = updated.execution_log or []
                run.executionErrors = updated.execution_errors or []
                run.missingObjects = updated.missing_objects or []
                run.requiresDdlUpload = bool(updated.requires_ddl_upload)
                run.resumeFromStage = updated.resume_from_stage or ""
                run.lastExecutedFileIndex = int(updated.last_executed_file_index)
                run.ddlUploadPath = updated.ddl_upload_path or ""
                persist_runs_locked()

        def emit_execute_events(updated: MigrationContext) -> None:
            execution_log = updated.execution_log or []
            with RUN_LOCK:
                start_index = max(0, int(run.executionEventCursor))
            new_entries = execution_log[start_index:]

            for file_entry in new_entries:
                for statement_entry in file_entry.get("statements", []):
                    append_event(
                        run,
                        "execute_sql:statement",
                        {
                            "runId": run_id,
                            "file": file_entry.get("file"),
                            "fileIndex": file_entry.get("index"),
                            "statementIndex": statement_entry.get("statement_index"),
                            "statement": statement_entry.get("statement"),
                            "status": statement_entry.get("status"),
                            "rowCount": statement_entry.get("row_count", 0),
                            "outputPreview": statement_entry.get("output_preview", []),
                        },
                    )
                    statement_index = statement_entry.get("statement_index")
                    label = f"Stmt {int(statement_index) + 1}" if isinstance(statement_index, int) else "Stmt ?"
                    output_preview = statement_entry.get("output_preview", [])
                    output_text = ""
                    if isinstance(output_preview, list) and output_preview:
                        try:
                            output_text = json.dumps(output_preview, ensure_ascii=False, default=str, indent=2)
                        except Exception:
                            output_text = str(output_preview)
                    append_chat_message(
                        run,
                        role="agent",
                        kind="sql_statement",
                        content=label,
                        step={"id": "execute_sql", "label": STEP_LABELS["execute_sql"]},
                        sql={
                            "statement": str(statement_entry.get("statement") or ""),
                            "output": output_text,
                        },
                    )
                if file_entry.get("status") == "failed":
                    append_event(
                        run,
                        "execute_sql:error",
                        {
                            "runId": run_id,
                            "file": file_entry.get("file"),
                            "fileIndex": file_entry.get("index"),
                            "errorType": file_entry.get("error_type"),
                            "errorMessage": file_entry.get("error_message"),
                            "failedStatement": file_entry.get("failed_statement"),
                            "failedStatementIndex": file_entry.get("failed_statement_index"),
                        },
                    )
                    failed_statement_index = file_entry.get("failed_statement_index")
                    label = (
                        f"Stmt {int(failed_statement_index) + 1} ERROR"
                        if isinstance(failed_statement_index, int)
                        else "Stmt ? ERROR"
                    )
                    append_chat_message(
                        run,
                        role="error",
                        kind="sql_error",
                        content=label,
                        step={"id": "execute_sql", "label": STEP_LABELS["execute_sql"]},
                        sql={
                            "error": str(file_entry.get("error_message") or ""),
                            "failedStatement": str(file_entry.get("failed_statement") or ""),
                            "output": f"Error type: {str(file_entry.get('error_type') or 'execution_error')}",
                        },
                    )
            with RUN_LOCK:
                run.executionEventCursor = len(execution_log)
                persist_runs_locked()

        def sync_validation_state(updated: MigrationContext) -> None:
            with RUN_LOCK:
                run.validationIssues = updated.validation_issues or []
                persist_runs_locked()
            for issue in updated.validation_issues:
                append_event(run, "validation:issue", issue if isinstance(issue, dict) else {"message": str(issue)})

        def sync_self_heal_state(updated: MigrationContext) -> None:
            with RUN_LOCK:
                run.selfHealIteration = int(updated.self_heal_iteration)
                persist_runs_locked()
            append_event(
                run,
                "selfheal:iteration",
                {"iteration": updated.self_heal_iteration, "runId": run_id},
            )
            add_log(run, f"Self-heal iteration {updated.self_heal_iteration} applied; re-running execute_sql.")

        node_handlers: Dict[str, Dict[str, Any]] = {
            "init_project": {
                "fn": init_project_node,
                "success_log": "Project initialized.",
                "post_hook": None,
            },
            "add_source_code": {
                "fn": add_source_code_node,
                "success_log": "Source code added.",
                "post_hook": None,
            },
            "apply_schema_mapping": {
                "fn": apply_schema_mapping_node,
                "success_log": "Schema mapping applied.",
                "post_hook": None,
            },
            "convert_code": {
                "fn": convert_code_node,
                "success_log": "Code conversion complete.",
                "post_hook": None,
            },
            "execute_sql": {
                "fn": execute_sql_node,
                "success_log": "Execute SQL stage completed.",
                "post_hook": lambda updated: (sync_execution_state(updated), emit_execute_events(updated)),
            },
            "self_heal": {
                "fn": self_heal_node,
                "success_log": None,
                "post_hook": sync_self_heal_state,
            },
            "validate": {
                "fn": validate_node,
                "success_log": None,
                "post_hook": sync_validation_state,
            },
            "human_review": {
                "fn": human_review_node,
                "success_log": None,
                "post_hook": None,
            },
            "finalize": {
                "fn": finalize_node,
                "success_log": None,
                "post_hook": None,
            },
        }

        orchestrator = SnowflakeCortexOrchestrator(
            timeout_seconds=ORCHESTRATOR_TIMEOUT_SECONDS,
            retries=ORCHESTRATOR_RETRIES,
        )
        current_step = resume_from_stage if resume_ddl_path and resume_from_stage in node_handlers else "init_project"

        while current_step != WORKFLOW_END_STEP:
            ensure_not_canceled(run_id)

            if current_step not in node_handlers:
                raise RuntimeError(f"Unknown workflow step: {current_step}")

            handler = node_handlers[current_step]
            result = run_node_safe(
                run,
                run_id,
                context,
                current_step,
                handler["fn"],
                success_log=handler["success_log"],
                post_hook=handler["post_hook"],
            )
            context = result.context

            next_step = _resolve_next_step(
                run,
                context,
                current_step,
                success=result.success,
                orchestrator=orchestrator,
            )

            if not result.success and next_step == current_step:
                context.node_retry_counts[current_step] = int(context.node_retry_counts.get(current_step, 0)) + 1

            if next_step == WORKFLOW_END_STEP:
                if current_step == "finalize" and result.success and context.current_stage == MigrationState.COMPLETED:
                    set_run_status(run, "completed")
                    append_event(run, "run:completed", {"runId": run_id})
                    append_chat_message(
                        run,
                        role="system",
                        kind="run_status",
                        content="Migration completed.",
                    )
                    return

                reason = context.human_intervention_reason or result.error or "Migration stopped before completion"
                set_run_status(run, "failed", reason)
                append_event(run, "run:failed", {"runId": run_id, "reason": reason})
                if current_step == "human_review" or context.current_stage == MigrationState.HUMAN_REVIEW:
                    add_log(run, reason, step_id="human_review")
                append_chat_message(
                    run,
                    role="error",
                    kind="run_status",
                    content=reason,
                )
                return

            current_step = next_step
    except Exception as exc:
        message = str(exc)
        canceled = message == "Run canceled"
        status = "canceled" if canceled else "failed"
        set_run_status(run, status, message)
        append_event(run, "run:failed", {"runId": run_id, "reason": message})
        append_chat_message(
            run,
            role="error",
            kind="run_status",
            content=message or "Run failed",
        )
    finally:
        with RUN_LOCK:
            if PROJECT_LOCKS.get(run.projectId) == run_id:
                del PROJECT_LOCKS[run.projectId]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


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


def _start_run_record(request: StartRunRequest, resume_config: Optional[ResumeRunConfig] = None) -> StartRunResponse:
    if not Path(request.sourcePath).exists():
        raise HTTPException(status_code=404, detail="Source file path not found")
    if request.schemaPath and not Path(request.schemaPath).exists():
        raise HTTPException(status_code=404, detail="Schema file path not found")

    with RUN_LOCK:
        locked_by = PROJECT_LOCKS.get(request.projectId)
        if locked_by and RUNS.get(locked_by) and RUNS[locked_by].status == "running":
            raise HTTPException(status_code=409, detail="Project already has an active run")

        run_id = str(uuid.uuid4())
        output_dir = OUTPUT_ROOT / request.projectId / run_id
        output_dir.mkdir(parents=True, exist_ok=True)

        ddl_upload_path = ""
        missing_objects = []
        requires_ddl_upload = False
        resume_from_stage = ""
        last_executed_file_index = -1

        if resume_config:
            safe_name = _sanitize_upload_filename(resume_config.ddl_filename)
            ddl_file_path = output_dir / f"resume-ddl-{safe_name}"
            ddl_file_path.write_bytes(resume_config.ddl_content)
            ddl_upload_path = str(ddl_file_path.resolve())
            missing_objects = list(resume_config.missing_objects)
            requires_ddl_upload = False
            resume_from_stage = resume_config.resume_from_stage or "execute_sql"
            last_executed_file_index = max(-1, int(resume_config.last_executed_file_index))

        record = RunRecord(
            runId=run_id,
            projectId=request.projectId,
            projectName=request.projectName,
            sourceId=request.sourceId,
            schemaId=request.schemaId or "",
            sourceLanguage=request.sourceLanguage,
            sourcePath=request.sourcePath,
            schemaPath=request.schemaPath or "",
            sfAccount=request.sfAccount,
            sfUser=request.sfUser,
            sfRole=request.sfRole,
            sfWarehouse=request.sfWarehouse,
            sfDatabase=request.sfDatabase,
            sfSchema=request.sfSchema,
            sfAuthenticator=request.sfAuthenticator,
            status="queued",
            createdAt=now_iso(),
            updatedAt=now_iso(),
            steps=get_steps_template(),
            outputDir=str(output_dir),
            missingObjects=missing_objects,
            requiresDdlUpload=requires_ddl_upload,
            resumeFromStage=resume_from_stage,
            lastExecutedFileIndex=last_executed_file_index,
            ddlUploadPath=ddl_upload_path,
        )

        RUNS[run_id] = record
        PROJECT_LOCKS[request.projectId] = run_id
        CANCEL_FLAGS[run_id] = threading.Event()
        persist_runs_locked()

    worker = threading.Thread(target=execute_run_sync, args=(run_id,), daemon=True)
    worker.start()
    return StartRunResponse(runId=run_id)


@app.post("/v1/runs/start", response_model=StartRunResponse)
def start_run(request: StartRunRequest, x_execution_token: Optional[str] = Header(default=None)) -> StartRunResponse:
    require_auth(x_execution_token)
    return _start_run_record(request)


@app.get("/v1/runs")
def list_runs(
    x_execution_token: Optional[str] = Header(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    status: Optional[str] = Query(default=None),
    projectId: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    require_auth(x_execution_token)
    with RUN_LOCK:
        items = list(RUNS.values())
    if status:
        items = [run for run in items if run.status == status]
    if projectId:
        items = [run for run in items if run.projectId == projectId]
    items.sort(key=lambda run: run.updatedAt, reverse=True)

    summaries: List[Dict[str, Any]] = []
    for run in items[:limit]:
        summaries.append(
            {
                "runId": run.runId,
                "projectId": run.projectId,
                "projectName": run.projectName,
                "sourceId": run.sourceId,
                "schemaId": run.schemaId,
                "sourceLanguage": run.sourceLanguage,
                "status": run.status,
                "createdAt": run.createdAt,
                "updatedAt": run.updatedAt,
                "error": run.error,
                "missingObjects": run.missingObjects,
                "requiresDdlUpload": run.requiresDdlUpload,
                "resumeFromStage": run.resumeFromStage,
                "lastExecutedFileIndex": run.lastExecutedFileIndex,
                "selfHealIteration": run.selfHealIteration,
                "steps": [asdict(step) for step in run.steps],
            }
        )
    return {"runs": summaries}


@app.get("/v1/runs/{run_id}")
def get_run(run_id: str, x_execution_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    require_auth(x_execution_token)
    with RUN_LOCK:
        run = RUNS.get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return asdict(run)


@app.post("/v1/runs/{run_id}/cancel")
def cancel_run(run_id: str, x_execution_token: Optional[str] = Header(default=None)) -> Dict[str, str]:
    require_auth(x_execution_token)
    with RUN_LOCK:
        run = RUNS.get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        flag = CANCEL_FLAGS.get(run_id)
        if flag:
            flag.set()
    return {"status": "canceled"}


@app.post("/v1/runs/{run_id}/retry", response_model=StartRunResponse)
def retry_run(run_id: str, x_execution_token: Optional[str] = Header(default=None)) -> StartRunResponse:
    require_auth(x_execution_token)
    with RUN_LOCK:
        existing = RUNS.get(run_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Run not found")
    req = _request_from_run(existing)
    return _start_run_record(req)


@app.post("/v1/runs/{run_id}/resume", response_model=StartRunResponse)
async def resume_run(
    run_id: str,
    ddl_file: UploadFile = File(...),
    resume_from_stage: str = Form(default="execute_sql"),
    last_executed_file_index: int = Form(default=-1),
    missing_objects: str = Form(default=""),
    x_execution_token: Optional[str] = Header(default=None),
) -> StartRunResponse:
    require_auth(x_execution_token)
    if not ddl_file.filename:
        raise HTTPException(status_code=400, detail="DDL file is required")

    with RUN_LOCK:
        existing = RUNS.get(run_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Run not found")
        if not existing.requiresDdlUpload:
            raise HTTPException(status_code=409, detail="Run is not waiting for DDL upload")

    ddl_content = await ddl_file.read()
    if not ddl_content:
        raise HTTPException(status_code=400, detail="Uploaded DDL file is empty")

    objects_from_form: List[str] = []
    if missing_objects.strip():
        try:
            parsed = json.loads(missing_objects)
            if isinstance(parsed, list):
                objects_from_form = [str(item).strip() for item in parsed if str(item).strip()]
        except Exception:
            objects_from_form = [item.strip() for item in missing_objects.split(",") if item.strip()]

    objects = objects_from_form or list(existing.missingObjects)
    req = _request_from_run(existing)
    resume_config = ResumeRunConfig(
        ddl_content=ddl_content,
        ddl_filename=ddl_file.filename,
        missing_objects=objects,
        resume_from_stage=resume_from_stage,
        last_executed_file_index=last_executed_file_index,
    )
    return _start_run_record(req, resume_config=resume_config)


@app.get("/v1/runs/{run_id}/events")
async def stream_events(
    run_id: str,
    x_execution_token: Optional[str] = Header(default=None),
    last_event_id: Optional[str] = Header(default=None),
) -> StreamingResponse:
    require_auth(x_execution_token)
    with RUN_LOCK:
        if run_id not in RUNS:
            raise HTTPException(status_code=404, detail="Run not found")

    async def iterator():
        idx = 0
        if last_event_id is not None:
            try:
                idx = max(0, int(last_event_id) + 1)
            except ValueError:
                idx = 0
        heartbeat_at = time.time()
        while True:
            with RUN_LOCK:
                run = RUNS.get(run_id)
                if not run:
                    break
                events = run.events[idx:]
                status = run.status
                total_events = len(run.events)
            for event in events:
                event_id = idx
                yield f"event: {event['type']}\n".encode("utf-8")
                yield f"id: {event_id}\n".encode("utf-8")
                payload = event.get("payload", {})
                yield f"data: {json.dumps(payload)}\n\n".encode("utf-8")
                idx += 1
            now = time.time()
            if now - heartbeat_at >= 20:
                heartbeat_at = now
                yield b": heartbeat\n\n"
            if status in ("completed", "failed", "canceled") and idx >= total_events:
                break
            await asyncio.sleep(0.25)

    return StreamingResponse(iterator(), media_type="text/event-stream")


