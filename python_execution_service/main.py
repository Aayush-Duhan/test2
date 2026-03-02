from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from agentic_core.decision import should_continue, should_continue_after_execute
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
from agentic_core.state import MigrationContext, MigrationState


EXECUTION_TOKEN = os.getenv("EXECUTION_TOKEN", "local-dev-token")
OUTPUT_ROOT = Path(os.getenv("PYTHON_EXEC_OUTPUT_ROOT", "outputs")).resolve()
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
RUN_INDEX_PATH = OUTPUT_ROOT / "run_index.json"


class StartRunRequest(BaseModel):
    projectId: str
    projectName: str
    sourceId: str
    schemaId: str
    sourceLanguage: str = "teradata"
    sourcePath: str
    schemaPath: str
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
class Artifact:
    name: str
    type: str
    path: str
    createdAt: str


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
    artifacts: List[Artifact] = field(default_factory=list)
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
    outputDir: str = ""
    ddlUploadPath: str = ""


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
    "finalize": "Finalize artifacts",
}

app = FastAPI(title="Python Execution Service", version="0.1.0")
logger = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def _serialize_run_record(run: RunRecord) -> Dict[str, Any]:
    return asdict(run)


def _deserialize_run_record(payload: Dict[str, Any]) -> RunRecord:
    steps = [RunStep(**step) for step in payload.get("steps", [])]
    artifacts = [Artifact(**artifact) for artifact in payload.get("artifacts", [])]
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
        artifacts=artifacts,
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
        events=payload.get("events", []),
    )


def persist_runs_locked() -> None:
    snapshot = [_serialize_run_record(run) for run in RUNS.values()]
    RUN_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = RUN_INDEX_PATH.with_name(
        f"{RUN_INDEX_PATH.stem}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2, default=str)

        for attempt in range(6):
            try:
                os.replace(temp_path, RUN_INDEX_PATH)
                return
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.05 * (attempt + 1))
    except Exception as exc:
        logger.warning("Failed to persist run index: %s", exc)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass


def persist_runs() -> None:
    with RUN_LOCK:
        persist_runs_locked()


def load_persisted_runs() -> None:
    if not RUN_INDEX_PATH.exists():
        return
    try:
        payload = json.loads(RUN_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, list):
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


load_persisted_runs()


def append_event(run: RunRecord, event_type: str, payload: Dict[str, Any]) -> None:
    event = {"type": event_type, "payload": payload, "timestamp": now_iso()}
    with RUN_LOCK:
        run.events.append(event)
        run.updatedAt = event["timestamp"]
        persist_runs_locked()
    events_file = Path(run.outputDir) / "events.jsonl"
    events_file.parent.mkdir(parents=True, exist_ok=True)
    with events_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")


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


def add_log(run: RunRecord, message: str) -> None:
    line = str(message).strip()
    if not line:
        return
    with RUN_LOCK:
        run.logs.append(line)
        persist_runs_locked()
    append_event(run, "log", {"message": line})


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


def run_node(run: RunRecord, run_id: str, state: MigrationContext, step_id: str, node_fn) -> MigrationContext:
    ensure_not_canceled(run_id)
    update_step(run, step_id, "running")
    append_event(run, "step:started", {"runId": run_id, "stepId": step_id, "label": STEP_LABELS[step_id]})
    state = node_fn(state)
    if state.current_stage == MigrationState.ERROR:
        update_step(run, step_id, "failed")
        raise RuntimeError(state.errors[-1] if state.errors else f"Step failed: {step_id}")
    update_step(run, step_id, "completed")
    append_event(run, "step:completed", {"runId": run_id, "stepId": step_id, "label": STEP_LABELS[step_id]})
    return state


def attach_artifacts(run: RunRecord, state: MigrationContext) -> None:
    created = []
    for file_path in state.output_files:
        path_obj = Path(file_path)
        if path_obj.exists():
            artifact = Artifact(
                name=path_obj.name,
                type="sql" if path_obj.suffix.lower() == ".sql" else "other",
                path=str(path_obj.resolve()),
                createdAt=now_iso(),
            )
            created.append(artifact)
    if state.summary_report:
        report_path = Path(run.outputDir) / "summary_report.json"
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(state.summary_report, handle, indent=2, default=str)
        created.append(
            Artifact(
                name="summary_report.json",
                type="report",
                path=str(report_path.resolve()),
                createdAt=now_iso(),
            )
        )
    with RUN_LOCK:
        run.artifacts.extend(created)
        persist_runs_locked()
    for artifact in created:
        append_event(
            run,
            "artifact",
            {"name": artifact.name, "type": artifact.type, "createdAt": artifact.createdAt},
        )


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
        def activity_log_sink(entry: Dict[str, Any]) -> None:
            formatted = format_activity_log_entry(entry)
            if formatted:
                add_log(run, formatted)

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

        context = run_node(run, run_id, context, "init_project", init_project_node)
        add_log(run, "Project initialized.")
        context = run_node(run, run_id, context, "add_source_code", add_source_code_node)
        add_log(run, "Source code added.")
        context = run_node(run, run_id, context, "apply_schema_mapping", apply_schema_mapping_node)
        add_log(run, "Schema mapping applied.")
        context = run_node(run, run_id, context, "convert_code", convert_code_node)
        add_log(run, "Code conversion complete.")

        def sync_execution_state() -> None:
            with RUN_LOCK:
                run.executionLog = context.execution_log or []
                run.executionErrors = context.execution_errors or []
                run.missingObjects = context.missing_objects or []
                run.requiresDdlUpload = bool(context.requires_ddl_upload)
                run.resumeFromStage = context.resume_from_stage or ""
                run.lastExecutedFileIndex = int(context.last_executed_file_index)
                run.ddlUploadPath = context.ddl_upload_path or ""
                persist_runs_locked()

        def emit_execute_events() -> None:
            for file_entry in context.execution_log or []:
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

        while True:
            context = run_node(run, run_id, context, "execute_sql", execute_sql_node)
            add_log(run, "Execute SQL stage completed.")
            sync_execution_state()
            emit_execute_events()

            execute_decision = should_continue_after_execute(context)
            if execute_decision == "human_review":
                context = run_node(run, run_id, context, "human_review", human_review_node)
                add_log(run, "Execution paused for human review (DDL upload required).")
                set_run_status(run, "failed", context.human_intervention_reason or "Execution paused for human review")
                append_event(
                    run,
                    "run:failed",
                    {"runId": run_id, "reason": context.human_intervention_reason or "Execution paused for human review"},
                )
                return

            if execute_decision == "self_heal":
                if context.self_heal_iteration >= context.max_self_heal_iterations:
                    context.requires_human_intervention = True
                    context.human_intervention_reason = (
                        f"Could not resolve execution errors after {context.max_self_heal_iterations} self-heal iteration(s)."
                    )
                    context = run_node(run, run_id, context, "human_review", human_review_node)
                    set_run_status(run, "failed", context.human_intervention_reason)
                    append_event(
                        run,
                        "run:failed",
                        {"runId": run_id, "reason": context.human_intervention_reason},
                    )
                    return

                context = run_node(run, run_id, context, "self_heal", self_heal_node)
                with RUN_LOCK:
                    run.selfHealIteration = int(context.self_heal_iteration)
                    persist_runs_locked()
                append_event(
                    run,
                    "selfheal:iteration",
                    {"iteration": context.self_heal_iteration, "runId": run_id},
                )
                add_log(run, f"Self-heal iteration {context.self_heal_iteration} applied; re-running execute_sql.")
                continue

            break

        context = run_node(run, run_id, context, "validate", validate_node)
        with RUN_LOCK:
            run.validationIssues = context.validation_issues or []
            persist_runs_locked()
        for issue in context.validation_issues:
            append_event(run, "validation:issue", issue if isinstance(issue, dict) else {"message": str(issue)})

        validation_decision = should_continue(context)
        if validation_decision != "finalize":
            if not context.human_intervention_reason:
                context.human_intervention_reason = "Validation failed after execution"
            context = run_node(run, run_id, context, "human_review", human_review_node)
            reason = context.human_intervention_reason or "Validation failed after execution"
            set_run_status(run, "failed", reason)
            append_event(run, "run:failed", {"runId": run_id, "reason": reason})
            return

        context = run_node(run, run_id, context, "finalize", finalize_node)
        attach_artifacts(run, context)
        set_run_status(run, "completed")
        append_event(run, "run:completed", {"runId": run_id})
    except Exception as exc:
        message = str(exc)
        canceled = message == "Run canceled"
        status = "canceled" if canceled else "failed"
        set_run_status(run, status, message)
        append_event(run, "run:failed", {"runId": run_id, "reason": message})
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
    if not Path(request.schemaPath).exists():
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
            schemaId=request.schemaId,
            sourceLanguage=request.sourceLanguage,
            sourcePath=request.sourcePath,
            schemaPath=request.schemaPath,
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
                "artifacts": [
                    {"name": item.name, "type": item.type, "createdAt": item.createdAt}
                    for item in run.artifacts
                ],
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
        payload = asdict(run)
        for artifact in payload["artifacts"]:
            artifact.pop("path", None)
        return payload


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
async def stream_events(run_id: str, x_execution_token: Optional[str] = Header(default=None)) -> StreamingResponse:
    require_auth(x_execution_token)
    with RUN_LOCK:
        if run_id not in RUNS:
            raise HTTPException(status_code=404, detail="Run not found")

    async def iterator():
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
                idx += 1
                yield f"event: {event['type']}\n".encode("utf-8")
                payload = event.get("payload", {})
                yield f"data: {json.dumps(payload)}\n\n".encode("utf-8")
            now = time.time()
            if now - heartbeat_at >= 20:
                heartbeat_at = now
                yield b": heartbeat\n\n"
            if status in ("completed", "failed", "canceled") and idx >= total_events:
                break
            await asyncio.sleep(0.25)

    return StreamingResponse(iterator(), media_type="text/event-stream")


@app.get("/v1/runs/{run_id}/artifacts/{name}")
def download_artifact(run_id: str, name: str, x_execution_token: Optional[str] = Header(default=None)):
    require_auth(x_execution_token)
    with RUN_LOCK:
        run = RUNS.get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        artifact = next((item for item in run.artifacts if item.name == name), None)
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        file_path = artifact.path
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Artifact file missing")
    return FileResponse(path=file_path, filename=name)
