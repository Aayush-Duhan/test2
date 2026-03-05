"""Pydantic request/response models and internal dataclasses."""

import uuid
from dataclasses import dataclass, field
from typing import Any, TypedDict

from pydantic import BaseModel

from agentic_core.state import MigrationContext


# ── Pydantic request / response models ──────────────────────────

class StartRunRequest(BaseModel):
    projectId: str
    projectName: str
    sourceId: str
    schemaId: str | None = None
    sourceLanguage: str = "teradata"
    sourcePath: str
    schemaPath: str | None = None
    sfAccount: str | None = None
    sfUser: str | None = None
    sfRole: str | None = None
    sfWarehouse: str | None = None
    sfDatabase: str | None = None
    sfSchema: str | None = None
    sfAuthenticator: str | None = None


class StartRunResponse(BaseModel):
    runId: str


# ── Internal dataclasses ────────────────────────────────────────

@dataclass
class RunStep:
    id: str
    label: str
    status: str = "pending"
    startedAt: str | None = None
    endedAt: str | None = None


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
    sfAccount: str | None
    sfUser: str | None
    sfRole: str | None
    sfWarehouse: str | None
    sfDatabase: str | None
    sfSchema: str | None
    sfAuthenticator: str | None
    status: str
    createdAt: str
    updatedAt: str
    steps: list[RunStep] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)
    validationIssues: list[dict[str, Any]] = field(default_factory=list)
    executionLog: list[dict[str, Any]] = field(default_factory=list)
    executionErrors: list[dict[str, Any]] = field(default_factory=list)
    missingObjects: list[str] = field(default_factory=list)
    requiresDdlUpload: bool = False
    resumeFromStage: str = ""
    lastExecutedFileIndex: int = -1
    selfHealIteration: int = 0
    error: str | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    messages: list[dict[str, Any]] = field(default_factory=list)
    outputDir: str = ""
    ddlUploadPath: str = ""
    executionEventCursor: int = 0


@dataclass
class ResumeRunConfig:
    ddl_content: bytes
    ddl_filename: str
    missing_objects: list[str] = field(default_factory=list)
    resume_from_stage: str = "execute_sql"
    last_executed_file_index: int = -1


# ── LangGraph typed-dict state ──────────────────────────────────

class WorkflowState(TypedDict):
    context: MigrationContext
