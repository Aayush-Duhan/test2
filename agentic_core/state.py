from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


class MigrationState(Enum):
    IDLE = "idle"
    INIT_PROJECT = "init_project"
    ADD_SOURCE_CODE = "add_source_code"
    APPLY_SCHEMA_MAPPING = "apply_schema_mapping"
    CONVERT_CODE = "convert_code"
    EXECUTE_SQL = "execute_sql"
    SELF_HEAL = "self_heal"
    VALIDATE = "validate"
    HUMAN_REVIEW = "human_review"
    FINALIZE = "finalize"
    ERROR = "error"
    COMPLETED = "completed"


@dataclass
class MigrationContext:
    project_name: str = ""
    project_path: str = ""
    source_language: str = "teradata"
    target_platform: str = "snowflake"

    sf_account: str = ""
    sf_user: str = ""
    sf_role: str = ""
    sf_warehouse: str = ""
    sf_database: str = ""
    sf_schema: str = ""
    sf_authenticator: str = "externalbrowser"

    source_files: List[str] = field(default_factory=list)
    mapping_csv_path: str = ""
    source_directory: str = ""

    current_file: Optional[str] = None
    current_stage: MigrationState = MigrationState.IDLE

    original_code: str = ""
    schema_mapped_code: str = ""
    converted_code: str = ""
    final_code: str = ""
    statement_type: str = "mixed"
    converted_files: List[str] = field(default_factory=list)

    scai_project_initialized: bool = False
    scai_source_added: bool = False
    scai_converted: bool = False

    self_heal_iteration: int = 0
    max_self_heal_iterations: int = 5  # configurable; default 5
    self_heal_issues: List[Dict] = field(default_factory=list)
    self_heal_log: List[Dict] = field(default_factory=list)

    validation_results: Dict = field(default_factory=dict)
    validation_passed: bool = False
    validation_issues: List[Dict] = field(default_factory=list)

    execution_passed: bool = False
    execution_errors: List[Dict] = field(default_factory=list)
    execution_log: List[Dict] = field(default_factory=list)
    missing_objects: List[str] = field(default_factory=list)
    last_executed_file_index: int = -1

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3

    decision_history: List[Dict] = field(default_factory=list)
    orchestrator_history: List[Dict[str, Any]] = field(default_factory=list)
    last_step_error: str = ""
    last_step_success: bool = False
    node_retry_counts: Dict[str, int] = field(default_factory=dict)
    requires_human_intervention: bool = False
    human_intervention_reason: str = ""
    requires_ddl_upload: bool = False
    ddl_upload_path: str = ""
    resume_from_stage: str = ""
    
    activity_log: List[Dict] = field(default_factory=list)
    activity_log_sink: Optional[Callable[[Dict[str, Any]], None]] = None

    report_context: Dict[str, Any] = field(default_factory=dict)
    ignored_report_codes: List[str] = field(default_factory=list)
    report_scan_summary: Dict[str, Any] = field(default_factory=dict)

    output_path: str = ""
    output_files: List[str] = field(default_factory=list)
    summary_report: Dict = field(default_factory=dict)

    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    session_id: str = ""
