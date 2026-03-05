"""
Workflow nodes for the LangGraph Autonomous Migration Platform.

This module defines all the nodes that execute specific steps in the migration workflow.
Each node takes a MigrationContext, performs its operation, and returns the updated context.
"""

import os
import shutil
import subprocess
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
import time

from .state import MigrationContext, MigrationState
from .report_memory import build_report_context_memory, load_ignored_report_codes

# Configure logging
logger = logging.getLogger(__name__)

def _decode_cli_stream(data: bytes, *, strip: bool = True) -> str:
    """Decode CLI bytes robustly across Windows code pages."""
    if not data:
        return ""
    for encoding in ("utf-8", "utf-8-sig", "cp437", "cp1252", "latin-1"):
        try:
            decoded = data.decode(encoding)
            return decoded.strip() if strip else decoded
        except Exception:
            continue
    fallback = data.decode("utf-8", errors="replace")
    return fallback.strip() if strip else fallback


def _run_scai_command(
    cmd: List[str],
    cwd: str,
    max_retries: int = 4,
    on_command: Optional[Callable[[Dict[str, Any]], None]] = None,
    on_line: Optional[Callable[[str, str], None]] = None,
) -> tuple[int, str, str]:
    """Run a CLI command with streamed stdout/stderr callbacks and retry support."""
    last_return_code = 1
    last_stdout_str = ""
    last_stderr_str = ""

    def emit_command(payload: Dict[str, Any]) -> None:
        if callable(on_command):
            try:
                on_command(payload)
            except Exception:
                pass

    def emit_line(stream: str, text: str) -> None:
        if callable(on_line):
            try:
                on_line(stream, text)
            except Exception:
                pass

    for attempt in range(1, max_retries + 1):
        command_str = " ".join(cmd)
        logger.debug(f"[SCAI CMD] Executing attempt {attempt}/{max_retries}: {command_str} in {cwd}")
        emit_command({"command": command_str, "cwd": cwd, "attempt": attempt})

        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            text=False,
            bufsize=0,
        )

        stdout_lines: List[str] = []
        stderr_lines: List[str] = []

        def reader(stream_name: str, stream, buffer: List[str]) -> None:
            if stream is None:
                return
            try:
                while True:
                    chunk = stream.readline()
                    if not chunk:
                        break
                    line = _decode_cli_stream(chunk, strip=False).rstrip("\r\n")
                    if not line:
                        continue
                    buffer.append(line)
                    emit_line(stream_name, line)
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

        stdout_thread = threading.Thread(
            target=reader,
            args=("stdout", process.stdout, stdout_lines),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=reader,
            args=("stderr", process.stderr, stderr_lines),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        return_code = process.wait()
        stdout_thread.join()
        stderr_thread.join()

        stdout_str = "\n".join(stdout_lines).strip()
        stderr_str = "\n".join(stderr_lines).strip()
        last_return_code = return_code
        last_stdout_str = stdout_str
        last_stderr_str = stderr_str

        # Check for license issues or other random failures that might warrant a retry
        output_lower = stdout_str.lower() + stderr_str.lower()
        if return_code != 0 and ("license" in output_lower or "unauthorized" in output_lower or "unauthenticated" in output_lower):
            logger.warning(f"[SCAI CMD] License/Auth issue detected on attempt {attempt}: \nSTDOUT: {stdout_str}\nSTDERR: {stderr_str}")
            if attempt < max_retries:
                logger.info(f"[SCAI CMD] Retrying command in 2 seconds (attempt {attempt+1}/{max_retries})...")
                time.sleep(2)
                continue
                
        # Also print detailed logs to help debug if it fails for other reasons
        logger.debug(f"[SCAI CMD] Attempt {attempt} completed with code {return_code}.")
        if return_code != 0:
            logger.error(f"[SCAI CMD] Command failed with code {return_code}.\nSTDOUT: {stdout_str}\nSTDERR: {stderr_str}")
            
        return return_code, stdout_str, stderr_str
        
    return last_return_code, last_stdout_str, last_stderr_str


def _make_terminal_callbacks(state: MigrationContext, step_id: str) -> tuple[Callable[[Dict[str, Any]], None], Callable[[str, str], None]]:
    def on_command(payload: Dict[str, Any]) -> None:
        log_event(
            state,
            "info",
            "terminal command",
            {
                "terminal": {
                    "type": "command",
                    "stepId": step_id,
                    "command": payload.get("command", ""),
                    "cwd": payload.get("cwd", ""),
                    "attempt": payload.get("attempt"),
                }
            },
        )

    def on_line(stream: str, text: str) -> None:
        log_event(
            state,
            "info",
            "terminal line",
            {
                "terminal": {
                    "type": "line",
                    "stepId": step_id,
                    "stream": stream,
                    "text": text,
                }
            },
        )

    return on_command, on_line


def process_sql_with_pandas_replace(*args, **kwargs):
    """Compatibility wrapper for schema mapping implementation."""
    from scripts.schema_conversion_teradata_to_snowflake import process_sql_with_pandas_replace as _impl
    return _impl(*args, **kwargs)


def apply_self_healing(*args, **kwargs):
    """Compatibility wrapper for integration self-heal."""
    from .integrations import apply_self_healing as _impl
    return _impl(*args, **kwargs)


def apply_simple_code_fixes(*args, **kwargs):
    """Compatibility wrapper for simple code fixes."""
    from .integrations import apply_simple_code_fixes as _impl
    return _impl(*args, **kwargs)


def format_self_heal_report(*args, **kwargs):
    from .integrations import format_self_heal_report as _impl
    return _impl(*args, **kwargs)


def validate_code(*args, **kwargs):
    """Compatibility wrapper for validation implementation."""
    from .integrations import validate_code as _impl
    return _impl(*args, **kwargs)


def format_validation_report(*args, **kwargs):
    from .integrations import format_validation_report as _impl
    return _impl(*args, **kwargs)

def is_error_state(state: MigrationContext) -> bool:
    """Return True if the workflow is already in an error state."""
    return state.current_stage == MigrationState.ERROR


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
                    with open(file_path, "r", encoding="utf-8") as f:
                        contents.append(f"-- FILE: {filename}\n{f.read()}\n")
                except Exception as e:
                    logger.warning(f"Failed to read {file_path}: {e}")
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

def log_event(state: MigrationContext, level: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
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
            # Never allow log sink failures to break workflow execution.
            pass


def init_project_node(state: MigrationContext) -> MigrationContext:
    """
    Initialize scai project using subprocess.

    Creates a new scai project directory and runs 'scai init' command
    with the specified source language and project name.

    Args:
        state: Current migration context

    Returns:
        Updated migration context with project initialization status
    """
    if is_error_state(state):
        return state

    logger.info(f"Initializing project: {state.project_name}")
    log_event(state, "info", f"Initializing project: {state.project_name}")

    try:
        project_path = os.path.join("projects", state.project_name)

        if os.path.isdir(project_path):
            entries = [entry for entry in os.listdir(project_path) if entry not in {".DS_Store", "Thumbs.db", "desktop.ini"}]
            if entries:
                warning_msg = (
                    f"Project directory already exists and is not empty. "
                    f"Resetting before init: {project_path}"
                )
                logger.warning(warning_msg)
                state.warnings.append(warning_msg)
                log_event(state, "warning", warning_msg)
                shutil.rmtree(project_path, ignore_errors=True)

        os.makedirs(project_path, exist_ok=True)

        cmd = [
            "scai", "init",
            "-l", state.source_language,
            "-n", state.project_name
        ]

        on_command, on_line = _make_terminal_callbacks(state, "init_project")
        return_code, stdout, stderr = _run_scai_command(
            cmd,
            project_path,
            on_command=on_command,
            on_line=on_line,
        )

        if return_code != 0:
            error_detail = stderr or stdout or f"Exit code {return_code}"
            error_msg = f"Failed to initialize project: {error_detail}"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_project_initialized = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.project_path = project_path
        state.scai_project_initialized = True
        state.current_stage = MigrationState.INIT_PROJECT
        state.updated_at = datetime.now()
        logger.info(f"Project initialized successfully at: {project_path}")
        log_event(state, "info", f"Project initialized at: {project_path}")

    except Exception as e:
        error_msg = f"Exception during project initialization: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_project_initialized = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state


def add_source_code_node(state: MigrationContext) -> MigrationContext:
    """
    Add source code files to scai project.

    Copies source files/directories to the project's source directory
    and runs 'scai code add' to register them with scai.

    Args:
        state: Current migration context

    Returns:
        Updated migration context with source code added status
    """
    if is_error_state(state):
        return state

    logger.info(f"Adding source code for project: {state.project_name}")
    log_event(state, "info", f"Adding source code for project: {state.project_name}")

    try:
        source_dir = os.path.join(state.project_path, "source")
        source_dir_abs = os.path.abspath(source_dir)

        source_input = state.source_directory or (state.source_files[0] if state.source_files else "")
        if not source_input:
            error_msg = "No source directory provided for code add"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        source_input_abs = os.path.abspath(source_input)
        if os.path.isfile(source_input_abs):
            source_input_abs = os.path.dirname(source_input_abs)

        if not os.path.isdir(source_input_abs):
            fallback_dir = source_dir_abs
            os.makedirs(fallback_dir, exist_ok=True)
            warning_msg = (
                f"Source directory does not exist: {source_input_abs}. "
                f"Using fallback directory: {fallback_dir}"
            )
            logger.warning(warning_msg)
            state.warnings.append(warning_msg)
            log_event(state, "warning", warning_msg)
            source_input_abs = fallback_dir

        # Ensure scai destination is clean to avoid FDS0002
        if os.path.isdir(source_dir_abs):
            shutil.rmtree(source_dir_abs)

        cmd = ["scai", "code", "add", "-i", source_input_abs]
        on_command, on_line = _make_terminal_callbacks(state, "add_source_code")
        return_code, stdout, stderr = _run_scai_command(
            cmd,
            state.project_path,
            on_command=on_command,
            on_line=on_line,
        )

        if return_code != 0:
            error_detail = stderr or stdout or "Unknown error"
            error_msg = f"Failed to add source code: {error_detail}"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_source_added = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.scai_source_added = True
        state.current_stage = MigrationState.ADD_SOURCE_CODE
        state.updated_at = datetime.now()
        logger.info("Source code added successfully")
        log_event(state, "info", "Source code added successfully")

        if not state.original_code:
            state.original_code = read_sql_files(source_dir)

    except Exception as e:
        error_msg = f"Exception during source code addition: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_source_added = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state


def apply_schema_mapping_node(state: MigrationContext) -> MigrationContext:
    """
    Apply schema mapping using existing schema conversion logic.

    Imports and uses process_sql_with_pandas_replace from
    scripts/schema_conversion_teradata_to_snowflake.py to apply
    schema mappings to the source SQL files.

    Args:
        state: Current migration context

    Returns:
        Updated migration context with schema mapping applied
    """
    if is_error_state(state):
        return state

    logger.info(f"Applying schema mapping for project: {state.project_name}")
    log_event(state, "info", f"Applying schema mapping for project: {state.project_name}")

    try:
        source_dir = os.path.join(state.project_path, "source")
        mapping_path = (state.mapping_csv_path or "").strip()

        if not mapping_path:
            msg = "No schema mapping file provided; skipping schema mapping step."
            logger.info(msg)
            log_event(state, "info", msg)
            state.current_stage = MigrationState.APPLY_SCHEMA_MAPPING
            state.updated_at = datetime.now()
            state.schema_mapped_code = read_sql_files(source_dir)
            return state

        mapped_dir = os.path.join(state.project_path, "source_mapped")
        os.makedirs(mapped_dir, exist_ok=True)

        # Define a logging callback that appends to warnings
        def log_callback(msg):
            state.warnings.append(str(msg))
            logger.info(f"Schema mapping: {msg}")
            log_event(state, "info", f"Schema mapping: {msg}")

        process_sql_with_pandas_replace(
            csv_file_path=mapping_path,
            sql_file_path=source_dir,
            output_dir=mapped_dir,
            logg=log_callback
        )

        # Replace original source with mapped source
        if os.path.isdir(source_dir):
            shutil.rmtree(source_dir)
        if os.path.isdir(mapped_dir):
            shutil.move(mapped_dir, source_dir)
        else:
            os.makedirs(source_dir, exist_ok=True)
            warning_msg = f"Mapped output directory not found after schema mapping: {mapped_dir}"
            state.warnings.append(warning_msg)
            log_event(state, "warning", warning_msg)

        state.current_stage = MigrationState.APPLY_SCHEMA_MAPPING
        state.updated_at = datetime.now()
        logger.info("Schema mapping applied successfully")
        log_event(state, "info", "Schema mapping applied successfully")

        state.schema_mapped_code = read_sql_files(source_dir)

    except Exception as e:
        error_msg = f"Exception during schema mapping: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state


def convert_code_node(state: MigrationContext) -> MigrationContext:
    """
    Run scai code convert to convert source code to target platform.

    Executes 'scai code convert' command to perform the code conversion.

    Args:
        state: Current migration context

    Returns:
        Updated migration context with conversion status
    """
    if is_error_state(state):
        return state

    logger.info(f"Converting code for project: {state.project_name}")
    log_event(state, "info", f"Converting code for project: {state.project_name}")

    try:
        cmd = ["scai", "code", "convert"]
        on_command, on_line = _make_terminal_callbacks(state, "convert_code")
        return_code, stdout, stderr = _run_scai_command(
            cmd,
            state.project_path,
            on_command=on_command,
            on_line=on_line,
        )

        if return_code != 0:
            error_detail = stderr or stdout or "Unknown error"
            error_msg = f"Failed to convert code: {error_detail}"
            logger.error(error_msg)
            state.errors.append(error_msg)
            state.scai_converted = False
            state.current_stage = MigrationState.ERROR
            log_event(state, "error", error_msg)
            return state

        state.scai_converted = True
        state.current_stage = MigrationState.CONVERT_CODE
        state.updated_at = datetime.now()
        logger.info("Code conversion completed successfully")
        log_event(state, "info", "Code conversion completed successfully")

        converted_dir = os.path.join(state.project_path, "converted")
        converted_files = list_sql_files(converted_dir)
        state.converted_files = converted_files
        state.converted_code = read_sql_files(converted_dir)
        if not state.converted_code:
            state.converted_code = state.schema_mapped_code or state.original_code or ""
            if state.converted_code:
                warning_msg = "Converted output files not found; using in-memory SQL content."
                state.warnings.append(warning_msg)
                log_event(state, "warning", warning_msg)

        report_context = build_report_context_memory(state)
        state.report_context = report_context
        state.ignored_report_codes = report_context.get("ignored_codes", [])
        state.report_scan_summary = report_context.get("report_scan_summary", {})

    except Exception as e:
        error_msg = f"Exception during code conversion: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.scai_converted = False
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state


def apply_uploaded_ddl_and_resume(state: MigrationContext) -> MigrationContext:
    """Apply uploaded DDL script and prepare workflow to resume execute_sql."""
    if not state.ddl_upload_path or not os.path.exists(state.ddl_upload_path):
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.human_intervention_reason = "DDL upload is required to resolve missing objects."
        log_event(state, "warning", "DDL upload path missing for resume")
        return state

    try:
        from .snowflake_runtime import (
            build_chat_snowflake_from_context,
            execute_sql_with_chat_runtime,
            close_runtime,
        )

        with open(state.ddl_upload_path, "r", encoding="utf-8-sig") as ddl_file:
            ddl_sql = ddl_file.read()

        if not ddl_sql.strip():
            state.current_stage = MigrationState.HUMAN_REVIEW
            state.requires_human_intervention = True
            state.human_intervention_reason = "Uploaded DDL file is empty."
            log_event(state, "warning", "Uploaded DDL file is empty")
            return state

        chat_model = build_chat_snowflake_from_context(state)
        try:
            execute_sql_with_chat_runtime(chat_model, ddl_sql)
        finally:
            close_runtime(chat_model)

        state.requires_ddl_upload = False
        state.ddl_upload_path = ""
        state.resume_from_stage = "execute_sql"
        state.requires_human_intervention = False
        state.human_intervention_reason = ""
        log_event(state, "info", "Uploaded DDL executed successfully, resuming SQL execution")
        return state
    except Exception as exc:
        error_msg = f"Failed to execute uploaded DDL: {exc}"
        state.errors.append(error_msg)
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.requires_ddl_upload = True
        state.human_intervention_reason = error_msg
        log_event(state, "error", error_msg)
        return state


def execute_sql_node(state: MigrationContext) -> MigrationContext:
    """Execute converted SQL files in Snowflake via ChatSnowflakeCortex session."""
    if is_error_state(state):
        return state

    logger.info("Executing converted SQL for project: %s", state.project_name)
    state.current_stage = MigrationState.EXECUTE_SQL
    state.updated_at = datetime.now()
    log_event(state, "info", "Executing converted SQL")

    if state.requires_ddl_upload:
        state = apply_uploaded_ddl_and_resume(state)
        if state.requires_ddl_upload:
            return state

    converted_dir = os.path.join(state.project_path, "converted")
    sql_files = list_sql_files(converted_dir)

    try:
        from .snowflake_runtime import (
            build_chat_snowflake_from_context,
            execute_sql_with_chat_runtime,
            classify_snowflake_error,
            close_runtime,
        )

        chat_model = build_chat_snowflake_from_context(state)
        try:
            if sql_files:
                start_index = max(0, state.last_executed_file_index + 1)
                for index in range(start_index, len(sql_files)):
                    sql_file = sql_files[index]
                    with open(sql_file, "r", encoding="utf-8-sig") as file_handle:
                        sql_text = file_handle.read()
                    if not sql_text.strip():
                        state.execution_log.append(
                            {"file": sql_file, "index": index, "status": "skipped_empty"}
                        )
                        state.last_executed_file_index = index
                        continue

                    statement_results = execute_sql_with_chat_runtime(chat_model, sql_text)
                    state.execution_log.append(
                        {
                            "file": sql_file,
                            "index": index,
                            "status": "success",
                            "statements": statement_results,
                        }
                    )
                    state.last_executed_file_index = index
            elif state.converted_code.strip():
                statement_results = execute_sql_with_chat_runtime(chat_model, state.converted_code)
                state.execution_log.append(
                    {
                        "file": "in_memory_converted_code",
                        "index": 0,
                        "status": "success",
                        "statements": statement_results,
                    }
                )
                state.last_executed_file_index = 0
            else:
                raise ValueError("No converted SQL files or converted_code found for execution.")
        finally:
            close_runtime(chat_model)

        state.execution_passed = True
        state.execution_errors = []
        state.missing_objects = []
        state.validation_issues = []
        state.updated_at = datetime.now()
        log_event(state, "info", "Converted SQL execution completed successfully")
        return state

    except Exception as exc:
        error_message = str(exc)
        error_type, object_name = ("execution_error", "")
        failed_statement = ""
        failed_statement_index = -1
        partial_results: List[Dict[str, Any]] = []
        if hasattr(exc, "statement"):
            failed_statement = str(getattr(exc, "statement", ""))
            failed_statement_index = int(getattr(exc, "statement_index", -1))
            partial_results = list(getattr(exc, "partial_results", []) or [])
        try:
            from .snowflake_runtime import classify_snowflake_error
            error_type, object_name = classify_snowflake_error(error_message)
        except Exception:
            pass

        state.execution_passed = False
        state.execution_errors.append(
            {
                "type": error_type,
                "message": error_message,
                "object_name": object_name,
                "stage": "execute_sql",
                "statement": failed_statement,
                "statement_index": failed_statement_index,
            }
        )
        state.execution_log.append(
            {
                "file": sql_files[state.last_executed_file_index + 1] if sql_files and state.last_executed_file_index + 1 < len(sql_files) else "unknown",
                "index": state.last_executed_file_index + 1,
                "status": "failed",
                "error_type": error_type,
                "error_message": error_message,
                "missing_object": object_name,
                "statements": partial_results,
                "failed_statement": failed_statement,
                "failed_statement_index": failed_statement_index,
            }
        )

        if error_type == "missing_object":
            if object_name:
                normalized_obj = object_name.strip()
                if normalized_obj and normalized_obj not in state.missing_objects:
                    state.missing_objects.append(normalized_obj)
            state.requires_ddl_upload = True
            state.requires_human_intervention = True
            state.resume_from_stage = "execute_sql"
            state.current_stage = MigrationState.HUMAN_REVIEW
            missing_detail = ", ".join(state.missing_objects) if state.missing_objects else "unresolved object"
            state.human_intervention_reason = (
                f"Missing object detected during execution: {missing_detail}. "
                "Upload DDL script to create required objects, then resume."
            )
            log_event(state, "warning", state.human_intervention_reason)
            state.updated_at = datetime.now()
            return state

        state.validation_issues.append(
            {
                "type": "execution_error",
                "severity": "error",
                "message": error_message,
            }
        )
        log_event(state, "error", f"Execution failed, routing to self-heal: {error_message}")
        state.updated_at = datetime.now()
        return state


def self_heal_node(state: MigrationContext) -> MigrationContext:
    """
    Attempt to fix issues found during validation.

    This node integrates with existing self-healing scripts in:
    - scripts/SELF_HEALING_CODE.py
    - scripts/self_healing_script.py

    The self-healing process:
    1. Reads the current converted code
    2. Identifies issues from validation results
    3. Applies fixes using the self-healing scripts
    4. Updates the code artifacts (converted_code, final_code)
    5. Logs all self-healing actions to state.self_heal_log

    Args:
        state: Current migration context

    Returns:
        Updated migration context with self-healing iteration incremented
    """
    if is_error_state(state):
        return state

    logger.info(f"Self-healing iteration {state.self_heal_iteration + 1} for project: {state.project_name}")
    log_event(state, "info", f"Self-healing iteration {state.self_heal_iteration + 1}")

    try:
        state.self_heal_iteration += 1
        state.current_stage = MigrationState.SELF_HEAL

        # Refresh report-based context memory before each healing iteration
        report_context = build_report_context_memory(state)
        state.report_context = report_context
        state.ignored_report_codes = report_context.get("ignored_codes", load_ignored_report_codes())
        state.report_scan_summary = report_context.get("report_scan_summary", {})

        # Determine which code to heal (converted_code)
        code_to_heal = state.converted_code

        if not code_to_heal:
            error_msg = "No code available for self-healing"
            logger.warning(error_msg)
            state.warnings.append(error_msg)
            state.updated_at = datetime.now()
            log_event(state, "warning", error_msg)
            return state

        # Define a logging callback that appends to state.warnings
        def log_callback(msg):
            state.warnings.append(f"[Self-Heal Iter {state.self_heal_iteration}] {msg}")
            logger.info(f"Self-healing: {msg}")
            log_event(state, "info", f"Self-healing: {msg}")

        # Apply self-healing via Snowflake Cortex
        heal_result = apply_self_healing(
            code=code_to_heal,
            issues=state.validation_issues,
            state=state,
            iteration=state.self_heal_iteration,
            statement_type=state.statement_type,
            logger_callback=log_callback
        )

        # Log the self-healing result
        log_callback(format_self_heal_report(heal_result))

        # Update state with self-healing results
        if heal_result.success:
            # Update the code artifacts
            state.converted_code = heal_result.fixed_code

            if state.converted_files:
                for file_path in state.converted_files:
                    try:
                        path_obj = Path(file_path)
                        path_obj.parent.mkdir(parents=True, exist_ok=True)
                        path_obj.write_text(heal_result.fixed_code, encoding="utf-8")
                    except Exception as file_exc:
                        msg = f"Failed to persist healed code to {file_path}: {file_exc}"
                        state.warnings.append(msg)
                        log_event(state, "warning", msg)

            # Also update final_code if this is the last iteration or no more issues
            if heal_result.issues_fixed == 0 or state.self_heal_iteration >= state.max_self_heal_iterations:
                state.final_code = heal_result.fixed_code

            # Append to self-heal log
            state.self_heal_log.append({
                "iteration": state.self_heal_iteration,
                "timestamp": heal_result.timestamp,
                "success": heal_result.success,
                "fixes_applied": heal_result.fixes_applied,
                "issues_fixed": heal_result.issues_fixed,
                "llm_provider": "snowflake_cortex"
            })

            logger.info(f"Self-healing iteration {state.self_heal_iteration} completed successfully")
            log_event(state, "info", f"Self-healing iteration {state.self_heal_iteration} completed")
        else:
            # Self-healing failed
            error_msg = heal_result.error_message or "Self-healing failed"
            state.errors.append(f"[Self-Heal Iter {state.self_heal_iteration}] {error_msg}")
            log_event(state, "error", f"Self-heal failed: {error_msg}")

            # Append to self-heal log even on failure
            state.self_heal_log.append({
                "iteration": state.self_heal_iteration,
                "timestamp": heal_result.timestamp,
                "success": heal_result.success,
                "error": heal_result.error_message,
                "llm_provider": "snowflake_cortex"
            })

            logger.warning(f"Self-healing iteration {state.self_heal_iteration} failed: {error_msg}")

        state.updated_at = datetime.now()

    except Exception as e:
        error_msg = f"Exception during self-healing: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

        # Append to self-heal log even on exception
        state.self_heal_log.append({
            "iteration": state.self_heal_iteration,
            "timestamp": datetime.now().isoformat(),
            "success": False,
            "error": error_msg
        })

    return state


def validate_node(state: MigrationContext) -> MigrationContext:
    """
    Validate converted output.

    This node integrates with existing validation scripts in:
    - scripts/VALIDATION_TAB.py
    - scripts/validation2.py

    The validation process:
    1. Reads the current code (converted_code)
    2. Runs syntax checks
    3. Runs Snowflake compilation checks (if connection available)
    4. Runs linting/validation from the validation scripts
    5. Populates state.validation_passed, state.validation_issues, state.validation_results

    Args:
        state: Current migration context

    Returns:
        Updated migration context with validation results
    """
    if is_error_state(state):
        return state

    logger.info(f"Validating converted code for project: {state.project_name}")
    log_event(state, "info", f"Validating converted code for project: {state.project_name}")

    try:
        state.current_stage = MigrationState.VALIDATE
        state.validation_issues = []

        # Determine which code to validate (converted_code)
        code_to_validate = state.converted_code

        if not code_to_validate:
            error_msg = "No code available for validation"
            logger.warning(error_msg)
            state.warnings.append(error_msg)
            state.validation_passed = False
            state.validation_issues.append({
                "type": "validation_error",
                "severity": "error",
                "message": error_msg
            })
            state.updated_at = datetime.now()
            log_event(state, "warning", error_msg)
            return state

        # Define a logging callback that appends to state.warnings
        def log_callback(msg):
            state.warnings.append(f"[Validation] {msg}")
            logger.info(f"Validation: {msg}")
            log_event(state, "info", f"Validation: {msg}")

        # Run validation
        validation_result = validate_code(
            code=code_to_validate,
            original_code=state.original_code if state.original_code else None,
            state=state,
            logger_callback=log_callback
        )

        # Log the validation result
        log_callback(format_validation_report(validation_result))

        # Update state with validation results
        state.validation_passed = validation_result.passed
        state.validation_issues = validation_result.issues
        state.validation_results = validation_result.results

        # If validation passed, update final_code
        if validation_result.passed:
            state.final_code = code_to_validate
            logger.info("Validation passed - code is ready for finalization")
            log_event(state, "info", "Validation passed")
        else:
            logger.warning(f"Validation failed with {len(validation_result.issues)} issues")
            log_event(state, "warning", f"Validation failed with {len(validation_result.issues)} issues")

        state.updated_at = datetime.now()

    except Exception as e:
        error_msg = f"Exception during validation: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        state.validation_passed = False
        state.validation_issues.append({
            "type": "validation_error",
            "severity": "error",
            "message": error_msg
        })
        log_event(state, "error", error_msg)

    return state


def human_review_node(state: MigrationContext) -> MigrationContext:
    """
    Pause workflow for user intervention via Streamlit UI.

    This node sets the state to HUMAN_REVIEW, which should trigger
    the UI to display the current state and allow user interaction.

    Args:
        state: Current migration context

    Returns:
        Updated migration context with human review stage set
    """
    if is_error_state(state):
        return state

    logger.info(f"Requesting human review for project: {state.project_name}")
    log_event(state, "info", "Human review requested")

    try:
        state.current_stage = MigrationState.HUMAN_REVIEW
        state.requires_human_intervention = True
        state.updated_at = datetime.now()
        logger.info("Human review requested - workflow paused")

    except Exception as e:
        error_msg = f"Exception during human review setup: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state


def finalize_node(state: MigrationContext) -> MigrationContext:
    """
    Finalize migration and generate output.

    Collects all output files, generates a summary report,
    and sets the final state to COMPLETED.

    Args:
        state: Current migration context

    Returns:
        Updated migration context with finalization complete
    """
    if is_error_state(state):
        return state

    logger.info(f"Finalizing migration for project: {state.project_name}")
    log_event(state, "info", f"Finalizing migration for project: {state.project_name}")

    try:
        output_dir = os.path.join("outputs", state.project_name)
        os.makedirs(output_dir, exist_ok=True)

        # Collect output files from various scai directories
        converted_dir = os.path.join(state.project_path, "converted")

        if os.path.exists(converted_dir):
            for root, _, files in os.walk(converted_dir):
                for file in files:
                    src_path = os.path.join(root, file)
                    rel_path = os.path.relpath(src_path, converted_dir)
                    dst_path = os.path.join(output_dir, "converted", rel_path)
                    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                    shutil.copy2(src_path, dst_path)
                    state.output_files.append(dst_path)

        # Generate summary report
        state.summary_report = {
            "project_name": state.project_name,
            "source_language": state.source_language,
            "target_platform": state.target_platform,
            "scai_project_initialized": state.scai_project_initialized,
            "scai_source_added": state.scai_source_added,
            "scai_converted": state.scai_converted,
            "self_heal_iterations": state.self_heal_iteration,
            "validation_passed": state.validation_passed,
            "validation_issues_count": len(state.validation_issues),
            "errors_count": len(state.errors),
            "warnings_count": len(state.warnings),
            "output_files_count": len(state.output_files),
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
        }

        state.output_path = output_dir
        state.validation_passed = True
        state.current_stage = MigrationState.COMPLETED
        state.updated_at = datetime.now()
        logger.info(f"Migration finalized. Output at: {output_dir}")
        log_event(state, "info", f"Migration finalized. Output at: {output_dir}")

    except Exception as e:
        error_msg = f"Exception during finalization: {str(e)}"
        logger.error(error_msg)
        state.errors.append(error_msg)
        state.current_stage = MigrationState.ERROR
        log_event(state, "error", error_msg)

    return state
