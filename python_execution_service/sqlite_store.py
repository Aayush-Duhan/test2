from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT_DIR / "data" / "app.db"
SCHEMA_PATH = ROOT_DIR / "db" / "schema_v1.sql"


def _db_path() -> Path:
    configured = os.getenv("APP_SQLITE_PATH", "").strip()
    if configured:
        return Path(configured).resolve()
    return DEFAULT_DB_PATH.resolve()


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


@contextmanager
def connect() -> Iterable[sqlite3.Connection]:
    db_path = _db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_schema() -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with connect() as conn:
        conn.executescript(sql)
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(runs)").fetchall()
        }
        if "missing_objects_json" not in columns:
            conn.execute(
                "ALTER TABLE runs ADD COLUMN missing_objects_json TEXT NOT NULL DEFAULT '[]'"
            )
        conn.execute(
            """
            INSERT OR REPLACE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            ("v1", _now_iso()),
        )


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_load(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return fallback


def save_run_snapshot(run: Dict[str, Any]) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO runs(
                run_id, project_id, project_name, source_id, schema_id,
                source_language, source_path, schema_path, status, created_at,
                updated_at, error, sf_account, sf_user, sf_role, sf_warehouse,
                sf_database, sf_schema, sf_authenticator, requires_ddl_upload,
                resume_from_stage, last_executed_file_index, self_heal_iteration,
                missing_objects_json, output_dir, ddl_upload_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                project_id = excluded.project_id,
                project_name = excluded.project_name,
                source_id = excluded.source_id,
                schema_id = excluded.schema_id,
                source_language = excluded.source_language,
                source_path = excluded.source_path,
                schema_path = excluded.schema_path,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                error = excluded.error,
                sf_account = excluded.sf_account,
                sf_user = excluded.sf_user,
                sf_role = excluded.sf_role,
                sf_warehouse = excluded.sf_warehouse,
                sf_database = excluded.sf_database,
                sf_schema = excluded.sf_schema,
                sf_authenticator = excluded.sf_authenticator,
                requires_ddl_upload = excluded.requires_ddl_upload,
                resume_from_stage = excluded.resume_from_stage,
                last_executed_file_index = excluded.last_executed_file_index,
                self_heal_iteration = excluded.self_heal_iteration,
                missing_objects_json = excluded.missing_objects_json,
                output_dir = excluded.output_dir,
                ddl_upload_path = excluded.ddl_upload_path
            """,
            (
                run["runId"],
                run["projectId"],
                run["projectName"],
                run.get("sourceId", ""),
                run.get("schemaId"),
                run.get("sourceLanguage", "teradata"),
                run.get("sourcePath", ""),
                run.get("schemaPath", ""),
                run.get("status", "failed"),
                run.get("createdAt", _now_iso()),
                run.get("updatedAt", _now_iso()),
                run.get("error"),
                run.get("sfAccount"),
                run.get("sfUser"),
                run.get("sfRole"),
                run.get("sfWarehouse"),
                run.get("sfDatabase"),
                run.get("sfSchema"),
                run.get("sfAuthenticator"),
                1 if run.get("requiresDdlUpload") else 0,
                run.get("resumeFromStage", ""),
                int(run.get("lastExecutedFileIndex", -1)),
                int(run.get("selfHealIteration", 0)),
                _json_dump(run.get("missingObjects", [])),
                run.get("outputDir", ""),
                run.get("ddlUploadPath", ""),
            ),
        )

        conn.execute("DELETE FROM run_steps WHERE run_id = ?", (run["runId"],))
        for step in run.get("steps", []):
            conn.execute(
                """
                INSERT INTO run_steps(run_id, step_id, label, status, started_at, ended_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run["runId"],
                    step.get("id", ""),
                    step.get("label", ""),
                    step.get("status", "pending"),
                    step.get("startedAt"),
                    step.get("endedAt"),
                ),
            )

        conn.execute("DELETE FROM run_artifacts WHERE run_id = ?", (run["runId"],))
        for artifact in run.get("artifacts", []):
            conn.execute(
                """
                INSERT INTO run_artifacts(run_id, name, type, path, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run["runId"],
                    artifact.get("name", ""),
                    artifact.get("type", "other"),
                    artifact.get("path", ""),
                    artifact.get("createdAt", _now_iso()),
                ),
            )

        conn.execute("DELETE FROM run_validation_issues WHERE run_id = ?", (run["runId"],))
        for item in run.get("validationIssues", []):
            conn.execute(
                "INSERT INTO run_validation_issues(run_id, payload_json) VALUES (?, ?)",
                (run["runId"], _json_dump(item)),
            )

        conn.execute("DELETE FROM run_execution_entries WHERE run_id = ?", (run["runId"],))
        for item in run.get("executionLog", []):
            conn.execute(
                "INSERT INTO run_execution_entries(run_id, entry_type, payload_json) VALUES (?, ?, ?)",
                (run["runId"], "log", _json_dump(item)),
            )
        for item in run.get("executionErrors", []):
            conn.execute(
                "INSERT INTO run_execution_entries(run_id, entry_type, payload_json) VALUES (?, ?, ?)",
                (run["runId"], "error", _json_dump(item)),
            )


def append_run_log(run_id: str, message: str, created_at: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO run_logs(run_id, message, created_at)
            VALUES (?, ?, ?)
            """,
            (run_id, message, created_at),
        )


def append_run_event(run_id: str, event_type: str, payload: Dict[str, Any], timestamp: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO run_events(run_id, event_type, payload_json, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, event_type, _json_dump(payload), timestamp),
        )


def append_run_message(run_id: str, message: Dict[str, Any]) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO run_messages(
              run_id, msg_id, ts, role, kind, content, step_json, sql_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                message.get("id", ""),
                message.get("ts", _now_iso()),
                message.get("role", "agent"),
                message.get("kind", "log"),
                message.get("content", ""),
                _json_dump(message["step"]) if "step" in message else None,
                _json_dump(message["sql"]) if "sql" in message else None,
            ),
        )


def list_runs() -> List[Dict[str, Any]]:
    with connect() as conn:
        run_rows = conn.execute(
            """
            SELECT
              run_id, project_id, project_name, source_id, schema_id, source_language,
              source_path, schema_path, status, created_at, updated_at, error,
              sf_account, sf_user, sf_role, sf_warehouse, sf_database, sf_schema,
              sf_authenticator, requires_ddl_upload, resume_from_stage,
              last_executed_file_index, self_heal_iteration, missing_objects_json,
              output_dir, ddl_upload_path
            FROM runs
            """
        ).fetchall()

        result: List[Dict[str, Any]] = []
        for row in run_rows:
            run_id = row[0]
            steps = conn.execute(
                """
                SELECT step_id, label, status, started_at, ended_at
                FROM run_steps
                WHERE run_id = ?
                ORDER BY rowid
                """,
                (run_id,),
            ).fetchall()
            artifacts = conn.execute(
                """
                SELECT name, type, path, created_at
                FROM run_artifacts
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
            logs = conn.execute(
                """
                SELECT message
                FROM run_logs
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
            events = conn.execute(
                """
                SELECT event_type, payload_json, timestamp
                FROM run_events
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
            messages = conn.execute(
                """
                SELECT msg_id, ts, role, kind, content, step_json, sql_json
                FROM run_messages
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
            validation_issues = conn.execute(
                """
                SELECT payload_json
                FROM run_validation_issues
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
            execution_entries = conn.execute(
                """
                SELECT entry_type, payload_json
                FROM run_execution_entries
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()

            execution_log: List[Dict[str, Any]] = []
            execution_errors: List[Dict[str, Any]] = []
            for entry_type, payload_json in execution_entries:
                item = _json_load(payload_json, {})
                if entry_type == "error":
                    execution_errors.append(item)
                else:
                    execution_log.append(item)

            result.append(
                {
                    "runId": run_id,
                    "projectId": row[1],
                    "projectName": row[2],
                    "sourceId": row[3],
                    "schemaId": row[4] or "",
                    "sourceLanguage": row[5] or "teradata",
                    "sourcePath": row[6] or "",
                    "schemaPath": row[7] or "",
                    "status": row[8] or "failed",
                    "createdAt": row[9] or _now_iso(),
                    "updatedAt": row[10] or _now_iso(),
                    "error": row[11],
                    "sfAccount": row[12],
                    "sfUser": row[13],
                    "sfRole": row[14],
                    "sfWarehouse": row[15],
                    "sfDatabase": row[16],
                    "sfSchema": row[17],
                    "sfAuthenticator": row[18],
                    "requiresDdlUpload": bool(row[19]),
                    "resumeFromStage": row[20] or "",
                    "lastExecutedFileIndex": int(row[21] if row[21] is not None else -1),
                    "selfHealIteration": int(row[22] if row[22] is not None else 0),
                    "missingObjects": _json_load(row[23] or "[]", []),
                    "outputDir": row[24] or "",
                    "ddlUploadPath": row[25] or "",
                    "steps": [
                        {
                            "id": step[0],
                            "label": step[1],
                            "status": step[2],
                            "startedAt": step[3],
                            "endedAt": step[4],
                        }
                        for step in steps
                    ],
                    "artifacts": [
                        {
                            "name": artifact[0],
                            "type": artifact[1],
                            "path": artifact[2],
                            "createdAt": artifact[3],
                        }
                        for artifact in artifacts
                    ],
                    "logs": [log[0] for log in logs],
                    "events": [
                        {
                            "type": event[0],
                            "payload": _json_load(event[1], {}),
                            "timestamp": event[2],
                        }
                        for event in events
                    ],
                    "messages": [
                        {
                            "id": message[0],
                            "ts": message[1],
                            "role": message[2],
                            "kind": message[3],
                            "content": message[4],
                            **(
                                {"step": _json_load(message[5], {})}
                                if message[5]
                                else {}
                            ),
                            **(
                                {"sql": _json_load(message[6], {})}
                                if message[6]
                                else {}
                            ),
                        }
                        for message in messages
                    ],
                    "validationIssues": [
                        _json_load(issue[0], {}) for issue in validation_issues
                    ],
                    "executionLog": execution_log,
                    "executionErrors": execution_errors,
                }
            )
    return result
