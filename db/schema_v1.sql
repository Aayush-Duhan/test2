CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  source_language TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
  source_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
  filename TEXT NOT NULL,
  filepath TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sources_project_created_at
  ON sources(project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS schemas (
  schema_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
  filename TEXT NOT NULL,
  filepath TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schemas_project_created_at
  ON schemas(project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  project_name TEXT NOT NULL,
  source_id TEXT NOT NULL,
  schema_id TEXT,
  source_language TEXT NOT NULL,
  source_path TEXT NOT NULL,
  schema_path TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  error TEXT,
  sf_account TEXT,
  sf_user TEXT,
  sf_role TEXT,
  sf_warehouse TEXT,
  sf_database TEXT,
  sf_schema TEXT,
  sf_authenticator TEXT,
  requires_ddl_upload INTEGER NOT NULL DEFAULT 0,
  resume_from_stage TEXT NOT NULL DEFAULT '',
  last_executed_file_index INTEGER NOT NULL DEFAULT -1,
  self_heal_iteration INTEGER NOT NULL DEFAULT 0,
  missing_objects_json TEXT NOT NULL DEFAULT '[]',
  output_dir TEXT NOT NULL,
  ddl_upload_path TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_runs_project_updated_at
  ON runs(project_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status_updated_at
  ON runs(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS run_steps (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  step_id TEXT NOT NULL,
  label TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT,
  ended_at TEXT,
  PRIMARY KEY (run_id, step_id)
);

CREATE TABLE IF NOT EXISTS run_artifacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_artifacts_run_id_id
  ON run_artifacts(run_id, id);

CREATE TABLE IF NOT EXISTS run_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_logs_run_id_id
  ON run_logs(run_id, id);

CREATE TABLE IF NOT EXISTS run_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  timestamp TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id_id
  ON run_events(run_id, id);

CREATE TABLE IF NOT EXISTS run_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  msg_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  role TEXT NOT NULL,
  kind TEXT NOT NULL,
  content TEXT NOT NULL,
  step_json TEXT,
  sql_json TEXT,
  UNIQUE (run_id, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_run_messages_run_id_id
  ON run_messages(run_id, id);

CREATE TABLE IF NOT EXISTS run_validation_issues (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_validation_issues_run_id_id
  ON run_validation_issues(run_id, id);

CREATE TABLE IF NOT EXISTS run_execution_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  entry_type TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_execution_entries_run_id_id
  ON run_execution_entries(run_id, id);
