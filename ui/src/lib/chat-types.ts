import type { StepState } from "@/lib/migration-types";

/* ── Session & Run ───────────────────────────────────────── */

export type ExecuteStatementEvent = {
  file?: string;
  fileIndex?: number;
  statementIndex?: number;
  statement?: string;
  status?: string;
  rowCount?: number;
  outputPreview?: unknown[];
};

export type ExecuteErrorEvent = {
  file?: string;
  fileIndex?: number;
  errorType?: string;
  errorMessage?: string;
  failedStatement?: string;
  failedStatementIndex?: number;
};

export type SessionSummary = {
  runId: string;
  projectName: string;
  status: string;
  createdAt: string;
  updatedAt: string;
  error?: string | null;
  requiresDdlUpload?: boolean;
  executionErrors?: ExecuteErrorEvent[];
};


/* ── Chat messages ───────────────────────────────────────── */

export type ChatMessageRole = "system" | "agent" | "error" | "user";
export type ChatMessageKind =
  | "step_started"
  | "step_completed"
  | "log"
  | "thinking"
  | "terminal_progress"
  | "sql_statement"
  | "sql_error"
  | "run_status"
  | "agent_response"
  | "user_input"
  | "agent_thinking"
  | "tool_result";

export type ChatSqlDetails = {
  statement?: string;
  output?: string;
  error?: string;
  failedStatement?: string;
};

export type ChatMessage = {
  id: string;
  ts?: string;
  role: ChatMessageRole;
  content: string;
  kind: ChatMessageKind;
  step?: { id: string; label: string };
  sql?: ChatSqlDetails;
};

/* ── Step blueprint ──────────────────────────────────────── */

export const STEP_BLUEPRINT: StepState[] = [
  { id: "init_project", label: "Initialize project", status: "pending" },
  { id: "add_source_code", label: "Ingest source SQL", status: "pending" },
  { id: "apply_schema_mapping", label: "Apply schema mapping", status: "pending" },
  { id: "convert_code", label: "Convert SQL", status: "pending" },
  { id: "execute_sql", label: "Execute SQL", status: "pending" },
  { id: "validate", label: "Validate output", status: "pending" },
  { id: "human_review", label: "Human review", status: "pending" },
  { id: "finalize", label: "Finalize output", status: "pending" },
];

export const STATUS_MAP: Record<string, string> = {
  pending: "pending",
  running: "in-progress",
  completed: "completed",
  failed: "failed",
  skipped: "skipped",
};
