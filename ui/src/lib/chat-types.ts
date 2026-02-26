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
  selfHealIteration?: number;
  executionErrors?: ExecuteErrorEvent[];
};

export type ArtifactSummary = { name: string; type: string; createdAt: string };

/* ── Chat messages ───────────────────────────────────────── */

export type ChatMessageRole = "system" | "agent" | "error" | "user";
export type ChatMessageKind = "text" | "sql_statement" | "sql_error" | "thinking";

export type ChatSqlDetails = {
  statement?: string;
  output?: string;
  error?: string;
  failedStatement?: string;
};

export type ChatMessage = {
  id: string;
  role: ChatMessageRole;
  content: string;
  kind: ChatMessageKind;
  sql?: ChatSqlDetails;
};

/* ── Execution tracker ───────────────────────────────────── */

export type CurrentExecution = {
  fileIndex: number;
  statementIndex: number;
  elapsedMs: number;
  rowsReturned: number;
  status: "Idle" | "Running" | "Succeeded" | "Failed" | "Paused";
};

/* ── Step blueprint ──────────────────────────────────────── */

export const STEP_BLUEPRINT: StepState[] = [
  { id: "init_project", label: "Initialize project", status: "pending" },
  { id: "add_source_code", label: "Ingest source SQL", status: "pending" },
  { id: "apply_schema_mapping", label: "Apply schema mapping", status: "pending" },
  { id: "convert_code", label: "Convert SQL", status: "pending" },
  { id: "execute_sql", label: "Execute SQL", status: "pending" },
  { id: "self_heal", label: "Self-heal fixes", status: "pending" },
  { id: "validate", label: "Validate output", status: "pending" },
  { id: "human_review", label: "Human review", status: "pending" },
  { id: "finalize", label: "Finalize artifacts", status: "pending" },
];

export const INITIAL_EXECUTION: CurrentExecution = {
  fileIndex: -1,
  statementIndex: -1,
  elapsedMs: 0,
  rowsReturned: 0,
  status: "Idle",
};

export const STATUS_MAP: Record<string, string> = {
  pending: "pending",
  running: "in-progress",
  completed: "completed",
  failed: "failed",
  skipped: "skipped",
};
