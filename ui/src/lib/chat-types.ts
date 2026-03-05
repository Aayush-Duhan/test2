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

export type OrchestratorDecisionEvent = {
  from_step?: string;
  candidate_steps?: string[];
  selected_step?: string;
  confidence?: number;
  reason?: string;
  summary?: string;
  next_steps?: string[];
  attempt?: number;
  latency_ms?: number;
  model?: string;
  status?: string;
  error?: string | null;
  resolved_step?: string;
  guarded_candidates?: string[];
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


/* ── Chat messages ───────────────────────────────────────── */

export type ChatMessageRole = "system" | "agent" | "error" | "user";
export type ChatMessageKind =
  | "step_started"
  | "step_completed"
  | "log"
  | "thinking"
  | "sql_statement"
  | "sql_error"
  | "run_status";

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

export type TerminalStream = "stdout" | "stderr" | "meta";

export type TerminalEvent =
  | {
      type: "terminal:command";
      runId: string;
      ts: string;
      stepId?: string;
      command: string;
      cwd?: string;
      attempt?: number;
    }
  | {
      type: "terminal:line";
      runId: string;
      ts: string;
      stepId?: string;
      stream: TerminalStream;
      text: string;
    };

export function isTerminalChatKind(kind: ChatMessageKind): kind is "log" | "thinking" {
  return kind === "log" || kind === "thinking";
}

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
  { id: "finalize", label: "Finalize output", status: "pending" },
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
