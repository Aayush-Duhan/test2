import type { UIMessage } from "ai";
import type { StepState } from "@/lib/migration-types";

export type ExecuteStatementEvent = {
  runId?: string;
  file?: string;
  fileIndex?: number;
  statementIndex?: number;
  statement?: string;
  status?: string;
  rowCount?: number;
  outputPreview?: unknown[];
};

export type ExecuteErrorEvent = {
  runId?: string;
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

export type ChatMessageRole = "agent" | "error" | "user";
export type ChatMessageKind =
  | "agent_response"
  | "agent_thinking"
  | "tool_result"
  | "sql_statement"
  | "sql_error"
  | "user_input";

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

export type RunSyncDataPart = {
  runId: string;
  status: string;
  steps: StepState[];
  requiresDdlUpload: boolean;
  resumeFromStage: string;
  lastExecutedFileIndex: number;
  missingObjects: string[];
  executionErrors: ExecuteErrorEvent[];
};

export type RunStatusDataPart = {
  runId: string;
  status: string;
  error?: string | null;
  requiresDdlUpload?: boolean;
  resumeFromStage?: string;
  lastExecutedFileIndex?: number;
  missingObjects?: string[];
};

export type StepStatusDataPart = {
  runId: string;
  stepId: string;
  label: string;
  status: string;
};

export type TerminalProgressDataPart = {
  runId: string;
  text: string;
  isProgress: boolean;
  stepId?: string;
  stepLabel?: string;
};

export type StreamCursorDataPart = {
  nextPartIndex: number;
};

export type RunChatDataTypes = {
  "run-sync": RunSyncDataPart;
  "run-status": RunStatusDataPart;
  "step-status": StepStatusDataPart;
  "sql-statement": ExecuteStatementEvent;
  "sql-error": ExecuteErrorEvent;
  "terminal-progress": TerminalProgressDataPart;
  "stream-cursor": StreamCursorDataPart;
};

export type RunUiMessage = UIMessage<never, RunChatDataTypes>;

export type RunStreamPart =
  | { type: "start"; messageId: string }
  | { type: "text-start"; id: string }
  | { type: "text-delta"; id: string; delta: string }
  | { type: "text-end"; id: string }
  | { type: "reasoning-start"; id: string }
  | { type: "reasoning-delta"; id: string; delta: string }
  | { type: "reasoning-end"; id: string }
  | { type: "tool-input-start"; toolCallId: string; toolName: string }
  | { type: "tool-input-delta"; toolCallId: string; inputTextDelta: string }
  | { type: "tool-input-available"; toolCallId: string; toolName: string; input: Record<string, unknown> }
  | { type: "tool-output-available"; toolCallId: string; output: unknown }
  | { type: "finish"; messageMetadata?: Record<string, unknown> }
  | { type: "error"; errorText: string }
  | { type: "abort"; reason: string }
  | { type: "data-run-sync"; data: RunSyncDataPart }
  | { type: "data-run-status"; data: RunStatusDataPart }
  | { type: "data-step-status"; data: StepStatusDataPart }
  | { type: "data-sql-statement"; data: ExecuteStatementEvent }
  | { type: "data-sql-error"; data: ExecuteErrorEvent }
  | { type: "data-terminal-progress"; data: TerminalProgressDataPart }
  | { type: "data-stream-cursor"; data: StreamCursorDataPart; transient?: boolean };

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
