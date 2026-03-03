import type { StepState } from "@/lib/migration-types";
import type { Task } from "@/components/ui/agent-plan";
import {
  STEP_BLUEPRINT,
  STATUS_MAP,
  type ChatMessage,
  type ChatMessageRole,
  type ChatMessageKind,
  type ChatSqlDetails,
  type ExecuteStatementEvent,
  type ExecuteErrorEvent,
} from "@/lib/chat-types";

/* ── Predicates ──────────────────────────────────────────── */

export const isTerminal = (s: string) =>
  ["completed", "failed", "canceled"].includes(s);

export const isActive = (s: string) =>
  ["running", "queued"].includes(s);

/* ── Text helpers ────────────────────────────────────────── */

export function sanitizeMessageContent(content: string): string {
  const trimmed = content.trim();
  const withoutTimePrefix = trimmed.replace(
    /^\[\d{1,2}:\d{2}(?::\d{2})?\]\s*/,
    "",
  );
  return withoutTimePrefix.length > 0 ? withoutTimePrefix : trimmed;
}

/* ── Message factories ───────────────────────────────────── */

export function makeMessage(
  role: ChatMessageRole,
  content: string,
  kind: ChatMessageKind = "log",
  sql?: ChatSqlDetails,
): ChatMessage {
  return {
    id:
      globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`,
    role,
    content: sanitizeMessageContent(content),
    kind,
    sql,
  };
}

/**
 * Creates a "thinking" message — visually distinct shimmer bubble used
 * to surface LLM reasoning during self-heal and other analysis steps.
 */
export function makeThinkingMessage(content: string): ChatMessage {
  return makeMessage("agent", content, "thinking");
}

/* ── Step / Task merging ─────────────────────────────────── */

export function mergeSteps(steps?: StepState[]): StepState[] {
  if (!steps?.length) return STEP_BLUEPRINT;
  return STEP_BLUEPRINT.map(
    (base) => steps.find((x) => x.id === base.id) ?? base,
  );
}

export function buildTasks(steps: StepState[], runStatus: string): Task[] {
  const workflowStatus =
    runStatus === "completed"
      ? "completed"
      : runStatus === "failed" || runStatus === "canceled"
        ? "failed"
        : runStatus === "running" || runStatus === "queued"
          ? "in-progress"
          : "pending";

  return [
    {
      id: "workflow",
      title: "Autonomous Migration Workflow",
      description:
        "Live agent execution across validation, self-heal, and finalize.",
      status: workflowStatus,
      priority: "high",
      level: 0,
      dependencies: [],
      subtasks: steps.map((step) => ({
        id: step.id,
        title: step.label,
        description: "Agent stage in progress.",
        status: STATUS_MAP[step.status] ?? "pending",
        priority: "medium",
      })),
    },
  ];
}

/* ── Execution log helpers ───────────────────────────────── */

export function flattenExecutionLog(log: unknown): ExecuteStatementEvent[] {
  if (!Array.isArray(log)) return [];
  const out: ExecuteStatementEvent[] = [];
  for (const fileEntry of log) {
    if (!fileEntry || typeof fileEntry !== "object") continue;
    const row = fileEntry as Record<string, unknown>;
    if (!Array.isArray(row.statements)) continue;
    for (const s of row.statements) {
      if (!s || typeof s !== "object") continue;
      const stmt = s as Record<string, unknown>;
      out.push({
        file: row.file as string | undefined,
        fileIndex: row.index as number | undefined,
        statementIndex: stmt.statement_index as number | undefined,
        statement: stmt.statement as string | undefined,
        status: stmt.status as string | undefined,
        rowCount: stmt.row_count as number | undefined,
        outputPreview: Array.isArray(stmt.output_preview)
          ? (stmt.output_preview as unknown[])
          : [],
      });
    }
  }
  return out;
}

function formatOutputPreview(preview: unknown[]): string | undefined {
  if (!Array.isArray(preview) || preview.length === 0) return undefined;
  const lines: string[] = [];
  preview.forEach((row, idx) => {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      const entries = Object.entries(row as Record<string, unknown>);
      if (entries.length === 0) {
        lines.push(`Row ${idx + 1}: (empty)`);
        return;
      }
      if (entries.length === 1) {
        const [key, value] = entries[0];
        lines.push(`${key}: ${String(value ?? "")}`);
        return;
      }
      lines.push(`Row ${idx + 1}:`);
      entries.forEach(([key, value]) =>
        lines.push(`- ${key}: ${String(value ?? "")}`),
      );
      return;
    }
    lines.push(String(row ?? ""));
  });
  return lines.join("\n");
}

export function makeSqlStatementMessage(
  entry: ExecuteStatementEvent,
): ChatMessage {
  const label =
    typeof entry.statementIndex === "number"
      ? `Stmt ${entry.statementIndex + 1}`
      : "Stmt ?";
  const output = formatOutputPreview(
    Array.isArray(entry.outputPreview) ? entry.outputPreview : [],
  );
  return makeMessage("agent", label, "sql_statement", {
    statement:
      typeof entry.statement === "string" && entry.statement.trim().length > 0
        ? entry.statement
        : undefined,
    output,
  });
}

export function makeSqlErrorMessage(
  entry: ExecuteErrorEvent,
  prefix?: string,
): ChatMessage {
  const label =
    typeof entry.failedStatementIndex === "number"
      ? `Stmt ${entry.failedStatementIndex + 1}`
      : "Stmt ?";
  const errorType =
    typeof entry.errorType === "string" && entry.errorType.trim().length > 0
      ? entry.errorType
      : "execution_error";
  const errorMessage =
    typeof entry.errorMessage === "string" &&
    entry.errorMessage.trim().length > 0
      ? `${prefix ? `${prefix}\n\n` : ""}${entry.errorMessage}`
      : prefix;
  return makeMessage("error", `${label} • ERROR`, "sql_error", {
    error: errorMessage,
    failedStatement:
      typeof entry.failedStatement === "string" &&
      entry.failedStatement.trim().length > 0
        ? entry.failedStatement
        : undefined,
    output: `Error type: ${errorType}`,
  });
}

export function buildSqlExecutionMessages(
  statements: ExecuteStatementEvent[],
  errors: ExecuteErrorEvent[],
): ChatMessage[] {
  const byStatement = new Map<string, ExecuteErrorEvent[]>();
  const makeKey = (fileIndex?: number, statementIndex?: number) =>
    `${typeof fileIndex === "number" ? fileIndex : "x"}:${typeof statementIndex === "number" ? statementIndex : "x"}`;

  for (const err of errors) {
    const key = makeKey(err.fileIndex, err.failedStatementIndex);
    const bucket = byStatement.get(key) ?? [];
    bucket.push(err);
    byStatement.set(key, bucket);
  }

  const out: ChatMessage[] = [];
  for (const stmt of statements) {
    out.push(makeSqlStatementMessage(stmt));
    const key = makeKey(stmt.fileIndex, stmt.statementIndex);
    const related = byStatement.get(key);
    if (!related || related.length === 0) continue;
    while (related.length > 0) {
      const err = related.shift();
      if (err) out.push(makeSqlErrorMessage(err));
    }
    byStatement.delete(key);
  }

  byStatement.forEach((leftovers) => {
    leftovers.forEach((err) => out.push(makeSqlErrorMessage(err)));
  });

  return out;
}
