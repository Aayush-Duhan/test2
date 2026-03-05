"use client";

import * as React from "react";
import { ChevronRight } from "lucide-react";
import { PromptBox } from "@/components/ui/chatgpt-prompt-input";
import type { Task } from "@/components/ui/agent-plan";
import { SidebarInset } from "@/components/ui/sidebar";
import { sanitizeMessageContent } from "@/lib/chat-helpers";
import type { ChatMessage } from "@/lib/chat-types";
import { SetupWizard } from "@/components/ui/setup-wizard";
import { Workbench } from "@/components/workbench";
import { workbenchStore, type UploadedFile } from "@/lib/workbench-store";

interface ChatPanelProps {
  runId: string | null;
  projectId: string | null;
  isHydratingRun?: boolean;
  status: string;
  error: string | null;
  isBusy: boolean;
  messages: ChatMessage[];
  tasks: Task[];
  requiresDdlUpload: boolean;
  resumeFromStage: string;
  lastExecutedFileIndex: number;
  missingObjects: string[];
  isAgentThinking?: boolean;
  uploadedFiles?: UploadedFile[];
  onCreateProject: () => void;
  onRetryRun: () => void;
  onResetSession: () => void;
  onPickDdlFile: () => void;
  onSendAgentMessage?: (message: string) => void;
}

export function ChatPanel({
  runId,
  projectId,
  isHydratingRun = false,
  status,
  error,
  isBusy,
  messages,
  tasks,
  requiresDdlUpload,
  resumeFromStage,
  lastExecutedFileIndex,
  missingObjects,
  isAgentThinking = false,
  uploadedFiles = [],
  onCreateProject,
  onRetryRun,
  onPickDdlFile,
  onSendAgentMessage,
}: ChatPanelProps) {
  const isSessionFinished = runId !== null && ["failed", "canceled"].includes(status);
  const isAgentPhase = status === "completed";
  const hasActiveRun = runId !== null;
  const isChatInputEnabled = hasActiveRun && isAgentPhase && !isSessionFinished && !!onSendAgentMessage;

  const chatInputHint = !hasActiveRun
    ? null
    : isSessionFinished
      ? "This session has ended. Retry or start a new session to continue chatting with the agent."
      : !isAgentPhase
        ? "Agent chat unlocks automatically after CLI conversion completes."
        : "Press Enter to send. Use Shift + Enter for a new line.";

  React.useEffect(() => {
    if (uploadedFiles.length > 0) {
      workbenchStore.addUploadedFiles(uploadedFiles);
    }
  }, [uploadedFiles]);

  React.useEffect(() => {
    if (!runId) {
      workbenchStore.clearFiles();
    }
  }, [runId]);

  const syncProjectFiles = React.useCallback(async (activeProjectId: string) => {
    try {
      const [sourceRes, outputRes] = await Promise.all([
        fetch(`/api/projects/${activeProjectId}/files?path=source`, { cache: "no-store" }),
        fetch(`/api/projects/${activeProjectId}/files?path=output`, { cache: "no-store" }),
      ]);

      const files: UploadedFile[] = [];

      const appendFiles = (payload: unknown) => {
        if (!payload || typeof payload !== "object") return;
        const items = (payload as {
          items?: Array<{
            type?: string;
            content?: string;
            name?: string;
            path?: string;
            isBinary?: boolean;
          }>;
        }).items;

        if (!Array.isArray(items)) return;

        for (const item of items) {
          if (item?.type !== "file" || !item.content || !item.path) continue;
          files.push({
            name: item.name ?? item.path,
            content: item.content,
            relativePath: item.path,
            isBinary: item.isBinary,
          });
        }
      };

      if (sourceRes.ok) appendFiles(await sourceRes.json());
      if (outputRes.ok) appendFiles(await outputRes.json());

      if (files.length > 0) {
        workbenchStore.addUploadedFiles(files);
      }
    } catch (syncError) {
      console.error("Failed to sync project files:", syncError);
    }
  }, []);

  React.useEffect(() => {
    if (!projectId) return;

    // Eager initial sync so files appear immediately on mount
    void syncProjectFiles(projectId);

    // Subscribe to the file-watcher SSE stream for instant updates.
    // The watcher monitors the entire project root so it detects files
    // even when parent directories are created after it starts.
    const source = new EventSource(`/api/projects/${projectId}/watch`);
    let debounceTimer: ReturnType<typeof setTimeout> | null = null;

    source.addEventListener("files:changed", () => {
      // Debounce rapid bursts (e.g. multi-file writes) into a single sync
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        void syncProjectFiles(projectId);
      }, 300);
    });

    return () => {
      source.close();
      if (debounceTimer) clearTimeout(debounceTimer);
    };
  }, [projectId, runId, status, syncProjectFiles]);

  return (
    <div className="flex min-h-0 flex-1 overflow-hidden">
      <SidebarInset className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#1a1a1a]">
        <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-hidden p-4">
          {runId ? (
            <>
              {requiresDdlUpload && (
                <DdlUploadBanner
                  isBusy={isBusy}
                  resumeFromStage={resumeFromStage}
                  lastExecutedFileIndex={lastExecutedFileIndex}
                  missingObjects={missingObjects}
                  onPickDdlFile={onPickDdlFile}
                  onRetryRun={onRetryRun}
                />
              )}

              <MessageList
                tasks={tasks}
                messages={messages}
                error={error}
                status={status}
                isAgentThinking={isAgentThinking}
              />
            </>
          ) : isHydratingRun ? (
            <div className="flex flex-1 items-center justify-center">
              <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/70">Loading session...</div>
            </div>
          ) : (
            <div className="flex flex-1 items-center justify-center">
              <SetupWizard onStartMigration={onCreateProject} isBusy={isBusy} />
            </div>
          )}
        </div>

        {hasActiveRun && (
          <div className="shrink-0 border-t border-white/10 px-4 py-3">
            <PromptBox
              placeholder={
                isChatInputEnabled
                  ? "Ask the agent to review or fix converted code..."
                  : "Chat will be available after conversion completes..."
              }
              onSend={onSendAgentMessage}
              disabled={!isChatInputEnabled}
              isSending={isChatInputEnabled && isBusy}
            />

            {chatInputHint && <p className="mt-2 px-1 text-xs text-white/60">{chatInputHint}</p>}

          </div>
        )}
      </SidebarInset>

      <Workbench chatStarted={!!runId} isStreaming={isAgentThinking} />
    </div>
  );
}

function DdlUploadBanner({
  isBusy,
  resumeFromStage,
  lastExecutedFileIndex,
  missingObjects,
  onPickDdlFile,
  onRetryRun,
}: {
  isBusy: boolean;
  resumeFromStage: string;
  lastExecutedFileIndex: number;
  missingObjects: string[];
  onPickDdlFile: () => void;
  onRetryRun: () => void;
}) {
  return (
    <div className="rounded-2xl border border-amber-400/40 bg-amber-500/10 p-4">
      <h3 className="text-sm font-semibold text-amber-100">Execution Paused: Missing Objects</h3>
      <p className="mt-1 text-xs text-amber-50/80">Upload DDL and resume from checkpoint.</p>
      <div className="mt-2 flex flex-wrap gap-2 text-xs text-amber-100">
        <span className="rounded-full bg-amber-500/20 px-2 py-1">Resume from {resumeFromStage || "execute_sql"}</span>
        <span className="rounded-full bg-amber-500/20 px-2 py-1">
          Last executed file {lastExecutedFileIndex >= 0 ? lastExecutedFileIndex + 1 : "-"}
        </span>
        {missingObjects.map((obj) => (
          <span key={obj} className="rounded-full bg-amber-500/20 px-2 py-1">
            {obj}
          </span>
        ))}
      </div>
      <div className="mt-3 flex items-center gap-2">
        <button
          type="button"
          disabled={isBusy}
          onClick={onPickDdlFile}
          className="rounded-full border border-amber-300/40 px-3 py-1 text-xs text-amber-100 hover:bg-amber-500/20 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {isBusy ? "Resuming..." : "Upload DDL and Resume"}
        </button>
        <button
          type="button"
          onClick={onRetryRun}
          className="rounded-full border border-white/25 px-3 py-1 text-xs text-white/85 hover:bg-white/10"
        >
          Retry run
        </button>
      </div>
    </div>
  );
}

function MessageList({
  tasks,
  messages,
  error,
  status,
  isAgentThinking = false,
}: {
  tasks: Task[];
  messages: ChatMessage[];
  error: string | null;
  status: string;
  isAgentThinking?: boolean;
}) {
  const scrollRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const isNearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 120;
    if (isNearBottom) {
      el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
    }
  }, [messages, isAgentThinking]);

  return (
    <div ref={scrollRef} className="scrollbar-dark flex min-h-0 flex-1 flex-col overflow-y-auto px-1 py-2">
      {messages.length === 0 ? (
        <p className="text-sm text-white/50">Flow updates will appear here once the migration starts.</p>
      ) : (
        <NodeLogTimeline messages={messages} tasks={tasks} runStatus={status} />
      )}

      {isAgentThinking && (
        <div className="mt-3 flex justify-start">
          <div className="rounded-2xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/70">Agent is working...</div>
        </div>
      )}

      {error && (
        <div className="mt-3 flex justify-start">
          <div className="max-w-[90%] whitespace-pre-wrap rounded-2xl border border-red-400/30 bg-red-500/10 px-3 py-2 text-sm leading-relaxed text-red-100">
            {sanitizeMessageContent(error)}
          </div>
        </div>
      )}
    </div>
  );
}

function NodeLogTimeline({
  messages,
  tasks,
  runStatus,
}: {
  messages: ChatMessage[];
  tasks: Task[];
  runStatus: string;
}) {
  const workflow = tasks[0];
  const subtasks = workflow?.subtasks ?? [];

  const stepStatusById = new Map(subtasks.map((step) => [step.id, step.status]));
  const hasStepMessages = React.useMemo(
    () => messages.some((m) => m.kind === "step_started" || m.kind === "step_completed"),
    [messages],
  );

  return (
    <div className="space-y-3">
      {!hasStepMessages &&
        subtasks
          .filter((step) => step.status !== "pending")
          .map((step) => (
            <StepCheckpointCard key={`fallback-${step.id}`} title={step.title} status={step.status} runStatus={runStatus} />
          ))}

      {messages.map((message, index) => {
        const renderKey = `${message.id}-${message.ts ?? "no-ts"}-${index}`;

        if (message.kind === "step_started" || message.kind === "step_completed") {
          const title = message.step?.label ?? sanitizeMessageContent(message.content);
          const statusFromTasks = message.step?.id ? stepStatusById.get(message.step.id) : undefined;
          const fallbackStatus = message.kind === "step_completed" ? "completed" : "in-progress";
          return (
            <StepCheckpointCard
              key={renderKey}
              title={title}
              status={statusFromTasks ?? fallbackStatus}
              runStatus={runStatus}
            />
          );
        }

        return <ChatBubble key={renderKey} message={message} />;
      })}
    </div>
  );
}

function StepCheckpointCard({
  title,
  status,
  runStatus,
}: {
  title: string;
  status?: string;
  runStatus: string;
}) {
  const isDone = status === "completed";
  const isActive = status === "in-progress";
  const rowClass = isDone
    ? "border-emerald-400/30 bg-emerald-500/10"
    : isActive || runStatus === "running" || runStatus === "queued"
      ? "border-amber-400/40 bg-amber-500/10"
      : "border-white/10 bg-white/5";

  const label = isDone ? "Complete" : isActive ? "In Progress" : "Started";

  return (
    <div className={`rounded-xl border px-3 py-2 ${rowClass}`}>
      <div className="flex items-center justify-between gap-2">
        <p className="text-sm font-medium text-white/90">{title}</p>
        <span className="rounded-full border border-white/20 bg-white/10 px-2 py-0.5 text-[11px] text-white/80">
          {label}
        </span>
      </div>
    </div>
  );
}
function ChatBubble({ message: m }: { message: ChatMessage }) {
  const isUser = m.role === "user";
  const isSystem = m.role === "system";
  const isError = m.role === "error";
  const isSqlRow = m.kind === "sql_statement" || m.kind === "sql_error";

  if (isSqlRow && m.sql) {
    const rowClass = isError
      ? "border border-red-400/30 bg-red-500/10 text-red-100"
      : "border border-white/10 bg-[#1f1f1f] text-white/92";

    return (
      <div className={`flex ${isUser ? "justify-end" : "justify-start"}`}>
        <details className={`group max-w-[90%] rounded-2xl px-3 py-2 ${rowClass}`}>
          <summary className="flex cursor-pointer list-none items-center gap-2 text-sm leading-relaxed">
            <ChevronRight className="h-4 w-4 shrink-0 transition-transform group-open:rotate-90" />
            <span className="font-medium">{m.content}</span>
          </summary>
          <div className="mt-3 space-y-2 border-t border-white/10 pt-3">
            {m.sql.statement && <SqlBlockSection title="Query" content={m.sql.statement} />}
            {m.sql.failedStatement && <SqlBlockSection title="Query" content={m.sql.failedStatement} />}
            {m.sql.output && <SqlBlockSection title="Output" content={m.sql.output} />}
            {m.sql.error && <SqlBlockSection title="Error" content={m.sql.error} isError />}
          </div>
        </details>
      </div>
    );
  }

  const bubbleClass = isUser
    ? "bg-blue-500/85 text-white"
    : isError
      ? "border border-red-400/30 bg-red-500/10 text-red-100"
      : isSystem
        ? "border border-white/10 bg-white/5 text-white/80"
        : "border border-white/10 bg-[#1f1f1f] text-white/92";

  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"}`}>
      <div className={`max-w-[90%] whitespace-pre-wrap rounded-2xl px-3 py-2 text-sm leading-relaxed ${bubbleClass}`}>
        {buildPlainMessageBody(m)}
      </div>
    </div>
  );
}

function SqlBlockSection({
  title,
  content,
  isError = false,
}: {
  title: "Query" | "Output" | "Error";
  content: string;
  isError?: boolean;
}) {
  return (
    <div>
      <div className="mb-1 flex items-center gap-2">
        <span
          className={`rounded-full border px-2 py-0.5 text-[11px] ${isError
            ? "border-red-400/40 bg-red-500/15 text-red-100"
            : "border-white/20 bg-white/10 text-white/80"
            }`}
        >
          {title}
        </span>
      </div>
      <pre
        className={`max-h-52 overflow-auto whitespace-pre-wrap rounded-xl border px-3 py-2 text-xs ${isError
          ? "border-red-400/30 bg-red-500/10 text-red-100"
          : "border-white/10 bg-black/30 text-white/85"
          }`}
      >
        {content}
      </pre>
    </div>
  );
}

function buildPlainMessageBody(message: ChatMessage): string {
  const chunks: string[] = [message.content];

  if (message.sql?.statement) chunks.push(`SQL:\n${message.sql.statement}`);
  if (message.sql?.output) chunks.push(`Output:\n${message.sql.output}`);
  if (message.sql?.error) chunks.push(`Error:\n${message.sql.error}`);
  if (message.sql?.failedStatement) chunks.push(`Failed SQL:\n${message.sql.failedStatement}`);

  return chunks.filter((chunk) => chunk.trim().length > 0).join("\n\n");
}

