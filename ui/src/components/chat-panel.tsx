"use client";

import * as React from "react";
import { ChevronRight, ArrowDown } from "lucide-react";
import { PromptBox } from "@/components/ui/chatgpt-prompt-input";
import AgentPlan, { type Task } from "@/components/ui/agent-plan";
import { SidebarInset } from "@/components/ui/sidebar";
import type { ChatMessage } from "@/lib/chat-types";
import { SetupWizard } from "@/components/ui/setup-wizard";
import { Workbench } from "@/components/workbench";
import { workbenchStore, type UploadedFile } from "@/lib/workbench-store";
import { createScopedLogger } from "@/lib/logger";
import { useSnapScroll } from "@/hooks/useSnapScroll";

const logger = createScopedLogger('ChatPanel');

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
      logger.error("Failed to sync project files:", syncError);
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

  const chatMessages = React.useMemo(
    () => messages.filter((m) => m.kind !== "step_started" && m.kind !== "step_completed" && m.kind !== "terminal_progress"),
    [messages],
  );

  return (
    <div className="flex min-h-0 flex-1 overflow-hidden">
      <SidebarInset className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#1a1a1a]">
        {runId ? (
          <>
            {/* Agent Plan — compact progress tracker */}
            <div className="shrink-0 p-4 pb-0">
              <AgentPlan tasks={tasks} readOnly />
            </div>

            {requiresDdlUpload && (
              <div className="shrink-0 px-4 pt-3">
                <DdlUploadBanner
                  isBusy={isBusy}
                  resumeFromStage={resumeFromStage}
                  lastExecutedFileIndex={lastExecutedFileIndex}
                  missingObjects={missingObjects}
                  onPickDdlFile={onPickDdlFile}
                  onRetryRun={onRetryRun}
                />
              </div>
            )}

            {/* Chat messages area */}
            <ChatMessageArea
              messages={chatMessages}
              error={error}
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

/** Merged timeline item — either a chat message or a terminal command block */

function ChatMessageArea({
  messages,
  error,
  isAgentThinking = false,
}: {
  messages: ChatMessage[];
  error: string | null;
  isAgentThinking?: boolean;
}) {
  const [messageRef, scrollRef, isAtBottom, scrollToBottom] = useSnapScroll();

  const hasContent = messages.length > 0 || isAgentThinking || error;

  return (
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden">
      <div ref={scrollRef} className="scrollbar-dark flex min-h-0 flex-1 flex-col overflow-y-auto px-4 py-3">
        {!hasContent ? (
          <div className="flex flex-1 items-center justify-center">
            <p className="text-sm text-white/40">Chat messages will appear here.</p>
          </div>
        ) : (
          <div ref={messageRef} className="space-y-3">
            {messages.map((m, i) => (
              <ChatBubble key={`msg-${m.id}-${i}`} message={m} />
            ))}

            {isAgentThinking && (
              <div className="flex items-center gap-2 py-1 text-sm text-white/50">
                <span className="inline-flex gap-1">
                  <span className="animate-bounce" style={{ animationDelay: '0ms' }}>·</span>
                  <span className="animate-bounce" style={{ animationDelay: '150ms' }}>·</span>
                  <span className="animate-bounce" style={{ animationDelay: '300ms' }}>·</span>
                </span>
                <span>Agent is working</span>
              </div>
            )}

            {error && (
              <div className="border-l-2 border-red-400/60 pl-3 text-sm leading-relaxed text-red-200">
                {error}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Scroll-to-bottom FAB */}
      {!isAtBottom && (
        <button
          type="button"
          onClick={scrollToBottom}
          className="absolute bottom-3 right-5 flex h-8 w-8 items-center justify-center rounded-full border border-white/15 bg-[#252525] text-white/70 shadow-lg transition-all hover:bg-[#303030] hover:text-white"
          aria-label="Scroll to bottom"
        >
          <ArrowDown className="h-4 w-4" />
        </button>
      )}
    </div>
  );
}

function ChatBubble({ message: m }: { message: ChatMessage }) {
  const isUser = m.role === "user";
  const isError = m.role === "error";
  const isSqlRow = m.kind === "sql_statement" || m.kind === "sql_error";

  if (isSqlRow && m.sql) {
    return (
      <details className="group">
        <summary className="flex cursor-pointer list-none items-center gap-2 text-sm leading-relaxed text-white/80 hover:text-white/95">
          <ChevronRight className="h-3.5 w-3.5 shrink-0 text-white/40 transition-transform group-open:rotate-90" />
          <span className={isError ? "text-red-300" : "text-white/80"}>{m.content}</span>
        </summary>
        <div className="ml-5 mt-2 space-y-2 border-l border-white/10 pl-3">
          {m.sql.statement && <SqlBlockSection title="Query" content={m.sql.statement} />}
          {m.sql.failedStatement && <SqlBlockSection title="Query" content={m.sql.failedStatement} />}
          {m.sql.output && <SqlBlockSection title="Output" content={m.sql.output} />}
          {m.sql.error && <SqlBlockSection title="Error" content={m.sql.error} isError />}
        </div>
      </details>
    );
  }

  const textClass = isUser
    ? "text-blue-300"
    : isError
      ? "text-red-300"
      : "text-white/85";

  const prefix = isUser ? "You" : isError ? "Error" : null;

  return (
    <div className={`whitespace-pre-wrap text-sm leading-relaxed ${textClass}`}>
      {prefix && <span className="mr-2 text-xs font-semibold uppercase tracking-wider opacity-50">{prefix}</span>}
      {buildPlainMessageBody(m)}
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
