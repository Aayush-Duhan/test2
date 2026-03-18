"use client";

import * as React from "react";
import { ArrowDown, ChevronRight } from "lucide-react";
import { PromptBox } from "@/components/ui/chatgpt-prompt-input";
import AgentPlan, { type Task } from "@/components/ui/agent-plan";
import { SidebarInset } from "@/components/ui/sidebar";
import { SetupWizard } from "@/components/ui/setup-wizard";
import { Button } from "@/components/ui/button";
import { ConfirmDialog } from "@/components/ui/confirm-dialog";
import { AssistantMessage } from "@/components/chat/AssistantMessage";
import { UserMessage } from "@/components/chat/UserMessage";
import { CodeBlock } from "@/components/chat/CodeBlock";
import { Workbench } from "@/components/workbench";
import type { ChatMessage } from "@/lib/chat-types";
import { createScopedLogger } from "@/lib/logger";
import { isActive } from "@/lib/chat-helpers";
import { workbenchStore, type UploadedFile } from "@/lib/workbench-store";
import { useSnapScroll } from "@/hooks/useSnapScroll";

const logger = createScopedLogger("ChatPanel");

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
  isCanceling?: boolean;
  uploadedFiles?: UploadedFile[];
  onCreateProject: () => void;
  onRetryRun: () => void;
  onPickDdlFile: () => void;
  onSendAgentMessage?: (message: string) => void;
  onCancelRun?: () => void;
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
  isCanceling = false,
  uploadedFiles = [],
  onCreateProject,
  onRetryRun,
  onPickDdlFile,
  onSendAgentMessage,
  onCancelRun,
}: ChatPanelProps) {
  const isSessionFinished = runId !== null && ["failed", "canceled"].includes(status);
  const hasActiveRun = runId !== null;
  const isChatInputEnabled = hasActiveRun && !isSessionFinished && !!onSendAgentMessage && !isCanceling;
  const canCancel = Boolean(runId) && isActive(status) && Boolean(onCancelRun);
  const [showCancelDialog, setShowCancelDialog] = React.useState(false);

  React.useEffect(() => {
    if (!canCancel && showCancelDialog) {
      setShowCancelDialog(false);
    }
  }, [canCancel, showCancelDialog]);

  const handleConfirmCancel = React.useCallback(() => {
    setShowCancelDialog(false);
    onCancelRun?.();
  }, [onCancelRun]);

  const chatInputHint = !hasActiveRun
    ? null
    : isCanceling
      ? "Stopping run... This may take a moment."
      : isSessionFinished
        ? "This session has ended. Retry or start a new session to continue chatting with the agent."
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
        if (!payload || typeof payload !== "object") {
          return;
        }

        const items = (payload as {
          items?: Array<{
            type?: string;
            content?: string;
            name?: string;
            path?: string;
            isBinary?: boolean;
          }>;
        }).items;

        if (!Array.isArray(items)) {
          return;
        }

        for (const item of items) {
          if (item?.type !== "file" || !item.content || !item.path) {
            continue;
          }

          files.push({
            name: item.name ?? item.path,
            content: item.content,
            relativePath: item.path,
            isBinary: item.isBinary,
          });
        }
      };

      if (sourceRes.ok) {
        appendFiles(await sourceRes.json());
      }

      if (outputRes.ok) {
        appendFiles(await outputRes.json());
      }

      if (files.length > 0) {
        workbenchStore.addUploadedFiles(files);
      }
    } catch (syncError) {
      logger.error("Failed to sync project files:", syncError);
    }
  }, []);

  React.useEffect(() => {
    if (!projectId) {
      return;
    }

    void syncProjectFiles(projectId);

    const source = new EventSource(`/api/projects/${projectId}/watch`);
    let debounceTimer: ReturnType<typeof setTimeout> | null = null;

    source.addEventListener("files:changed", () => {
      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }

      debounceTimer = setTimeout(() => {
        void syncProjectFiles(projectId);
      }, 300);
    });

    return () => {
      source.close();

      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }
    };
  }, [projectId, runId, status, syncProjectFiles]);

  const chatMessages = React.useMemo(() => messages, [messages]);

  return (
    <div className="flex min-h-0 flex-1 overflow-hidden">
      <SidebarInset className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#1a1a1a]">
        {runId ? (
          <>
            <div className="shrink-0 p-4 pb-0">
              <AgentPlan
                tasks={tasks}
                readOnly
                headerActions={canCancel ? (
                  <>
                    <Button
                      type="button"
                      variant="destructive"
                      size="sm"
                      disabled={isCanceling}
                      onClick={() => setShowCancelDialog(true)}
                    >
                      {isCanceling ? "Stopping..." : "Stop run"}
                    </Button>
                    <ConfirmDialog
                      open={showCancelDialog}
                      onOpenChange={setShowCancelDialog}
                      title="Stop run?"
                      description="This will cancel the current run. You can retry or start a new run later."
                      confirmLabel="Stop run"
                      cancelLabel="Cancel"
                      confirmDisabled={isCanceling}
                      onConfirm={handleConfirmCancel}
                    />
                  </>
                ) : null}
              />
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

            <ChatMessageArea messages={chatMessages} error={error} isAgentThinking={isAgentThinking} />
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
            {messages.map((message) => (
              <MemoizedChatBubble key={message.id} message={message} />
            ))}

            {isAgentThinking && (
              <div className="w-full max-w-[min(46rem,92%)] rounded-[calc(0.75rem-1px)] bg-gradient-to-b from-[#232326] from-30% to-transparent px-4 py-1.5 text-white/72">
                <span className="animate-pulse">...</span>
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

      {!isAtBottom && (
        <button
          type="button"
          onClick={scrollToBottom}
          className="absolute right-5 bottom-3 flex h-8 w-8 items-center justify-center rounded-full border border-white/15 bg-[#252525] text-white/70 shadow-lg transition-all hover:bg-[#303030] hover:text-white"
          aria-label="Scroll to bottom"
        >
          <ArrowDown className="h-4 w-4" />
        </button>
      )}
    </div>
  );
}

function ChatBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";
  const isError = message.role === "error";
  const isAgentResponse = message.kind === "agent_response";
  const isAgentThinking = message.kind === "agent_thinking";
  const isSqlRow = message.kind === "sql_statement" || message.kind === "sql_error";
  const isToolResult = message.kind === "tool_result";

  if (isSqlRow && message.sql) {
    return (
      <details className="group">
        <summary className="flex cursor-pointer list-none items-center gap-2 text-sm leading-relaxed text-white/80 hover:text-white/95">
          <ChevronRight className="h-3.5 w-3.5 shrink-0 text-white/40 transition-transform group-open:rotate-90" />
          <span className={isError ? "text-red-300" : "text-white/80"}>{message.content}</span>
        </summary>
        <div className="ml-5 mt-2 space-y-2 border-l border-white/10 pl-3">
          {message.sql.statement && <SqlBlockSection title="Query" content={message.sql.statement} />}
          {message.sql.failedStatement && <SqlBlockSection title="Query" content={message.sql.failedStatement} />}
          {message.sql.output && <SqlBlockSection title="Output" content={message.sql.output} />}
          {message.sql.error && <SqlBlockSection title="Error" content={message.sql.error} isError />}
        </div>
      </details>
    );
  }

  if (isToolResult) {
    const parsed = parseToolResultPayload(message.content);
    const toolName = typeof parsed?.tool === "string" ? parsed.tool : "tool";
    const summary =
      typeof parsed?.summary === "string"
        ? parsed.summary
        : typeof parsed?.message === "string"
          ? parsed.message
          : typeof parsed?.error === "string"
            ? parsed.error
            : "";
    const truncated = parsed?.truncated === true;
    const success =
      typeof parsed?.success === "boolean" ? parsed.success : undefined;
    const prettyPayload = parsed ? JSON.stringify(parsed, null, 2) : message.content;

    return (
      <div className="flex w-full justify-start">
        <div className="w-full max-w-[min(46rem,92%)] rounded-[calc(0.75rem-1px)] bg-[#202124] px-4 py-3 text-white/92">
          <details className="group">
            <summary className="flex cursor-pointer list-none items-center gap-2 text-sm font-medium text-white/85">
              <span className="rounded-full border border-white/15 bg-white/5 px-2 py-0.5 text-[11px] uppercase tracking-wide text-white/70">
                Tool Result
              </span>
              <span className="text-white/90">{toolName}</span>
              {success === true && (
                <span className="rounded-full border border-emerald-400/40 bg-emerald-500/15 px-2 py-0.5 text-[11px] text-emerald-100">
                  Success
                </span>
              )}
              {success === false && (
                <span className="rounded-full border border-red-400/40 bg-red-500/15 px-2 py-0.5 text-[11px] text-red-100">
                  Failed
                </span>
              )}
            </summary>
            {(summary || truncated) && (
              <p className="mt-2 text-xs text-white/70">
                {summary}
                {summary && truncated ? " " : ""}
                {truncated ? "(Preview truncated)" : ""}
              </p>
            )}
            <div className="mt-3 overflow-hidden rounded-lg border border-white/10 bg-black/30">
              <CodeBlock code={prettyPayload} language="json" disableCopy={false} />
            </div>
          </details>
        </div>
      </div>
    );
  }

  if (isAgentThinking || isAgentResponse) {
    return (
      <div className="flex w-full justify-start">
        <div
          className={[
            "w-full max-w-[min(46rem,92%)] rounded-[calc(0.75rem-1px)] px-4 py-1.5 text-white/92",
            isAgentThinking ? "bg-gradient-to-b from-[#232326] from-30% to-transparent" : "bg-[#232326]",
          ].join(" ")}
        >
          <AssistantMessage content={message.content} />
        </div>
      </div>
    );
  }

  if (isUser) {
    const isMultiline = message.content.includes("\n");

    return (
      <div className="flex justify-end">
        <div
          data-multiline={isMultiline ? "" : undefined}
          className="relative max-w-[min(46rem,92%)] rounded-[22px] bg-white/10 px-4 py-1.5 text-white/95 data-[multiline]:py-3"
        >
          <UserMessage content={message.content} />
        </div>
      </div>
    );
  }

  const textClass = isError ? "text-red-300" : "text-white/85";
  const prefix = isError ? "Error" : null;

  return (
    <div className={`whitespace-pre-wrap text-sm leading-relaxed ${textClass}`}>
      {prefix && <span className="mr-2 text-xs font-semibold uppercase tracking-wider opacity-50">{prefix}</span>}
      {buildPlainMessageBody(message)}
    </div>
  );
}

const MemoizedChatBubble = React.memo(
  ChatBubble,
  (previous, next) => (
    previous.message.id === next.message.id &&
    previous.message.role === next.message.role &&
    previous.message.kind === next.message.kind &&
    previous.message.content === next.message.content &&
    previous.message.sql?.statement === next.message.sql?.statement &&
    previous.message.sql?.failedStatement === next.message.sql?.failedStatement &&
    previous.message.sql?.output === next.message.sql?.output &&
    previous.message.sql?.error === next.message.sql?.error
  ),
);

function parseToolResultPayload(content: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(content);
    if (parsed && typeof parsed === "object") {
      return parsed as Record<string, unknown>;
    }
  } catch {
    // ignore
  }
  return null;
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
          className={[
            "rounded-full border px-2 py-0.5 text-[11px]",
            isError ? "border-red-400/40 bg-red-500/15 text-red-100" : "border-white/20 bg-white/10 text-white/80",
          ].join(" ")}
        >
          {title}
        </span>
      </div>
      <pre
        className={[
          "max-h-52 overflow-auto whitespace-pre-wrap rounded-xl border px-3 py-2 text-xs",
          isError ? "border-red-400/30 bg-red-500/10 text-red-100" : "border-white/10 bg-black/30 text-white/85",
        ].join(" ")}
      >
        {content}
      </pre>
    </div>
  );
}

function buildPlainMessageBody(message: ChatMessage): string {
  const chunks: string[] = [message.content];

  if (message.sql?.statement) {
    chunks.push(`SQL:\n${message.sql.statement}`);
  }

  if (message.sql?.output) {
    chunks.push(`Output:\n${message.sql.output}`);
  }

  if (message.sql?.error) {
    chunks.push(`Error:\n${message.sql.error}`);
  }

  if (message.sql?.failedStatement) {
    chunks.push(`Failed SQL:\n${message.sql.failedStatement}`);
  }

  return chunks.filter((chunk) => chunk.trim().length > 0).join("\n\n");
}
