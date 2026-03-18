"use client";

import * as React from "react";
import { useParams, useRouter } from "next/navigation";
import { v4 as uuidv4 } from "uuid";
import { SidebarProvider } from "@/components/ui/sidebar";
import { Header } from "@/components/header";
import { SessionSidebar } from "@/components/session-sidebar";
import { ChatPanel } from "@/components/chat-panel";
import { workbenchStore } from "@/lib/workbench-store";
import type { StepState } from "@/lib/migration-types";
import {
  STEP_BLUEPRINT,
  type ChatMessage,
  type ExecuteErrorEvent,
  type ExecuteStatementEvent,
  type RunStreamPart,
} from "@/lib/chat-types";
import {
  buildTasks,
  flattenExecutionLog,
  isActive,
  makeMessage,
  makeSqlErrorMessage,
  makeSqlStatementMessage,
  mergeSteps,
} from "@/lib/chat-helpers";
import { getWizardState } from "@/lib/wizard-store";
import { StreamingMessageParser } from "@/lib/runtime/message-parser";

function isChatMessage(value: unknown): value is ChatMessage {
  if (!value || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  return (
    typeof row.id === "string" &&
    typeof row.role === "string" &&
    typeof row.kind === "string" &&
    typeof row.content === "string"
  );
}

function isRunStreamPart(value: unknown): value is RunStreamPart {
  return Boolean(value && typeof value === "object" && typeof (value as { type?: unknown }).type === "string");
}

export default function SessionsPage() {
  const router = useRouter();
  const params = useParams<{ id?: string }>();
  const routeRunId = typeof params?.id === "string" ? params.id : null;

  const [, setPromptMode] = React.useState<"project" | "chat">("project");
  const [projectId, setProjectId] = React.useState<string | null>(null);
  const [sourceId, setSourceId] = React.useState<string | null>(null);
  const [schemaId, setSchemaId] = React.useState<string | null>(null);
  const [runId, setRunId] = React.useState<string | null>(null);
  const isHydratingRouteRun = Boolean(routeRunId) && runId !== routeRunId;

  const [steps, setSteps] = React.useState<StepState[]>(STEP_BLUEPRINT);
  const [status, setStatus] = React.useState("idle");
  const [error, setError] = React.useState<string | null>(null);
  const [isBusy, setIsBusy] = React.useState(false);
  const [isCanceling, setIsCanceling] = React.useState(false);
  const [messages, setMessages] = React.useState<ChatMessage[]>([]);
  const [selectedSessionId, setSelectedSessionId] = React.useState<string | null>(null);
  const [sidebarReloadKey, setSidebarReloadKey] = React.useState(0);
  const [, setExecuteStatements] = React.useState<ExecuteStatementEvent[]>([]);
  const [, setExecuteErrors] = React.useState<ExecuteErrorEvent[]>([]);
  const [requiresDdlUpload, setRequiresDdlUpload] = React.useState(false);
  const [missingObjects, setMissingObjects] = React.useState<string[]>([]);
  const [resumeFromStage, setResumeFromStage] = React.useState("");
  const [lastExecutedFileIndex, setLastExecutedFileIndex] = React.useState(-1);
  const [isAgentThinking, setIsAgentThinking] = React.useState(false);

  const ddlFileInputRef = React.useRef<HTMLInputElement>(null);
  const activeMessageIdRef = React.useRef<string | null>(null);
  const toolInputRef = React.useRef<Map<string, { toolName: string; inputText: string; input?: Record<string, unknown> }>>(new Map());
  const rawAssistantTextRef = React.useRef<Map<string, string>>(new Map());
  const parserRef = React.useRef(
    new StreamingMessageParser({
      callbacks: {
        onArtifactOpen: (data) => {
          workbenchStore.showWorkbench.set(true);
          workbenchStore.addArtifact(data);
        },
        onArtifactClose: (data) => {
          workbenchStore.updateArtifact(data, { closed: true });
        },
        onActionOpen: (data) => {
          if (data.action.type !== "shell") {
            void workbenchStore.addAction(data);
          }
        },
        onActionClose: (data) => {
          if (data.action.type === "shell") {
            void workbenchStore.addAction(data);
          }
          void workbenchStore.runAction(data);
        },
      },
    }),
  );

  const reloadSidebar = React.useCallback(() => setSidebarReloadKey((k) => k + 1), []);
  const tasks = React.useMemo(() => buildTasks(steps, status), [steps, status]);

  const rehydrateParsedMessages = React.useCallback((nextMessages: ChatMessage[]) => {
    parserRef.current.reset();
    rawAssistantTextRef.current.clear();
    workbenchStore.artifacts.set({});
    for (const message of nextMessages) {
      if (message.role !== "agent" || message.kind !== "agent_response") continue;
      rawAssistantTextRef.current.set(message.id, message.content);
      parserRef.current.parse(message.id, message.content);
    }
  }, []);

  const applyRunStreamPart = React.useCallback((part: RunStreamPart) => {
    switch (part.type) {
      case "start":
        activeMessageIdRef.current = part.messageId;
        rawAssistantTextRef.current.set(part.messageId, "");
        setMessages((prev) => (
          prev.some((message) => message.id === part.messageId)
            ? prev
            : [...prev, { id: part.messageId, role: "agent", kind: "agent_response", content: "" }]
        ));
        return;
      case "text-start":
        return;
      case "text-delta": {
        const messageId = activeMessageIdRef.current;
        if (!messageId) return;
        const rawSoFar = `${rawAssistantTextRef.current.get(messageId) ?? ""}${part.delta}`;
        rawAssistantTextRef.current.set(messageId, rawSoFar);
        const parsedDelta = parserRef.current.parse(messageId, rawSoFar);
        setMessages((prev) => prev.map((message) => (
          message.id === messageId
            ? { ...message, role: "agent", kind: "agent_response", content: `${message.content}${parsedDelta}` }
            : message
        )));
        return;
      }
      case "text-end":
        return;
      case "reasoning-start":
      case "reasoning-delta":
        setIsAgentThinking(true);
        return;
      case "reasoning-end":
        setIsAgentThinking(false);
        return;
      case "tool-input-start":
        toolInputRef.current.set(part.toolCallId, { toolName: part.toolName, inputText: "" });
        setIsAgentThinking(true);
        return;
      case "tool-input-delta": {
        const current = toolInputRef.current.get(part.toolCallId);
        if (current) {
          current.inputText += part.inputTextDelta;
        }
        return;
      }
      case "tool-input-available": {
        const current = toolInputRef.current.get(part.toolCallId);
        toolInputRef.current.set(part.toolCallId, {
          toolName: part.toolName,
          inputText: current?.inputText ?? JSON.stringify(part.input),
          input: part.input,
        });
        return;
      }
      case "tool-output-available": {
        const toolState = toolInputRef.current.get(part.toolCallId);
        const output = typeof part.output === "string" ? part.output : JSON.stringify(part.output, null, 2);
        setMessages((prev) => [
          ...prev,
          makeMessage(
            "agent",
            output,
            "tool_result",
          ),
        ]);
        if (toolState) {
          toolInputRef.current.delete(part.toolCallId);
        }
        setIsAgentThinking(false);
        return;
      }
      case "finish":
        if (activeMessageIdRef.current) {
          rawAssistantTextRef.current.delete(activeMessageIdRef.current);
        }
        activeMessageIdRef.current = null;
        setIsAgentThinking(false);
        return;
      case "error":
        setError(part.errorText);
        setIsAgentThinking(false);
        return;
      case "abort":
        setError(part.reason);
        setIsAgentThinking(false);
        return;
      case "data-run-sync":
        setStatus(part.data.status);
        setSteps(mergeSteps(part.data.steps));
        setRequiresDdlUpload(Boolean(part.data.requiresDdlUpload));
        setResumeFromStage(part.data.resumeFromStage ?? "");
        setLastExecutedFileIndex(part.data.lastExecutedFileIndex ?? -1);
        setMissingObjects(part.data.missingObjects ?? []);
        setExecuteErrors(part.data.executionErrors ?? []);
        return;
      case "data-run-status":
        setStatus(part.data.status);
        setError(typeof part.data.error === "string" && part.data.error.length > 0 ? part.data.error : null);
        setRequiresDdlUpload(Boolean(part.data.requiresDdlUpload));
        setResumeFromStage(part.data.resumeFromStage ?? "");
        setLastExecutedFileIndex(part.data.lastExecutedFileIndex ?? -1);
        setMissingObjects(part.data.missingObjects ?? []);
        if (!isActive(part.data.status)) {
          setIsAgentThinking(false);
          setIsCanceling(false);
          reloadSidebar();
        }
        return;
      case "data-step-status":
        setSteps((prev) => mergeSteps(prev.map((step) => (
          step.id === part.data.stepId ? { ...step, status: part.data.status as StepState["status"] } : step
        ))));
        if (part.data.status === "running") {
          setIsAgentThinking(part.data.stepId === "convert_code" || part.data.stepId === "validate");
        }
        return;
      case "data-sql-statement":
        setExecuteStatements((prev) => [...prev, part.data]);
        setMessages((prev) => [...prev, makeSqlStatementMessage(part.data)]);
        return;
      case "data-sql-error": {
        setExecuteErrors((prev) => [...prev, part.data]);
        const missing =
          (part.data.errorType ?? "").toLowerCase().includes("missing") ||
          (part.data.errorMessage ?? "").toLowerCase().includes("does not exist");
        if (missing) {
          setRequiresDdlUpload(true);
        }
        setMessages((prev) => [...prev, makeSqlErrorMessage(part.data)]);
        return;
      }
      case "data-terminal-progress":
        return;
      default:
        return;
    }
  }, [reloadSidebar]);

  const hydrateRun = React.useCallback(async (targetRunId: string) => {
    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    parserRef.current.reset();
    rawAssistantTextRef.current.clear();
    const res = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
    if (!res.ok) {
      setError("Unable to load session");
      setIsBusy(false);
      return;
    }
    const data = await res.json();
    setRunId(targetRunId);
    setSelectedSessionId(targetRunId);
    setPromptMode("chat");
    setStatus(data.status ?? "idle");
    setSteps(mergeSteps(data.steps));
    setProjectId(data.projectId ?? null);
    setSourceId(data.sourceId ?? null);
    setSchemaId(data.schemaId ?? null);
    setExecuteStatements(flattenExecutionLog(data.executionLog));
    setExecuteErrors(Array.isArray(data.executionErrors) ? data.executionErrors : []);
    setRequiresDdlUpload(Boolean(data.requiresDdlUpload));
    setMissingObjects(Array.isArray(data.missingObjects) ? data.missingObjects : []);
    setResumeFromStage(typeof data.resumeFromStage === "string" ? data.resumeFromStage : "");
    setLastExecutedFileIndex(typeof data.lastExecutedFileIndex === "number" ? data.lastExecutedFileIndex : -1);
    const hydratedMessages = Array.isArray(data.messages) ? data.messages.filter(isChatMessage) : [];
    rehydrateParsedMessages(hydratedMessages);
    setMessages(hydratedMessages);
    setError(typeof data.error === "string" && data.error.length > 0 ? data.error : null);
    setIsAgentThinking(false);
    setIsBusy(false);
  }, [rehydrateParsedMessages]);

  const reconcileRunSnapshot = React.useCallback(async (targetRunId: string) => {
    try {
      const res = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      setStatus(typeof data.status === "string" ? data.status : "idle");
      setSteps(mergeSteps(data.steps));
      setRequiresDdlUpload(Boolean(data.requiresDdlUpload));
      setMissingObjects(Array.isArray(data.missingObjects) ? data.missingObjects : []);
      setResumeFromStage(typeof data.resumeFromStage === "string" ? data.resumeFromStage : "");
      setLastExecutedFileIndex(typeof data.lastExecutedFileIndex === "number" ? data.lastExecutedFileIndex : -1);
      setExecuteStatements(flattenExecutionLog(data.executionLog));
      setExecuteErrors(Array.isArray(data.executionErrors) ? data.executionErrors : []);
      if (Array.isArray(data.messages)) {
        const hydratedMessages = data.messages.filter(isChatMessage);
        rehydrateParsedMessages(hydratedMessages);
        setMessages(hydratedMessages);
      }
      if (!isActive(data.status ?? "idle")) {
        setIsAgentThinking(false);
        setIsCanceling(false);
        reloadSidebar();
      }
      setError(typeof data.error === "string" && data.error.length > 0 ? data.error : null);
    } catch {
      // best-effort
    }
  }, [rehydrateParsedMessages, reloadSidebar]);

  const uploadSource = async (pid: string, f: File) => {
    setIsBusy(true);
    setError(null);
    const fd = new FormData();
    fd.append("file", f);
    const res = await fetch(`/api/projects/${pid}/source`, { method: "POST", body: fd });
    if (!res.ok) {
      setError("Upload failed");
      setIsBusy(false);
      return;
    }
    const data = await res.json();
    setSourceId(data.sourceId);
    setIsBusy(false);
    return data.sourceId as string;
  };

  const uploadSchema = async (pid: string, f: File) => {
    setIsBusy(true);
    setError(null);
    const fd = new FormData();
    fd.append("file", f);
    const res = await fetch(`/api/projects/${pid}/schema`, { method: "POST", body: fd });
    if (!res.ok) {
      setError("Schema upload failed");
      setIsBusy(false);
      return;
    }
    const data = await res.json();
    setSchemaId(data.schemaId);
    setIsBusy(false);
    return data.schemaId as string;
  };

  const startRun = async (
    pid?: string,
    sid?: string,
    scid?: string,
    lang?: string,
    creds?: {
      sfAccount?: string;
      sfUser?: string;
      sfRole?: string;
      sfWarehouse?: string;
      sfDatabase?: string;
      sfSchema?: string;
      sfAuthenticator?: string;
    },
  ) => {
    const activePid = pid ?? projectId;
    const activeSid = sid ?? sourceId;
    const activeScid = scid ?? schemaId ?? undefined;
    if (!activePid || !activeSid) return;

    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    setSteps(STEP_BLUEPRINT);
    setMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);
    parserRef.current.reset();
    rawAssistantTextRef.current.clear();

    const res = await fetch("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        projectId: activePid,
        sourceId: activeSid,
        schemaId: activeScid,
        sourceLanguage: lang,
        ...(creds ?? {}),
      }),
    });
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      setError(typeof payload?.error === "string" ? payload.error : "Failed to start run");
      setIsBusy(false);
      return;
    }

    const data = await res.json();
    setRunId(data.runId);
    setSelectedSessionId(data.runId);
    setStatus("running");
    setPromptMode("chat");
    setIsBusy(false);
    reloadSidebar();
    router.replace(`/sessions/${data.runId}`);
  };

  const retryRun = async () => {
    if (!runId) return;
    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    const res = await fetch(`/api/runs/${runId}/retry`, { method: "POST" });
    if (!res.ok) {
      setError("Retry failed");
      setIsBusy(false);
      return;
    }

    const data = await res.json();
    setRunId(data.runId);
    setSelectedSessionId(data.runId);
    setStatus("running");
    setSteps(STEP_BLUEPRINT);
    setMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);
    setRequiresDdlUpload(false);
    setMissingObjects([]);
    setResumeFromStage("");
    setLastExecutedFileIndex(-1);
    parserRef.current.reset();
    setIsBusy(false);
    reloadSidebar();
    router.replace(`/sessions/${data.runId}`);
  };

  const handleResumeWithDdl = async (ddlFile: File) => {
    if (!runId) return;
    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    try {
      const fd = new FormData();
      fd.append("ddlFile", ddlFile);
      fd.append("resumeFromStage", resumeFromStage || "execute_sql");
      fd.append("lastExecutedFileIndex", String(lastExecutedFileIndex));
      fd.append("missingObjects", JSON.stringify(missingObjects));

      const res = await fetch(`/api/runs/${runId}/resume`, { method: "POST", body: fd });
      if (!res.ok) {
        const payload = await res.json().catch(() => ({}));
        setError(payload?.error ?? "Resume failed");
        setIsBusy(false);
        return;
      }

      const data = await res.json();
      setRunId(data.runId);
      setSelectedSessionId(data.runId);
      setStatus("running");
      setSteps(STEP_BLUEPRINT);
      setMessages([]);
      setExecuteStatements([]);
      setExecuteErrors([]);
      setRequiresDdlUpload(false);
      setMissingObjects([]);
      setResumeFromStage("");
      setLastExecutedFileIndex(-1);
      parserRef.current.reset();
      rawAssistantTextRef.current.clear();
      setIsBusy(false);
      reloadSidebar();
      router.replace(`/sessions/${data.runId}`);
    } catch {
      setError("Resume failed");
      setIsBusy(false);
    }
  };

  const onPickDdlFile = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const picked = event.target.files?.[0] ?? null;
    if (!picked) return;
    await handleResumeWithDdl(picked);
    event.target.value = "";
  };

  const handleConfirm = async () => {
    const wizardState = getWizardState();
    const sourceFileToUpload = wizardState.sourceFiles[0]?.file ?? null;
    const mappingFileToUpload = wizardState.mappingFiles[0]?.file ?? null;
    const wizardLanguage = wizardState.sourceLanguage;

    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    const projectName = uuidv4();

    try {
      const res = await fetch("/api/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: projectName, sourceLanguage: wizardLanguage }),
      });
      if (!res.ok) {
        setError("Unable to create project");
        setIsBusy(false);
        return;
      }

      const data = await res.json();
      setProjectId(data.projectId);
      setPromptMode("chat");

      const uploadedSourceId = sourceFileToUpload ? (await uploadSource(data.projectId, sourceFileToUpload)) ?? null : null;
      const uploadedSchemaId = mappingFileToUpload ? (await uploadSchema(data.projectId, mappingFileToUpload)) ?? null : null;

      if (uploadedSourceId) {
        await startRun(data.projectId, uploadedSourceId, uploadedSchemaId ?? undefined, wizardLanguage, {
          sfAccount: wizardState.sfAccount,
          sfUser: wizardState.sfUser,
          sfRole: wizardState.sfRole,
          sfWarehouse: wizardState.sfWarehouse,
          sfDatabase: wizardState.sfDatabase,
          sfSchema: wizardState.sfSchema,
          sfAuthenticator: wizardState.sfAuthenticator,
        });
      } else {
        setError("Uploads incomplete. Please retry attaching files.");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to start migration");
    } finally {
      setIsBusy(false);
    }
  };

  React.useEffect(() => {
    if (!routeRunId || routeRunId === runId) return;
    void hydrateRun(routeRunId);
  }, [routeRunId, runId, hydrateRun]);

  React.useEffect(() => {
    if (!runId || !isActive(status)) return;
    const timer = setInterval(() => {
      void reconcileRunSnapshot(runId);
    }, 4000);
    return () => clearInterval(timer);
  }, [runId, status, reconcileRunSnapshot]);

  React.useEffect(() => {
    if (!runId || !isActive(status)) return;
    const source = new EventSource(`/api/runs/${runId}/stream`);

    source.onmessage = (event) => {
      if (event.data === "[DONE]") {
        setIsAgentThinking(false);
        return;
      }
      try {
        const payload = JSON.parse(event.data);
        if (!isRunStreamPart(payload)) return;
        applyRunStreamPart(payload);
      } catch {
        // ignore malformed frames
      }
    };

    source.onerror = () => {
      source.close();
      void reconcileRunSnapshot(runId);
    };

    return () => source.close();
  }, [applyRunStreamPart, reconcileRunSnapshot, runId, status]);

  const cancelRun = async () => {
    if (!runId || !isActive(status) || isCanceling) return;
    setIsCanceling(true);
    try {
      const res = await fetch(`/api/runs/${runId}/cancel`, { method: "POST" });
      if (!res.ok) {
        setError("Unable to cancel run");
        setIsCanceling(false);
      }
    } catch {
      setError("Unable to cancel run");
      setIsCanceling(false);
    }
  };

  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-[#1a1a1a]"
      style={{ ["--header-h" as string]: "48px" }}
    >
      <Header showWorkbenchToggle={!!runId} />

      <input
        ref={ddlFileInputRef}
        type="file"
        accept=".sql,.ddl,.txt"
        className="hidden"
        onChange={(event) => void onPickDdlFile(event)}
      />

      <SidebarProvider className="sidebar-offset min-h-0 flex-1">
        <div className="flex min-h-0 w-full flex-1">
          <SessionSidebar selectedSessionId={selectedSessionId} reloadKey={sidebarReloadKey} />

          <ChatPanel
            runId={runId}
            projectId={projectId}
            isHydratingRun={isHydratingRouteRun}
            status={status}
            error={error}
            isBusy={isBusy}
            isCanceling={isCanceling}
            messages={messages}
            tasks={tasks}
            requiresDdlUpload={requiresDdlUpload}
            resumeFromStage={resumeFromStage}
            lastExecutedFileIndex={lastExecutedFileIndex}
            missingObjects={missingObjects}
            isAgentThinking={isAgentThinking}
            onCreateProject={handleConfirm}
            onRetryRun={retryRun}
            onPickDdlFile={() => ddlFileInputRef.current?.click()}
            onCancelRun={cancelRun}
            onSendAgentMessage={runId ? async (message: string) => {
              try {
                const response = await fetch(`/api/runs/${runId}/chat`, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({ message }),
                });

                if (!response.ok) {
                  const payload = await response.json().catch(() => ({}));
                  setError(typeof payload?.detail === "string" ? payload.detail : "Unable to send message");
                  return;
                }

                setMessages((prev) => [...prev, makeMessage("user", message, "user_input")]);
                setError(null);
                setStatus("queued");
                setIsAgentThinking(true);
                await reconcileRunSnapshot(runId);
              } catch {
                setError("Unable to send message");
              }
            } : undefined}
          />
        </div>
      </SidebarProvider>
    </div>
  );
}
