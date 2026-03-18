"use client";

import * as React from "react";
import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport } from "ai";
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
  type ExecuteErrorEvent,
  type ExecuteStatementEvent,
  type RunUiMessage,
} from "@/lib/chat-types";
import {
  buildTasks,
  flattenExecutionLog,
  isActive,
  mergeSteps,
} from "@/lib/chat-helpers";
import {
  convertUiMessagesToChatMessages,
  getLatestUserMessageText,
  getUiMessageText,
  replayStreamPartsToUiMessages,
} from "@/lib/run-chat";
import { getWizardState } from "@/lib/wizard-store";
import { StreamingMessageParser } from "@/lib/runtime/message-parser";

type RunDetailResponse = {
  runId: string;
  projectId?: string;
  sourceId?: string;
  schemaId?: string;
  status?: string;
  steps?: StepState[];
  executionLog?: unknown[];
  executionErrors?: ExecuteErrorEvent[];
  missingObjects?: string[];
  requiresDdlUpload?: boolean;
  resumeFromStage?: string;
  lastExecutedFileIndex?: number;
  error?: string | null;
  messages?: unknown[];
  streamParts?: unknown[];
};

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
  const [selectedSessionId, setSelectedSessionId] = React.useState<string | null>(null);
  const [sidebarReloadKey, setSidebarReloadKey] = React.useState(0);
  const [, setExecuteStatements] = React.useState<ExecuteStatementEvent[]>([]);
  const [, setExecuteErrors] = React.useState<ExecuteErrorEvent[]>([]);
  const [requiresDdlUpload, setRequiresDdlUpload] = React.useState(false);
  const [missingObjects, setMissingObjects] = React.useState<string[]>([]);
  const [resumeFromStage, setResumeFromStage] = React.useState("");
  const [lastExecutedFileIndex, setLastExecutedFileIndex] = React.useState(-1);
  const [initialChatMessages, setInitialChatMessages] = React.useState<RunUiMessage[]>([]);
  const [parsedAssistantText, setParsedAssistantText] = React.useState<Map<string, string>>(new Map());

  const ddlFileInputRef = React.useRef<HTMLInputElement>(null);
  const streamCursorRef = React.useRef(0);
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

  const reloadSidebar = React.useCallback(() => setSidebarReloadKey((key) => key + 1), []);
  const tasks = React.useMemo(() => buildTasks(steps, status), [steps, status]);

  const transport = React.useMemo(() => new DefaultChatTransport<RunUiMessage>({
    prepareSendMessagesRequest: ({ id, messages }) => ({
      api: `/api/runs/${id}/chat`,
      body: {
        message: getLatestUserMessageText(messages),
        fromPartIndex: streamCursorRef.current,
      },
    }),
    prepareReconnectToStreamRequest: ({ id }) => ({
      api: `/api/runs/${id}/chat/stream?fromPartIndex=${streamCursorRef.current}`,
    }),
  }), []);

  const handleStreamData = React.useCallback((part: RunUiMessage["parts"][number]) => {
    switch (part.type) {
      case "data-stream-cursor":
        streamCursorRef.current = part.data.nextPartIndex;
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
          setIsCanceling(false);
          reloadSidebar();
        }
        return;
      case "data-step-status":
        setSteps((previous) => mergeSteps(previous.map((step) => (
          step.id === part.data.stepId ? { ...step, status: part.data.status as StepState["status"] } : step
        ))));
        return;
      case "data-sql-statement":
        setExecuteStatements((previous) => [...previous, part.data]);
        return;
      case "data-sql-error": {
        setExecuteErrors((previous) => [...previous, part.data]);
        const missing =
          (part.data.errorType ?? "").toLowerCase().includes("missing") ||
          (part.data.errorMessage ?? "").toLowerCase().includes("does not exist");
        if (missing) {
          setRequiresDdlUpload(true);
        }
        return;
      }
      case "data-terminal-progress":
        return;
      default:
        return;
    }
  }, [reloadSidebar]);

  const {
    messages: chatMessages,
    sendMessage,
    resumeStream,
    stop,
    status: chatStatus,
    error: chatError,
  } = useChat<RunUiMessage>({
    id: runId ?? "draft",
    messages: initialChatMessages,
    transport,
    onData: handleStreamData,
    onError: (chatStreamError) => {
      setError(chatStreamError.message);
    },
  });

  const transcriptMessages = React.useMemo(
    () => convertUiMessagesToChatMessages(chatMessages, parsedAssistantText),
    [chatMessages, parsedAssistantText],
  );

  const isAgentThinking = React.useMemo(() => {
    if (!runId || isCanceling) {
      return false;
    }

    if (chatStatus === "submitted" || chatStatus === "streaming") {
      return true;
    }

    return steps.some((step) => (
      step.status === "running" && (step.id === "convert_code" || step.id === "validate")
    ));
  }, [chatStatus, isCanceling, runId, steps]);

  const hydrateRun = React.useCallback(async (targetRunId: string) => {
    setIsBusy(true);
    setError(null);
    setIsCanceling(false);

    const response = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
    if (!response.ok) {
      setError("Unable to load session");
      setIsBusy(false);
      return;
    }

    const data = await response.json() as RunDetailResponse;
    const streamParts = Array.isArray(data.streamParts) ? data.streamParts : [];

    streamCursorRef.current = streamParts.length;
    setInitialChatMessages(replayStreamPartsToUiMessages(data.streamParts ?? [], data.messages ?? []));
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
    setError(typeof data.error === "string" && data.error.length > 0 ? data.error : null);
    setIsBusy(false);
  }, []);

  const reconcileRunSnapshot = React.useCallback(async (targetRunId: string) => {
    try {
      const response = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
      if (!response.ok) return;

      const data = await response.json() as RunDetailResponse;
      setStatus(typeof data.status === "string" ? data.status : "idle");
      setSteps(mergeSteps(data.steps));
      setRequiresDdlUpload(Boolean(data.requiresDdlUpload));
      setMissingObjects(Array.isArray(data.missingObjects) ? data.missingObjects : []);
      setResumeFromStage(typeof data.resumeFromStage === "string" ? data.resumeFromStage : "");
      setLastExecutedFileIndex(typeof data.lastExecutedFileIndex === "number" ? data.lastExecutedFileIndex : -1);
      setExecuteStatements(flattenExecutionLog(data.executionLog));
      setExecuteErrors(Array.isArray(data.executionErrors) ? data.executionErrors : []);
      if (!isActive(data.status ?? "idle")) {
        setIsCanceling(false);
        reloadSidebar();
      }
      setError(typeof data.error === "string" && data.error.length > 0 ? data.error : null);
    } catch {
      // best-effort
    }
  }, [reloadSidebar]);

  const uploadSource = async (pid: string, file: File) => {
    setIsBusy(true);
    setError(null);
    const formData = new FormData();
    formData.append("file", file);
    const response = await fetch(`/api/projects/${pid}/source`, { method: "POST", body: formData });
    if (!response.ok) {
      setError("Upload failed");
      setIsBusy(false);
      return;
    }
    const data = await response.json();
    setSourceId(data.sourceId);
    setIsBusy(false);
    return data.sourceId as string;
  };

  const uploadSchema = async (pid: string, file: File) => {
    setIsBusy(true);
    setError(null);
    const formData = new FormData();
    formData.append("file", file);
    const response = await fetch(`/api/projects/${pid}/schema`, { method: "POST", body: formData });
    if (!response.ok) {
      setError("Schema upload failed");
      setIsBusy(false);
      return;
    }
    const data = await response.json();
    setSchemaId(data.schemaId);
    setIsBusy(false);
    return data.schemaId as string;
  };

  const resetRunState = React.useCallback(() => {
    streamCursorRef.current = 0;
    setInitialChatMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);
    setRequiresDdlUpload(false);
    setMissingObjects([]);
    setResumeFromStage("");
    setLastExecutedFileIndex(-1);
    setParsedAssistantText(new Map());
    parserRef.current.reset();
    workbenchStore.artifacts.set({});
  }, []);

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
    const activeProjectId = pid ?? projectId;
    const activeSourceId = sid ?? sourceId;
    const activeSchemaId = scid ?? schemaId ?? undefined;
    if (!activeProjectId || !activeSourceId) return;

    setIsBusy(true);
    setError(null);
    setIsCanceling(false);
    setSteps(STEP_BLUEPRINT);
    resetRunState();

    const response = await fetch("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        projectId: activeProjectId,
        sourceId: activeSourceId,
        schemaId: activeSchemaId,
        sourceLanguage: lang,
        ...(creds ?? {}),
      }),
    });

    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      setError(typeof payload?.error === "string" ? payload.error : "Failed to start run");
      setIsBusy(false);
      return;
    }

    const data = await response.json();
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

    const response = await fetch(`/api/runs/${runId}/retry`, { method: "POST" });
    if (!response.ok) {
      setError("Retry failed");
      setIsBusy(false);
      return;
    }

    const data = await response.json();
    resetRunState();
    setRunId(data.runId);
    setSelectedSessionId(data.runId);
    setStatus("running");
    setSteps(STEP_BLUEPRINT);
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
      const formData = new FormData();
      formData.append("ddlFile", ddlFile);
      formData.append("resumeFromStage", resumeFromStage || "execute_sql");
      formData.append("lastExecutedFileIndex", String(lastExecutedFileIndex));
      formData.append("missingObjects", JSON.stringify(missingObjects));

      const response = await fetch(`/api/runs/${runId}/resume`, { method: "POST", body: formData });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        setError(payload?.error ?? "Resume failed");
        setIsBusy(false);
        return;
      }

      const data = await response.json();
      resetRunState();
      setRunId(data.runId);
      setSelectedSessionId(data.runId);
      setStatus("running");
      setSteps(STEP_BLUEPRINT);
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
      const response = await fetch("/api/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: projectName, sourceLanguage: wizardLanguage }),
      });
      if (!response.ok) {
        setError("Unable to create project");
        setIsBusy(false);
        return;
      }

      const data = await response.json();
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
    } catch (startError) {
      setError(startError instanceof Error ? startError.message : "Failed to start migration");
    } finally {
      setIsBusy(false);
    }
  };

  React.useEffect(() => {
    parserRef.current.reset();
    workbenchStore.artifacts.set({});

    const nextParsed = new Map<string, string>();
    for (const message of chatMessages) {
      if (message.role !== "assistant") continue;
      const rawText = getUiMessageText(message);
      if (!rawText.trim()) continue;
      nextParsed.set(message.id, parserRef.current.parse(message.id, rawText));
    }

    setParsedAssistantText(nextParsed);
  }, [chatMessages]);

  React.useEffect(() => {
    if (!chatError) return;
    setError(chatError.message);
  }, [chatError]);

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
    if (!runId || !isActive(status) || chatStatus !== "ready") return;
    void resumeStream();
  }, [chatStatus, resumeStream, runId, status]);

  React.useEffect(() => {
    if (!runId || isActive(status)) return;
    void stop();
  }, [runId, status, stop]);

  const cancelRun = async () => {
    if (!runId || !isActive(status) || isCanceling) return;
    setIsCanceling(true);
    try {
      const response = await fetch(`/api/runs/${runId}/cancel`, { method: "POST" });
      if (!response.ok) {
        setError("Unable to cancel run");
        setIsCanceling(false);
        return;
      }
      await stop();
    } catch {
      setError("Unable to cancel run");
      setIsCanceling(false);
    }
  };

  const handleSendAgentMessage = runId ? async (message: string) => {
    try {
      setError(null);
      setStatus("queued");
      await sendMessage({ text: message });
    } catch (sendError) {
      setError(sendError instanceof Error ? sendError.message : "Unable to send message");
    }
  } : undefined;

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
            isBusy={isBusy || chatStatus === "submitted" || chatStatus === "streaming"}
            isCanceling={isCanceling}
            messages={transcriptMessages}
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
            onSendAgentMessage={handleSendAgentMessage}
          />
        </div>
      </SidebarProvider>
    </div>
  );
}
