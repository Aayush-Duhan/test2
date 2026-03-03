"use client";

import * as React from "react";
import { useParams, useRouter } from "next/navigation";
import { v4 as uuidv4 } from "uuid";
import { SidebarProvider } from "@/components/ui/sidebar";
import { Header } from "@/components/header";
import { SessionSidebar } from "@/components/session-sidebar";
import { ChatPanel } from "@/components/chat-panel";
import type { StepState } from "@/lib/migration-types";
import {
  STEP_BLUEPRINT,
  INITIAL_EXECUTION,
  type ChatMessage,
  type ExecuteStatementEvent,
  type ExecuteErrorEvent,
  type CurrentExecution,
} from "@/lib/chat-types";
import {
  isActive,
  makeMessage,
  makeThinkingMessage,
  mergeSteps,
  buildTasks,
  flattenExecutionLog,
  makeSqlStatementMessage,
  makeSqlErrorMessage,
  buildSqlExecutionMessages,
} from "@/lib/chat-helpers";
import {
  getWizardState,
  resetWizard,
} from "@/lib/wizard-store";

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

type PersistedRunEvent = {
  type?: string;
  payload?: Record<string, unknown>;
};

function buildHydratedMessagesFallback(
  events: unknown,
  logs: unknown,
  statements: ExecuteStatementEvent[],
  errors: ExecuteErrorEvent[],
): ChatMessage[] {
  const timeline: ChatMessage[] = [];
  let usedEventTimeline = false;

  if (Array.isArray(events)) {
    const hasChatMessageEvents = events.some((raw) => {
      if (!raw || typeof raw !== "object") return false;
      const event = raw as PersistedRunEvent;
      if (event.type !== "chat:message") return false;
      return isChatMessage(event.payload);
    });

    for (const raw of events) {
      if (!raw || typeof raw !== "object") continue;
      const event = raw as PersistedRunEvent;
      const type = typeof event.type === "string" ? event.type : "";
      const payload = event.payload && typeof event.payload === "object" ? event.payload : {};

      if (type === "chat:message" && isChatMessage(payload)) {
        usedEventTimeline = true;
        timeline.push(payload);
        continue;
      }

      if (hasChatMessageEvents) {
        // When canonical chat events exist, ignore legacy event mirrors to prevent duplicates.
        continue;
      }

      if (type === "step:started") {
        usedEventTimeline = true;
        const label = typeof payload.label === "string" ? payload.label : payload.stepId;
        if (typeof label === "string" && label.length > 0) {
          timeline.push(makeMessage("system", `Starting: ${label}.`, "step_started"));
        }
        continue;
      }

      if (type === "step:completed") {
        usedEventTimeline = true;
        const label = typeof payload.label === "string" ? payload.label : payload.stepId;
        if (typeof label === "string" && label.length > 0) {
          timeline.push(makeMessage("system", `Completed: ${label}.`, "step_completed"));
        }
        continue;
      }

      if (type === "log") {
        usedEventTimeline = true;
        const message = typeof payload.message === "string" ? payload.message.trim() : "";
        if (message.length > 0) {
          timeline.push(makeMessage("agent", message, "log"));
        }
        continue;
      }

      if (type === "selfheal:iteration") {
        usedEventTimeline = true;
        timeline.push(
          makeThinkingMessage(
            `Self-heal iteration ${String(payload.iteration ?? "?")}\nAnalyzing execution errors and generating fixes via Snowflake Cortex...`
          )
        );
        continue;
      }

      if (type === "execute_sql:statement") {
        usedEventTimeline = true;
        timeline.push(makeSqlStatementMessage(payload as ExecuteStatementEvent));
        continue;
      }

      if (type === "execute_sql:error") {
        usedEventTimeline = true;
        const executeError = payload as ExecuteErrorEvent;
        const missing =
          (executeError.errorType ?? "").toLowerCase().includes("missing") ||
          (executeError.errorMessage ?? "").toLowerCase().includes("does not exist");
        timeline.push(
          makeSqlErrorMessage(
            executeError,
            missing ? "Execution paused: missing table/object detected." : undefined
          )
        );
        continue;
      }

      if (type === "run:completed") {
        usedEventTimeline = true;
        timeline.push(makeMessage("system", "Migration completed.", "run_status"));
        continue;
      }

      if (type === "run:failed") {
        usedEventTimeline = true;
        const reason = typeof payload.reason === "string" ? payload.reason : "Run failed";
        timeline.push(makeMessage("error", reason, "run_status"));
      }
    }
  }

  if (!usedEventTimeline && Array.isArray(logs)) {
    for (const raw of logs) {
      if (typeof raw === "string" && raw.trim().length > 0) {
        timeline.push(makeMessage("agent", raw, "log"));
      }
    }
  }

  if (!usedEventTimeline) {
    return [...timeline, ...buildSqlExecutionMessages(statements, errors)];
  }

  return timeline;
}

/* ================================================================
   SessionsPage â€” page-level orchestrator for migration sessions.
   All rendering is delegated to <Header>, <SessionSidebar>, and
   <ChatPanel>.  This file owns state, API calls, and SSE wiring.
   ================================================================ */

export default function SessionsPage() {
  const router = useRouter();
  const params = useParams<{ id?: string }>();
  const routeRunId = typeof params?.id === "string" ? params.id : null;

  /* â”€â”€ Core state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const [promptMode, setPromptMode] = React.useState<"project" | "chat">("project");

  const [projectId, setProjectId] = React.useState<string | null>(null);
  const [sourceId, setSourceId] = React.useState<string | null>(null);
  const [schemaId, setSchemaId] = React.useState<string | null>(null);
  const [runId, setRunId] = React.useState<string | null>(null);
  const isHydratingRouteRun = Boolean(routeRunId) && runId !== routeRunId;

  const [steps, setSteps] = React.useState<StepState[]>(STEP_BLUEPRINT);
  const [status, setStatus] = React.useState("idle");
  const [error, setError] = React.useState<string | null>(null);
  const [isBusy, setIsBusy] = React.useState(false);
  const [messages, setMessages] = React.useState<ChatMessage[]>([]);

  /* File inputs - kept for backward compatibility with upload functions */
  const [file, setFile] = React.useState<File | null>(null);
  const [schemaFile, setSchemaFile] = React.useState<File | null>(null);
  const [sourceLanguage, setSourceLanguage] = React.useState("Teradata");

  /* Sidebar reload â€” bump to tell the sidebar to re-fetch sessions */
  const [selectedSessionId, setSelectedSessionId] = React.useState<string | null>(null);
  const [sidebarReloadKey, setSidebarReloadKey] = React.useState(0);
  const reloadSidebar = React.useCallback(() => setSidebarReloadKey((k) => k + 1), []);

  /* Execution tracking */
  const [executeStatements, setExecuteStatements] = React.useState<ExecuteStatementEvent[]>([]);
  const [executeErrors, setExecuteErrors] = React.useState<ExecuteErrorEvent[]>([]);
  const [currentExecution, setCurrentExecution] = React.useState<CurrentExecution>(INITIAL_EXECUTION);

  /* DDL-resume state */
  const [requiresDdlUpload, setRequiresDdlUpload] = React.useState(false);
  const [missingObjects, setMissingObjects] = React.useState<string[]>([]);
  const [resumeFromStage, setResumeFromStage] = React.useState("");
  const [lastExecutedFileIndex, setLastExecutedFileIndex] = React.useState(-1);
  const [selfHealIteration, setSelfHealIteration] = React.useState(0);

  /* Thinking state â€” true while the agent is actively processing */
  const [isAgentThinking, setIsAgentThinking] = React.useState(false);
  /** Tracks the currently running step to contextualise log messages */
  const activeStepRef = React.useRef<string | null>(null);
  const chatSchemaReadyRef = React.useRef(false);

  const ddlFileInputRef = React.useRef<HTMLInputElement>(null);
  const startRef = React.useRef<number | null>(null);

  /* â”€â”€ Derived â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const tasks = React.useMemo(() => buildTasks(steps, status), [steps, status]);

  /* â”€â”€ Hydrate an existing run â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const hydrateRun = React.useCallback(async (targetRunId: string) => {
    setIsBusy(true);
    setError(null);
    const res = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
    if (!res.ok) { setError("Unable to load session"); setIsBusy(false); return; }
    const data = await res.json();

    setRunId(targetRunId);
    setSelectedSessionId(targetRunId);
    setPromptMode("chat");
    setStatus(data.status ?? "idle");
    setSteps(mergeSteps(data.steps));
    setProjectId(data.projectId ?? null);
    setSourceId(data.sourceId ?? null);
    setSchemaId(data.schemaId ?? null);

    const s = flattenExecutionLog(data.executionLog);
    const e: ExecuteErrorEvent[] = Array.isArray(data.executionErrors) ? data.executionErrors : [];
    setExecuteStatements(s);
    setExecuteErrors(e);
    setRequiresDdlUpload(Boolean(data.requiresDdlUpload));
    setMissingObjects(Array.isArray(data.missingObjects) ? data.missingObjects : []);
    setResumeFromStage(typeof data.resumeFromStage === "string" ? data.resumeFromStage : "");
    setLastExecutedFileIndex(typeof data.lastExecutedFileIndex === "number" ? data.lastExecutedFileIndex : -1);
    setSelfHealIteration(typeof data.selfHealIteration === "number" ? data.selfHealIteration : 0);

    if (s.length) {
      const last = s[s.length - 1];
      setCurrentExecution({
        fileIndex: typeof last.fileIndex === "number" ? last.fileIndex : -1,
        statementIndex: typeof last.statementIndex === "number" ? last.statementIndex : -1,
        elapsedMs: startRef.current ? Date.now() - startRef.current : 0,
        rowsReturned: last.rowCount ?? 0,
        status: data.status === "completed"
          ? "Succeeded"
          : data.status === "failed" && Boolean(data.requiresDdlUpload)
            ? "Paused"
            : data.status === "failed"
              ? "Failed"
              : "Running",
      });
    }

    const hydratedMessages = Array.isArray(data.messages)
      ? data.messages.filter(isChatMessage)
      : [];
    if (hydratedMessages.length > 0) {
      chatSchemaReadyRef.current = true;
      setMessages(hydratedMessages);
    } else {
      chatSchemaReadyRef.current = false;
      setMessages(buildHydratedMessagesFallback(data.events, data.logs, s, e));
    }
    if (typeof data.error === "string" && data.error.length) setError(data.error);
    setIsBusy(false);
  }, []);

  // Reconcile client state with server run snapshot when stream events are missed.
  const reconcileRunSnapshot = React.useCallback(async (targetRunId: string) => {
    try {
      const res = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      const snapshotMessages = Array.isArray(data.messages)
        ? data.messages.filter(isChatMessage)
        : [];
      if (snapshotMessages.length > 0) {
        chatSchemaReadyRef.current = true;
        setMessages(snapshotMessages);
      } else if (!chatSchemaReadyRef.current) {
        const ss = flattenExecutionLog(data.executionLog);
        const se: ExecuteErrorEvent[] = Array.isArray(data.executionErrors) ? data.executionErrors : [];
        setMessages(buildHydratedMessagesFallback(data.events, data.logs, ss, se));
      }

      const nextStatus = typeof data.status === "string" ? data.status : "idle";
      setStatus(nextStatus);
      setSteps(mergeSteps(data.steps));
      setRequiresDdlUpload(Boolean(data.requiresDdlUpload));
      setMissingObjects(Array.isArray(data.missingObjects) ? data.missingObjects : []);
      setResumeFromStage(typeof data.resumeFromStage === "string" ? data.resumeFromStage : "");
      setLastExecutedFileIndex(typeof data.lastExecutedFileIndex === "number" ? data.lastExecutedFileIndex : -1);
      if (typeof data.error === "string" && data.error.length > 0) {
        setError(data.error);
      }

      if (nextStatus === "completed") {
        setIsAgentThinking(false);
        activeStepRef.current = null;
        setCurrentExecution((prev) => ({
          ...prev,
          status: "Succeeded",
          elapsedMs: startRef.current ? Date.now() - startRef.current : prev.elapsedMs,
        }));
        reloadSidebar();
      }

      if (nextStatus === "failed" || nextStatus === "canceled") {
        setIsAgentThinking(false);
        activeStepRef.current = null;
        reloadSidebar();
      }
    } catch {
      // no-op: reconciliation is best-effort
    }
  }, [reloadSidebar]);

  /* â”€â”€ Reset to blank state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const resetToNewSession = React.useCallback(() => {
    setRunId(null);
    setSelectedSessionId(null);
    setStatus("idle");
    setSteps(STEP_BLUEPRINT);
    setMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);
    setRequiresDdlUpload(false);
    setMissingObjects([]);
    setResumeFromStage("");
    setLastExecutedFileIndex(-1);
    setSelfHealIteration(0);
    setCurrentExecution(INITIAL_EXECUTION);
    setIsAgentThinking(false);
    activeStepRef.current = null;
    setError(null);
    setPromptMode("project");
    // Reset the wizard store as well
    resetWizard();
    router.push("/sessions");
  }, [router]);

  /* â”€â”€ Upload helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const uploadSource = async (pid?: string, f?: File | null) => {
    const activePid = pid ?? projectId;
    const activeFile = f ?? file;
    if (!activePid || !activeFile) return;
    setIsBusy(true);
    setError(null);
    const fd = new FormData();
    fd.append("file", activeFile);
    const res = await fetch(`/api/projects/${activePid}/source`, { method: "POST", body: fd });
    if (!res.ok) { setError("Upload failed"); setIsBusy(false); return; }
    const data = await res.json();
    setSourceId(data.sourceId);
    setIsBusy(false);
    return data.sourceId as string;
  };

  const uploadSchema = async (pid?: string, f?: File | null) => {
    const activePid = pid ?? projectId;
    const activeFile = f ?? schemaFile;
    if (!activePid || !activeFile) return;
    setIsBusy(true);
    setError(null);
    const fd = new FormData();
    fd.append("file", activeFile);
    const res = await fetch(`/api/projects/${activePid}/schema`, { method: "POST", body: fd });
    if (!res.ok) { setError("Schema upload failed"); setIsBusy(false); return; }
    const data = await res.json();
    setSchemaId(data.schemaId);
    setIsBusy(false);
    return data.schemaId as string;
  };

  /* â”€â”€ Start / retry run â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const startRun = async (pid?: string, sid?: string, scid?: string, lang?: string) => {
    const activePid = pid ?? projectId;
    const activeSid = sid ?? sourceId;
    const activeScid = scid ?? schemaId;
    if (!activePid || !activeSid || !activeScid) return;

    setIsBusy(true);
    setError(null);
    setSteps(STEP_BLUEPRINT);
    setMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);
    setCurrentExecution({ ...INITIAL_EXECUTION, status: "Running" });
    startRef.current = Date.now();

    const res = await fetch("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        projectId: activePid,
        sourceId: activeSid,
        schemaId: activeScid,
        sourceLanguage: lang ?? sourceLanguage,
      }),
    });
    if (!res.ok) { setError("Failed to start run"); setIsBusy(false); return; }

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
    const res = await fetch(`/api/runs/${runId}/retry`, { method: "POST" });
    if (!res.ok) { setError("Retry failed"); setIsBusy(false); return; }

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
    setCurrentExecution({ ...INITIAL_EXECUTION, status: "Running" });
    startRef.current = Date.now();
    setIsBusy(false);
    reloadSidebar();
    router.replace(`/sessions/${data.runId}`);
  };

  /* â”€â”€ DDL resume â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const handleResumeWithDdl = async (ddlFile: File) => {
    if (!runId) return;
    setIsBusy(true);
    setError(null);
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
      setMessages((prev) => [
        ...prev,
        makeMessage("system", `Uploaded DDL (${ddlFile.name}). Resuming from checkpoint.`),
      ]);
      setExecuteStatements([]);
      setExecuteErrors([]);
      setRequiresDdlUpload(false);
      setMissingObjects([]);
      setResumeFromStage("");
      setLastExecutedFileIndex(-1);
      setCurrentExecution({ ...INITIAL_EXECUTION, status: "Running" });
      startRef.current = Date.now();
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

  /* â”€â”€ Confirm project creation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  const handleConfirm = async () => {
    // Get files and language from wizard store
    const wizardState = getWizardState();
    const wizardSourceFiles = wizardState.sourceFiles;
    const wizardMappingFiles = wizardState.mappingFiles;
    const wizardLanguage = wizardState.sourceLanguage;
    
    // Get the first source file and mapping file (if any)
    const sourceFileToUpload = wizardSourceFiles[0]?.file ?? file;
    const mappingFileToUpload = wizardMappingFiles[0]?.file ?? schemaFile;
    
    setIsBusy(true);
    setError(null);
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
      setSourceLanguage(wizardLanguage);
      setMessages((prev) => [...prev, makeMessage("system", `Project created: ${projectName}`)]);

      const uploadedSourceId = sourceFileToUpload ? (await uploadSource(data.projectId, sourceFileToUpload)) ?? null : sourceId;
      const uploadedSchemaId = mappingFileToUpload ? (await uploadSchema(data.projectId, mappingFileToUpload)) ?? null : schemaId;
      
      if (uploadedSourceId && uploadedSchemaId) {
        await startRun(data.projectId, uploadedSourceId, uploadedSchemaId, wizardLanguage);
      } else if (uploadedSourceId) {
        // Schema is optional, proceed without it
        await startRun(data.projectId, uploadedSourceId, uploadedSchemaId ?? "", wizardLanguage);
      } else {
        setError("Uploads incomplete. Please retry attaching files.");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to start migration");
    } finally {
      setIsBusy(false);
    }
  };

  /* â”€â”€ Effects â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  React.useEffect(() => {
    if (!routeRunId || routeRunId === runId) return;
    void hydrateRun(routeRunId);
  }, [routeRunId, runId, hydrateRun]);

  // Fallback guard: while active, periodically sync status to avoid stale "running" UI.
  React.useEffect(() => {
    if (!runId || !isActive(status)) return;
    const timer = setInterval(() => {
      void reconcileRunSnapshot(runId);
    }, 4000);
    return () => clearInterval(timer);
  }, [runId, status, reconcileRunSnapshot]);

  /* â”€â”€ SSE stream â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  React.useEffect(() => {
    if (!runId || !isActive(status)) return;
    const source = new EventSource(`/api/runs/${runId}/stream`);

    source.addEventListener("chat:message", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      if (!isChatMessage(payload)) return;
      chatSchemaReadyRef.current = true;
      setMessages((prev) => [...prev, payload]);
      if (payload.kind === "thinking") {
        setIsAgentThinking(true);
      }
    });

    source.addEventListener("step:started", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = payload.stepId ?? null;
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "running" } : s)));
      if (!chatSchemaReadyRef.current) {
        const label = typeof payload?.label === "string" ? payload.label : payload.stepId;
        if (label) {
          setMessages((prev) => [...prev, makeMessage("system", `Starting: ${label}.`, "step_started")]);
        }
      }

      /* Turn on the "thinking" indicator for LLM-heavy steps */
      const thinkingSteps = ["self_heal", "convert_code", "validate"];
      if (thinkingSteps.includes(payload.stepId)) {
        setIsAgentThinking(true);
      }

      if (payload.stepId === "execute_sql") {
        setCurrentExecution((prev) => ({
          ...prev,
          status: "Running",
          elapsedMs: startRef.current ? Date.now() - startRef.current : prev.elapsedMs,
        }));
      }
    });

    source.addEventListener("step:completed", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = null;
      setIsAgentThinking(false);
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "completed" } : s)));
      if (!chatSchemaReadyRef.current) {
        const label = typeof payload?.label === "string" ? payload.label : payload.stepId;
        if (label) {
          setMessages((prev) => [...prev, makeMessage("system", `Completed: ${label}.`, "step_completed")]);
        }
      }
    });

    source.addEventListener("run:completed", () => {
      setStatus("completed");
      setIsAgentThinking(false);
      activeStepRef.current = null;
      setCurrentExecution((prev) => ({
        ...prev,
        status: "Succeeded",
        elapsedMs: startRef.current ? Date.now() - startRef.current : prev.elapsedMs,
      }));
      if (!chatSchemaReadyRef.current) {
        setMessages((prev) => [...prev, makeMessage("system", "Migration completed.", "run_status")]);
      }
      void reconcileRunSnapshot(runId);
      reloadSidebar();
    });

    source.addEventListener("run:failed", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      const reason = payload.reason || "Run failed";
      setError(reason);
      setIsAgentThinking(false);
      activeStepRef.current = null;
      setStatus(reason === "canceled" ? "canceled" : "failed");
      const paused =
        String(reason).toLowerCase().includes("upload ddl") ||
        String(reason).toLowerCase().includes("missing object");
      setCurrentExecution((prev) => ({
        ...prev,
        status: paused ? "Paused" : "Failed",
        elapsedMs: startRef.current ? Date.now() - startRef.current : prev.elapsedMs,
      }));
      if (paused) setRequiresDdlUpload(true);
      if (!chatSchemaReadyRef.current) {
        setMessages((prev) => [...prev, makeMessage("error", paused ? `Execution paused: ${reason}` : reason, "run_status")]);
      }
      void reconcileRunSnapshot(runId);
      reloadSidebar();
    });

    source.addEventListener("log", (event) => {
      if (chatSchemaReadyRef.current) return;
      const payload = JSON.parse((event as MessageEvent).data);
      const message = typeof payload?.message === "string" ? payload.message.trim() : "";
      if (!message) return;
      const thinkingSteps = ["self_heal", "convert_code", "validate"];
      const step = activeStepRef.current;
      if (step && thinkingSteps.includes(step)) {
        setMessages((prev) => [...prev, makeThinkingMessage(message)]);
      } else {
        setMessages((prev) => [...prev, makeMessage("agent", message, "log")]);
      }
    });

    source.addEventListener("selfheal:iteration", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      const iter = Number(payload?.iteration ?? 0);
      if (Number.isFinite(iter)) setSelfHealIteration(iter);
      setIsAgentThinking(true);
      if (!chatSchemaReadyRef.current) {
        setMessages((prev) => [
          ...prev,
          makeThinkingMessage(
            `Self-heal iteration ${payload?.iteration ?? "?"}\nAnalyzing execution errors and generating fixes via Snowflake Cortex...`
          ),
        ]);
      }
    });

    source.addEventListener("execute_sql:statement", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as ExecuteStatementEvent;
      setExecuteStatements((prev) => [...prev, payload]);
      setCurrentExecution({
        fileIndex: typeof payload.fileIndex === "number" ? payload.fileIndex : -1,
        statementIndex: typeof payload.statementIndex === "number" ? payload.statementIndex : -1,
        elapsedMs: startRef.current ? Date.now() - startRef.current : 0,
        rowsReturned: payload.rowCount ?? 0,
        status: "Running",
      });
    });

    source.addEventListener("execute_sql:error", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as ExecuteErrorEvent;
      setExecuteErrors((prev) => [...prev, payload]);
      const missing =
        (payload.errorType ?? "").toLowerCase().includes("missing") ||
        (payload.errorMessage ?? "").toLowerCase().includes("does not exist");
      if (missing) {
        setRequiresDdlUpload(true);
        const m = (payload.errorMessage ?? "").match(/['\"]([^'\"]+)['\"]/);
        if (m?.[1]) setMissingObjects((prev) => (prev.includes(m[1]) ? prev : [...prev, m[1]]));
      }
      setCurrentExecution((prev) => ({
        ...prev,
        status: missing ? "Paused" : "Failed",
        elapsedMs: startRef.current ? Date.now() - startRef.current : prev.elapsedMs,
      }));
    });

    source.onerror = () => {
      source.close();
      void reconcileRunSnapshot(runId);
    };
    return () => source.close();
  }, [runId, status, reloadSidebar, reconcileRunSnapshot]);

  /* â”€â”€ Render â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-[#1a1a1a]"
      style={{ ["--header-h" as string]: "48px" }}
    >
      <Header />

      {/* Hidden DDL file input */}
      <input
        ref={ddlFileInputRef}
        type="file"
        accept=".sql,.ddl,.txt"
        className="hidden"
        onChange={(event) => void onPickDdlFile(event)}
      />

      <SidebarProvider className="sidebar-offset min-h-0 flex-1">
        <div className="flex min-h-0 w-full flex-1">
          <SessionSidebar
            selectedSessionId={selectedSessionId}
            reloadKey={sidebarReloadKey}
          />

          <ChatPanel
            runId={runId}
            projectId={projectId}
            isHydratingRun={isHydratingRouteRun}
            status={status}
            error={error}
            isBusy={isBusy}
            messages={messages}
            tasks={tasks}
            requiresDdlUpload={requiresDdlUpload}
            resumeFromStage={resumeFromStage}
            lastExecutedFileIndex={lastExecutedFileIndex}
            missingObjects={missingObjects}
            isAgentThinking={isAgentThinking}
            onCreateProject={handleConfirm}
            onRetryRun={retryRun}
            onResetSession={resetToNewSession}
            onPickDdlFile={() => ddlFileInputRef.current?.click()}
          />
        </div>
      </SidebarProvider>
    </div>
  );
}
