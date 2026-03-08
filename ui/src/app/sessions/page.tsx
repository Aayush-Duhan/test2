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
  type ExecuteStatementEvent,
  type ExecuteErrorEvent,
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

/** Step IDs where the agent performs LLM-heavy work (thinking indicator). */
const THINKING_STEPS = ["self_heal", "convert_code", "validate"];

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
      const event = raw as { type?: string; payload?: Record<string, unknown> };
      if (event.type !== "chat:message") return false;
      return isChatMessage(event.payload);
    });

    for (const raw of events) {
      if (!raw || typeof raw !== "object") continue;
      const event = raw as { type?: string; payload?: Record<string, unknown> };
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
        const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
        const label = typeof payload.label === "string" ? payload.label : stepId;
        if (typeof label === "string" && label.length > 0) {
          timeline.push(makeMessage("system", `Starting: ${label}.`, "step_started", undefined, stepId ? { id: stepId, label } : undefined));
        }
        continue;
      }

      if (type === "step:completed") {
        usedEventTimeline = true;
        const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
        const label = typeof payload.label === "string" ? payload.label : stepId;
        if (typeof label === "string" && label.length > 0) {
          timeline.push(makeMessage("system", `Completed: ${label}.`, "step_completed", undefined, stepId ? { id: stepId, label } : undefined));
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
   SessionsPage -- page-level orchestrator for migration sessions.
   All rendering is delegated to <Header>, <SessionSidebar>, and
   <ChatPanel>.  This file owns state, API calls, and SSE wiring.
   ================================================================ */

export default function SessionsPage() {
  const router = useRouter();
  const params = useParams<{ id?: string }>();
  const routeRunId = typeof params?.id === "string" ? params.id : null;

  /* -- Core state -------------------------------------------------------------------------------- */
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

  /* Sidebar reload -- bump to tell the sidebar to re-fetch sessions */
  const [selectedSessionId, setSelectedSessionId] = React.useState<string | null>(null);
  const [sidebarReloadKey, setSidebarReloadKey] = React.useState(0);
  const reloadSidebar = React.useCallback(() => setSidebarReloadKey((k) => k + 1), []);

  /* Execution tracking (used for hydration fallback) */
  const [executeStatements, setExecuteStatements] = React.useState<ExecuteStatementEvent[]>([]);
  const [executeErrors, setExecuteErrors] = React.useState<ExecuteErrorEvent[]>([]);

  /* DDL-resume state */
  const [requiresDdlUpload, setRequiresDdlUpload] = React.useState(false);
  const [missingObjects, setMissingObjects] = React.useState<string[]>([]);
  const [resumeFromStage, setResumeFromStage] = React.useState("");
  const [lastExecutedFileIndex, setLastExecutedFileIndex] = React.useState(-1);

  /* Thinking state -- true while the agent is actively processing */
  const [isAgentThinking, setIsAgentThinking] = React.useState(false);
  /** Tracks the currently running step to contextualise log messages */
  const activeStepRef = React.useRef<string | null>(null);
  const chatSchemaReadyRef = React.useRef(false);

  const ddlFileInputRef = React.useRef<HTMLInputElement>(null);

  /* -- Derived ----------------------------------------------------------------------------------- */
  const tasks = React.useMemo(() => buildTasks(steps, status), [steps, status]);

  /* -- Hydrate an existing run ------------------------------------------------------------------- */
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

  /* -- Reset to blank state ---------------------------------------------------------------------- */
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
    setIsAgentThinking(false);
    activeStepRef.current = null;
    setError(null);
    setPromptMode("project");
    // Reset the wizard store as well
    resetWizard();
    router.push("/sessions");
  }, [router]);

  /* -- Upload helpers ---------------------------------------------------------------------------- */
  const uploadSource = async (pid: string, f: File) => {
    setIsBusy(true);
    setError(null);
    const fd = new FormData();
    fd.append("file", f);
    const res = await fetch(`/api/projects/${pid}/source`, { method: "POST", body: fd });
    if (!res.ok) { setError("Upload failed"); setIsBusy(false); return; }
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
    if (!res.ok) { setError("Schema upload failed"); setIsBusy(false); return; }
    const data = await res.json();
    setSchemaId(data.schemaId);
    setIsBusy(false);
    return data.schemaId as string;
  };

  /* -- Start / retry run ------------------------------------------------------------------------- */
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
    setSteps(STEP_BLUEPRINT);
    setMessages([]);
    setExecuteStatements([]);
    setExecuteErrors([]);

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
    setIsBusy(false);
    reloadSidebar();
    router.replace(`/sessions/${data.runId}`);
  };

  /* -- DDL resume -------------------------------------------------------------------------------- */
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

  /* -- Confirm project creation ------------------------------------------------------------------ */
  const handleConfirm = async () => {
    // Get files and language from wizard store
    const wizardState = getWizardState();
    const wizardSourceFiles = wizardState.sourceFiles;
    const wizardMappingFiles = wizardState.mappingFiles;
    const wizardLanguage = wizardState.sourceLanguage;
    
    const sourceFileToUpload = wizardSourceFiles[0]?.file ?? null;
    const mappingFileToUpload = wizardMappingFiles[0]?.file ?? null;
    
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
      setMessages((prev) => [...prev, makeMessage("system", `Project created: ${projectName}`)]);

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

  /* -- Effects ----------------------------------------------------------------------------------- */
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

  /* -- SSE stream -------------------------------------------------------------------------------- */
  React.useEffect(() => {
    if (!runId || !isActive(status)) return;
    const source = new EventSource(`/api/runs/${runId}/stream`);

    source.addEventListener("chat:message", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      if (!isChatMessage(payload)) return;
      chatSchemaReadyRef.current = true;

      const isTerminalOutput = payload.kind === "log" || payload.kind === "terminal_progress";
      if (isTerminalOutput) {
        workbenchStore.appendTerminalLine(
          payload.content,
          payload.kind === "terminal_progress",
        );
        return;
      }

      setMessages((prev) => [...prev, payload]);
      if (payload.kind === "thinking") {
        setIsAgentThinking(true);
      }
    });

    source.addEventListener("step:started", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = payload.stepId ?? null;
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "running" } : s)));

      const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
      const label = typeof payload?.label === "string" ? payload.label : stepId;
      if (label) {
        workbenchStore.appendTerminalLine(`$ ${label}`, false);
      }
      if (!chatSchemaReadyRef.current && label) {
        setMessages((prev) => [
          ...prev,
          makeMessage("system", `Starting: ${label}.`, "step_started", undefined, stepId ? { id: stepId, label } : undefined),
        ]);
      }

      if (THINKING_STEPS.includes(payload.stepId)) {
        setIsAgentThinking(true);
      }
    });

    source.addEventListener("step:completed", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = null;
      setIsAgentThinking(false);
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "completed" } : s)));
      if (!chatSchemaReadyRef.current) {
        const stepId = typeof payload.stepId === "string" ? payload.stepId : "";
        const label = typeof payload?.label === "string" ? payload.label : stepId;
        if (label) {
          setMessages((prev) => [
            ...prev,
            makeMessage("system", `Completed: ${label}.`, "step_completed", undefined, stepId ? { id: stepId, label } : undefined),
          ]);
        }
      }
    });

    source.addEventListener("run:completed", () => {
      setStatus("completed");
      setIsAgentThinking(false);
      activeStepRef.current = null;
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
      const isProgress = !!payload?.is_progress;
      const step = activeStepRef.current;
      if (step && THINKING_STEPS.includes(step)) {
        setMessages((prev) => [...prev, makeThinkingMessage(message)]);
      } else {
        workbenchStore.appendTerminalLine(message, isProgress);
      }
    });

    source.addEventListener("selfheal:iteration", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
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
    });

    source.onerror = () => {
      source.close();
      void reconcileRunSnapshot(runId);
    };
    return () => source.close();
  }, [runId, status, reloadSidebar, reconcileRunSnapshot]);

  /* -- Render ------------------------------------------------------------------------------------ */
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
            onPickDdlFile={() => ddlFileInputRef.current?.click()}
          />
        </div>
      </SidebarProvider>
    </div>
  );
}
