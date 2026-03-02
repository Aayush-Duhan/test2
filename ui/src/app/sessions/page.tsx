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

function stripLogTags(message: string): string {
  return message.replace(/^\s*(?:\[[^\]]+\]\s*)+/, "").trim();
}

function cleanTerminalOutput(message: string): string {
  const ansiStripped = message.replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, "");
  return ansiStripped
    .split(/\r?\n/)
    .map((line) =>
      line
        .replace(/[\u2500-\u257f\u2580-\u259f]/g, " ")
        .replace(/[À-ÿ]/g, " ")
        .replace(/[=]{3,}/g, " ")
        .replace(/[?]{5,}/g, " ")
        .replace(/\s{2,}/g, " ")
        .trim(),
    )
    .filter((line) => line.length > 0)
    .filter((line) => !/^[=\-_*~.#|:+`^]+$/.test(line))
    .join("\n");
}

function parseLogTimestamp(raw: string, fallbackTime: number): number {
  const match = raw.match(/^\[(\d{1,2}):(\d{2})(?::(\d{2}))?\]/);
  if (!match) return fallbackTime;

  const [, hh, mm, ss] = match;
  const base = new Date(fallbackTime);
  base.setHours(Number(hh), Number(mm), Number(ss ?? 0), 0);
  return base.getTime();
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

    const runStart = Date.parse(typeof data.createdAt === "string" ? data.createdAt : "") || Date.now();
    const timeline: Array<{ order: number; time: number; message: ChatMessage }> = [];
    let order = 0;

    if (Array.isArray(data.logs)) {
      for (const rawLog of data.logs) {
        if (typeof rawLog !== "string") continue;
        const cleaned = cleanTerminalOutput(stripLogTags(rawLog));
        if (!cleaned) continue;
        timeline.push({
          order: order++,
          time: parseLogTimestamp(rawLog, runStart + order),
          message: makeMessage("agent", cleaned),
        });
      }
    }

    if (Array.isArray(data.steps)) {
      for (const step of data.steps) {
        if (!step || typeof step !== "object") continue;
        const label = typeof step.label === "string" ? step.label : "";
        if (!label) continue;

        const startedAt = typeof step.startedAt === "string" ? Date.parse(step.startedAt) : Number.NaN;
        if (!Number.isNaN(startedAt)) {
          timeline.push({
            order: order++,
            time: startedAt,
            message: makeMessage("system", `Starting: ${label}.`),
          });
        }

        const endedAt = typeof step.endedAt === "string" ? Date.parse(step.endedAt) : Number.NaN;
        if (!Number.isNaN(endedAt) && step.status === "completed") {
          timeline.push({
            order: order++,
            time: endedAt,
            message: makeMessage("system", `Completed: ${label}.`),
          });
        }
      }
    }

    timeline.sort((a, b) => (a.time === b.time ? a.order - b.order : a.time - b.time));
    const baseMessages = timeline.map((item) => item.message);

    setMessages([...baseMessages, ...buildSqlExecutionMessages(s, e)]);
    if (typeof data.error === "string" && data.error.length) setError(data.error);
    setIsBusy(false);
  }, []);

  // Reconcile client state with server run snapshot when stream events are missed.
  const reconcileRunSnapshot = React.useCallback(async (targetRunId: string) => {
    try {
      const res = await fetch(`/api/runs/${targetRunId}`, { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();

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
        setMessages((prev) =>
          prev.some((m) => m.role === "system" && m.content === "Migration completed.")
            ? prev
            : [...prev, makeMessage("system", "Migration completed.")]
        );
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

    source.addEventListener("step:started", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = payload.stepId ?? null;
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "running" } : s)));
      const label = typeof payload?.label === "string" ? payload.label : payload.stepId;
      if (label) setMessages((prev) => [...prev, makeMessage("system", `Starting: ${label}.`)]);

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
        setMessages((prev) => [...prev, makeMessage("agent", "Running SQL execution.")]);
      }
    });

    source.addEventListener("step:completed", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      activeStepRef.current = null;
      setIsAgentThinking(false);
      setSteps((prev) => prev.map((s) => (s.id === payload.stepId ? { ...s, status: "completed" } : s)));
      const label = typeof payload?.label === "string" ? payload.label : payload.stepId;
      if (label) setMessages((prev) => [...prev, makeMessage("system", `Completed: ${label}.`)]);
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
      setMessages((prev) => [...prev, makeMessage("system", "Migration completed.")]);
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
      setMessages((prev) => [
        ...prev,
        makeMessage("error", paused ? `Execution paused: ${reason}` : reason),
      ]);
      reloadSidebar();
    });

    source.addEventListener("log", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      if (!payload.message) return;

      const message = cleanTerminalOutput(stripLogTags(payload.message));
      if (!message) return;

      /* During LLM-heavy steps, surface log messages as "thinking" bubbles */
      const thinkingSteps = ["self_heal", "convert_code", "validate"];
      const step = activeStepRef.current;
      if (step && thinkingSteps.includes(step)) {
        setMessages((prev) => [...prev, makeThinkingMessage(message)]);
      } else {
        setMessages((prev) => [...prev, makeMessage("agent", message)]);
      }
    });

    source.addEventListener("selfheal:iteration", (event) => {
      const payload = JSON.parse((event as MessageEvent).data);
      const iter = Number(payload?.iteration ?? 0);
      if (Number.isFinite(iter)) setSelfHealIteration(iter);
      setIsAgentThinking(true);
      setMessages((prev) => [
        ...prev,
        makeThinkingMessage(
          `Self-heal iteration ${payload?.iteration ?? "?"}\nAnalyzing execution errors and generating fixes via Snowflake Cortex...`
        ),
      ]);
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
      setMessages((prev) => [...prev, makeSqlStatementMessage(payload)]);
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
      setMessages((prev) => [
        ...prev,
        makeSqlErrorMessage(payload, missing ? "Execution paused: missing table/object detected." : undefined),
      ]);
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
