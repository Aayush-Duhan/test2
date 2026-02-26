import { EventEmitter } from "events";
import crypto from "crypto";
import {
  ArtifactRecord,
  MigrationContext,
  RunEvent,
  RunRecord,
  RunStatus,
  StepId,
  StepState,
  STEP_LABELS,
} from "./migration-types";
import { getProject, getSource, loadRuns, saveRuns } from "./storage";
import { getStepOrder, nodes, shouldContinue } from "./workflow";

type Listener = (event: RunEvent) => void;

interface RunRuntime {
  abortController: AbortController;
  eventHub: EventEmitter;
}

const runs = new Map<string, RunRecord>();
const runtimes = new Map<string, RunRuntime>();
let initialized = false;

function now() {
  return new Date().toISOString();
}

function buildSteps(): StepState[] {
  return getStepOrder().map((step) => ({
    id: step.id,
    label: step.label,
    status: "pending",
  }));
}

async function ensureInitialized() {
  if (initialized) return;
  const stored = await loadRuns<RunRecord[]>([]);
  for (const run of stored) {
    runs.set(run.runId, run);
  }
  initialized = true;
}

class NotFoundError extends Error {
  code = "NOT_FOUND";
  constructor(message: string) {
    super(message);
    this.name = "NotFoundError";
  }
}

function persistRuns() {
  return saveRuns(Array.from(runs.values()));
}

function createContext(run: RunRecord): MigrationContext {
  return {
    projectId: run.projectId,
    projectName: run.projectName,
    sourceId: run.sourceId,
    currentStage: "init_project",
    errors: [],
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
    artifacts: run.artifacts,
    attempts: 0,
    needsHumanReview: false,
  };
}

function updateRun(runId: string, update: Partial<RunRecord>) {
  const current = runs.get(runId);
  if (!current) return;
  const next = { ...current, ...update, updatedAt: now() };
  runs.set(runId, next);
  void persistRuns();
}

function updateStep(runId: string, stepId: string, update: Partial<StepState>) {
  const run = runs.get(runId);
  if (!run) return;
  const steps = run.steps.map((step) =>
    step.id === stepId ? { ...step, ...update } : step
  );
  updateRun(runId, { steps });
}

function emit(runId: string, event: RunEvent) {
  const runtime = runtimes.get(runId);
  if (!runtime) return;
  runtime.eventHub.emit("event", event);
}

function log(runId: string, message: string) {
  const run = runs.get(runId);
  if (!run) return;
  const entry = `[${new Date().toLocaleTimeString()}] ${message}`;
  const logs = [...run.logs, entry];
  updateRun(runId, { logs });
  emit(runId, { type: "log", payload: { message: entry } });
}

export async function startRun(projectId: string, sourceId: string) {
  await ensureInitialized();
  const project = await getProject(projectId);
  const source = await getSource(sourceId);
  if (!project || !source) {
    throw new NotFoundError("Project or source not found");
  }

  const runId = crypto.randomUUID();
  const createdAt = now();
  const record: RunRecord = {
    runId,
    projectId,
    projectName: project.name,
    sourceId,
    status: "queued",
    steps: buildSteps(),
    logs: [],
    artifacts: [],
    createdAt,
    updatedAt: createdAt,
  };
  runs.set(runId, record);
  await persistRuns();

  const runtime: RunRuntime = {
    abortController: new AbortController(),
    eventHub: new EventEmitter(),
  };
  runtimes.set(runId, runtime);

  void executeRun(runId, runtime.abortController.signal);
  return runId;
}

export async function retryRun(runId: string) {
  await ensureInitialized();
  const run = runs.get(runId);
  if (!run) {
    throw new Error("Run not found");
  }
  return startRun(run.projectId, run.sourceId);
}

export async function cancelRun(runId: string) {
  await ensureInitialized();
  const runtime = runtimes.get(runId);
  if (runtime) {
    runtime.abortController.abort();
  }
  updateRun(runId, { status: "canceled", error: "Canceled by user" });
  emit(runId, { type: "run:failed", payload: { runId, reason: "canceled" } });
  return { status: "canceled" as RunStatus };
}

export async function getRun(runId: string) {
  await ensureInitialized();
  return runs.get(runId);
}

export async function subscribe(runId: string, listener: Listener) {
  await ensureInitialized();
  let runtime = runtimes.get(runId);
  if (!runtime) {
    runtime = { abortController: new AbortController(), eventHub: new EventEmitter() };
    runtimes.set(runId, runtime);
  }
  runtime.eventHub.on("event", listener);
  return () => runtime?.eventHub.off("event", listener);
}

async function executeRun(runId: string, signal: AbortSignal) {
  const run = runs.get(runId);
  if (!run) return;
  updateRun(runId, { status: "running" });
  emit(runId, { type: "run:started", payload: { runId } });

  let ctx = createContext(run);
  const addArtifact = (artifact: ArtifactRecord) => {
    const current = runs.get(runId);
    if (!current) return;
    const artifacts = [...current.artifacts, artifact];
    updateRun(runId, { artifacts });
    emit(runId, { type: "artifact", payload: { ...artifact } });
  };

  try {
    let currentStep: StepId = "init_project";
    while (true) {
      if (signal.aborted) {
        throw new Error("Run canceled");
      }
      const step: StepId = currentStep;
      updateStep(runId, step, { status: "running", startedAt: now() });
      emit(runId, {
        type: "step:started",
        payload: { runId, stepId: step, label: STEP_LABELS[step], timestamp: now() },
      });
      ctx = await nodes[step]({
        ctx,
        runId,
        signal,
        log: (message) => log(runId, message),
        addArtifact,
      });
      updateStep(runId, step, { status: "completed", endedAt: now() });
      emit(runId, {
        type: "step:completed",
        payload: { runId, stepId: step, label: STEP_LABELS[step], timestamp: now() },
      });
      ctx.currentStage = step;
      if (step === "finalize") {
        break;
      }
      currentStep = shouldContinue(ctx);
      if (currentStep === step) {
        break;
      }
    }
    updateRun(runId, { status: "completed" });
    emit(runId, { type: "run:completed", payload: { runId } });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Run failed";
    const status = message === "Run canceled" ? "canceled" : "failed";
    updateRun(runId, { status, error: message });
    emit(runId, { type: "run:failed", payload: { runId, reason: message } });
  }
}
