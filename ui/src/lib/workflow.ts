import { promises as fs } from "fs";
import path from "path";
import {
  ArtifactRecord,
  MigrationContext,
  StepId,
  STEP_LABELS,
} from "./migration-types";
import { getOutputDir, getSource } from "./storage";

export interface NodeContext {
  ctx: MigrationContext;
  runId: string;
  signal: AbortSignal;
  log: (message: string) => void;
  addArtifact: (artifact: ArtifactRecord) => void;
}

export type NodeHandler = (input: NodeContext) => Promise<MigrationContext>;

const MAX_SELF_HEAL_ATTEMPTS = 1;

const stepOrder: StepId[] = [
  "init_project",
  "add_source_code",
  "apply_schema_mapping",
  "convert_code",
  "self_heal",
  "validate",
  "human_review",
  "finalize",
];

function timestamp() {
  return new Date().toISOString();
}

function ensureNotAborted(signal: AbortSignal) {
  if (signal.aborted) {
    const error = new Error("Run canceled");
    (error as Error & { code?: string }).code = "CANCELED";
    throw error;
  }
}

async function sleep(ms: number, signal: AbortSignal) {
  ensureNotAborted(signal);
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => resolve(), ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timeout);
        reject(new Error("Run canceled"));
      },
      { once: true }
    );
  });
}

function updateStage(ctx: MigrationContext, stage: StepId) {
  return { ...ctx, currentStage: stage, updatedAt: timestamp() };
}

const initProject: NodeHandler = async ({ ctx, signal, log }) => {
  ensureNotAborted(signal);
  log("Initializing project metadata and workspace.");
  await sleep(600, signal);
  return updateStage(ctx, "init_project");
};

const addSourceCode: NodeHandler = async ({ ctx, signal, log }) => {
  ensureNotAborted(signal);
  log("Validating and indexing source SQL.");
  await sleep(700, signal);
  return updateStage(ctx, "add_source_code");
};

const applySchemaMapping: NodeHandler = async ({ ctx, signal, log }) => {
  ensureNotAborted(signal);
  log("Applying schema mapping heuristics.");
  await sleep(800, signal);
  return updateStage(ctx, "apply_schema_mapping");
};

const convertCode: NodeHandler = async ({ ctx, runId, signal, log, addArtifact }) => {
  ensureNotAborted(signal);
  log("Converting SQL to target dialect.");
  await sleep(900, signal);
  const source = await getSource(ctx.sourceId);
  const outputDir = await getOutputDir(ctx.projectId, runId);
  const targetName = "converted.sql";
  const outputPath = path.join(outputDir, targetName);
  const sourceInfo = source ? `-- Source: ${source.filename}\n` : "";
  await fs.writeFile(
    outputPath,
    `${sourceInfo}-- Converted at ${timestamp()}\nSELECT * FROM example_table;`,
    "utf-8"
  );
  addArtifact({
    name: targetName,
    type: "sql",
    url: `/api/runs/${runId}/artifacts/${encodeURIComponent(targetName)}`,
    createdAt: timestamp(),
  });
  return updateStage(ctx, "convert_code");
};

const selfHeal: NodeHandler = async ({ ctx, signal, log }) => {
  ensureNotAborted(signal);
  log("Attempting self-heal pass.");
  await sleep(700, signal);
  const next = { ...ctx, errors: [], attempts: ctx.attempts + 1 };
  return updateStage(next, "self_heal");
};

const validate: NodeHandler = async ({ ctx, signal, log }) => {
  ensureNotAborted(signal);
  log("Running validation suite.");
  await sleep(800, signal);
  const next = { ...ctx };
  if (next.attempts < MAX_SELF_HEAL_ATTEMPTS && next.errors.length === 0) {
    next.errors = ["Detected minor schema drift; self-heal recommended."];
  }
  return updateStage(next, "validate");
};

const humanReview: NodeHandler = async ({ ctx, signal, log }) => {
  ensureNotAborted(signal);
  log("Human review checkpoint reached; auto-approving for MVP.");
  await sleep(500, signal);
  const next = { ...ctx, needsHumanReview: false, errors: [] };
  return updateStage(next, "human_review");
};

const finalize: NodeHandler = async ({ ctx, runId, signal, log, addArtifact }) => {
  ensureNotAborted(signal);
  log("Finalizing artifacts and summary report.");
  await sleep(700, signal);
  const outputDir = await getOutputDir(ctx.projectId, runId);
  const reportName = "migration-report.txt";
  await fs.writeFile(
    path.join(outputDir, reportName),
    `Migration Report\nProject: ${ctx.projectName}\nGenerated: ${timestamp()}\n`,
    "utf-8"
  );
  addArtifact({
    name: reportName,
    type: "report",
    url: `/api/runs/${runId}/artifacts/${encodeURIComponent(reportName)}`,
    createdAt: timestamp(),
  });
  return updateStage(ctx, "finalize");
};

const executeSql: NodeHandler = async ({ ctx, signal, log }) => {
  ensureNotAborted(signal);
  log("Executing SQL statements (handled by Python backend).");
  await sleep(500, signal);
  return updateStage(ctx, "execute_sql");
};

export const nodes: Record<StepId, NodeHandler> = {
  init_project: initProject,
  add_source_code: addSourceCode,
  apply_schema_mapping: applySchemaMapping,
  convert_code: convertCode,
  execute_sql: executeSql,
  self_heal: selfHeal,
  validate: validate,
  human_review: humanReview,
  finalize: finalize,
};

export function shouldContinue(ctx: MigrationContext): StepId {
  if (ctx.currentStage === "validate") {
    if (ctx.errors.length > 0 && ctx.attempts < MAX_SELF_HEAL_ATTEMPTS) {
      return "self_heal";
    }
    if (ctx.errors.length > 0) {
      return "human_review";
    }
    return "finalize";
  }
  if (ctx.currentStage === "human_review") {
    return "finalize";
  }
  const currentIndex = stepOrder.indexOf(ctx.currentStage);
  return stepOrder[Math.min(currentIndex + 1, stepOrder.length - 1)];
}

export function getStepOrder() {
  return stepOrder.map((step) => ({
    id: step,
    label: STEP_LABELS[step],
  }));
}
