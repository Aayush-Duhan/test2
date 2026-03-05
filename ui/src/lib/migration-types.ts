export type StepId =
  | "init_project"
  | "add_source_code"
  | "apply_schema_mapping"
  | "convert_code"
  | "execute_sql"
  | "self_heal"
  | "validate"
  | "human_review"
  | "finalize";

export type StepStatus = "pending" | "running" | "completed" | "failed" | "skipped";

export type RunStatus = "queued" | "running" | "completed" | "failed" | "canceled";

export interface StepState {
  id: StepId;
  label: string;
  status: StepStatus;
  startedAt?: string;
  endedAt?: string;
}

export interface MigrationContext {
  projectId: string;
  projectName: string;
  sourceId: string;
  currentStage: StepId;
  errors: string[];
  createdAt: string;
  updatedAt: string;
  attempts: number;
  needsHumanReview: boolean;
}

export type RunEventType =
  | "run:started"
  | "run:completed"
  | "run:failed"
  | "step:started"
  | "step:completed"
  | "log"
  | "execute_sql:statement"
  | "execute_sql:error";

export interface RunEvent {
  type: RunEventType;
  payload: Record<string, unknown>;
}

export interface RunRecord {
  runId: string;
  projectId: string;
  projectName: string;
  sourceId: string;
  status: RunStatus;
  steps: StepState[];
  logs: string[];
  createdAt: string;
  updatedAt: string;
  error?: string;
}

export const STEP_LABELS: Record<StepId, string> = {
  init_project: "Initialize project",
  add_source_code: "Ingest source SQL",
  apply_schema_mapping: "Apply schema mapping",
  convert_code: "Convert SQL",
  execute_sql: "Execute SQL",
  self_heal: "Self-heal fixes",
  validate: "Validate output",
  human_review: "Human review",
  finalize: "Finalize output",
};
