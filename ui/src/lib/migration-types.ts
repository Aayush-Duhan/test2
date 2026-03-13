export type StepId =
  | "init_project"
  | "add_source_code"
  | "apply_schema_mapping"
  | "convert_code"
  | "execute_sql"
  | "validate"
  | "human_review"
  | "finalize";

export type StepStatus = "pending" | "running" | "completed" | "failed" | "skipped";

export interface StepState {
  id: StepId;
  label: string;
  status: StepStatus;
  startedAt?: string;
  endedAt?: string;
}
