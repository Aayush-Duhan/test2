import { getPythonRun } from "@/lib/python-execution-client";
import { handlePythonResponse, withErrorHandling } from "@/lib/api-utils";

export const runtime = "nodejs";

interface PythonRunDetail {
  runId: string;
  projectId: string;
  status: string;
  steps: unknown;
  artifacts: unknown[];
  logs: string[];
  projectName: string;
  sourceId: string;
  schemaId: string;
  sourceLanguage: string;
  sfAccount?: string;
  sfUser?: string;
  sfRole?: string;
  sfWarehouse?: string;
  sfDatabase?: string;
  sfSchema?: string;
  sfAuthenticator?: string;
  createdAt: string;
  updatedAt: string;
  error?: string;
  validationIssues?: unknown[];
  executionLog?: unknown[];
  executionErrors?: unknown[];
  missingObjects?: string[];
  requiresDdlUpload?: boolean;
  resumeFromStage?: string;
  lastExecutedFileIndex?: number;
  selfHealIteration?: number;
  events?: unknown[];
}

export async function GET(_: Request, { params }: { params: Promise<{ id: string }> }) {
  return withErrorHandling(async () => {
    const { id } = await params;
    const response = await getPythonRun(id);

    return handlePythonResponse<PythonRunDetail, Record<string, unknown>>(
      response,
      (run) => ({
        runId: run.runId,
        projectId: run.projectId,
        status: run.status,
        steps: run.steps,
        artifacts: run.artifacts,
        logs: run.logs,
        projectName: run.projectName,
        sourceId: run.sourceId,
        schemaId: run.schemaId,
        sourceLanguage: run.sourceLanguage,
        sfAccount: run.sfAccount ?? "",
        sfUser: run.sfUser ?? "",
        sfRole: run.sfRole ?? "",
        sfWarehouse: run.sfWarehouse ?? "",
        sfDatabase: run.sfDatabase ?? "",
        sfSchema: run.sfSchema ?? "",
        sfAuthenticator: run.sfAuthenticator ?? "externalbrowser",
        createdAt: run.createdAt,
        updatedAt: run.updatedAt,
        error: run.error ?? null,
        validationIssues: run.validationIssues ?? [],
        executionLog: run.executionLog ?? [],
        executionErrors: run.executionErrors ?? [],
        missingObjects: run.missingObjects ?? [],
        requiresDdlUpload: run.requiresDdlUpload ?? false,
        resumeFromStage: run.resumeFromStage ?? "",
        lastExecutedFileIndex: run.lastExecutedFileIndex ?? -1,
        selfHealIteration: run.selfHealIteration ?? 0,
        events: run.events ?? [],
      }),
      "Run lookup failed"
    );
  }, "Run lookup failed");
}
