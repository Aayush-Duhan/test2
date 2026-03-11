import { NextResponse } from "next/server";
import { getProject, getSchema, getSource } from "@/lib/storage";
import { listPythonRuns, startPythonRun } from "@/lib/python-execution-client";
import { handlePythonResponse, withErrorHandling } from "@/lib/api-utils";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger('api/runs');

export const runtime = "nodejs";

interface PythonRunItem {
  runId: string;
  projectName: string;
  status: string;
  updatedAt: string;
  createdAt: string;
  error?: string;
  missingObjects?: string[];
  requiresDdlUpload?: boolean;
  lastExecutedFileIndex?: number;
  selfHealIteration?: number;
  executionErrors?: unknown[];
}

interface PythonRunsResponse {
  runs?: PythonRunItem[];
}

interface PythonStartResponse {
  runId: string;
}

export async function GET(request: Request) {
  return withErrorHandling(async () => {
    const { searchParams } = new URL(request.url);
    const limitRaw = searchParams.get("limit");
    const status = searchParams.get("status") ?? undefined;
    const projectId = searchParams.get("projectId") ?? undefined;
    const limit = limitRaw ? Number(limitRaw) : undefined;

    const response = await listPythonRuns({
      limit: Number.isFinite(limit) ? limit : undefined,
      status,
      projectId,
    });

    return handlePythonResponse<PythonRunsResponse, { runs: unknown[] }>(
      response,
      (payload) => {
        const runs = Array.isArray(payload?.runs) ? payload.runs : [];
        return {
          runs: runs.map((run) => ({
            runId: run.runId,
            projectName: run.projectName,
            status: run.status,
            updatedAt: run.updatedAt,
            createdAt: run.createdAt,
            error: run.error ?? null,
            missingObjects: Array.isArray(run.missingObjects) ? run.missingObjects : [],
            requiresDdlUpload: Boolean(run.requiresDdlUpload),
            lastExecutedFileIndex:
              typeof run.lastExecutedFileIndex === "number" ? run.lastExecutedFileIndex : -1,
            selfHealIteration: typeof run.selfHealIteration === "number" ? run.selfHealIteration : 0,
            executionErrors: Array.isArray(run.executionErrors) ? run.executionErrors : [],
          })),
        };
      },
      "Unable to list runs"
    );
  }, "Unable to list runs");
}

export async function POST(request: Request) {
  return withErrorHandling(async () => {
    const body = await request.json().catch(() => null);
    if (!body?.projectId || !body?.sourceId || !body?.sourceLanguage) {
      return NextResponse.json(
        { error: "projectId, sourceId and sourceLanguage required" },
        { status: 400 }
      );
    }

    const [project, source] = await Promise.all([
      getProject(body.projectId),
      getSource(body.sourceId),
    ]);
    if (!project || !source) {
      return NextResponse.json({ error: "Project/source not found" }, { status: 404 });
    }

    const schemaId = typeof body.schemaId === "string" && body.schemaId.trim().length > 0
      ? body.schemaId.trim()
      : undefined;
    const schema = schemaId ? await getSchema(schemaId) : undefined;
    if (schemaId && !schema) {
      return NextResponse.json({ error: "Schema not found" }, { status: 404 });
    }

    const pythonPayload = {
      projectId: body.projectId,
      projectName: project.name,
      sourceId: body.sourceId,
      schemaId,
      sourceLanguage: body.sourceLanguage,
      sourcePath: source.filepath,
      schemaPath: schema?.filepath,
      sfAccount: typeof body.sfAccount === "string" ? body.sfAccount : undefined,
      sfUser: typeof body.sfUser === "string" ? body.sfUser : undefined,
      sfRole: typeof body.sfRole === "string" ? body.sfRole : undefined,
      sfWarehouse: typeof body.sfWarehouse === "string" ? body.sfWarehouse : undefined,
      sfDatabase: typeof body.sfDatabase === "string" ? body.sfDatabase : undefined,
      sfSchema: typeof body.sfSchema === "string" ? body.sfSchema : undefined,
      sfAuthenticator: typeof body.sfAuthenticator === "string" ? body.sfAuthenticator : undefined,
    };
    const response = await startPythonRun(pythonPayload);

    if (!response.ok && response.status === 422) {
      const detail = await response.clone().text().catch(() => "unable to read response body");
      logger.error("Python /v1/runs/start returned 422", {
        payload: pythonPayload,
        responseBody: detail,
      });
    }

    return handlePythonResponse<PythonStartResponse, { runId: string }>(
      response,
      (payload) => ({ runId: payload.runId }),
      "Unable to start run"
    );
  }, "Unable to start run");
}
