import { NextResponse } from "next/server";
import { getProject, getSchema, getSource } from "@/lib/storage";
import { listPythonRuns, startPythonRun } from "@/lib/python-execution-client";
import { handlePythonResponse, withErrorHandling } from "@/lib/api-utils";

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
    if (!body?.projectId || !body?.sourceId || !body?.schemaId || !body?.sourceLanguage) {
      return NextResponse.json(
        { error: "projectId, sourceId, schemaId and sourceLanguage required" },
        { status: 400 }
      );
    }

    const [project, source, schema] = await Promise.all([
      getProject(body.projectId),
      getSource(body.sourceId),
      getSchema(body.schemaId),
    ]);
    if (!project || !source || !schema) {
      return NextResponse.json({ error: "Project/source/schema not found" }, { status: 404 });
    }

    const response = await startPythonRun({
      projectId: body.projectId,
      projectName: project.name,
      sourceId: body.sourceId,
      schemaId: body.schemaId,
      sourceLanguage: body.sourceLanguage,
      sourcePath: source.filepath,
      schemaPath: schema.filepath,
    });

    return handlePythonResponse<PythonStartResponse, { runId: string }>(
      response,
      (payload) => ({ runId: payload.runId }),
      "Unable to start run"
    );
  }, "Unable to start run");
}
