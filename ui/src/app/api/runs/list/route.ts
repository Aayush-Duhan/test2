import { NextResponse } from "next/server";
import { listPythonRuns } from "@/lib/python-execution-client";

export const runtime = "nodejs";

export async function GET(request: Request) {
  try {
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

    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      const message = payload?.detail ?? payload?.error ?? "Unable to list runs";
      return NextResponse.json({ error: message }, { status: response.status >= 500 ? 503 : response.status });
    }

    const payload = await response.json();
    const runs = Array.isArray(payload?.runs) ? payload.runs : [];
    return NextResponse.json({
      runs: runs.map((run: Record<string, unknown>) => ({
        runId: run.runId,
        projectName: run.projectName,
        status: run.status,
        updatedAt: run.updatedAt,
        createdAt: run.createdAt,
        error: run.error ?? null,
      })),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to list runs";
    return NextResponse.json({ error: message }, { status: 503 });
  }
}
