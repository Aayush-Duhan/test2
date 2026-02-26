import { retryPythonRun } from "@/lib/python-execution-client";
import { handlePythonResponse, withErrorHandling } from "@/lib/api-utils";

export const runtime = "nodejs";

interface PythonRetryResponse {
  runId: string;
}

export async function POST(_: Request, { params }: { params: Promise<{ id: string }> }) {
  return withErrorHandling(async () => {
    const { id } = await params;
    const response = await retryPythonRun(id);

    return handlePythonResponse<PythonRetryResponse, { runId: string }>(
      response,
      (payload) => ({ runId: payload.runId }),
      "Unable to retry run"
    );
  }, "Unable to retry run");
}

