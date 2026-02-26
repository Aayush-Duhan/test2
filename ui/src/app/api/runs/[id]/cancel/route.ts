import { cancelPythonRun } from "@/lib/python-execution-client";
import { handlePythonResponse, withErrorHandling } from "@/lib/api-utils";

export const runtime = "nodejs";

interface PythonCancelResponse {
  status?: string;
}

export async function POST(_: Request, { params }: { params: Promise<{ id: string }> }) {
  return withErrorHandling(async () => {
    const { id } = await params;
    const response = await cancelPythonRun(id);

    return handlePythonResponse<PythonCancelResponse, { status: string }>(
      response,
      (result) => ({ status: result.status ?? "canceled" }),
      "Unable to cancel run"
    );
  }, "Unable to cancel run");
}

