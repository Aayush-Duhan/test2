import { resumePythonRun } from "@/lib/python-execution-client";
import {
  handlePythonResponse,
  withErrorHandling,
  requireFormDataFile,
  parseFormString,
  parseFormNumber,
  parseFormJsonArray,
} from "@/lib/api-utils";

export const runtime = "nodejs";

interface PythonResumeResponse {
  runId: string;
}

export async function POST(
  request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  return withErrorHandling(async () => {
    const { id } = await params;
    const formData = await request.formData();

    const [ddlFile, fileError] = requireFormDataFile(formData, "ddlFile", "DDL file is required");
    if (fileError) return fileError;

    const resumeFromStage = parseFormString(formData, "resumeFromStage", "execute_sql");
    const lastExecutedFileIndex = parseFormNumber(formData, "lastExecutedFileIndex", -1);
    const missingObjects = parseFormJsonArray<string>(formData, "missingObjects");

    const response = await resumePythonRun({
      runId: id,
      ddlFile,
      resumeFromStage,
      lastExecutedFileIndex,
      missingObjects,
    });

    return handlePythonResponse<PythonResumeResponse, { runId: string }>(
      response,
      (payload) => ({ runId: payload.runId }),
      "Unable to resume run"
    );
  }, "Unable to resume run");
}

