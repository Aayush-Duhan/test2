const baseUrl = process.env.PYTHON_EXECUTION_URL ?? "http://127.0.0.1:8090";
const executionToken = process.env.EXECUTION_TOKEN ?? "local-dev-token";

type RequestOptions = {
  method?: string;
  body?: unknown;
  headers?: Record<string, string>;
};

async function request(path: string, options: RequestOptions = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method: options.method ?? "GET",
    headers: {
      "Content-Type": "application/json",
      "X-Execution-Token": executionToken,
      ...(options.headers ?? {}),
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
    cache: "no-store",
  });
  return response;
}

export async function startPythonRun(payload: {
  projectId: string;
  projectName: string;
  sourceId: string;
  schemaId: string;
  sourceLanguage: string;
  sourcePath: string;
  schemaPath: string;
}) {
  return request("/v1/runs/start", { method: "POST", body: payload });
}

export async function listPythonRuns(params?: {
  limit?: number;
  status?: string;
  projectId?: string;
}) {
  const search = new URLSearchParams();
  if (typeof params?.limit === "number") {
    search.set("limit", String(params.limit));
  }
  if (params?.status) {
    search.set("status", params.status);
  }
  if (params?.projectId) {
    search.set("projectId", params.projectId);
  }
  const query = search.toString();
  const path = query ? `/v1/runs?${query}` : "/v1/runs";
  return request(path);
}

export async function getPythonRun(runId: string) {
  return request(`/v1/runs/${runId}`);
}

export async function cancelPythonRun(runId: string) {
  return request(`/v1/runs/${runId}/cancel`, { method: "POST" });
}

export async function retryPythonRun(runId: string) {
  return request(`/v1/runs/${runId}/retry`, { method: "POST" });
}

export async function resumePythonRun(payload: {
  runId: string;
  ddlFile: File;
  resumeFromStage?: string;
  lastExecutedFileIndex?: number;
  missingObjects?: string[];
}) {
  const formData = new FormData();
  formData.append("ddl_file", payload.ddlFile);
  formData.append("resume_from_stage", payload.resumeFromStage ?? "execute_sql");
  formData.append("last_executed_file_index", String(payload.lastExecutedFileIndex ?? -1));
  formData.append("missing_objects", JSON.stringify(payload.missingObjects ?? []));

  return fetch(`${baseUrl}/v1/runs/${payload.runId}/resume`, {
    method: "POST",
    headers: {
      "X-Execution-Token": executionToken,
    },
    body: formData,
    cache: "no-store",
  });
}

export async function getPythonRunEvents(runId: string) {
  return fetch(`${baseUrl}/v1/runs/${runId}/events`, {
    headers: {
      "X-Execution-Token": executionToken,
    },
    cache: "no-store",
  });
}
