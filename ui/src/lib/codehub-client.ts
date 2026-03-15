import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("lib/codehub-client");

const DEFAULT_OFFERING_PATH = "/offers/teams/{offeringName}";
const DEFAULT_BRANCHES_PATH = "/offers/{offeringId}/repositories/{repoName}/branches";
const DEFAULT_TREE_PATH = "/offers/{offeringId}/repositories/{repoName}/tree";
const DEFAULT_FILES_PATH = "/offers/{offeringId}/repositories/{repoName}/files";

type HttpMethod = "GET" | "POST";

interface CodeHubConfig {
  apiBaseUrl: string;
  tenantId: string;
  clientId: string;
  clientSecret: string;
  scope: string;
  offeringByNamePath: string;
  branchesPath: string;
  treePath: string;
  filesPath: string;
}

interface TokenCacheEntry {
  accessToken: string;
  expiresAt: number;
}

export interface CodeHubRepository {
  id?: number | string | null;
  name: string;
  visibility?: string | null;
  technology?: string | null;
  template?: string | null;
  topics?: string[];
}

export interface CodeHubOffering {
  id: string;
  teamName: string;
  repositories: CodeHubRepository[];
}

export interface CodeHubBranch {
  name: string;
  isDefault: boolean;
}

export interface CodeHubTreeEntry {
  path: string;
  sha: string;
  size: number;
}

export interface CodeHubFetchedFile {
  path: string;
  content: string;
  size: number;
}

export class CodeHubApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "CodeHubApiError";
    this.status = status;
  }
}

let tokenCache: TokenCacheEntry | null = null;

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getConfig(): CodeHubConfig {
  return {
    apiBaseUrl: requireEnv("CODEHUB_API_BASE_URL").replace(/\/+$/, ""),
    tenantId: requireEnv("CODEHUB_TENANT_ID"),
    clientId: requireEnv("CODEHUB_CLIENT_ID"),
    clientSecret: requireEnv("CODEHUB_CLIENT_SECRET"),
    scope: requireEnv("CODEHUB_SCOPE"),
    offeringByNamePath:
      process.env.CODEHUB_OFFERING_BY_NAME_PATH?.trim() || DEFAULT_OFFERING_PATH,
    branchesPath:
      process.env.CODEHUB_BRANCHES_PATH?.trim() || DEFAULT_BRANCHES_PATH,
    treePath: process.env.CODEHUB_TREE_PATH?.trim() || DEFAULT_TREE_PATH,
    filesPath: process.env.CODEHUB_FILES_PATH?.trim() || DEFAULT_FILES_PATH,
  };
}

function buildPath(
  template: string,
  replacements: Record<string, string | number | undefined | null>
): string {
  return template.replace(/\{([^}]+)\}/g, (_, key: string) => {
    const value = replacements[key];
    if (value == null || value === "") {
      throw new Error(`Missing CodeHub path value for ${key}`);
    }

    return encodeURIComponent(String(value));
  });
}

function appendQuery(
  url: string,
  params: Record<string, string | number | undefined | null>
): string {
  const resolved = new URL(url);

  for (const [key, value] of Object.entries(params)) {
    if (value == null || value === "") continue;
    resolved.searchParams.set(key, String(value));
  }

  return resolved.toString();
}

async function readResponseBody(response: Response): Promise<unknown> {
  const contentType = response.headers.get("content-type") ?? "";

  if (contentType.includes("application/json")) {
    return response.json();
  }

  return response.text();
}

function getErrorMessage(payload: unknown, fallback: string): string {
  if (typeof payload === "string" && payload.trim()) {
    return payload;
  }

  if (payload && typeof payload === "object") {
    const candidate = [
      "message",
      "error",
      "detail",
      "title",
      "description",
    ].find((key) => typeof (payload as Record<string, unknown>)[key] === "string");

    if (candidate) {
      return String((payload as Record<string, unknown>)[candidate]);
    }
  }

  return fallback;
}

async function getAccessToken(): Promise<string> {
  if (tokenCache && tokenCache.expiresAt > Date.now() + 60_000) {
    return tokenCache.accessToken;
  }

  const config = getConfig();
  const tokenUrl = `https://login.microsoftonline.com/${config.tenantId}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: config.clientId,
    client_secret: config.clientSecret,
    scope: config.scope,
  });

  const response = await fetch(tokenUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
    cache: "no-store",
  });

  const payload = await readResponseBody(response);
  if (!response.ok) {
    logger.error("Failed to acquire CodeHub token", response.status);
    throw new CodeHubApiError("CodeHub authentication failed.", 503);
  }

  const accessToken =
    payload && typeof payload === "object"
      ? (payload as { access_token?: string }).access_token
      : undefined;
  const expiresIn =
    payload && typeof payload === "object"
      ? (payload as { expires_in?: number }).expires_in
      : undefined;

  if (!accessToken) {
    throw new CodeHubApiError("CodeHub authentication failed.", 503);
  }

  tokenCache = {
    accessToken,
    expiresAt: Date.now() + Math.max((expiresIn ?? 300) - 30, 60) * 1000,
  };

  return accessToken;
}

async function requestCodeHub<T>(
  path: string,
  method: HttpMethod,
  fallbackMessage: string,
  body?: unknown
): Promise<T> {
  const token = await getAccessToken();
  const config = getConfig();

  const response = await fetch(`${config.apiBaseUrl}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
    cache: "no-store",
  });

  const payload = await readResponseBody(response);

  if (!response.ok) {
    const genericMessage =
      response.status === 401 || response.status === 403
        ? "CodeHub authentication failed."
        : fallbackMessage;
    const message = getErrorMessage(payload, genericMessage);
    throw new CodeHubApiError(message, response.status);
  }

  return payload as T;
}

function normalizeRepository(item: unknown): CodeHubRepository | null {
  if (!item || typeof item !== "object") return null;

  const source = item as Record<string, unknown>;
  const name = typeof source.name === "string" ? source.name : null;
  if (!name) return null;

  return {
    id:
      typeof source.id === "string" || typeof source.id === "number"
        ? source.id
        : null,
    name,
    visibility: typeof source.visibility === "string" ? source.visibility : null,
    technology: typeof source.technology === "string" ? source.technology : null,
    template: typeof source.template === "string" ? source.template : null,
    topics: Array.isArray(source.topics)
      ? source.topics.filter((topic): topic is string => typeof topic === "string")
      : [],
  };
}

function normalizeBranches(payload: unknown): {
  branches: CodeHubBranch[];
  defaultBranch: string;
} {
  const source =
    payload && typeof payload === "object" ? (payload as Record<string, unknown>) : null;

  const branchItems = Array.isArray(payload)
    ? payload
    : Array.isArray(source?.branches)
      ? source?.branches
      : Array.isArray(source?.items)
        ? source?.items
        : Array.isArray(source?.refs)
          ? source?.refs
          : [];

  const branches = branchItems
    .map((item) => {
      if (typeof item === "string") {
        return { name: item, isDefault: false };
      }

      if (!item || typeof item !== "object") return null;

      const branch = item as Record<string, unknown>;
      const nameCandidate =
        typeof branch.name === "string"
          ? branch.name
          : typeof branch.displayName === "string"
            ? branch.displayName
            : typeof branch.ref === "string"
              ? branch.ref.replace(/^refs\/heads\//, "")
              : null;

      if (!nameCandidate) return null;

      return {
        name: nameCandidate,
        isDefault:
          branch.isDefault === true ||
          branch.default === true ||
          branch.is_default === true,
      };
    })
    .filter((entry): entry is CodeHubBranch => entry !== null);

  const defaultBranchCandidate =
    typeof source?.defaultBranch === "string"
      ? source.defaultBranch
      : typeof source?.default_branch === "string"
        ? source.default_branch
        : branches.find((branch) => branch.isDefault)?.name ?? "";

  const deduped = Array.from(
    new Map(branches.map((branch) => [branch.name, branch])).values()
  );

  return {
    branches: deduped,
    defaultBranch: defaultBranchCandidate || deduped[0]?.name || "",
  };
}

function normalizeTree(payload: unknown): {
  tree: CodeHubTreeEntry[];
  truncated: boolean;
  defaultBranch: string;
} {
  const source =
    payload && typeof payload === "object" ? (payload as Record<string, unknown>) : null;

  const entries = Array.isArray(payload)
    ? payload
    : Array.isArray(source?.tree)
      ? source.tree
      : Array.isArray(source?.items)
        ? source.items
        : Array.isArray(source?.entries)
          ? source.entries
          : [];

  const tree = entries
    .map((entry) => {
      if (!entry || typeof entry !== "object") return null;

      const item = entry as Record<string, unknown>;
      const type =
        typeof item.type === "string"
          ? item.type.toLowerCase()
          : typeof item.kind === "string"
            ? item.kind.toLowerCase()
            : "blob";

      if (type === "tree" || type === "directory" || type === "folder") {
        return null;
      }

      const path =
        typeof item.path === "string"
          ? item.path
          : typeof item.name === "string"
            ? item.name
            : null;

      if (!path) return null;

      return {
        path,
        sha:
          typeof item.sha === "string"
            ? item.sha
            : typeof item.id === "string"
              ? item.id
              : path,
        size:
          typeof item.size === "number"
            ? item.size
            : typeof item.length === "number"
              ? item.length
              : 0,
      };
    })
    .filter((entry): entry is CodeHubTreeEntry => entry !== null);

  return {
    tree,
    truncated: source?.truncated === true,
    defaultBranch:
      typeof source?.defaultBranch === "string"
        ? source.defaultBranch
        : typeof source?.default_branch === "string"
          ? source.default_branch
          : "",
  };
}

function normalizeFiles(payload: unknown): {
  files: CodeHubFetchedFile[];
  errors?: string[];
} {
  const source =
    payload && typeof payload === "object" ? (payload as Record<string, unknown>) : null;

  const fileItems = Array.isArray(payload)
    ? payload
    : Array.isArray(source?.files)
      ? source.files
      : Array.isArray(source?.items)
        ? source.items
        : [];

  const files = fileItems
    .map((item) => {
      if (!item || typeof item !== "object") return null;

      const file = item as Record<string, unknown>;
      const path =
        typeof file.path === "string"
          ? file.path
          : typeof file.name === "string"
            ? file.name
            : null;

      if (!path) return null;

      const rawContent = typeof file.content === "string" ? file.content : "";
      const encoding =
        typeof file.encoding === "string" ? file.encoding.toLowerCase() : "utf-8";
      const content =
        encoding === "base64"
          ? Buffer.from(rawContent, "base64").toString("utf-8")
          : rawContent;

      return {
        path,
        content,
        size:
          typeof file.size === "number"
            ? file.size
            : Buffer.byteLength(content, "utf-8"),
      };
    })
    .filter((entry): entry is CodeHubFetchedFile => entry !== null);

  const errors = Array.isArray(source?.errors)
    ? source.errors.filter((item): item is string => typeof item === "string")
    : undefined;

  return { files, errors };
}

export async function getOfferingByName(offeringName: string): Promise<CodeHubOffering> {
  const config = getConfig();
  const path = buildPath(config.offeringByNamePath, { offeringName });
  const payload = await requestCodeHub<Record<string, unknown>>(
    path,
    "GET",
    "Failed to load offering."
  );

  const repositories = Array.isArray(payload.repositories)
    ? payload.repositories
        .map((repository) => normalizeRepository(repository))
        .filter((repository): repository is CodeHubRepository => repository !== null)
    : [];

  return {
    id:
      typeof payload.id === "string"
        ? payload.id
        : typeof payload._id === "string"
          ? payload._id
          : "",
    teamName: typeof payload.teamName === "string" ? payload.teamName : offeringName,
    repositories,
  };
}

export async function listBranches(input: {
  offeringId: string;
  repositoryName: string;
  repositoryId?: string | number | null;
}): Promise<{ branches: CodeHubBranch[]; defaultBranch: string }> {
  const config = getConfig();
  const path = buildPath(config.branchesPath, {
    offeringId: input.offeringId,
    repoName: input.repositoryName,
    repositoryName: input.repositoryName,
    repoId: input.repositoryId,
    repositoryId: input.repositoryId,
  });

  const payload = await requestCodeHub<unknown>(
    path,
    "GET",
    "Failed to load branches."
  );

  return normalizeBranches(payload);
}

export async function getTree(input: {
  offeringId: string;
  repositoryName: string;
  repositoryId?: string | number | null;
  branch?: string;
}): Promise<{ tree: CodeHubTreeEntry[]; truncated: boolean; defaultBranch: string }> {
  const config = getConfig();
  const path = buildPath(config.treePath, {
    offeringId: input.offeringId,
    repoName: input.repositoryName,
    repositoryName: input.repositoryName,
    repoId: input.repositoryId,
    repositoryId: input.repositoryId,
    branch: input.branch,
  });

  const payload = await requestCodeHub<unknown>(
    appendQuery(`${config.apiBaseUrl}${path}`, { branch: input.branch }).replace(
      config.apiBaseUrl,
      ""
    ),
    "GET",
    "Failed to load repository tree."
  );

  const normalized = normalizeTree(payload);
  return {
    tree: normalized.tree,
    truncated: normalized.truncated,
    defaultBranch: normalized.defaultBranch || input.branch || "",
  };
}

export async function getFiles(input: {
  offeringId: string;
  repositoryName: string;
  repositoryId?: string | number | null;
  branch?: string;
  paths: string[];
}): Promise<{ files: CodeHubFetchedFile[]; errors?: string[] }> {
  const config = getConfig();
  const pathTemplate = config.filesPath;

  if (pathTemplate.includes("{path}")) {
    const results = await Promise.allSettled(
      input.paths.map(async (filePath) => {
        const path = buildPath(pathTemplate, {
          offeringId: input.offeringId,
          repoName: input.repositoryName,
          repositoryName: input.repositoryName,
          repoId: input.repositoryId,
          repositoryId: input.repositoryId,
          path: filePath,
        });

        const payload = await requestCodeHub<unknown>(
          appendQuery(`${config.apiBaseUrl}${path}`, { branch: input.branch }).replace(
            config.apiBaseUrl,
            ""
          ),
          "GET",
          `Failed to load ${filePath}.`
        );

        const normalized = normalizeFiles({ files: [{ ...(payload as object), path: filePath }] });
        return normalized.files[0];
      })
    );

    const files: CodeHubFetchedFile[] = [];
    const errors: string[] = [];

    for (const result of results) {
      if (result.status === "fulfilled" && result.value) {
        files.push(result.value);
      } else if (result.status === "rejected") {
        errors.push(result.reason instanceof Error ? result.reason.message : "Unknown file error");
      }
    }

    return { files, errors: errors.length > 0 ? errors : undefined };
  }

  const path = buildPath(pathTemplate, {
    offeringId: input.offeringId,
    repoName: input.repositoryName,
    repositoryName: input.repositoryName,
    repoId: input.repositoryId,
    repositoryId: input.repositoryId,
  });

  const payload = await requestCodeHub<unknown>(
    path,
    "POST",
    "Failed to load repository files.",
    {
      branch: input.branch,
      paths: input.paths,
      files: input.paths,
    }
  );

  return normalizeFiles(payload);
}

export function isCodeHubApiError(error: unknown): error is CodeHubApiError {
  return error instanceof CodeHubApiError;
}
