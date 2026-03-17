import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("lib/github-client");

const DEFAULT_API_BASE_URL = "https://api.github.com";
const DEFAULT_USER_AGENT = "db-migration-ui";

type HttpMethod = "GET" | "POST";

interface GitHubConfig {
  apiBaseUrl: string;
  userAgent: string;
}

interface TokenizedRequest {
  token: string;
}

interface GitHubRepoApiResponse {
  id?: number;
  name?: string;
  visibility?: string;
  private?: boolean;
  archived?: boolean;
  default_branch?: string;
  topics?: string[];
}

interface GitHubBranchApiResponse {
  name?: string;
}

interface GitHubTreeApiResponse {
  truncated?: boolean;
  tree?: Array<{
    path?: string;
    sha?: string;
    size?: number;
    type?: string;
  }>;
}

interface GitHubBlobApiResponse {
  content?: string;
  encoding?: string;
  size?: number;
}

interface GitHubContentApiResponse {
  content?: string;
  encoding?: string;
  size?: number;
}

export interface GitHubRepository {
  id?: number | null;
  name: string;
  visibility?: string | null;
  private?: boolean;
  archived?: boolean;
  topics?: string[];
}

export interface GitHubBranch {
  name: string;
  isDefault: boolean;
}

export interface GitHubTreeEntry {
  path: string;
  sha: string;
  size: number;
}

export interface GitHubFetchedFile {
  path: string;
  content: string;
  size: number;
}

export class GitHubApiError extends Error {
  status: number;
  ssoUrl?: string | null;

  constructor(message: string, status: number, ssoUrl?: string | null) {
    super(message);
    this.name = "GitHubApiError";
    this.status = status;
    this.ssoUrl = ssoUrl;
  }
}

function getConfig(): GitHubConfig {
  return {
    apiBaseUrl: (process.env.GITHUB_ENTERPRISE_API_BASE_URL || DEFAULT_API_BASE_URL).replace(
      /\/+$/,
      ""
    ),
    userAgent: process.env.GITHUB_USER_AGENT?.trim() || DEFAULT_USER_AGENT,
  };
}

function parseSsoUrl(headerValue: string | null): string | null {
  if (!headerValue) return null;
  if (!headerValue.toLowerCase().includes("required")) return null;
  const match = headerValue.match(/url=([^;]+)/i);
  return match ? match[1] : null;
}

function hasNextPage(linkHeader: string | null): boolean {
  if (!linkHeader) return false;
  return linkHeader.split(",").some((segment) => segment.includes('rel="next"'));
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
    const candidate = ["message", "error", "detail", "title"].find(
      (key) => typeof (payload as Record<string, unknown>)[key] === "string"
    );

    if (candidate) {
      return String((payload as Record<string, unknown>)[candidate]);
    }
  }

  return fallback;
}

async function requestGitHub<T>(
  path: string,
  token: string,
  method: HttpMethod = "GET",
  body?: unknown
): Promise<{ data: T; headers: Headers }> {
  const config = getConfig();
  const response = await fetch(`${config.apiBaseUrl}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "User-Agent": config.userAgent,
      "X-GitHub-Api-Version": "2022-11-28",
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
    cache: "no-store",
  });

  const payload = await readResponseBody(response);

  if (!response.ok) {
    const ssoUrl = parseSsoUrl(response.headers.get("x-github-sso"));
    const fallbackMessage =
      response.status === 401
        ? "Invalid GitHub token."
        : response.status === 403
          ? ssoUrl
            ? "SSO authorization required."
            : "GitHub access denied."
          : "GitHub request failed.";
    const message = getErrorMessage(payload, fallbackMessage);
    logger.warn(`GitHub request failed (${response.status}): ${message}`);
    throw new GitHubApiError(message, response.status, ssoUrl);
  }

  return { data: payload as T, headers: response.headers };
}

function normalizeRepository(item: GitHubRepoApiResponse): GitHubRepository | null {
  const name = typeof item.name === "string" ? item.name : null;
  if (!name) return null;

  return {
    id: typeof item.id === "number" ? item.id : null,
    name,
    visibility: typeof item.visibility === "string" ? item.visibility : null,
    private: typeof item.private === "boolean" ? item.private : undefined,
    archived: typeof item.archived === "boolean" ? item.archived : undefined,
    topics: Array.isArray(item.topics)
      ? item.topics.filter((topic): topic is string => typeof topic === "string")
      : [],
  };
}

function decodeContent(content: string, encoding?: string): string {
  if (!content) return "";
  const normalizedEncoding = encoding?.toLowerCase();
  if (normalizedEncoding === "base64") {
    return Buffer.from(content, "base64").toString("utf-8");
  }

  return content;
}

async function getRepositoryDetails(input: {
  org: string;
  repositoryName: string;
} & TokenizedRequest): Promise<GitHubRepoApiResponse> {
  const org = encodeURIComponent(input.org);
  const repo = encodeURIComponent(input.repositoryName);
  const { data } = await requestGitHub<GitHubRepoApiResponse>(
    `/repos/${org}/${repo}`,
    input.token
  );
  return data;
}

export async function listOrgRepositories(input: {
  org: string;
} & TokenizedRequest): Promise<{ repositories: GitHubRepository[]; hasMore: boolean }> {
  const org = encodeURIComponent(input.org);
  const { data, headers } = await requestGitHub<GitHubRepoApiResponse[]>(
    `/orgs/${org}/repos?per_page=100&type=all&sort=full_name&direction=asc`,
    input.token
  );

  const repositories = Array.isArray(data)
    ? data
        .map((item) => normalizeRepository(item))
        .filter((item): item is GitHubRepository => item !== null)
    : [];

  return {
    repositories,
    hasMore: hasNextPage(headers.get("link")),
  };
}

export async function listBranches(input: {
  org: string;
  repositoryName: string;
} & TokenizedRequest): Promise<{ branches: GitHubBranch[]; defaultBranch: string }> {
  const repoDetails = await getRepositoryDetails(input);
  const org = encodeURIComponent(input.org);
  const repo = encodeURIComponent(input.repositoryName);

  const { data } = await requestGitHub<GitHubBranchApiResponse[]>(
    `/repos/${org}/${repo}/branches?per_page=100`,
    input.token
  );

  const defaultBranch =
    typeof repoDetails.default_branch === "string" ? repoDetails.default_branch : "";

  const branches = Array.isArray(data)
    ? data
        .map((branch) => {
          const name = typeof branch.name === "string" ? branch.name : null;
          if (!name) return null;
          return { name, isDefault: name === defaultBranch };
        })
        .filter((entry): entry is GitHubBranch => entry !== null)
    : [];

  return {
    branches,
    defaultBranch,
  };
}

export async function getTree(input: {
  org: string;
  repositoryName: string;
  branch?: string;
} & TokenizedRequest): Promise<{
  tree: GitHubTreeEntry[];
  truncated: boolean;
  defaultBranch: string;
}> {
  const repoDetails = await getRepositoryDetails(input);
  const defaultBranch =
    typeof repoDetails.default_branch === "string" ? repoDetails.default_branch : "";
  const branch = input.branch || defaultBranch;
  if (!branch) {
    throw new GitHubApiError("Repository default branch not found.", 422);
  }

  const org = encodeURIComponent(input.org);
  const repo = encodeURIComponent(input.repositoryName);
  const ref = encodeURIComponent(branch);

  const { data } = await requestGitHub<GitHubTreeApiResponse>(
    `/repos/${org}/${repo}/git/trees/${ref}?recursive=1`,
    input.token
  );

  const tree = Array.isArray(data?.tree)
    ? data.tree
        .map((entry) => {
          if (!entry || entry.type !== "blob") return null;
          const path = typeof entry.path === "string" ? entry.path : null;
          if (!path) return null;

          return {
            path,
            sha: typeof entry.sha === "string" ? entry.sha : path,
            size: typeof entry.size === "number" ? entry.size : 0,
          };
        })
        .filter((entry): entry is GitHubTreeEntry => entry !== null)
    : [];

  return {
    tree,
    truncated: data?.truncated === true,
    defaultBranch,
  };
}

export async function getFiles(input: {
  org: string;
  repositoryName: string;
  branch?: string;
  files: Array<{ path: string; sha?: string | null }>;
} & TokenizedRequest): Promise<{ files: GitHubFetchedFile[]; errors?: string[] }> {
  const org = encodeURIComponent(input.org);
  const repo = encodeURIComponent(input.repositoryName);
  const branch = input.branch;

  const results = await Promise.allSettled(
    input.files.map(async (file) => {
      if (file.sha) {
        const { data } = await requestGitHub<GitHubBlobApiResponse>(
          `/repos/${org}/${repo}/git/blobs/${encodeURIComponent(file.sha)}`,
          input.token
        );

        if (!data.content) {
          throw new Error(`Empty blob content for ${file.path}.`);
        }

        const content = decodeContent(data.content, data.encoding);
        return {
          path: file.path,
          content,
          size: typeof data.size === "number" ? data.size : Buffer.byteLength(content, "utf-8"),
        } satisfies GitHubFetchedFile;
      }

      const encodedPath = file.path
        .split("/")
        .map((segment) => encodeURIComponent(segment))
        .join("/");
      const refQuery = branch ? `?ref=${encodeURIComponent(branch)}` : "";

      const { data } = await requestGitHub<GitHubContentApiResponse>(
        `/repos/${org}/${repo}/contents/${encodedPath}${refQuery}`,
        input.token
      );

      if (!data.content) {
        throw new Error(`Empty content for ${file.path}.`);
      }

      const content = decodeContent(data.content, data.encoding);
      return {
        path: file.path,
        content,
        size: typeof data.size === "number" ? data.size : Buffer.byteLength(content, "utf-8"),
      } satisfies GitHubFetchedFile;
    })
  );

  const files: GitHubFetchedFile[] = [];
  const errors: string[] = [];

  for (const result of results) {
    if (result.status === "fulfilled") {
      files.push(result.value);
    } else {
      errors.push(result.reason instanceof Error ? result.reason.message : "Unknown file error");
    }
  }

  return { files, errors: errors.length > 0 ? errors : undefined };
}

export function isGitHubApiError(error: unknown): error is GitHubApiError {
  return error instanceof GitHubApiError;
}
