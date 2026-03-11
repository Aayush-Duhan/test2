import { NextResponse } from "next/server";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/github/tree");

export const runtime = "nodejs";

const GITHUB_API = "https://api.github.com";

interface TreeEntry {
  path: string;
  mode: string;
  type: "blob" | "tree";
  sha: string;
  size?: number;
}

interface GitHubTreeResponse {
  sha: string;
  tree: TreeEntry[];
  truncated: boolean;
}

function buildHeaders(token?: string): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  return headers;
}

/**
 * POST /api/github/tree
 *
 * Accepts: { owner, repo, branch?, token? }
 * Returns: { tree: Array<{ path, sha, size }>, truncated, defaultBranch }
 *
 * Uses the Git Database API to fetch the recursive tree in a single request.
 * Falls back to iterative fetching if the tree is truncated.
 */
export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    if (!body?.owner || !body?.repo) {
      return NextResponse.json(
        { error: "owner and repo are required" },
        { status: 400 }
      );
    }

    const { owner, repo, token } = body as {
      owner: string;
      repo: string;
      branch?: string;
      token?: string;
    };
    let branch = typeof body.branch === "string" && body.branch.trim().length > 0
      ? body.branch.trim()
      : undefined;

    const headers = buildHeaders(token);

    // 1. Resolve default branch if none provided
    if (!branch) {
      const repoRes = await fetch(`${GITHUB_API}/repos/${owner}/${repo}`, { headers });
      if (!repoRes.ok) {
        const status = repoRes.status;
        const msg =
          status === 404
            ? "Repository not found. Check the owner/repo or provide a token for private repos."
            : status === 403
              ? "GitHub API rate limit exceeded. Please provide a Personal Access Token."
              : `GitHub API error: ${repoRes.statusText}`;
        return NextResponse.json({ error: msg }, { status });
      }
      const repoData = (await repoRes.json()) as { default_branch?: string };
      branch = repoData.default_branch ?? "main";
    }

    // 2. Fetch the tree recursively
    const treeUrl = `${GITHUB_API}/repos/${owner}/${repo}/git/trees/${branch}?recursive=1`;
    const treeRes = await fetch(treeUrl, { headers });
    if (!treeRes.ok) {
      const status = treeRes.status;
      const msg =
        status === 404
          ? `Branch "${branch}" not found in ${owner}/${repo}.`
          : status === 409
            ? "Repository is empty (no commits)."
            : `GitHub API error fetching tree: ${treeRes.statusText}`;
      return NextResponse.json({ error: msg }, { status: status === 409 ? 422 : status });
    }

    const treeData = (await treeRes.json()) as GitHubTreeResponse;

    // 3. Filter to blobs only and return essential fields
    const blobs = treeData.tree
      .filter((entry) => entry.type === "blob")
      .map((entry) => ({
        path: entry.path,
        sha: entry.sha,
        size: entry.size ?? 0,
      }));

    logger.info(`Fetched tree for ${owner}/${repo}@${branch}: ${blobs.length} files`);

    return NextResponse.json({
      tree: blobs,
      truncated: treeData.truncated,
      defaultBranch: branch,
    });
  } catch (err) {
    logger.error("Unexpected error in /api/github/tree:", err);
    return NextResponse.json(
      { error: "Internal server error" },
      { status: 500 }
    );
  }
}
