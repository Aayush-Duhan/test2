import { NextResponse } from "next/server";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/github/files");

export const runtime = "nodejs";

const GITHUB_API = "https://api.github.com";
const MAX_FILES_PER_REQUEST = 50;

interface FileRequest {
  path: string;
  sha: string;
}

interface GitHubBlobResponse {
  sha: string;
  content: string;
  encoding: "base64" | "utf-8";
  size: number;
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
 * POST /api/github/files
 *
 * Accepts: { owner, repo, files: Array<{ path, sha }>, token? }
 * Returns: { files: Array<{ path, content, size }> }
 *
 * Fetches the raw content of each requested file via the Blobs API,
 * decodes from base64, and returns plain text content.
 */
export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    if (!body?.owner || !body?.repo || !Array.isArray(body?.files)) {
      return NextResponse.json(
        { error: "owner, repo, and files[] are required" },
        { status: 400 }
      );
    }

    const { owner, repo, token } = body as {
      owner: string;
      repo: string;
      token?: string;
    };
    const files = (body.files as FileRequest[]).slice(0, MAX_FILES_PER_REQUEST);

    if (files.length === 0) {
      return NextResponse.json({ files: [] });
    }

    const headers = buildHeaders(token);

    // Fetch all blobs concurrently (capped at MAX_FILES_PER_REQUEST)
    const results = await Promise.allSettled(
      files.map(async (file) => {
        const blobUrl = `${GITHUB_API}/repos/${owner}/${repo}/git/blobs/${file.sha}`;
        const res = await fetch(blobUrl, { headers });
        if (!res.ok) {
          throw new Error(`Failed to fetch ${file.path}: ${res.statusText}`);
        }
        const blob = (await res.json()) as GitHubBlobResponse;

        // Decode base64 content
        let content: string;
        if (blob.encoding === "base64") {
          content = Buffer.from(blob.content, "base64").toString("utf-8");
        } else {
          content = blob.content;
        }

        return {
          path: file.path,
          content,
          size: blob.size,
        };
      })
    );

    const fetched: Array<{ path: string; content: string; size: number }> = [];
    const errors: string[] = [];

    for (const result of results) {
      if (result.status === "fulfilled") {
        fetched.push(result.value);
      } else {
        errors.push(result.reason?.message ?? "Unknown error");
      }
    }

    if (errors.length > 0) {
      logger.warn(`Some files failed to fetch from ${owner}/${repo}:`, errors);
    }

    logger.info(
      `Fetched ${fetched.length}/${files.length} files from ${owner}/${repo}`
    );

    return NextResponse.json({
      files: fetched,
      errors: errors.length > 0 ? errors : undefined,
    });
  } catch (err) {
    logger.error("Unexpected error in /api/github/files:", err);
    return NextResponse.json(
      { error: "Internal server error" },
      { status: 500 }
    );
  }
}
