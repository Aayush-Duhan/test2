import { NextResponse } from "next/server";
import { getFiles, isGitHubApiError } from "@/lib/github-client";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/github/files");

export const runtime = "nodejs";

interface FileRequest {
  path: string;
  sha?: string | null;
}

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    const token = typeof body?.token === "string" ? body.token.trim() : "";
    const org = typeof body?.org === "string" ? body.org.trim() : "";
    const repositoryName =
      typeof body?.repositoryName === "string" ? body.repositoryName.trim() : "";
    const branch =
      typeof body?.branch === "string" && body.branch.trim().length > 0
        ? body.branch.trim()
        : undefined;
    const files = Array.isArray(body?.files) ? (body.files as FileRequest[]) : [];
    const validFiles = files.filter(
      (f) => typeof f?.path === "string" && f.path.length > 0
    );

    if (!token || !org || !repositoryName || validFiles.length === 0) {
      return NextResponse.json(
        { error: "token, org, repositoryName, and files[] are required" },
        { status: 400 }
      );
    }

    const result = await getFiles({
      token,
      org,
      repositoryName,
      branch,
      files: validFiles.map((f) => ({ path: f.path, sha: f.sha })),
    });

    if (result.errors?.length) {
      logger.warn(
        `Loaded ${result.files.length}/${validFiles.length} files from ${org}/${repositoryName}`
      );
    } else {
      logger.info(
        `Loaded ${result.files.length} files from ${org}/${repositoryName}`
      );
    }

    return NextResponse.json(result);
  } catch (error) {
    if (isGitHubApiError(error)) {
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`File lookup failed (${status}): ${error.message}`);
      return NextResponse.json(
        { error: error.message, ssoUrl: error.ssoUrl ?? undefined },
        { status }
      );
    }

    logger.error("Unexpected file lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load repository files." },
      { status: 503 }
    );
  }
}
