import { NextResponse } from "next/server";
import { listBranches, isGitHubApiError } from "@/lib/github-client";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/github/branches");

export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    const token = typeof body?.token === "string" ? body.token.trim() : "";
    const org = typeof body?.org === "string" ? body.org.trim() : "";
    const repositoryName =
      typeof body?.repositoryName === "string" ? body.repositoryName.trim() : "";

    if (!token || !org || !repositoryName) {
      return NextResponse.json(
        { error: "token, org, and repositoryName are required" },
        { status: 400 }
      );
    }

    const result = await listBranches({ token, org, repositoryName });

    logger.info(`Loaded ${result.branches.length} branches for ${org}/${repositoryName}`);

    return NextResponse.json(result);
  } catch (error) {
    if (isGitHubApiError(error)) {
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`Branch lookup failed (${status}): ${error.message}`);
      return NextResponse.json(
        { error: error.message, ssoUrl: error.ssoUrl ?? undefined },
        { status }
      );
    }

    logger.error("Unexpected branch lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load branches." },
      { status: 503 }
    );
  }
}
