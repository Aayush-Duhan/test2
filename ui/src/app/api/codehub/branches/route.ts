import { NextResponse } from "next/server";
import { isCodeHubApiError, listBranches } from "@/lib/codehub-client";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/codehub/branches");

export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    const offeringId =
      typeof body?.offeringId === "string" ? body.offeringId.trim() : "";
    const repositoryName =
      typeof body?.repositoryName === "string" ? body.repositoryName.trim() : "";
    const repositoryId =
      typeof body?.repositoryId === "string" || typeof body?.repositoryId === "number"
        ? body.repositoryId
        : null;

    if (!offeringId || !repositoryName) {
      return NextResponse.json(
        { error: "offeringId and repositoryName are required" },
        { status: 400 }
      );
    }

    const result = await listBranches({
      offeringId,
      repositoryName,
      repositoryId,
    });

    logger.info(
      `Loaded ${result.branches.length} branches for ${repositoryName}`
    );

    return NextResponse.json(result);
  } catch (error) {
    if (isCodeHubApiError(error)) {
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`Branch lookup failed (${status}): ${error.message}`);
      return NextResponse.json({ error: error.message }, { status });
    }

    logger.error("Unexpected branch lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load branches." },
      { status: 503 }
    );
  }
}
