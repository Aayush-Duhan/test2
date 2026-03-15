import { NextResponse } from "next/server";
import { getTree, isCodeHubApiError } from "@/lib/codehub-client";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/codehub/tree");

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
    const branch =
      typeof body?.branch === "string" && body.branch.trim().length > 0
        ? body.branch.trim()
        : undefined;

    if (!offeringId || !repositoryName) {
      return NextResponse.json(
        { error: "offeringId and repositoryName are required" },
        { status: 400 }
      );
    }

    const result = await getTree({
      offeringId,
      repositoryName,
      repositoryId,
      branch,
    });

    logger.info(
      `Loaded tree for ${repositoryName}@${result.defaultBranch || branch || "default"}: ${result.tree.length} files`
    );

    return NextResponse.json(result);
  } catch (error) {
    if (isCodeHubApiError(error)) {
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`Tree lookup failed (${status}): ${error.message}`);
      return NextResponse.json({ error: error.message }, { status });
    }

    logger.error("Unexpected tree lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load repository tree." },
      { status: 503 }
    );
  }
}
