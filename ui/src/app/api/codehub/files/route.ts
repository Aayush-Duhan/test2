import { NextResponse } from "next/server";
import { getFiles, isCodeHubApiError } from "@/lib/codehub-client";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/codehub/files");

export const runtime = "nodejs";

interface FileRequest {
  path: string;
}

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
    const files = Array.isArray(body?.files) ? (body.files as FileRequest[]) : [];
    const paths = files
      .map((file) => (typeof file?.path === "string" ? file.path : ""))
      .filter((path) => path.length > 0);

    if (!offeringId || !repositoryName || paths.length === 0) {
      return NextResponse.json(
        { error: "offeringId, repositoryName, and files[] are required" },
        { status: 400 }
      );
    }

    const result = await getFiles({
      offeringId,
      repositoryName,
      repositoryId,
      branch,
      paths,
    });

    if (result.errors?.length) {
      logger.warn(
        `Loaded ${result.files.length}/${paths.length} files from ${repositoryName}`
      );
    } else {
      logger.info(`Loaded ${result.files.length} files from ${repositoryName}`);
    }

    return NextResponse.json(result);
  } catch (error) {
    if (isCodeHubApiError(error)) {
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`File lookup failed (${status}): ${error.message}`);
      return NextResponse.json({ error: error.message }, { status });
    }

    logger.error("Unexpected file lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load repository files." },
      { status: 503 }
    );
  }
}
