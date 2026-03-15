import { NextResponse } from "next/server";
import { getOfferingByName, isCodeHubApiError } from "@/lib/codehub-client";
import { createScopedLogger } from "@/lib/logger";

const logger = createScopedLogger("api/codehub/offering");

export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    const offeringName =
      typeof body?.offeringName === "string" ? body.offeringName.trim() : "";

    if (!offeringName) {
      return NextResponse.json(
        { error: "offeringName is required" },
        { status: 400 }
      );
    }

    const offering = await getOfferingByName(offeringName);

    logger.info(
      `Loaded offering ${offering.teamName} (${offering.repositories.length} repos)`
    );

    return NextResponse.json({
      offeringId: offering.id,
      teamName: offering.teamName,
      repositories: offering.repositories,
    });
  } catch (error) {
    if (isCodeHubApiError(error)) {
      const message =
        error.status === 404 ? "Offering not found." : error.message;
      const status = error.status >= 500 ? 503 : error.status;
      logger.warn(`Offering lookup failed (${status}): ${message}`);
      return NextResponse.json({ error: message }, { status });
    }

    logger.error("Unexpected offering lookup error:", error);
    return NextResponse.json(
      { error: "Failed to load offering." },
      { status: 503 }
    );
  }
}
