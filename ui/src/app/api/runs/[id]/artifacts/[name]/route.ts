import { NextResponse } from "next/server";
import { getPythonArtifact } from "@/lib/python-execution-client";
import { withErrorHandling } from "@/lib/api-utils";

export const runtime = "nodejs";

export async function GET(
  _: Request,
  { params }: { params: Promise<{ id: string; name: string }> },
) {
  return withErrorHandling(async () => {
    const { id, name } = await params;
    const response = await getPythonArtifact(id, name);

    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      const message = (payload as { detail?: string; error?: string })?.detail 
        ?? (payload as { detail?: string; error?: string })?.error 
        ?? "Artifact fetch failed";
      return NextResponse.json(
        { error: message },
        { status: response.status >= 500 ? 503 : response.status }
      );
    }

    const data = await response.arrayBuffer();
    return new NextResponse(data, {
      headers: {
        "Content-Type": response.headers.get("Content-Type") ?? "application/octet-stream",
        "Content-Disposition": `attachment; filename="${name}"`,
      },
    });
  }, "Artifact fetch failed");
}

