import { getPythonRunEvents } from "@/lib/python-execution-client";
import { withErrorHandling } from "@/lib/api-utils";

export const runtime = "nodejs";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  return withErrorHandling(async () => {
    const { id } = await params;
    const lastEventId = request.headers.get("last-event-id");
    const response = await getPythonRunEvents(id, lastEventId);

    if (!response.ok || !response.body) {
      const payload = await response.json().catch(() => ({}));
      const message = (payload as { detail?: string; error?: string })?.detail
        ?? (payload as { detail?: string; error?: string })?.error
        ?? "Unable to open stream";
      return new Response(message, { status: response.status >= 500 ? 503 : response.status });
    }

    return new Response(response.body, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
      },
    });
  }, "Unable to open stream");
}

