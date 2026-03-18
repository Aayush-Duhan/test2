import { getPythonRunStream } from "@/lib/python-execution-client";
import { withErrorHandling } from "@/lib/api-utils";

export const runtime = "nodejs";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  return withErrorHandling(async () => {
    const { id } = await params;
    const lastEventId = request.headers.get("last-event-id");
    const response = await getPythonRunStream(id, lastEventId);

    if (!response.ok || !response.body) {
      const payload = await response.json().catch(() => ({}));
      const message = (payload as { detail?: string; error?: string })?.detail
        ?? (payload as { detail?: string; error?: string })?.error
        ?? "Unable to open stream";
      return new Response(message, { status: response.status >= 500 ? 503 : response.status });
    }

    return new Response(response.body, {
      headers: {
        "Content-Type": response.headers.get("Content-Type") ?? "text/event-stream",
        "Cache-Control": response.headers.get("Cache-Control") ?? "no-cache, no-transform",
        Connection: response.headers.get("Connection") ?? "keep-alive",
        "x-vercel-ai-ui-message-stream": response.headers.get("x-vercel-ai-ui-message-stream") ?? "v1",
        "x-vercel-ai-protocol": response.headers.get("x-vercel-ai-protocol") ?? "data",
      },
    });
  }, "Unable to open stream");
}

