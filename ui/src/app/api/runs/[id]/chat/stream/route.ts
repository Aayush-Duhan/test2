import { createRunChatStreamResponse, parseRunChatCursor } from "@/lib/server/run-chat-stream";

export const runtime = "nodejs";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  try {
    const { id } = await params;
    const url = new URL(request.url);
    return createRunChatStreamResponse({
      runId: id,
      fromPartIndex: parseRunChatCursor(url.searchParams.get("fromPartIndex")),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to resume chat stream";
    return new Response(message, { status: 503 });
  }
}
