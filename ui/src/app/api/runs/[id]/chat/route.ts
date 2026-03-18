import { sendChatMessage } from "@/lib/python-execution-client";
import { createRunChatStreamResponse, parseRunChatCursor } from "@/lib/server/run-chat-stream";

export const runtime = "nodejs";

export async function POST(req: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const body = await req.json();
    const message = typeof body?.message === "string" ? body.message.trim() : "";

    if (!message) {
      return new Response("Message is required", { status: 400 });
    }

    const response = await sendChatMessage(id, message);
    if (!response.ok) {
      const text = await response.text();
      const status = response.status >= 500 ? 503 : response.status;
      return new Response(text || "Unable to send message", { status });
    }

    return createRunChatStreamResponse({
      runId: id,
      fromPartIndex: parseRunChatCursor(body?.fromPartIndex),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to send message";
    return new Response(message, { status: 503 });
  }
}
