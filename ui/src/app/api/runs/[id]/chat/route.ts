import { sendChatMessage } from "@/lib/python-execution-client";
import { handlePythonResponse, withErrorHandling } from "@/lib/api-utils";

export const runtime = "nodejs";

interface PythonChatResponse {
  status?: string;
}

export async function POST(req: Request, { params }: { params: Promise<{ id: string }> }) {
  return withErrorHandling(async () => {
    const { id } = await params;
    const body = await req.json();
    const message = typeof body?.message === "string" ? body.message : "";
    if (!message.trim()) {
      return Response.json({ error: "Message is required" }, { status: 400 });
    }

    const response = await sendChatMessage(id, message);

    return handlePythonResponse<PythonChatResponse, { status: string }>(
      response,
      (result) => ({ status: result.status ?? "queued" }),
      "Unable to send message"
    );
  }, "Unable to send message");
}
