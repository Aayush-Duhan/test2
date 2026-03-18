import { UI_MESSAGE_STREAM_HEADERS } from "ai";
import { isActive } from "@/lib/chat-helpers";
import { getPythonRun, getPythonRunStream } from "@/lib/python-execution-client";

const encoder = new TextEncoder();

const TRANSIENT_TYPES = new Set([
  "data-run-sync",
  "data-run-status",
  "data-step-status",
  "data-terminal-progress",
]);

function parsePartIndex(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return Math.floor(parsed);
}

async function readErrorMessage(response: Response, fallbackMessage: string): Promise<string> {
  try {
    const payload = await response.json() as { detail?: unknown; error?: unknown };
    const message = payload.detail ?? payload.error;
    if (typeof message === "string" && message.length > 0) {
      return message;
    }
  } catch {
    // ignore malformed JSON
  }

  try {
    const text = await response.text();
    if (text.trim().length > 0) {
      return text.trim();
    }
  } catch {
    // ignore empty body
  }

  return fallbackMessage;
}

function encodeData(payload: unknown): Uint8Array {
  return encoder.encode(`data: ${JSON.stringify(payload)}\n\n`);
}

function encodeDone(): Uint8Array {
  return encoder.encode("data: [DONE]\n\n");
}

function normalizeStreamPart(payload: unknown): Record<string, unknown> | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const rawPart = payload as Record<string, unknown>;
  if (typeof rawPart.type !== "string") {
    return null;
  }

  const normalized = Object.fromEntries(
    Object.entries(rawPart).filter(([key]) => key !== "ts"),
  );

  if (TRANSIENT_TYPES.has(rawPart.type)) {
    normalized.transient = true;
  }

  if (rawPart.type === "tool-input-start" || rawPart.type === "tool-input-available") {
    normalized.dynamic = true;
  }

  return normalized;
}

type RunSnapshot = {
  status?: string;
  streamParts?: unknown[];
};

export async function createRunChatStreamResponse({
  runId,
  fromPartIndex,
}: {
  runId: string;
  fromPartIndex: number;
}): Promise<Response> {
  const snapshotResponse = await getPythonRun(runId);
  if (!snapshotResponse.ok) {
    const message = await readErrorMessage(snapshotResponse, "Unable to load run");
    const status = snapshotResponse.status >= 500 ? 503 : snapshotResponse.status;
    return new Response(message, { status });
  }

  const snapshot = await snapshotResponse.json() as RunSnapshot;
  const nextPartIndex = parsePartIndex(fromPartIndex);
  const knownPartCount = Array.isArray(snapshot.streamParts) ? snapshot.streamParts.length : 0;

  if (!isActive(snapshot.status ?? "idle") && nextPartIndex >= knownPartCount) {
    return new Response(null, { status: 204 });
  }

  const upstreamResponse = await getPythonRunStream(
    runId,
    nextPartIndex > 0 ? String(nextPartIndex - 1) : null,
  );

  if (!upstreamResponse.ok || !upstreamResponse.body) {
    const message = await readErrorMessage(upstreamResponse, "Unable to open chat stream");
    const status = upstreamResponse.status >= 500 ? 503 : upstreamResponse.status;
    return new Response(message, { status });
  }

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const reader = upstreamResponse.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      const flushEvent = async (rawEvent: string): Promise<boolean> => {
        if (!rawEvent.trim()) {
          return false;
        }

        let eventId: number | null = null;
        const dataLines: string[] = [];

        for (const line of rawEvent.split(/\r?\n/)) {
          if (!line || line.startsWith(":")) {
            continue;
          }

          if (line.startsWith("id:")) {
            const parsedId = Number(line.slice(3).trim());
            if (Number.isFinite(parsedId)) {
              eventId = parsedId;
            }
            continue;
          }

          if (line.startsWith("data:")) {
            dataLines.push(line.slice(5).trimStart());
          }
        }

        if (dataLines.length === 0) {
          return false;
        }

        const payloadText = dataLines.join("\n");
        if (payloadText === "[DONE]") {
          controller.enqueue(encodeDone());
          return true;
        }

        let payload: unknown;
        try {
          payload = JSON.parse(payloadText);
        } catch {
          return false;
        }

        const normalizedPart = normalizeStreamPart(payload);
        if (!normalizedPart) {
          return false;
        }

        controller.enqueue(encodeData(normalizedPart));

        if (eventId !== null) {
          controller.enqueue(encodeData({
            type: "data-stream-cursor",
            data: { nextPartIndex: eventId + 1 },
            transient: true,
          }));
        }

        return normalizedPart.type === "finish";
      };

      try {
        while (true) {
          const { done, value } = await reader.read();
          buffer += decoder.decode(value, { stream: !done });

          let separatorIndex = buffer.indexOf("\n\n");
          while (separatorIndex >= 0) {
            const rawEvent = buffer.slice(0, separatorIndex);
            buffer = buffer.slice(separatorIndex + 2);

            if (await flushEvent(rawEvent)) {
              controller.enqueue(encodeDone());
              controller.close();
              await reader.cancel();
              return;
            }

            separatorIndex = buffer.indexOf("\n\n");
          }

          if (done) {
            break;
          }
        }

        if (buffer.trim().length > 0 && await flushEvent(buffer)) {
          controller.enqueue(encodeDone());
          controller.close();
          await reader.cancel();
          return;
        }

        controller.enqueue(encodeDone());
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
    async cancel() {
      await upstreamResponse.body?.cancel();
    },
  });

  return new Response(stream, {
    headers: {
      ...UI_MESSAGE_STREAM_HEADERS,
      "cache-control": "no-cache, no-transform",
      "x-vercel-ai-protocol": "data",
    },
  });
}

export function parseRunChatCursor(value: unknown): number {
  return parsePartIndex(value);
}
