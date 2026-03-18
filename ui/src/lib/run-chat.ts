import type { ChatMessage, ExecuteErrorEvent, ExecuteStatementEvent, RunUiMessage } from "@/lib/chat-types";
import { makeSqlErrorMessage, makeSqlStatementMessage } from "@/lib/chat-helpers";

type ToolPartRecord = {
  type: string;
  toolCallId?: string;
  toolName?: string;
  state?: string;
  output?: unknown;
  errorText?: string;
};

export function getUiMessageText(message: Pick<RunUiMessage, "parts">): string {
  return message.parts
    .map((part) => (part.type === "text" && typeof part.text === "string" ? part.text : ""))
    .join("");
}

export function getUiMessageReasoningText(message: Pick<RunUiMessage, "parts">): string {
  return message.parts
    .map((part) => (part.type === "reasoning" && typeof part.text === "string" ? part.text : ""))
    .join("");
}

export function getLatestUserMessageText(messages: RunUiMessage[]): string {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message.role !== "user") continue;
    const text = getUiMessageText(message).trim();
    if (text.length > 0) {
      return text;
    }
  }
  return "";
}

function isChatMessage(value: unknown): value is ChatMessage {
  if (!value || typeof value !== "object") return false;
  const row = value as Record<string, unknown>;
  return (
    typeof row.id === "string" &&
    typeof row.role === "string" &&
    typeof row.kind === "string" &&
    typeof row.content === "string"
  );
}

function createTextMessage(id: string, role: "user" | "assistant", content: string): RunUiMessage {
  return {
    id,
    role,
    parts: [{ type: "text", text: content, state: "done" }],
  };
}

/* ── Stream-part replay ──────────────────────────────────────
 *
 * Replays stored streamParts[] into RunUiMessage[] for hydration on
 * page refresh. This replaces the old per-kind snapshot-message approach
 * and is lossless: every part type the backend emits is preserved
 * without requiring explicit per-kind handling.
 *
 * User messages are sourced from the snapshot messages[] array since
 * they are not emitted as stream parts.
 */

type ReplayToolAccum = {
  toolName: string;
  input: unknown;
  output: unknown;
};

type ReplayMessageState = {
  msg: RunUiMessage;
  textAccum: Map<string, string>;
  reasoningAccum: Map<string, string>;
  toolAccum: Map<string, ReplayToolAccum>;
};

function ensureAssistantMessage(
  state: { current: ReplayMessageState | null; lastAssistant: ReplayMessageState | null },
  messages: RunUiMessage[],
): ReplayMessageState {
  if (state.current) return state.current;
  if (state.lastAssistant) return state.lastAssistant;

  // Create an implicit assistant message for orphaned parts
  const implicit: ReplayMessageState = {
    msg: { id: `replay-${messages.length}`, role: "assistant", parts: [] },
    textAccum: new Map(),
    reasoningAccum: new Map(),
    toolAccum: new Map(),
  };
  messages.push(implicit.msg);
  state.current = implicit;
  state.lastAssistant = implicit;
  return implicit;
}

export function replayStreamPartsToUiMessages(
  streamParts: unknown[],
  snapshotMessages: unknown[],
): RunUiMessage[] {
  // ── 1. Collect user messages from snapshot (in order) ────────
  const userMessages: { id: string; content: string; index: number }[] = [];
  for (let i = 0; i < snapshotMessages.length; i++) {
    const raw = snapshotMessages[i];
    if (!isChatMessage(raw)) continue;
    if (raw.role === "user" && raw.kind === "user_input") {
      userMessages.push({ id: raw.id, content: raw.content, index: i });
    }
  }

  // ── 2. Collect start-part messageIds with their stream order ──
  //    Used to interleave user messages at correct positions.
  const startPartIds: string[] = [];
  for (const raw of streamParts) {
    const part = raw as Record<string, unknown>;
    if (part.type === "start" && typeof part.messageId === "string") {
      startPartIds.push(part.messageId);
    }
  }

  // ── 3. Map each user message to the assistant message that follows it
  //    in the snapshot ordering. We use snapshot message IDs to find the
  //    assistant messageId that comes after each user message.
  const userInsertBefore = new Map<number, typeof userMessages>();
  let startIdx = 0;
  for (const user of userMessages) {
    // Find the next assistant snapshot message after this user message
    let nextAssistantMsgId: string | null = null;
    for (let j = user.index + 1; j < snapshotMessages.length; j++) {
      const raw = snapshotMessages[j];
      if (!isChatMessage(raw)) continue;
      if (raw.role !== "user") {
        nextAssistantMsgId = raw.id;
        break;
      }
    }

    // Find which start-part index this corresponds to
    let insertAt = startPartIds.length; // default: end
    if (nextAssistantMsgId) {
      for (let k = startIdx; k < startPartIds.length; k++) {
        if (startPartIds[k] === nextAssistantMsgId) {
          insertAt = k;
          startIdx = k;
          break;
        }
      }
    }

    const bucket = userInsertBefore.get(insertAt) ?? [];
    bucket.push(user);
    userInsertBefore.set(insertAt, bucket);
  }

  // ── 4. Replay stream parts into assistant UIMessages ─────────
  const assistantMessages: RunUiMessage[] = [];
  const state: {
    current: ReplayMessageState | null;
    lastAssistant: ReplayMessageState | null;
  } = { current: null, lastAssistant: null };

  for (const raw of streamParts) {
    const part = raw as Record<string, unknown>;
    const type = part.type as string;

    // Skip transient metadata parts (run status, step status, terminal, cursor)
    if (part.transient === true) continue;

    switch (type) {
      case "start": {
        const msgState: ReplayMessageState = {
          msg: { id: part.messageId as string, role: "assistant", parts: [] },
          textAccum: new Map(),
          reasoningAccum: new Map(),
          toolAccum: new Map(),
        };
        assistantMessages.push(msgState.msg);
        state.current = msgState;
        state.lastAssistant = msgState;
        break;
      }

      case "text-start": {
        const target = ensureAssistantMessage(state, assistantMessages);
        target.textAccum.set(part.id as string, "");
        break;
      }

      case "text-delta": {
        const target = ensureAssistantMessage(state, assistantMessages);
        const id = part.id as string;
        const existing = target.textAccum.get(id) ?? "";
        target.textAccum.set(id, existing + (part.delta as string));
        break;
      }

      case "text-end": {
        const target = ensureAssistantMessage(state, assistantMessages);
        const id = part.id as string;
        const text = target.textAccum.get(id) ?? "";
        if (text.length > 0) {
          target.msg.parts.push({ type: "text", text, state: "done" } as RunUiMessage["parts"][number]);
        }
        target.textAccum.delete(id);
        break;
      }

      case "reasoning-start": {
        const target = ensureAssistantMessage(state, assistantMessages);
        target.reasoningAccum.set(part.id as string, "");
        break;
      }

      case "reasoning-delta": {
        const target = ensureAssistantMessage(state, assistantMessages);
        const id = part.id as string;
        const existing = target.reasoningAccum.get(id) ?? "";
        target.reasoningAccum.set(id, existing + (part.delta as string));
        break;
      }

      case "reasoning-end": {
        const target = ensureAssistantMessage(state, assistantMessages);
        const id = part.id as string;
        const text = target.reasoningAccum.get(id) ?? "";
        if (text.length > 0) {
          target.msg.parts.push({ type: "reasoning", text, state: "done" } as RunUiMessage["parts"][number]);
        }
        target.reasoningAccum.delete(id);
        break;
      }

      case "tool-input-start": {
        const target = ensureAssistantMessage(state, assistantMessages);
        target.toolAccum.set(part.toolCallId as string, {
          toolName: (part.toolName as string) ?? "tool",
          input: undefined,
          output: undefined,
        });
        break;
      }

      case "tool-input-available": {
        const target = ensureAssistantMessage(state, assistantMessages);
        const tcId = part.toolCallId as string;
        const accum = target.toolAccum.get(tcId);
        if (accum) {
          accum.toolName = (part.toolName as string) ?? accum.toolName;
          accum.input = part.input;
        }
        break;
      }

      case "tool-output-available": {
        const target = ensureAssistantMessage(state, assistantMessages);
        const tcId = part.toolCallId as string;
        const accum = target.toolAccum.get(tcId);
        if (accum) {
          accum.output = part.output;
          target.msg.parts.push({
            type: "dynamic-tool",
            toolCallId: tcId,
            toolName: accum.toolName,
            state: "output-available",
            input: accum.input,
            output: accum.output,
          } as RunUiMessage["parts"][number]);
          target.toolAccum.delete(tcId);
        }
        break;
      }

      case "data-sql-statement":
      case "data-sql-error": {
        const target = ensureAssistantMessage(state, assistantMessages);
        target.msg.parts.push(part as RunUiMessage["parts"][number]);
        break;
      }

      case "finish": {
        // Flush any pending accumulators before closing this message
        if (state.current) {
          for (const [, text] of state.current.textAccum) {
            if (text.length > 0) {
              state.current.msg.parts.push({ type: "text", text, state: "done" } as RunUiMessage["parts"][number]);
            }
          }
          state.current.textAccum.clear();
          for (const [, text] of state.current.reasoningAccum) {
            if (text.length > 0) {
              state.current.msg.parts.push({ type: "reasoning", text, state: "done" } as RunUiMessage["parts"][number]);
            }
          }
          state.current.reasoningAccum.clear();
        }
        state.current = null;
        break;
      }

      // tool-input-delta, error, abort — skip (not needed for hydration)
      default:
        break;
    }
  }

  // ── 5. Interleave user messages with assistant messages ───────
  const result: RunUiMessage[] = [];
  let assistantIdx = 0;

  // Map each assistant message to its start-part index
  const assistantStartIndex = new Map<string, number>();
  for (let i = 0; i < startPartIds.length; i++) {
    assistantStartIndex.set(startPartIds[i], i);
  }

  for (let startI = 0; startI <= startPartIds.length; startI++) {
    // Insert user messages that belong before this assistant message
    const usersHere = userInsertBefore.get(startI);
    if (usersHere) {
      for (const user of usersHere) {
        result.push(createTextMessage(user.id, "user", user.content));
      }
    }

    // Insert the assistant message(s) for this start index
    while (assistantIdx < assistantMessages.length) {
      const aMsg = assistantMessages[assistantIdx];
      const aStartIdx = assistantStartIndex.get(aMsg.id);

      if (aStartIdx !== undefined && aStartIdx !== startI) break;
      if (aStartIdx === undefined && startI !== startPartIds.length) break;

      if (aMsg.parts.length > 0) {
        result.push(aMsg);
      }
      assistantIdx++;
    }
  }

  // Append any remaining assistant messages
  while (assistantIdx < assistantMessages.length) {
    const aMsg = assistantMessages[assistantIdx];
    if (aMsg.parts.length > 0) {
      result.push(aMsg);
    }
    assistantIdx++;
  }

  return result;
}


function getToolName(part: ToolPartRecord): string {
  if (typeof part.toolName === "string" && part.toolName.length > 0) {
    return part.toolName;
  }

  if (typeof part.type === "string" && part.type.startsWith("tool-")) {
    return part.type.slice(5);
  }

  return "tool";
}

function buildToolResultContent(part: ToolPartRecord): string {
  const toolName = getToolName(part);
  const basePayload =
    part.state === "output-error"
      ? { tool: toolName, error: part.errorText ?? "Tool execution failed" }
      : part.output;

  if (basePayload && typeof basePayload === "object" && !Array.isArray(basePayload)) {
    const objectPayload = basePayload as Record<string, unknown>;
    return JSON.stringify(
      "tool" in objectPayload ? objectPayload : { tool: toolName, ...objectPayload },
      null,
      2,
    );
  }

  return JSON.stringify({ tool: toolName, output: basePayload }, null, 2);
}

function createChatMessageId(messageId: string, suffix: string): string {
  return `${messageId}:${suffix}`;
}

function createAssistantTextMessage(
  messageId: string,
  partIndex: number,
  kind: "agent_response" | "agent_thinking",
  content: string,
): ChatMessage | null {
  const cleaned = content.trim();
  if (cleaned.length === 0) {
    return null;
  }

  return {
    id: createChatMessageId(messageId, `${kind}-${partIndex}`),
    role: "agent",
    kind,
    content: cleaned,
  };
}

export function convertUiMessagesToChatMessages(
  messages: RunUiMessage[],
  parsedAssistantText: ReadonlyMap<string, string>,
): ChatMessage[] {
  const transcript: ChatMessage[] = [];

  for (const message of messages) {
    if (message.role === "user") {
      const content = getUiMessageText(message).trim();
      if (content.length > 0) {
        transcript.push({
          id: createChatMessageId(message.id, "user"),
          role: "user",
          kind: "user_input",
          content,
        });
      }
      continue;
    }

    if (message.role !== "assistant") {
      continue;
    }

    const parsedText = (parsedAssistantText.get(message.id) ?? "").trim();
    const textPartCount = message.parts.filter((part) => (
      part.type === "text" &&
      typeof part.text === "string" &&
      part.text.trim().length > 0
    )).length;
    let consumedParsedText = false;
    let emittedAssistantContent = false;

    message.parts.forEach((part, index) => {
      if (part.type === "text") {
        const content = !consumedParsedText && textPartCount === 1 && parsedText.length > 0
          ? parsedText
          : part.text;
        const textMessage = createAssistantTextMessage(message.id, index, "agent_response", content);
        if (textMessage) {
          transcript.push(textMessage);
          emittedAssistantContent = true;
          if (content === parsedText) {
            consumedParsedText = true;
          }
        }
        return;
      }

      if (part.type === "reasoning") {
        const reasoningMessage = createAssistantTextMessage(message.id, index, "agent_thinking", part.text);
        if (reasoningMessage) {
          transcript.push(reasoningMessage);
          emittedAssistantContent = true;
        }
        return;
      }

      if (part.type === "dynamic-tool" || part.type.startsWith("tool-")) {
        const toolPart = part as ToolPartRecord;
        if (toolPart.state !== "output-available" && toolPart.state !== "output-error") {
          return;
        }
        transcript.push({
          id: createChatMessageId(message.id, `tool-${toolPart.toolCallId ?? index}`),
          role: "agent",
          kind: "tool_result",
          content: buildToolResultContent(toolPart),
        });
        return;
      }

      if (part.type === "data-sql-statement") {
        const sqlMessage = makeSqlStatementMessage(part.data as ExecuteStatementEvent);
        sqlMessage.id = createChatMessageId(message.id, `sql-statement-${index}`);
        transcript.push(sqlMessage);
        return;
      }

      if (part.type === "data-sql-error") {
        const sqlMessage = makeSqlErrorMessage(part.data as ExecuteErrorEvent);
        sqlMessage.id = createChatMessageId(message.id, `sql-error-${index}`);
        transcript.push(sqlMessage);
      }
    });

    if (!emittedAssistantContent && parsedText.length > 0) {
      const fallbackMessage = createAssistantTextMessage(message.id, message.parts.length, "agent_response", parsedText);
      if (fallbackMessage) {
        transcript.push(fallbackMessage);
      }
    }
  }

  return transcript;
}
