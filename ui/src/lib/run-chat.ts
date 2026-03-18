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

function parseToolOutput(content: string): unknown {
  try {
    return JSON.parse(content);
  } catch {
    return content;
  }
}

function parseStatementIndex(content: string): number | undefined {
  const match = content.match(/Stmt\s+(\d+)/i);
  if (!match) {
    return undefined;
  }
  const parsed = Number(match[1]);
  return Number.isFinite(parsed) ? parsed - 1 : undefined;
}

export function hydrateUiMessagesFromSnapshotMessages(messages: unknown[]): RunUiMessage[] {
  const transcript: RunUiMessage[] = [];

  for (const rawMessage of messages) {
    if (!isChatMessage(rawMessage)) {
      continue;
    }

    if (rawMessage.role === "user" && rawMessage.kind === "user_input") {
      transcript.push(createTextMessage(rawMessage.id, "user", rawMessage.content));
      continue;
    }

    if (rawMessage.kind === "agent_response") {
      transcript.push(createTextMessage(rawMessage.id, "assistant", rawMessage.content));
      continue;
    }

    if (rawMessage.kind === "tool_result") {
      const toolOutput = parseToolOutput(rawMessage.content);
      const toolName =
        toolOutput && typeof toolOutput === "object" && !Array.isArray(toolOutput) && typeof (toolOutput as { tool?: unknown }).tool === "string"
          ? (toolOutput as { tool: string }).tool
          : "tool";

      transcript.push({
        id: rawMessage.id,
        role: "assistant",
        parts: [{
          type: "dynamic-tool",
          toolCallId: `${rawMessage.id}-tool`,
          toolName,
          state: "output-available",
          input: undefined,
          output: toolOutput,
        }],
      });
      continue;
    }

    if (rawMessage.kind === "sql_statement") {
      transcript.push({
        id: rawMessage.id,
        role: "assistant",
        parts: [{
          type: "data-sql-statement",
          data: {
            statement: rawMessage.sql?.statement,
            statementIndex: parseStatementIndex(rawMessage.content),
            outputPreview: rawMessage.sql?.output ? [rawMessage.sql.output] : [],
          },
        }],
      });
      continue;
    }

    if (rawMessage.kind === "sql_error") {
      transcript.push({
        id: rawMessage.id,
        role: "assistant",
        parts: [{
          type: "data-sql-error",
          data: {
            errorType: rawMessage.content,
            errorMessage: rawMessage.sql?.error,
            failedStatement: rawMessage.sql?.failedStatement,
            failedStatementIndex: parseStatementIndex(rawMessage.content),
          },
        }],
      });
    }
  }

  return transcript;
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

    const parsedText = (parsedAssistantText.get(message.id) ?? getUiMessageText(message)).trim();
    if (parsedText.length > 0) {
      transcript.push({
        id: createChatMessageId(message.id, "assistant"),
        role: "agent",
        kind: "agent_response",
        content: parsedText,
      });
    }

    message.parts.forEach((part, index) => {
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
  }

  return transcript;
}
