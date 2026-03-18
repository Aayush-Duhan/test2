from __future__ import annotations

import json
import uuid
from typing import Any

from fastapi.responses import StreamingResponse


def format_sse_data(payload: dict[str, Any]) -> bytes:
    return f"data: {json.dumps(payload, separators=(',', ':'), ensure_ascii=False)}\n\n".encode("utf-8")


def format_sse_done() -> bytes:
    return b"data: [DONE]\n\n"


def patch_response_headers(response: StreamingResponse) -> StreamingResponse:
    response.headers["x-vercel-ai-ui-message-stream"] = "v1"
    response.headers["x-vercel-ai-protocol"] = "data"
    response.headers["Cache-Control"] = "no-cache, no-transform"
    response.headers["Connection"] = "keep-alive"
    response.headers["X-Accel-Buffering"] = "no"
    response.headers["Content-Type"] = "text/event-stream"
    return response


def create_start_part(message_id: str) -> dict[str, Any]:
    return {"type": "start", "messageId": message_id}


def create_text_start_part(text_id: str) -> dict[str, Any]:
    return {"type": "text-start", "id": text_id}


def create_text_delta_part(text_id: str, delta: str) -> dict[str, Any]:
    return {"type": "text-delta", "id": text_id, "delta": delta}


def create_text_end_part(text_id: str) -> dict[str, Any]:
    return {"type": "text-end", "id": text_id}


def create_reasoning_start_part(reasoning_id: str) -> dict[str, Any]:
    return {"type": "reasoning-start", "id": reasoning_id}


def create_reasoning_delta_part(reasoning_id: str, delta: str) -> dict[str, Any]:
    return {"type": "reasoning-delta", "id": reasoning_id, "delta": delta}


def create_reasoning_end_part(reasoning_id: str) -> dict[str, Any]:
    return {"type": "reasoning-end", "id": reasoning_id}


def create_tool_input_start_part(tool_call_id: str, tool_name: str, *, dynamic: bool = False) -> dict[str, Any]:
    part: dict[str, Any] = {"type": "tool-input-start", "toolCallId": tool_call_id, "toolName": tool_name}
    if dynamic:
        part["dynamic"] = True
    return part


def create_tool_input_delta_part(tool_call_id: str, input_text_delta: str) -> dict[str, Any]:
    return {"type": "tool-input-delta", "toolCallId": tool_call_id, "inputTextDelta": input_text_delta}


def create_tool_input_available_part(
    tool_call_id: str,
    tool_name: str,
    tool_input: dict[str, Any],
    *,
    dynamic: bool = False,
) -> dict[str, Any]:
    part: dict[str, Any] = {
        "type": "tool-input-available",
        "toolCallId": tool_call_id,
        "toolName": tool_name,
        "input": tool_input,
    }
    if dynamic:
        part["dynamic"] = True
    return part


def create_tool_output_available_part(tool_call_id: str, output: dict[str, Any] | str) -> dict[str, Any]:
    return {"type": "tool-output-available", "toolCallId": tool_call_id, "output": output}


def create_finish_part(message_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    if message_metadata:
        return {"type": "finish", "messageMetadata": message_metadata}
    return {"type": "finish"}


def create_abort_part(reason: str) -> dict[str, Any]:
    return {"type": "abort", "reason": reason}


def create_error_part(error_text: str) -> dict[str, Any]:
    return {"type": "error", "errorText": error_text}


def create_data_part(
    data_type: str,
    data: dict[str, Any],
    *,
    transient: bool = False,
    part_id: str | None = None,
) -> dict[str, Any]:
    part: dict[str, Any] = {"type": f"data-{data_type}", "data": data}
    if transient:
        part["transient"] = True
    if part_id:
        part["id"] = part_id
    return part


def generate_message_id() -> str:
    return f"msg-{uuid.uuid4().hex}"


def generate_text_id() -> str:
    return f"text-{uuid.uuid4().hex[:24]}"


def generate_reasoning_id() -> str:
    return f"reasoning-{uuid.uuid4().hex[:24]}"


def generate_tool_call_id() -> str:
    return f"call_{uuid.uuid4().hex[:32]}"
