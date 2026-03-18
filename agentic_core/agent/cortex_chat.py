"""Snowflake Cortex REST API client — streaming & native tool calling.

Replaces the old SQL-based snowflake.cortex.complete() approach with direct
HTTP calls to the Chat Completions endpoint:

    POST https://{host}/api/v2/cortex/v1/chat/completions

Supports:
- Real token-by-token SSE streaming
- Native tool calling (OpenAI function-calling format)
- Session token auth (extracted from the Snowpark connector)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterator, Optional

from agentic_core.models.context import MigrationContext
from agentic_core.runtime.snowflake_session import get_snowflake_session

logger = logging.getLogger(__name__)

DEFAULT_AGENT_MODEL = "claude-4-sonnet"


def get_agent_model_name() -> str:
    """Resolve the model name for the agent orchestrator."""
    return (
        os.getenv("SNOWFLAKE_CORTEX_AGENT_MODEL")
        or os.getenv("SNOWFLAKE_CORTEX_MODEL")
        or os.getenv("CORTEX_MODEL")
        or DEFAULT_AGENT_MODEL
    ).strip() or DEFAULT_AGENT_MODEL


def get_cortex_session(state: MigrationContext):
    """Get a Snowpark session for Cortex calls."""
    session = get_snowflake_session(state)
    if session is None:
        raise RuntimeError(
            "Failed to create Snowflake session for agent. "
            "Check Snowflake credentials (account, user, role, warehouse)."
        )
    return session


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_rest_url_and_headers(session: Any) -> tuple[str, dict[str, str]]:
    """Extract the REST API URL and auth headers from a Snowpark session.

    Uses the connector's internal HTTP session for TLS/proxy compatibility.
    """
    server_conn = getattr(session, "_conn", None)
    conn = getattr(server_conn, "_conn", None) if server_conn else None
    if conn is None:
        raise RuntimeError("Cannot access Snowflake connector for REST API.")

    rest = getattr(conn, "rest", None)
    if rest is None:
        raise RuntimeError("Snowflake connector has no REST handler.")

    token = getattr(rest, "token", None)
    if not token:
        raise RuntimeError("No valid auth token available for Cortex REST API.")

    host = getattr(conn, "host", None)
    if not host:
        raise RuntimeError("Cannot determine Snowflake host for REST API.")

    url = f"https://{host}/api/v2/cortex/v1/chat/completions"
    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    return url, headers


def _get_http_session(session: Any):
    """Get the connector's internal requests session for making HTTP calls."""
    server_conn = getattr(session, "_conn", None)
    conn = getattr(server_conn, "_conn", None) if server_conn else None
    rest = getattr(conn, "rest", None) if conn else None
    if rest is None:
        raise RuntimeError("Snowflake connector has no REST handler.")
    return rest


def _build_request_body(
    messages: list[dict],
    *,
    model: str,
    tools: list[dict] | None = None,
    tool_choice: str | dict = "auto",
    temperature: float = 0,
    max_tokens: int = 4096,
    top_p: float | None = None,
    stream: bool = False,
) -> dict:
    """Build the JSON request body for the Chat Completions endpoint."""
    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_completion_tokens": max_tokens,
        "temperature": temperature,
    }

    if top_p is not None:
        body["top_p"] = top_p

    if tools:
        body["tools"] = tools
        body["tool_choice"] = tool_choice

    if stream:
        body["stream"] = True
        body["stream_options"] = {"include_usage": True}

    return body


# ---------------------------------------------------------------------------
# Public API — Streaming
# ---------------------------------------------------------------------------

def stream_cortex_complete(
    session: Any,
    messages: list[dict[str, str]],
    *,
    tools: list[dict] | None = None,
    tool_choice: str | dict = "auto",
    model: str | None = None,
    temperature: float = 0,
    max_tokens: int = 4096,
    top_p: float | None = None,
) -> Iterator[dict]:
    """Stream from the Cortex REST API via SSE.

    Yields event dicts of the following types:
        {"type": "content_delta", "content": str}
        {"type": "tool_call_delta", "index": int, "id": str, "name": str, "arguments": str}
        {"type": "usage", "usage": dict}
        {"type": "done", "finish_reason": str}
    """
    model_name = model or get_agent_model_name()
    url, headers = _get_rest_url_and_headers(session)
    rest = _get_http_session(session)

    body = _build_request_body(
        messages,
        model=model_name,
        tools=tools,
        tool_choice=tool_choice,
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
        stream=True,
    )

    # Track tool call accumulation across chunks
    tool_calls_acc: dict[int, dict] = {}
    finish_reason = "stop"

    with rest.use_requests_session(url) as http_session:
        resp = http_session.post(
            url, json=body, headers=headers, stream=True, timeout=120,
        )
        try:
            resp.raise_for_status()

            for raw_line in resp.iter_lines(decode_unicode=True):
                if not raw_line or not raw_line.startswith("data: "):
                    continue

                data_str = raw_line[6:]

                if data_str.strip() == "[DONE]":
                    break

                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    logger.debug("Skipping unparseable SSE line: %s", data_str[:120])
                    continue

                choices = data.get("choices")
                if isinstance(choices, list) and choices:
                    choice = choices[0]
                    delta = choice.get("delta", {})

                    # Track finish_reason
                    fr = choice.get("finish_reason")
                    if fr:
                        finish_reason = fr

                    # Content delta
                    content = delta.get("content")
                    if content:
                        yield {"type": "content_delta", "content": content}

                    # Tool call deltas
                    tc_deltas = delta.get("tool_calls")
                    if tc_deltas:
                        for tc in tc_deltas:
                            idx = tc.get("index", 0)
                            if idx not in tool_calls_acc:
                                tool_calls_acc[idx] = {
                                    "id": tc.get("id", ""),
                                    "type": "function",
                                    "function": {
                                        "name": tc.get("function", {}).get("name", ""),
                                        "arguments": "",
                                    },
                                }
                            else:
                                # Accumulate
                                if tc.get("id"):
                                    tool_calls_acc[idx]["id"] = tc["id"]
                                fn = tc.get("function", {})
                                if fn.get("name"):
                                    tool_calls_acc[idx]["function"]["name"] = fn["name"]

                            # Append argument fragment
                            arg_chunk = tc.get("function", {}).get("arguments", "")
                            if arg_chunk:
                                tool_calls_acc[idx]["function"]["arguments"] += arg_chunk
                                yield {
                                    "type": "tool_call_delta",
                                    "index": idx,
                                    "id": tool_calls_acc[idx]["id"],
                                    "name": tool_calls_acc[idx]["function"]["name"],
                                    "arguments": arg_chunk,
                                }

                # Usage info (typically in the final chunk)
                usage = data.get("usage")
                if isinstance(usage, dict) and any(v for v in usage.values() if v):
                    yield {"type": "usage", "usage": usage}

        finally:
            resp.close()

    # Yield the final assembled tool calls (if any)
    if tool_calls_acc:
        assembled = [tool_calls_acc[i] for i in sorted(tool_calls_acc.keys())]
        yield {"type": "tool_calls_complete", "tool_calls": assembled}

    yield {"type": "done", "finish_reason": finish_reason}


# ---------------------------------------------------------------------------
# Convenience — collect streaming into a single response
# ---------------------------------------------------------------------------

def stream_cortex_to_response(
    session: Any,
    messages: list[dict[str, str]],
    **kwargs,
) -> dict:
    """Stream from the Cortex REST API but collect into a single response dict.

    Same return shape as call_cortex_complete() but uses streaming internally
    for better timeout behavior on long responses.
    """
    content_parts: list[str] = []
    tool_calls: list[dict] | None = None
    usage: dict | None = None
    finish_reason = "stop"

    for event in stream_cortex_complete(session, messages, **kwargs):
        if event["type"] == "content_delta":
            content_parts.append(event["content"])
        elif event["type"] == "tool_calls_complete":
            tool_calls = event["tool_calls"]
        elif event["type"] == "usage":
            usage = event["usage"]
        elif event["type"] == "done":
            finish_reason = event.get("finish_reason", "stop")

    content = "".join(content_parts) if content_parts else None

    return {
        "content": content,
        "tool_calls": tool_calls,
        "usage": usage,
        "finish_reason": finish_reason,
        "raw": {},
    }
