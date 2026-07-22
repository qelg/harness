from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator, Sequence

import httpx

from llm_harness.auth_plugins.token_store import CodexOAuthTokenStore
from llm_harness.config import Settings
from llm_harness.core.types import Message, ToolSpec

DEFAULT_CODEX_INSTRUCTIONS = "You are a helpful coding assistant."


class ChatGPTCodexProvider:
    name = "chatgpt-codex"

    def __init__(self, *, conn: sqlite3.Connection, settings: Settings) -> None:
        self.settings = settings
        self.base_url = settings.codex_oauth_base_url.rstrip("/")
        self.tokens = CodexOAuthTokenStore(conn=conn, settings=settings)

    async def stream_chat(
        self,
        *,
        model: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> AsyncIterator[str]:
        access_token = await self.tokens.access_token()
        payload = {
            "model": model,
            "instructions": DEFAULT_CODEX_INSTRUCTIONS,
            "stream": True,
            "input": [{"role": message.role.value, "content": message.content} for message in messages],
            "store": False,
        }
        if tools:
            payload["tools"] = [_responses_tool(tool) for tool in tools]
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "User-Agent": "llm-harness/0.1.0",
        }

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/responses",
                headers=headers,
                json=payload,
            ) as response:
                await _raise_for_status(response)
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data = line.removeprefix("data: ").strip()
                    if data == "[DONE]":
                        break
                    content = _content_from_event(json.loads(data))
                    if content:
                        yield content


def _content_from_event(event: dict) -> str | None:
    if event.get("type") == "response.output_text.delta":
        return event.get("delta")
    if event.get("type") == "response.output_text.done":
        return None

    choices = event.get("choices") or []
    if choices:
        delta = choices[0].get("delta", {})
        return delta.get("content")

    output = event.get("output") or []
    for item in output:
        for part in item.get("content") or []:
            if part.get("type") in {"output_text", "text"} and part.get("text"):
                return part["text"]
    return None


async def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
    body = (await response.aread()).decode(errors="replace")
    raise RuntimeError(f"Codex backend returned HTTP {response.status_code}: {body}")


def _responses_tool(tool: ToolSpec) -> dict:
    return {
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": tool.input_schema,
        "strict": False,
    }
