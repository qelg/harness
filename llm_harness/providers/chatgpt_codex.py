from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator, Sequence

import httpx

from llm_harness.auth_plugins.token_store import CodexOAuthTokenStore
from llm_harness.config import Settings
from llm_harness.core.types import Message


class ChatGPTCodexProvider:
    name = "chatgpt-codex"

    def __init__(self, *, conn: sqlite3.Connection, settings: Settings) -> None:
        self.settings = settings
        self.base_url = settings.codex_oauth_base_url.rstrip("/")
        self.tokens = CodexOAuthTokenStore(conn=conn, settings=settings)

    async def stream_chat(self, *, model: str, messages: Sequence[Message]) -> AsyncIterator[str]:
        access_token = await self.tokens.access_token()
        payload = {
            "model": model,
            "stream": True,
            "messages": [{"role": message.role.value, "content": message.content} for message in messages],
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "User-Agent": "llm-harness/0.1.0",
        }

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data = line.removeprefix("data: ").strip()
                    if data == "[DONE]":
                        break
                    content = _content_from_chunk(json.loads(data))
                    if content:
                        yield content


def _content_from_chunk(chunk: dict) -> str | None:
    choices = chunk.get("choices") or []
    if choices:
        delta = choices[0].get("delta", {})
        return delta.get("content")

    output = chunk.get("output") or []
    for item in output:
        for part in item.get("content") or []:
            if part.get("type") in {"output_text", "text"} and part.get("text"):
                return part["text"]
    return None
