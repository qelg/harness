from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Sequence

import httpx

from llm_harness.core.types import Message, ToolSpec

logger = logging.getLogger(__name__)


class OpenAICompatibleProvider:
    def __init__(
        self,
        *,
        name: str,
        base_url: str,
        api_key: str | None,
        extra_headers: dict[str, str] | None = None,
        log_provider_events: bool = False,
    ) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.extra_headers = extra_headers or {}
        self.log_provider_events = log_provider_events

    async def stream_chat(
        self,
        *,
        model: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> AsyncIterator[str]:
        if not self.api_key:
            raise RuntimeError(f"missing API key for provider {self.name}")

        payload = {
            "model": model,
            "stream": True,
            "messages": [{"role": message.role.value, "content": message.content} for message in messages],
        }
        if tools:
            payload["tools"] = [_openai_tool(tool) for tool in tools]
        if self.log_provider_events:
            logger.info(
                "%s request model=%s messages=%d tools=%s",
                self.name,
                model,
                len(messages),
                [tool.name for tool in tools],
            )
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            **self.extra_headers,
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
                    chunk = json.loads(data)
                    if self.log_provider_events:
                        logger.info("%s event %s", self.name, json.dumps(chunk, sort_keys=True))
                    delta = chunk["choices"][0].get("delta", {})
                    content = delta.get("content")
                    if content:
                        yield content


def _openai_tool(tool: ToolSpec) -> dict:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema,
        },
    }
