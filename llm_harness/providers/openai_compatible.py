from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Sequence

import httpx

from llm_harness.core.types import Message, ProviderStreamEvent, ToolSpec

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
        async for event in self.stream_response(model=model, messages=messages, tools=tools):
            if event.type == "delta" and event.delta:
                yield event.delta

    async def stream_response(
        self,
        *,
        model: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> AsyncIterator[ProviderStreamEvent]:
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
                chunks: list[dict] = []
                message = {"role": "assistant", "content": "", "tool_calls": []}
                usage = None
                finish_reason = None
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data = line.removeprefix("data: ").strip()
                    if data == "[DONE]":
                        break
                    chunk = json.loads(data)
                    chunks.append(chunk)
                    if self.log_provider_events:
                        logger.info("%s event %s", self.name, json.dumps(chunk, sort_keys=True))
                    choice = chunk["choices"][0]
                    finish_reason = choice.get("finish_reason") or finish_reason
                    usage = chunk.get("usage") or usage
                    delta = choice.get("delta", {})
                    content = delta.get("content")
                    if content:
                        message["content"] += content
                        yield ProviderStreamEvent(type="delta", delta=content, raw_event=chunk)
                    _merge_tool_calls(message["tool_calls"], delta.get("tool_calls") or [])
                yield ProviderStreamEvent(
                    type="completed",
                    response={
                        "object": "chat.completion",
                        "provider": self.name,
                        "model": model,
                        "message": message,
                        "usage": usage,
                        "finish_reason": finish_reason,
                        "chunks": chunks,
                    },
                )


def _openai_tool(tool: ToolSpec) -> dict:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema,
        },
    }


def _merge_tool_calls(target: list[dict], deltas: list[dict]) -> None:
    for delta in deltas:
        index = int(delta.get("index", len(target)))
        while len(target) <= index:
            target.append({"function": {"arguments": ""}})
        call = target[index]
        if delta.get("id"):
            call["id"] = delta["id"]
        if delta.get("type"):
            call["type"] = delta["type"]
        function = delta.get("function") or {}
        if function.get("name"):
            call.setdefault("function", {})["name"] = function["name"]
        if function.get("arguments"):
            call.setdefault("function", {})["arguments"] = (
                call.setdefault("function", {}).get("arguments", "") + function["arguments"]
            )
