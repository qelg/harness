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
            "stream_options": {"include_usage": True},
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
                accumulator = _ChatCompletionAccumulator(provider=self.name, model=model)
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data = line.removeprefix("data: ").strip()
                    if data == "[DONE]":
                        break
                    chunk = json.loads(data)
                    accumulator.add(chunk)
                    if self.log_provider_events:
                        logger.info("%s event %s", self.name, json.dumps(chunk, sort_keys=True))
                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    delta = choices[0].get("delta", {})
                    content = delta.get("content")
                    if content:
                        yield ProviderStreamEvent(type="delta", delta=content, raw_event=chunk)
                yield ProviderStreamEvent(
                    type="completed",
                    response=accumulator.response(include_chunks=self.log_provider_events),
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


class _ChatCompletionAccumulator:
    def __init__(self, *, provider: str, model: str) -> None:
        self.provider = provider
        self.model = model
        self.chunks: list[dict] = []
        self.response_fields: dict = {}
        self.choices: dict[int, dict] = {}
        self.usage = None

    def add(self, chunk: dict) -> None:
        self.chunks.append(chunk)
        for key in ("id", "object", "created", "model", "system_fingerprint"):
            if chunk.get(key) is not None:
                self.response_fields[key] = chunk[key]
        if chunk.get("usage") is not None:
            self.usage = chunk["usage"]

        for choice_chunk in chunk.get("choices") or []:
            index = int(choice_chunk.get("index", 0))
            choice = self.choices.setdefault(index, {"index": index, "message": {"role": "assistant"}})
            if choice_chunk.get("finish_reason") is not None:
                choice["finish_reason"] = choice_chunk["finish_reason"]
            if choice_chunk.get("native_finish_reason") is not None:
                choice["native_finish_reason"] = choice_chunk["native_finish_reason"]
            self._merge_delta(choice["message"], choice_chunk.get("delta") or {})

    def _merge_delta(self, message: dict, delta: dict) -> None:
        if delta.get("role"):
            message["role"] = delta["role"]
        for key in ("content", "reasoning", "reasoning_content", "refusal"):
            if delta.get(key):
                message[key] = message.get(key, "") + delta[key]
        if delta.get("reasoning_details"):
            _merge_reasoning_details(message.setdefault("reasoning_details", []), delta["reasoning_details"])
        if delta.get("annotations"):
            message.setdefault("annotations", []).extend(delta["annotations"])
        if delta.get("audio"):
            message["audio"] = delta["audio"]
        _merge_tool_calls(message.setdefault("tool_calls", []), delta.get("tool_calls") or [])

    def response(self, *, include_chunks: bool = False) -> dict:
        choices = [self.choices[index] for index in sorted(self.choices)]
        output = [_clean_message(choice["message"]) for choice in choices]
        response = {
            "object": "chat.completion",
            "provider": self.provider,
            "model": self.model,
            **self.response_fields,
            "choices": choices,
            "output": output,
            "usage": self.usage,
        }
        if choices:
            response["message"] = output[0]
            response["finish_reason"] = choices[0].get("finish_reason")
            if choices[0].get("native_finish_reason") is not None:
                response["native_finish_reason"] = choices[0]["native_finish_reason"]
        if include_chunks:
            response["stream_chunks"] = self.chunks
        return response


def _clean_message(message: dict) -> dict:
    cleaned = dict(message)
    if cleaned.get("tool_calls") == []:
        cleaned.pop("tool_calls")
    return cleaned


def _merge_reasoning_details(target: list[dict], deltas: list[dict]) -> None:
    for delta in deltas:
        index = delta.get("index")
        if not isinstance(index, int):
            target.append(dict(delta))
            continue
        while len(target) <= index:
            target.append({})
        current = target[index]
        for key, value in delta.items():
            if key == "text" and isinstance(value, str):
                current["text"] = current.get("text", "") + value
            else:
                current[key] = value
