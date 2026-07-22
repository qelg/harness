from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import AsyncIterator, Sequence

import httpx

from llm_harness.auth_plugins.token_store import CodexOAuthTokenStore
from llm_harness.config import Settings
from llm_harness.core.types import Message, ProviderStreamEvent, ToolSpec

DEFAULT_CODEX_INSTRUCTIONS = "You are a helpful coding assistant."
logger = logging.getLogger(__name__)


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
        if self.settings.log_provider_events:
            logger.info(
                "chatgpt-codex request model=%s messages=%d tools=%s",
                model,
                len(messages),
                [tool.name for tool in tools],
            )
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
                accumulator = _ResponsesAccumulator()
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data = line.removeprefix("data: ").strip()
                    if data == "[DONE]":
                        break
                    event = json.loads(data)
                    accumulator.add(event)
                    if self.settings.log_provider_events:
                        logger.info("chatgpt-codex event %s", json.dumps(event, sort_keys=True))
                    content = _content_from_event(event)
                    if content:
                        yield ProviderStreamEvent(type="delta", delta=content, raw_event=event)
                yield ProviderStreamEvent(
                    type="completed",
                    response=accumulator.response(include_stream_events=self.settings.log_provider_events),
                )


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


class _ResponsesAccumulator:
    def __init__(self) -> None:
        self.raw_events: list[dict] = []
        self.response_data: dict = {}
        self.output_by_index: dict[int, dict] = {}

    def add(self, event: dict) -> None:
        self.raw_events.append(event)
        if isinstance(event.get("response"), dict):
            _deep_update(self.response_data, event["response"])
            for index, item in enumerate(event["response"].get("output") or []):
                self._merge_output_item(index, item)

        output_index = event.get("output_index")
        if isinstance(output_index, int):
            item = event.get("item")
            if isinstance(item, dict):
                self._merge_output_item(output_index, item)
            self._merge_content_part(event, output_index)
            self._merge_output_text(event, output_index)
            self._merge_function_arguments(event, output_index)

    def _merge_output_item(self, output_index: int, item: dict) -> dict:
        current = self.output_by_index.setdefault(output_index, {})
        _deep_update(current, item)
        return current

    def _merge_content_part(self, event: dict, output_index: int) -> None:
        part = event.get("part")
        content_index = event.get("content_index")
        if not isinstance(part, dict) or not isinstance(content_index, int):
            return
        item = self.output_by_index.setdefault(output_index, {"content": []})
        content = item.setdefault("content", [])
        while len(content) <= content_index:
            content.append({})
        _deep_update(content[content_index], part)

    def _merge_output_text(self, event: dict, output_index: int) -> None:
        event_type = event.get("type")
        if event_type not in {"response.output_text.delta", "response.output_text.done"}:
            return
        content_index = event.get("content_index")
        if not isinstance(content_index, int):
            content_index = 0
        item = self.output_by_index.setdefault(output_index, {"type": "message", "content": []})
        content = item.setdefault("content", [])
        while len(content) <= content_index:
            content.append({"type": "output_text", "text": ""})
        part = content[content_index]
        part.setdefault("type", "output_text")
        if event_type == "response.output_text.delta":
            part["text"] = part.get("text", "") + str(event.get("delta", ""))
        elif event.get("text") is not None:
            part["text"] = event["text"]

    def _merge_function_arguments(self, event: dict, output_index: int) -> None:
        event_type = event.get("type")
        if event_type not in {"response.function_call_arguments.delta", "response.function_call_arguments.done"}:
            return
        item = self.output_by_index.setdefault(output_index, {"type": "function_call"})
        if event_type == "response.function_call_arguments.delta":
            item["arguments"] = item.get("arguments", "") + str(event.get("delta", ""))
        elif event.get("arguments") is not None:
            item["arguments"] = event["arguments"]

    def response(self, *, include_stream_events: bool = False) -> dict:
        combined = dict(self.response_data)
        if self.output_by_index:
            combined["output"] = [self.output_by_index[index] for index in sorted(self.output_by_index)]
        if include_stream_events:
            combined["stream_events"] = self.raw_events
        return combined


def _deep_update(target: dict, source: dict) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)
        else:
            target[key] = value
