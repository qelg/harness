from __future__ import annotations

import json
from typing import Any

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import ToolCallRequested, new_run_id


class ToolCallRequesterPlugin(EventConsumer):
    name = "tool-call-requester"
    subscriber = "plugin:tool-call-requester"
    event_filter = EventFilter(names=frozenset({"chat.message.assistant.created"}))

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        calls = _tool_calls_from_event(event)
        if not calls:
            return
        if await self._already_requested(bus, event):
            return

        for call in calls:
            await bus.append_message(
                ToolCallRequested(
                    session_id=event.tags["session"],
                    tool=call["tool"],
                    input=call["input"],
                    run_id=call.get("run_id") or new_run_id("tool"),
                ),
                producer=self.name,
                causation_id=event.id,
                correlation_id=event.correlation_id or event.id,
            )

    async def _already_requested(self, bus: EventBus, event: EventRecord) -> bool:
        requests = bus.replay(
            EventFilter(
                names=frozenset({ToolCallRequested.name}),
                tags={"session": event.tags["session"]},
            )
        )
        return any(request.causation_id == event.id for request in requests)


def _tool_calls_from_event(event: EventRecord) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    calls.extend(_tool_calls_from_output(event.payload.get("content")))
    provider_response = event.payload.get("metadata", {}).get("provider_response")
    if isinstance(provider_response, dict):
        calls.extend(_tool_calls_from_output(provider_response.get("output")))
        message = provider_response.get("message")
        if isinstance(message, dict):
            calls.extend(_tool_calls_from_openai_tool_calls(message.get("tool_calls")))
        for choice in provider_response.get("choices") or []:
            if isinstance(choice, dict):
                calls.extend(_tool_calls_from_openai_tool_calls(choice.get("message", {}).get("tool_calls")))
    return _dedupe_calls(calls)


def _tool_calls_from_output(output: Any) -> list[dict[str, Any]]:
    if not isinstance(output, list):
        return []
    calls: list[dict[str, Any]] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "function_call":
            name = item.get("name")
            if isinstance(name, str) and name:
                calls.append(
                    {
                        "tool": name,
                        "input": _parse_arguments(item.get("arguments")),
                        "run_id": item.get("call_id") or item.get("id"),
                    }
                )
            continue
        calls.extend(_tool_calls_from_openai_tool_calls(item.get("tool_calls")))
    return calls


def _tool_calls_from_openai_tool_calls(tool_calls: Any) -> list[dict[str, Any]]:
    if not isinstance(tool_calls, list):
        return []
    calls: list[dict[str, Any]] = []
    for call in tool_calls:
        if not isinstance(call, dict):
            continue
        function = call.get("function") or {}
        name = function.get("name")
        if not isinstance(name, str) or not name:
            continue
        calls.append(
            {
                "tool": name,
                "input": _parse_arguments(function.get("arguments")),
                "run_id": call.get("id"),
            }
        )
    return calls


def _parse_arguments(arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str) and arguments.strip():
        try:
            parsed = json.loads(arguments)
        except json.JSONDecodeError:
            return {"arguments": arguments}
        if isinstance(parsed, dict):
            return parsed
        return {"arguments": parsed}
    return {}


def _dedupe_calls(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str | None]] = set()
    for call in calls:
        key = (call["tool"], json.dumps(call["input"], sort_keys=True), call.get("run_id"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(call)
    return deduped
