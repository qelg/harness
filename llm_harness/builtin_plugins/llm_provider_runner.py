from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    MESSAGE_CREATED_NAMES,
    AssistantMessageCreated,
    LlmDelta,
    LlmRunFailed,
    LlmRunStarted,
    Message,
    ProviderStreamEvent,
    Role,
    ToolSpec,
)


class LlmProviderRunnerPlugin(EventConsumer):
    name = "llm-provider-runner"
    subscriber = "plugin:llm-provider-runner"
    event_filter = EventFilter(names=frozenset({"llm.run.requested"}))

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        if await self._already_finished(bus, event):
            return

        provider_name = event.tags["provider"]
        model = event.tags["model"]
        run_id = event.tags["run"]
        session_id = event.tags["session"]
        provider = None if registry is None else registry.providers.get(provider_name)
        toolsets = tuple(event.payload.get("toolsets", ()))

        if provider is None:
            await self._fail(
                bus,
                event,
                error=f"unknown provider: {provider_name}",
                provider_name=provider_name,
                model=model,
                run_id=run_id,
                session_id=session_id,
                retryable=False,
            )
            return

        try:
            tools = _tools_for_toolsets(registry, toolsets)
        except ValueError as exc:
            await self._fail(
                bus,
                event,
                error=str(exc),
                provider_name=provider_name,
                model=model,
                run_id=run_id,
                session_id=session_id,
                retryable=False,
            )
            return

        await bus.append_message(
            LlmRunStarted(session_id=session_id, provider=provider_name, model=model, run_id=run_id),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

        content_parts: list[str] = []
        provider_response: dict[str, Any] | None = None
        try:
            messages = self._messages_for_session(bus, session_id=session_id, before_event_id=event.id)
            sequence = 0
            async for stream_event in _stream_provider_response(provider, model=model, messages=messages, tools=tools):
                if stream_event.type == "completed":
                    provider_response = stream_event.response
                    continue
                if stream_event.type != "delta" or not stream_event.delta:
                    continue
                sequence += 1
                content_parts.append(stream_event.delta)
                await bus.publish_message_transient(
                    LlmDelta(
                        session_id=session_id,
                        provider=provider_name,
                        model=model,
                        run_id=run_id,
                        delta=stream_event.delta,
                        sequence=sequence,
                    ),
                    producer=self.name,
                    causation_id=event.id,
                    correlation_id=event.correlation_id or event.id,
                )
        except Exception as exc:
            await self._fail(
                bus,
                event,
                error=str(exc),
                provider_name=provider_name,
                model=model,
                run_id=run_id,
                session_id=session_id,
                retryable=True,
            )
            return

        await bus.append_message(
            AssistantMessageCreated(
                session_id=session_id,
                content=_assistant_content(content_parts, provider_response),
                provider=provider_name,
                model=model,
                run_id=run_id,
                metadata={"provider_response": provider_response} if provider_response is not None else {},
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    async def _already_finished(self, bus: EventBus, event: EventRecord) -> bool:
        finished = bus.replay(
            EventFilter(
                names=frozenset({"chat.message.assistant.created", "llm.run.failed"}),
                tags={"session": event.tags["session"], "run": event.tags["run"]},
            )
        )
        return bool(finished)

    async def _fail(
        self,
        bus: EventBus,
        event: EventRecord,
        *,
        error: str,
        provider_name: str,
        model: str,
        run_id: str,
        session_id: str,
        retryable: bool,
    ) -> None:
        await bus.append_message(
            LlmRunFailed(
                session_id=session_id,
                provider=provider_name,
                model=model,
                run_id=run_id,
                error=error,
                retryable=retryable,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    def _messages_for_session(self, bus: EventBus, *, session_id: str, before_event_id: int) -> list[Message]:
        events = bus.replay(EventFilter(names=MESSAGE_CREATED_NAMES, tags={"session": session_id}))
        messages: list[Message] = []
        for event in events:
            if event.id >= before_event_id:
                continue
            messages.append(_message_from_event(event))
        return messages


def _message_from_event(event: EventRecord) -> Message:
    return Message(
        id=event.id,
        session_id=event.tags["session"],
        role=Role(event.tags["role"]),
        content=event.payload["content"],
        provider=event.payload.get("provider"),
        model=event.payload.get("model"),
        metadata=event.payload.get("metadata", {}),
        created_at=datetime.fromtimestamp(event.created_at_ms / 1000, timezone.utc),
    )


def _assistant_content(content_parts: list[str], provider_response: dict[str, Any] | None) -> str:
    if provider_response and provider_response.get("output") is not None:
        return json.dumps(provider_response["output"], ensure_ascii=False, indent=2)
    return "".join(content_parts)


async def _stream_provider_response(provider: Any, *, model: str, messages: list[Message], tools: list[ToolSpec]):
    if hasattr(provider, "stream_response"):
        async for event in provider.stream_response(model=model, messages=messages, tools=tools):
            yield event
        return
    async for delta in provider.stream_chat(model=model, messages=messages, tools=tools):
        yield ProviderStreamEvent(type="delta", delta=delta)
    yield ProviderStreamEvent(type="completed", response=None)


def _tools_for_toolsets(registry: Any, toolsets: tuple[str, ...]) -> list[ToolSpec]:
    if registry is None:
        return []

    tools: list[ToolSpec] = []
    seen: set[str] = set()
    for toolset_name in toolsets:
        toolset = registry.toolsets.get(toolset_name)
        if toolset is None:
            raise ValueError(f"unknown toolset: {toolset_name}")
        for tool in toolset.tools(registry=registry):
            if tool.name in seen:
                continue
            seen.add(tool.name)
            tools.append(tool)
    return tools
