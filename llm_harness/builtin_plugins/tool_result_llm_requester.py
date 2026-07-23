from __future__ import annotations

from typing import Any

from llm_harness.builtin_plugins.model_choice import model_choice_for
from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import LlmRunRequested, ToolCallRequested, ToolMessageCreated, new_run_id


class ToolResultLlmRequesterPlugin(EventConsumer):
    name = "tool-result-llm-requester"
    subscriber = "plugin:tool-result-llm-requester"
    event_filter = EventFilter(names=frozenset({ToolMessageCreated.name}))

    def __init__(self, *, settings: Settings):
        self.settings = settings

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        request = _event_by_id(bus, event.causation_id)
        if request is None or request.name != ToolCallRequested.name:
            return
        assistant_id = request.causation_id
        if assistant_id is None:
            return
        assistant = _event_by_id(bus, assistant_id)
        if assistant is None or assistant.name != "chat.message.assistant.created":
            return
        if await self._already_requested(bus, assistant):
            return

        requests = _tool_requests_for_assistant(bus, assistant)
        if not requests:
            return
        completed_request_ids = _completed_tool_request_ids(bus, assistant)
        if any(tool_request.id not in completed_request_ids for tool_request in requests):
            return

        choice = model_choice_for(bus, assistant.session_id, self.settings)
        await bus.append_message(
            LlmRunRequested(
                session_id=assistant.tags["session"],
                provider=choice.provider,
                model=choice.model,
                run_id=new_run_id("llm"),
                toolsets=choice.toolsets,
                metadata={
                    "trigger": "tool_results_completed",
                    "assistant_message_event_id": assistant.id,
                    "tool_request_event_ids": [tool_request.id for tool_request in requests],
                    "tool_result_event_ids": sorted(completed_request_ids[tool_request.id] for tool_request in requests),
                },
            ),
            producer=self.name,
            causation_id=assistant.id,
            correlation_id=assistant.correlation_id or assistant.id,
        )

    async def _already_requested(self, bus: EventBus, assistant: EventRecord) -> bool:
        requests = bus.replay(
            EventFilter(
                names=frozenset({LlmRunRequested.name}),
                tags={"session": assistant.tags["session"]},
            )
        )
        return any(request.causation_id == assistant.id for request in requests)


def _event_by_id(bus: EventBus, event_id: int | None) -> EventRecord | None:
    if event_id is None:
        return None
    events = bus.replay(EventFilter(since_id=event_id - 1), limit=1)
    if events and events[0].id == event_id:
        return events[0]
    return None


def _tool_requests_for_assistant(bus: EventBus, assistant: EventRecord) -> list[EventRecord]:
    requests = bus.replay(
        EventFilter(
            names=frozenset({ToolCallRequested.name}),
            tags={"session": assistant.tags["session"]},
        )
    )
    return [request for request in requests if request.causation_id == assistant.id]


def _completed_tool_request_ids(bus: EventBus, assistant: EventRecord) -> dict[int, int]:
    results = bus.replay(
        EventFilter(
            names=frozenset({ToolMessageCreated.name}),
            tags={"session": assistant.tags["session"]},
        )
    )
    completed: dict[int, int] = {}
    for result in results:
        if result.causation_id is not None:
            completed[result.causation_id] = result.id
    return completed
