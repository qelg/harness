from __future__ import annotations

import asyncio
from typing import Any

from llm_harness.builtin_plugins.model_choice import model_choice_for
from llm_harness.builtin_plugins.queued_messages import (
    pending_queued_messages,
    queued_delivery_events,
)
from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    LlmRunRequested,
    QUEUE_AFTER_TOOL,
    ToolCallRequested,
    ToolMessageCreated,
    new_run_id,
)


class ToolResultLlmRequesterPlugin(EventConsumer):
    name = "tool-result-llm-requester"
    subscriber = "plugin:tool-result-llm-requester"
    event_filter = EventFilter(names=frozenset({ToolMessageCreated.name}))

    def __init__(self, *, settings: Settings):
        self.settings = settings
        # Parallel tool completions for one assistant must produce one and
        # only one follow-up run.  Event acknowledgements are ordered, but
        # callbacks are intentionally allowed to run concurrently.
        self._mutation_lock = asyncio.Lock()

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        async with self._mutation_lock:
            await self._process_event_locked(bus, event)

    async def _process_event_locked(self, bus: EventBus, event: EventRecord) -> None:
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
        completed_request_ids = _completed_tool_request_ids(bus, requests)

        if any(tool_request.id not in completed_request_ids for tool_request in requests):
            return

        # This is the point where the requester would otherwise call the model.
        # Insert all steering messages accepted since the previous LLM request
        # immediately before the one follow-up request.
        queued = pending_queued_messages(
            bus,
            session_id=assistant.tags["session"],
            mode=QUEUE_AFTER_TOOL,
        )
        if queued:
            await bus.append_batch(
                queued_delivery_events(
                    queued,
                    producer=self.name,
                    trigger_event_id=event.id,
                    correlation_id=assistant.correlation_id or assistant.id,
                    request_llm_from_last=False,
                )
            )

        choice = model_choice_for(bus, assistant.session_id, self.settings)
        await bus.append_message(
            LlmRunRequested(
                session_id=assistant.tags["session"],
                provider=choice.provider,
                model=choice.model,
                run_id=new_run_id("llm"),
                toolsets=choice.toolsets,
                thinking_level=choice.thinking_level,
                reasoning_summary=choice.reasoning_summary,
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
        return bool(bus.replay(
            EventFilter(
                names=frozenset({LlmRunRequested.name}),
                tags={"session": assistant.tags["session"]},
                causation_id=assistant.id,
            ),
            limit=1,
        ))


def _event_by_id(bus: EventBus, event_id: int | None) -> EventRecord | None:
    if event_id is None:
        return None
    return bus.get_event(event_id)


def _tool_requests_for_assistant(bus: EventBus, assistant: EventRecord) -> list[EventRecord]:
    requests = bus.replay(
        EventFilter(
            names=frozenset({ToolCallRequested.name}),
            tags={"session": assistant.tags["session"]},
        )
    )
    return [request for request in requests if request.causation_id == assistant.id]


def _completed_tool_request_ids(
    bus: EventBus, requests: list[EventRecord]
) -> dict[int, int]:
    """Return only results that exactly match a request from this assistant."""
    if not requests:
        return {}
    expected = {request.id: request for request in requests}
    session_id = requests[0].tags["session"]
    results = bus.replay(
        EventFilter(
            names=frozenset({ToolMessageCreated.name}),
            tags={"session": session_id},
        )
    )
    completed: dict[int, int] = {}
    for result in results:
        request = expected.get(result.causation_id)
        if request is None:
            continue
        if (
            result.tags.get("tool") != request.tags.get("tool")
            or result.tags.get("run") != request.tags.get("run")
        ):
            continue
        # The first valid result is the point at which this request became
        # complete. Replayed/duplicate results must not move the queue boundary
        # forward and pull a later queued command into an earlier LLM turn.
        completed.setdefault(request.id, result.id)
    return completed
