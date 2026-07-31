from __future__ import annotations

from typing import Any

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunFailed,
    SessionStateChanged,
    UserMessageCreated,
)


class SessionStatePlugin(EventConsumer):
    """Project chat activity into durable, session-scoped state events."""

    name = "session-state"
    subscriber = "plugin:session-state"
    event_filter = EventFilter(
        names=frozenset(
            {UserMessageCreated.name, AssistantMessageCreated.name, LlmRunFailed.name}
        )
    )

    async def process_event(
        self, bus: EventBus, event: EventRecord, *, registry: Any = None
    ) -> None:
        if self._already_projected(bus, event):
            return

        if event.name == UserMessageCreated.name:
            state = "running"
            read = None
            outcome = None
        elif event.name == LlmRunFailed.name:
            state = "finished"
            read = "unread"
            outcome = "failed"
        elif _contains_tool_call(event.payload.get("content")):
            # An assistant tool request is an intermediate result. The session
            # remains running until the provider creates a final answer.
            return
        elif _is_retryable_provider_result(event):
            # The retry plugin keeps this workflow alive. Do not briefly expose
            # a finished state while its backoff timer is running.
            return
        else:
            state = "finished"
            read = "unread"
            outcome = _finish_outcome(event)

        await bus.append_message(
            SessionStateChanged(
                session_id=event.tags["session"],
                state=state,
                source_event_id=event.id,
                read=read,
                outcome=outcome,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    def _already_projected(self, bus: EventBus, event: EventRecord) -> bool:
        return bool(bus.replay(
            EventFilter(
                names=frozenset({SessionStateChanged.name}),
                tags={"session": event.tags["session"]},
                causation_id=event.id,
                producer=self.name,
            ),
            limit=1,
        ))


def _contains_tool_call(content: Any) -> bool:
    if not isinstance(content, list):
        return False
    return any(
        isinstance(item, dict)
        and (item.get("type") == "function_call" or bool(item.get("tool_calls")))
        for item in content
    )


def _finish_outcome(event: EventRecord) -> str:
    metadata = event.payload.get("metadata")
    if isinstance(metadata, dict):
        response = metadata.get("provider_response")
        if isinstance(response, dict):
            finish_reason = response.get("finish_reason")
            if isinstance(finish_reason, str) and finish_reason:
                return finish_reason
            status = response.get("status")
            if isinstance(status, str) and status:
                return status
    return "completed"


def _is_retryable_provider_result(event: EventRecord) -> bool:
    metadata = event.payload.get("metadata")
    if not isinstance(metadata, dict):
        return False
    response = metadata.get("provider_response")
    if not isinstance(response, dict):
        return False
    error = response.get("error")
    return (
        isinstance(error, dict)
        and error.get("code") in {"server_is_overloaded", "server_error"}
    )
