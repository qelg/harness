from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import AssistantMessageCreated, LlmRunRequested, new_run_id

SERVER_OVERLOADED_ERROR_CODE = "server_is_overloaded"
SERVER_ERROR_CODE = "server_error"
RETRYABLE_PROVIDER_ERROR_CODES = frozenset(
    {SERVER_OVERLOADED_ERROR_CODE, SERVER_ERROR_CODE}
)


class ServerOverloadedRetryPlugin(EventConsumer):
    """Retry completed provider responses that report transient server errors."""

    # Keep this identity stable so deployments retain the durable event cursor.

    name = "server-overloaded-retry"
    subscriber = "plugin:server-overloaded-retry"
    event_filter = EventFilter(names=frozenset({AssistantMessageCreated.name}))

    def __init__(
        self,
        *,
        initial_delay_seconds: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if initial_delay_seconds < 0:
            raise ValueError("initial_delay_seconds must not be negative")
        self.initial_delay_seconds = initial_delay_seconds
        self.sleep = sleep

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        error_code = _provider_error_code(event)
        if error_code not in RETRYABLE_PROVIDER_ERROR_CODES:
            return
        if self._already_retried(bus, event):
            return

        previous_request = _event_by_id(bus, event.causation_id)
        retry_attempt = _next_retry_attempt(previous_request)
        delay_seconds = self.initial_delay_seconds * (2 ** (retry_attempt - 1))
        await self.sleep(delay_seconds)

        # Check again after sleeping so replay or another worker cannot create a
        # duplicate retry while this consumer is waiting.
        if self._already_retried(bus, event):
            return

        request_payload = previous_request.payload if previous_request is not None else {}
        previous_metadata = request_payload.get("metadata")
        metadata = dict(previous_metadata) if isinstance(previous_metadata, dict) else {}
        metadata.update(
            {
                "trigger": error_code,
                "retry_attempt": retry_attempt,
                "retry_delay_seconds": delay_seconds,
                "previous_run_id": event.tags["run"],
                "assistant_message_event_id": event.id,
            }
        )

        toolsets = request_payload.get("toolsets", ())
        if not isinstance(toolsets, (list, tuple)) or not all(isinstance(item, str) for item in toolsets):
            toolsets = ()
        user_message_event_id = request_payload.get("user_message_event_id")
        if not isinstance(user_message_event_id, int):
            user_message_event_id = None

        await bus.append_message(
            LlmRunRequested(
                session_id=event.tags["session"],
                provider=event.tags["provider"],
                model=event.tags["model"],
                run_id=new_run_id("llm"),
                toolsets=tuple(toolsets),
                user_message_event_id=user_message_event_id,
                metadata=metadata,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    def _already_retried(self, bus: EventBus, event: EventRecord) -> bool:
        requests = bus.replay(
            EventFilter(
                names=frozenset({LlmRunRequested.name}),
                tags={"session": event.tags["session"]},
            )
        )
        return any(request.causation_id == event.id and request.producer == self.name for request in requests)


def _provider_error_code(event: EventRecord) -> str | None:
    metadata = event.payload.get("metadata")
    if not isinstance(metadata, dict):
        return None
    provider_response = metadata.get("provider_response")
    if not isinstance(provider_response, dict):
        return None
    error = provider_response.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    return code if isinstance(code, str) else None


def _event_by_id(bus: EventBus, event_id: int | None) -> EventRecord | None:
    if event_id is None:
        return None
    events = bus.replay(EventFilter(since_id=event_id - 1), limit=1)
    if events and events[0].id == event_id:
        return events[0]
    return None


def _next_retry_attempt(previous_request: EventRecord | None) -> int:
    if previous_request is None or previous_request.name != LlmRunRequested.name:
        return 1
    metadata = previous_request.payload.get("metadata")
    if not isinstance(metadata, dict):
        return 1
    previous_attempt = metadata.get("retry_attempt")
    if not isinstance(previous_attempt, int) or isinstance(previous_attempt, bool) or previous_attempt < 0:
        return 1
    return previous_attempt + 1
