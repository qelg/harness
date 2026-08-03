from __future__ import annotations

import asyncio
from typing import Any

from llm_harness.builtin_plugins.queued_messages import (
    pending_queued_messages,
    queued_delivery_events,
    trigger_already_delivered_queue,
)
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunFailed,
    QUEUE_AFTER_RESPONSE,
    QUEUE_AFTER_TOOL,
    SecretAsk,
    ToolMessageCreated,
    SessionStateChanged,
    UserMessageCreated,
)


class SessionStatePlugin(EventConsumer):
    """Project chat activity into durable, session-scoped state events."""

    name = "session-state"
    subscriber = "plugin:session-state"
    event_filter = EventFilter(
        names=frozenset(
            {
                UserMessageCreated.name,
                AssistantMessageCreated.name,
                LlmRunFailed.name,
                SecretAsk.name,
                ToolMessageCreated.name,
            }
        )
    )

    def __init__(self) -> None:
        # A source event can be replayed while another callback is still
        # handling it.  Keep queue claim and idempotency checks in one critical
        # section for this event service.
        self._mutation_lock = asyncio.Lock()

    async def process_event(
        self, bus: EventBus, event: EventRecord, *, registry: Any = None
    ) -> None:
        async with self._mutation_lock:
            await self._process_event_locked(bus, event)

    async def _process_event_locked(
        self, bus: EventBus, event: EventRecord
    ) -> None:
        if event.name == ToolMessageCreated.name and event.tags.get("tool") != "retrieve-secret":
            return
        if self._already_projected(bus, event) or trigger_already_delivered_queue(
            bus,
            session_id=event.tags["session"],
            producer=self.name,
            trigger_event_id=event.id,
        ):
            return

        if event.name == UserMessageCreated.name:
            state = "running"
            read = None
            outcome = None
        elif event.name == SecretAsk.name:
            state = "secret.ask"
            read = None
            outcome = None
        elif event.name == ToolMessageCreated.name and event.tags.get("tool") == "retrieve-secret":
            state = "running"
            read = None
            outcome = None
        elif event.name == LlmRunFailed.name:
            queued = _pending_follow_up_messages(bus, event.tags["session"])
            if queued:
                await bus.append_batch(
                    queued_delivery_events(
                        queued,
                        producer=self.name,
                        trigger_event_id=event.id,
                        correlation_id=event.correlation_id or event.id,
                    )
                )
                return
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
            queued = _pending_follow_up_messages(bus, event.tags["session"])
            if queued:
                await bus.append_batch(
                    queued_delivery_events(
                        queued,
                        producer=self.name,
                        trigger_event_id=event.id,
                        correlation_id=event.correlation_id or event.id,
                    )
                )
                return
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


def _pending_follow_up_messages(
    bus: EventBus, session_id: str
) -> list[EventRecord]:
    """Return all queued input that can continue after a terminal run.

    An ``after_tool`` command normally gets released by the tool-result
    requester. If the model finishes without producing another tool call, that
    requester is never invoked, so the session-state fallback must release it
    here as well. Do not apply the latest-request lower boundary at this
    fallback: both queue modes are explicitly still unsent.
    """
    queued = [
        *pending_queued_messages(
            bus,
            session_id=session_id,
            mode=QUEUE_AFTER_RESPONSE,
            after_latest_request=False,
        ),
        *pending_queued_messages(
            bus,
            session_id=session_id,
            mode=QUEUE_AFTER_TOOL,
            after_latest_request=False,
        ),
    ]
    return sorted(queued, key=lambda event: event.id)


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
