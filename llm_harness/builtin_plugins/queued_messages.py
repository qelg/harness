from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from llm_harness.core.events import EventBus, EventFilter, EventRecord, EventToAppend
from llm_harness.core.types import (
    QUEUE_MODE,
    LlmRunRequested,
    QueuedMessage,
    UserMessageCreated,
)

# Metadata on emitted user messages is durable bookkeeping.  Besides making
# delivery auditable, it lets consumers distinguish one atomic queue drain from
# ordinary user messages and request exactly one follow-up LLM run.
QUEUED_MESSAGE_EVENT_ID = "queued_message_event_id"
QUEUED_MESSAGE_MODE = "queued_message_mode"
QUEUED_MESSAGE_TRIGGER_EVENT_ID = "queued_message_trigger_event_id"
QUEUED_MESSAGE_REQUESTS_LLM = "queued_message_requests_llm"


def pending_queued_messages(
    bus: EventBus,
    *,
    session_id: str,
    mode: str,
) -> list[EventRecord]:
    """Return undelivered queue commands submitted during the current turn.

    The latest LLM request is the lower boundary. Consumers call this exactly
    when they would otherwise request the model again or finish the session,
    so queue commands accepted before that decision are included even when
    event-consumer processing lagged behind the tool or assistant event.
    """
    requests = bus.replay(
        EventFilter(
            names=frozenset({LlmRunRequested.name}),
            tags={"session": session_id},
        )
    )
    after_id = requests[-1].id if requests else None
    queued = bus.replay(
        EventFilter(
            since_id=after_id,
            names=frozenset({QueuedMessage.name}),
            tags={"session": session_id, QUEUE_MODE: mode},
        )
    )
    return [event for event in queued if not queued_message_was_delivered(bus, event)]


def queued_message_was_delivered(bus: EventBus, queued: EventRecord) -> bool:
    return bool(
        bus.replay(
            EventFilter(
                names=frozenset({UserMessageCreated.name}),
                tags={"session": queued.tags["session"]},
                causation_id=queued.id,
            ),
            limit=1,
        )
    )


def queued_delivery_events(
    queued: Iterable[EventRecord],
    *,
    producer: str,
    trigger_event_id: int,
    correlation_id: int,
    request_llm_from_last: bool = True,
) -> list[EventToAppend]:
    """Build one atomic drain and optionally let its last message request a run."""
    messages = list(queued)
    events: list[EventToAppend] = []
    for index, event in enumerate(messages):
        metadata = event.payload.get("metadata")
        copied_metadata: dict[str, Any] = dict(metadata) if isinstance(metadata, dict) else {}
        copied_metadata.update(
            {
                QUEUED_MESSAGE_EVENT_ID: event.id,
                QUEUED_MESSAGE_MODE: event.payload.get("mode") or event.tags.get(QUEUE_MODE),
                QUEUED_MESSAGE_TRIGGER_EVENT_ID: trigger_event_id,
                QUEUED_MESSAGE_REQUESTS_LLM: (
                    request_llm_from_last and index == len(messages) - 1
                ),
            }
        )
        message = UserMessageCreated(
            session_id=event.tags["session"],
            content=str(event.payload.get("content", "")),
            metadata=copied_metadata,
        )
        events.append(
            EventToAppend(
                name=message.name,
                payload=message.payload(),
                tags=message.tags(),
                producer=producer,
                causation_id=event.id,
                correlation_id=correlation_id,
            )
        )
    return events


def trigger_already_delivered_queue(
    bus: EventBus,
    *,
    session_id: str,
    producer: str,
    trigger_event_id: int,
) -> bool:
    """Whether a previous atomic drain handled this workflow boundary."""
    delivered = bus.replay(
        EventFilter(
            names=frozenset({UserMessageCreated.name}),
            tags={"session": session_id},
            producer=producer,
        )
    )
    return any(
        isinstance(event.payload.get("metadata"), dict)
        and event.payload["metadata"].get(QUEUED_MESSAGE_TRIGGER_EVENT_ID)
        == trigger_event_id
        for event in delivered
    )
