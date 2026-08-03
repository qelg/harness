from __future__ import annotations

import asyncio
from typing import Any

from llm_harness.config import Settings
from llm_harness.builtin_plugins.model_choice import model_choice_for
from llm_harness.builtin_plugins.queued_messages import QUEUED_MESSAGE_REQUESTS_LLM
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    MESSAGE_CREATED_NAMES,
    LlmRetry,
    LlmRunRequested,
    SecretAsk,
    SessionCreated,
    ToolCallRequested,
    new_run_id,
)

NO_AUTO_LLM_RUN_SESSION_TAG = "no-auto-llm-run"
RETRY_BLOCKING_EVENT_NAMES = MESSAGE_CREATED_NAMES | frozenset({
    ToolCallRequested.name,
    SecretAsk.name,
})


class LlmRunRequesterPlugin(EventConsumer):
    name = "llm-run-requester"
    subscriber = "plugin:llm-run-requester"
    event_filter = EventFilter(names=frozenset({"chat.message.user.created", LlmRetry.name}))

    def __init__(self, *, settings: Settings):
        self.settings = settings
        self._mutation_lock = asyncio.Lock()

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        async with self._mutation_lock:
            await self._process_event_locked(bus, event)

    async def _process_event_locked(self, bus: EventBus, event: EventRecord) -> None:
        if event.name == LlmRetry.name:
            if not self._retry_follows_failure(bus, event):
                return
            user_message_event_id = self._failed_request_user_message_id(bus, event)
        else:
            if self._auto_run_disabled(bus, event) or not _message_requests_llm(event):
                return
            user_message_event_id = event.id
        if await self._already_requested(bus, event):
            return

        choice = model_choice_for(bus, event.session_id, self.settings)
        await bus.append_message(
            LlmRunRequested(
                session_id=event.tags["session"],
                provider=choice.provider,
                model=choice.model,
                run_id=new_run_id("llm"),
                toolsets=choice.toolsets,
                thinking_level=choice.thinking_level,
                reasoning_summary=choice.reasoning_summary,
                user_message_event_id=user_message_event_id,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    def _retry_follows_failure(self, bus: EventBus, event: EventRecord) -> bool:
        # A retry is an explicit second initiation, but it must still belong to
        # a failed run. Session metadata changes do not invalidate the retry;
        # new conversation/tool activity does.
        failed = bus.replay(
            EventFilter(
                names=frozenset({"llm.run.failed"}),
                tags={"session": event.tags["session"]},
                before_id=event.id,
            ),
            limit=1,
            latest=True,
        )
        if not failed:
            return False
        return not bus.replay(
            EventFilter(
                names=RETRY_BLOCKING_EVENT_NAMES,
                tags={"session": event.tags["session"]},
                since_id=failed[0].id,
                before_id=event.id,
            ),
            limit=1,
        )

    def _failed_request_user_message_id(self, bus: EventBus, event: EventRecord) -> int | None:
        failed = bus.replay(
            EventFilter(
                names=frozenset({"llm.run.failed"}),
                tags={"session": event.tags["session"]},
                before_id=event.id,
            ),
            limit=1,
            latest=True,
        )
        if not failed:
            return None
        request = bus.get_event(failed[0].causation_id)
        if request is None:
            return None
        value = request.payload.get("user_message_event_id")
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    def _auto_run_disabled(self, bus: EventBus, event: EventRecord) -> bool:
        sessions = bus.replay(
            EventFilter(
                names=frozenset({SessionCreated.name}),
                tags={
                    "session": event.tags["session"],
                    f"session_tag:{NO_AUTO_LLM_RUN_SESSION_TAG}": "true",
                },
            ),
            limit=1,
        )
        return bool(sessions)

    async def _already_requested(self, bus: EventBus, event: EventRecord) -> bool:
        return bool(bus.replay(
            EventFilter(
                names=frozenset({"llm.run.requested"}),
                tags={"session": event.tags["session"]},
                causation_id=event.id,
            ),
            limit=1,
        ))


def _message_requests_llm(event: EventRecord) -> bool:
    metadata = event.payload.get("metadata")
    return not (
        isinstance(metadata, dict)
        and metadata.get(QUEUED_MESSAGE_REQUESTS_LLM) is False
    )
