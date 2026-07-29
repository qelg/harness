from __future__ import annotations

from typing import Any

from llm_harness.config import Settings
from llm_harness.builtin_plugins.model_choice import model_choice_for
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import LlmRunRequested, SessionCreated, new_run_id

NO_AUTO_LLM_RUN_SESSION_TAG = "no-auto-llm-run"


class LlmRunRequesterPlugin(EventConsumer):
    name = "llm-run-requester"
    subscriber = "plugin:llm-run-requester"
    event_filter = EventFilter(names=frozenset({"chat.message.user.created"}))

    def __init__(self, *, settings: Settings):
        self.settings = settings

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        if self._auto_run_disabled(bus, event):
            return
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
                user_message_event_id=event.id,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

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
        requests = bus.replay(
            EventFilter(
                names=frozenset({"llm.run.requested"}),
                tags={"session": event.tags["session"]},
            )
        )
        return any(request.causation_id == event.id for request in requests)
