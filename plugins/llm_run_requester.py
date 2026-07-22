from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import LlmRunRequested, ModelSelected, new_run_id


@dataclass(frozen=True)
class ModelChoice:
    provider: str
    model: str


class LlmRunRequesterPlugin(EventConsumer):
    name = "llm-run-requester"
    subscriber = "plugin:llm-run-requester"
    event_filter = EventFilter(names=frozenset({"chat.message.user.created"}))

    def __init__(self, *, settings: Settings):
        self.settings = settings

    async def process_event(self, bus: EventBus, event: EventRecord, *, store: Any = None, registry: Any = None) -> None:
        if await self._already_requested(bus, event):
            return

        choice = self._model_choice_for(bus, event.session_id)
        await bus.append_message(
            LlmRunRequested(
                session_id=event.tags["session"],
                provider=choice.provider,
                model=choice.model,
                run_id=new_run_id("llm"),
                user_message_event_id=event.id,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    async def _already_requested(self, bus: EventBus, event: EventRecord) -> bool:
        requests = bus.replay(
            EventFilter(
                names=frozenset({"llm.run.requested"}),
                tags={"session": event.tags["session"]},
            )
        )
        return any(request.causation_id == event.id for request in requests)

    def _model_choice_for(self, bus: EventBus, session_id: str | None) -> ModelChoice:
        selected = bus.replay(EventFilter(names=frozenset({ModelSelected.name})))
        session_choice: ModelChoice | None = None
        global_choice: ModelChoice | None = None

        for event in selected:
            choice = ModelChoice(provider=event.tags["provider"], model=event.tags["model"])
            if session_id is not None and event.tags.get("session") == session_id:
                session_choice = choice
            elif "session" not in event.tags:
                global_choice = choice

        return session_choice or global_choice or ModelChoice(
            provider=self.settings.default_provider,
            model=self.settings.default_model,
        )
