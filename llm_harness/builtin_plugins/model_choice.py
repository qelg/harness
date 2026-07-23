from __future__ import annotations

from dataclasses import dataclass

from llm_harness.config import Settings
from llm_harness.core.events import EventBus, EventFilter
from llm_harness.core.types import ModelSelected


@dataclass(frozen=True)
class ModelChoice:
    provider: str
    model: str
    toolsets: tuple[str, ...]


def model_choice_for(bus: EventBus, session_id: str | None, settings: Settings) -> ModelChoice:
    selected = bus.replay(EventFilter(names=frozenset({ModelSelected.name})))
    session_choice: ModelChoice | None = None
    global_choice: ModelChoice | None = None

    for event in selected:
        choice = ModelChoice(
            provider=event.tags["provider"],
            model=event.tags["model"],
            toolsets=tuple(event.payload.get("toolsets", settings.default_toolsets)),
        )
        if session_id is not None and event.tags.get("session") == session_id:
            session_choice = choice
        elif "session" not in event.tags:
            global_choice = choice

    return session_choice or global_choice or ModelChoice(
        provider=settings.default_provider,
        model=settings.default_model,
        toolsets=settings.default_toolsets,
    )
