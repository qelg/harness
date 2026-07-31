from __future__ import annotations

import json
from dataclasses import dataclass

from llm_harness.config import Settings
from llm_harness.core.events import EventBus, EventFilter
from llm_harness.core.types import ModelSelected


@dataclass(frozen=True)
class ModelChoice:
    provider: str
    model: str
    toolsets: tuple[str, ...]
    thinking_level: str | None = None
    reasoning_summary: bool = False


def model_choice_for(bus: EventBus, session_id: str | None, settings: Settings) -> ModelChoice:
    if bus.projections_ready and session_id is not None:
        rows = bus.conn.execute(
            "SELECT scope_key, provider, model, toolsets_json, thinking_level, reasoning_summary "
            "FROM projected_model_selections WHERE scope_key IN ('global', ?) ORDER BY scope_key",
            (f"session:{session_id}",),
        ).fetchall()
        session_row = None
        global_row = None
        for row in rows:
            if row["scope_key"] == f"session:{session_id}":
                session_row = row
            elif row["scope_key"] == "global":
                global_row = row
        if session_row is not None:
            return ModelChoice(
                provider=session_row["provider"],
                model=session_row["model"],
                toolsets=tuple(json.loads(session_row["toolsets_json"])),
                thinking_level=session_row["thinking_level"],
                reasoning_summary=bool(session_row["reasoning_summary"]),
            )
        if global_row is not None:
            return ModelChoice(
                provider=global_row["provider"],
                model=global_row["model"],
                toolsets=tuple(json.loads(global_row["toolsets_json"])),
                thinking_level=global_row["thinking_level"],
                reasoning_summary=bool(global_row["reasoning_summary"]),
            )
        return ModelChoice(
            provider=settings.default_provider,
            model=settings.default_model,
            toolsets=settings.default_toolsets,
        )
    selected = bus.replay(EventFilter(names=frozenset({ModelSelected.name})))
    session_choice: ModelChoice | None = None
    global_choice: ModelChoice | None = None

    for event in selected:
        choice = ModelChoice(
            provider=event.tags["provider"],
            model=event.tags["model"],
            toolsets=tuple(event.payload.get("toolsets", settings.default_toolsets)),
            thinking_level=event.payload.get("thinking_level"),
            reasoning_summary=event.payload.get("reasoning_summary", False),
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
