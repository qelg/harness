from __future__ import annotations

from typing import Any

from llm_harness.builtin_plugins.llm_run_requester import NO_AUTO_LLM_RUN_SESSION_TAG
from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord, EventToAppend
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunRequested,
    SessionCreated,
    SessionRenamed,
    SessionStateChanged,
    SystemMessageCreated,
    UserMessageCreated,
    new_run_id,
    new_session_id,
    to_event_parts,
)

NAMER_SESSION_TAG = "namer"
NAMER_SYSTEM_PROMPT = (
    "Summarize the entire conversation, considering all user and assistant messages, "
    "in 5-10 words. Use the summary as the conversation title. "
    "Reply solely with the title. Do not use tools."
)
NAMER_USER_INSTRUCTION = (
    "Summarize the conversation above in 5-10 words and use the summary as its title. "
    "Reply solely with the title."
)


class NamerPlugin(EventConsumer):
    """Generate short session titles from snapshots of the visible conversation."""

    name = "namer"
    subscriber = "plugin:namer"
    event_filter = EventFilter(
        names=frozenset({SessionStateChanged.name, AssistantMessageCreated.name})
    )

    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings

    async def process_event(
        self, bus: EventBus, event: EventRecord, *, registry: Any = None
    ) -> None:
        if event.name == SessionStateChanged.name:
            await self._start_namer_session(bus, event)
        else:
            await self._rename_parent(bus, event)

    async def _start_namer_session(self, bus: EventBus, event: EventRecord) -> None:
        if event.tags.get("state") not in {"running", "finished"}:
            return
        parent_session_id = event.tags["session"]
        # Derived sessions are never named recursively. The relationship tag is
        # also what keeps them out of the top-level user session list.
        if _parent_session_for(bus, parent_session_id) is not None:
            return
        if not _is_actual_state_change(bus, event):
            return
        if _namer_started_for_state(bus, event):
            return

        namer_session_id = new_session_id()
        transcript: list[str] = []
        source_event_ids: list[int] = []
        for source in bus.replay(
            EventFilter(
                names=frozenset(
                    {UserMessageCreated.name, AssistantMessageCreated.name}
                ),
                tags={"session": parent_session_id},
            )
        ):
            if source.id >= event.id:
                continue
            content = source.payload.get("content")
            if (
                source.name == AssistantMessageCreated.name
                and _contains_tool_call(content)
            ):
                continue
            text = _text_content(content).strip()
            if not text:
                continue
            role = "User" if source.name == UserMessageCreated.name else "Assistant"
            transcript.append(f"{role}: {text}")
            source_event_ids.append(source.id)

        conversation = "\n\n".join(transcript)
        user_prompt = (
            f"{conversation}\n\n{NAMER_USER_INSTRUCTION}"
            if conversation
            else NAMER_USER_INSTRUCTION
        )
        messages: list[Any] = [
            SystemMessageCreated(
                session_id=namer_session_id,
                content=NAMER_SYSTEM_PROMPT,
                metadata={"namer": True},
            ),
            UserMessageCreated(
                session_id=namer_session_id,
                content=user_prompt,
                metadata={"source_event_ids": source_event_ids},
            ),
        ]

        created = SessionCreated(
            session_id=namer_session_id,
            session_tags=(NAMER_SESSION_TAG, NO_AUTO_LLM_RUN_SESSION_TAG),
            parent_session_id=parent_session_id,
            namer=True,
        )
        request = LlmRunRequested(
            session_id=namer_session_id,
            provider=self.settings.namer_provider,
            model=self.settings.namer_model,
            run_id=new_run_id("namer"),
            toolsets=(),
            metadata={
                "namer": True,
                "parent_session": parent_session_id,
                "state_event_id": event.id,
            },
        )
        correlation_id = event.correlation_id or event.id
        pending = [created, *messages, request]
        await bus.append_batch(
            [
                EventToAppend(
                    name=name,
                    payload=payload,
                    tags=tags,
                    producer=self.name,
                    causation_id=event.id,
                    correlation_id=correlation_id,
                )
                for name, payload, tags in map(to_event_parts, pending)
            ]
        )

    async def _rename_parent(self, bus: EventBus, event: EventRecord) -> None:
        # Copied assistant messages are authored by this plugin; only the
        # provider runner's new assistant reply should rename the parent.
        if event.producer == self.name:
            return
        namer_session = _session_created(bus, event.tags["session"])
        if namer_session is None or namer_session.tags.get("namer") != "true":
            return
        parent_session_id = namer_session.tags.get("parent_session")
        if parent_session_id is None or _already_renamed_from(bus, event):
            return
        title = _text_content(event.payload.get("content")).strip()
        if not title:
            return
        await bus.append_message(
            SessionRenamed(
                session_id=parent_session_id,
                title=title,
                namer_session_id=event.tags["session"],
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )


def _session_created(bus: EventBus, session_id: str) -> EventRecord | None:
    events = bus.replay(
        EventFilter(names=frozenset({SessionCreated.name}), tags={"session": session_id}),
        limit=1,
    )
    return events[0] if events else None


def _parent_session_for(bus: EventBus, session_id: str) -> str | None:
    created = _session_created(bus, session_id)
    if created is None:
        return None
    return created.tags.get("parent_session")


def _is_actual_state_change(bus: EventBus, event: EventRecord) -> bool:
    previous = [
        candidate
        for candidate in bus.replay(
            EventFilter(
                names=frozenset({SessionStateChanged.name}),
                tags={"session": event.tags["session"]},
            )
        )
        if candidate.id < event.id
    ]
    return not previous or previous[-1].tags.get("state") != event.tags.get("state")


def _namer_started_for_state(bus: EventBus, event: EventRecord) -> bool:
    sessions = bus.replay(EventFilter(names=frozenset({SessionCreated.name})))
    return any(
        candidate.producer == NamerPlugin.name
        and candidate.causation_id == event.id
        and candidate.tags.get("namer") == "true"
        for candidate in sessions
    )


def _already_renamed_from(bus: EventBus, event: EventRecord) -> bool:
    return any(
        candidate.producer == NamerPlugin.name and candidate.causation_id == event.id
        for candidate in bus.replay(
            EventFilter(names=frozenset({SessionRenamed.name}))
        )
    )


def _contains_tool_call(content: Any) -> bool:
    if not isinstance(content, list):
        return False
    return any(
        isinstance(item, dict)
        and (item.get("type") == "function_call" or bool(item.get("tool_calls")))
        for item in content
    )


def _text_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, dict):
            text = item.get("text")
            if isinstance(text, str):
                parts.append(text)
            nested = item.get("content")
            if isinstance(nested, str):
                parts.append(nested)
            elif isinstance(nested, list):
                extracted = _text_content(nested)
                if extracted:
                    parts.append(extracted)
    return " ".join(parts)
