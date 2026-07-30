from __future__ import annotations

import asyncio
from typing import Any

from llm_harness.builtin_plugins.model_choice import ModelChoice, model_choice_for
from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord, EventToAppend
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunRequested,
    ModelSelected,
    SessionCreated,
    SessionStateChanged,
    ToolCall,
    ToolCallRequested,
    ToolMessageCreated,
    ToolResult,
    ToolSession,
    UserMessageCreated,
    new_session_id,
    to_event_parts,
)

SUBAGENT_SESSION_TAG = "subagent"
SUBAGENT_RESPONSE_PREFIX = "subagent response:"


class SubagentTool:
    """Describe and validate requests handled by :class:`SubagentPlugin`."""

    name = "subagent"
    description = (
        "Start a subagent in a child session. The tool returns the new session ID immediately; "
        "after both sessions finish, the subagent's final response is sent back to this session. "
        "By default the subagent uses the calling model; optionally provide a model override."
    )
    input_schema = {
        "type": "object",
        "properties": {
            "context": {
                "type": "string",
                "description": "The task and context to give to the subagent.",
            },
            "model": {
                "type": "string",
                "description": (
                    "Optional model override. Defaults to the model that called the tool."
                ),
            },
        },
        "required": ["context"],
        "additionalProperties": False,
    }

    async def run(self, call: ToolCall) -> ToolResult:
        context = call.input.get("context")
        if not isinstance(context, str) or not context.strip():
            raise ValueError("tool input requires non-empty string field 'context'")
        model = call.input.get("model")
        if model is not None and (not isinstance(model, str) or not model.strip()):
            raise ValueError("tool input field 'model' must be a non-empty string")
        # Starting the child and persisting the acknowledgement must happen in
        # one event-store transaction, so the event consumer performs those
        # operations after this tool has validated the input.
        return ToolResult(
            output=context,
            metadata={"model": model.strip()} if isinstance(model, str) else {},
        )


class SubagentPlugin(EventConsumer):
    """Start child sessions and return their completed answers to their parents."""

    name = "subagent"
    subscriber = "plugin:subagent"
    event_filter = EventFilter(
        names=frozenset({ToolCallRequested.name, SessionStateChanged.name})
    )

    def __init__(self, *, tool: SubagentTool, settings: Settings | None = None):
        self.tool = tool
        self.settings = settings or Settings.from_env()
        # Keep the replay-before-append idempotency checks atomic even if this
        # consumer is configured with parallelity greater than one.
        self._mutation_lock = asyncio.Lock()

    async def process_event(
        self, bus: EventBus, event: EventRecord, *, registry: Any = None
    ) -> None:
        if event.name == ToolCallRequested.name:
            if event.tags.get("tool") == self.tool.name:
                async with self._mutation_lock:
                    await self._start_subagent(bus, event)
            return
        if event.tags.get("state") == "finished":
            async with self._mutation_lock:
                await self._copy_ready_responses(bus, event.tags["session"])

    async def _start_subagent(self, bus: EventBus, event: EventRecord) -> None:
        if _child_for_request(bus, event) is not None:
            return

        call = ToolCall(
            session=ToolSession(id=event.tags["session"]),
            name=self.tool.name,
            input=event.payload.get("input", {}),
        )
        validated = await self.tool.run(call)
        choice = _model_choice_for_request(bus, event, settings=self.settings)
        configured_model = validated.metadata.get("model")
        if isinstance(configured_model, str):
            choice = ModelChoice(
                provider=choice.provider,
                model=configured_model,
                toolsets=choice.toolsets,
                thinking_level=choice.thinking_level,
                reasoning_summary=choice.reasoning_summary,
            )
        child_session_id = new_session_id()
        correlation_id = event.correlation_id or event.id

        messages = (
            SessionCreated(
                session_id=child_session_id,
                title=SUBAGENT_SESSION_TAG,
                session_tags=(SUBAGENT_SESSION_TAG,),
                parent_session_id=event.tags["session"],
            ),
            ModelSelected(
                provider=choice.provider,
                model=choice.model,
                toolsets=choice.toolsets,
                thinking_level=choice.thinking_level,
                reasoning_summary=choice.reasoning_summary,
                session_id=child_session_id,
                metadata={
                    "subagent": True,
                    "parent_session_id": event.tags["session"],
                    "tool_request_event_id": event.id,
                },
            ),
            UserMessageCreated(
                session_id=child_session_id,
                content=validated.output,
                metadata={
                    "subagent": True,
                    "parent_session_id": event.tags["session"],
                    "tool_request_event_id": event.id,
                },
            ),
            ToolMessageCreated(
                session_id=event.tags["session"],
                content=f"subagent started: {child_session_id}",
                tool=self.tool.name,
                run_id=event.tags["run"],
                metadata={
                    "subagent_session_id": child_session_id,
                    "parent_session_id": event.tags["session"],
                    "provider": choice.provider,
                    "model": choice.model,
                },
            ),
        )
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
                for name, payload, tags in map(to_event_parts, messages)
            ]
        )

    async def _copy_ready_responses(
        self, bus: EventBus, changed_session_id: str
    ) -> None:
        candidates: dict[str, EventRecord] = {}

        changed_session = _session_created(bus, changed_session_id)
        if changed_session is not None and _is_subagent_session(changed_session):
            candidates[changed_session_id] = changed_session

        for child in _subagent_children(bus, changed_session_id):
            candidates[child.tags["session"]] = child

        for child in candidates.values():
            await self._copy_response_if_ready(bus, child)

    async def _copy_response_if_ready(
        self, bus: EventBus, child: EventRecord
    ) -> None:
        child_session_id = child.tags["session"]
        parent_session_id = child.tags.get("parent_session")
        if parent_session_id is None:
            return

        child_state = _latest_state(bus, child_session_id)
        parent_state = _latest_state(bus, parent_session_id)
        if not _is_finished(child_state) or not _is_finished(parent_state):
            return
        # A finished state from an earlier parent turn does not satisfy this
        # subagent call. For plugin-created children, require the parent to
        # have finished after the originating tool request.
        if child.causation_id is not None:
            parent_source_id = parent_state.payload.get("source_event_id")
            if (
                not isinstance(parent_source_id, int)
                or isinstance(parent_source_id, bool)
                or parent_source_id <= child.causation_id
            ):
                return
        if _response_already_copied(bus, parent_session_id, child_session_id):
            return

        response = _event_by_id(bus, child_state.payload.get("source_event_id"))
        if (
            response is None
            or response.name != AssistantMessageCreated.name
            or response.tags.get("session") != child_session_id
        ):
            return
        response_text = _text_content(response.payload.get("content")).strip()
        if not response_text:
            return

        await bus.append_message(
            UserMessageCreated(
                session_id=parent_session_id,
                content=f"{SUBAGENT_RESPONSE_PREFIX} {response_text}",
                metadata={
                    "subagent": True,
                    "subagent_session_id": child_session_id,
                    "subagent_response_event_id": response.id,
                    "subagent_state_event_id": child_state.id,
                },
            ),
            producer=self.name,
            causation_id=child_state.id,
            correlation_id=child_state.correlation_id or child_state.id,
        )


def _model_choice_for_request(
    bus: EventBus, request: EventRecord, *, settings: Settings
) -> ModelChoice:
    fallback = model_choice_for(bus, request.tags["session"], settings)
    assistant = _event_by_id(bus, request.causation_id)
    if assistant is None or assistant.name != AssistantMessageCreated.name:
        return fallback

    provider = assistant.tags.get("provider")
    model = assistant.tags.get("model")
    if not provider or not model:
        return fallback

    calling_run = _event_by_id(bus, assistant.causation_id)
    if calling_run is None or calling_run.name != LlmRunRequested.name:
        return ModelChoice(
            provider=provider,
            model=model,
            toolsets=fallback.toolsets,
            thinking_level=fallback.thinking_level,
            reasoning_summary=fallback.reasoning_summary,
        )
    return ModelChoice(
        provider=provider,
        model=model,
        toolsets=tuple(calling_run.payload.get("toolsets", fallback.toolsets)),
        thinking_level=calling_run.payload.get("thinking_level"),
        reasoning_summary=bool(calling_run.payload.get("reasoning_summary", False)),
    )


def _child_for_request(bus: EventBus, request: EventRecord) -> EventRecord | None:
    for event in bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}),
            tags={"parent_session": request.tags["session"]},
        )
    ):
        if (
            event.producer == SubagentPlugin.name
            and event.causation_id == request.id
            and _is_subagent_session(event)
        ):
            return event
    return None


def _session_created(bus: EventBus, session_id: str) -> EventRecord | None:
    events = bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}), tags={"session": session_id}
        ),
        limit=1,
    )
    return events[0] if events else None


def _subagent_children(bus: EventBus, parent_session_id: str) -> list[EventRecord]:
    return [
        event
        for event in bus.replay(
            EventFilter(
                names=frozenset({SessionCreated.name}),
                tags={"parent_session": parent_session_id},
            )
        )
        if _is_subagent_session(event)
    ]


def _is_subagent_session(event: EventRecord) -> bool:
    return event.tags.get(f"session_tag:{SUBAGENT_SESSION_TAG}") == "true"


def _latest_state(bus: EventBus, session_id: str) -> EventRecord | None:
    events = bus.replay(
        EventFilter(
            names=frozenset({SessionStateChanged.name}),
            tags={"session": session_id},
        )
    )
    return events[-1] if events else None


def _is_finished(event: EventRecord | None) -> bool:
    return event is not None and event.tags.get("state") == "finished"


def _response_already_copied(
    bus: EventBus, parent_session_id: str, child_session_id: str
) -> bool:
    for event in bus.replay(
        EventFilter(
            names=frozenset({UserMessageCreated.name}),
            tags={"session": parent_session_id},
        )
    ):
        metadata = event.payload.get("metadata")
        if (
            event.producer == SubagentPlugin.name
            and isinstance(metadata, dict)
            and metadata.get("subagent_session_id") == child_session_id
        ):
            return True
    return False


def _event_by_id(bus: EventBus, event_id: Any) -> EventRecord | None:
    if not isinstance(event_id, int) or isinstance(event_id, bool):
        return None
    events = bus.replay(EventFilter(since_id=event_id - 1), limit=1)
    if events and events[0].id == event_id:
        return events[0]
    return None


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
