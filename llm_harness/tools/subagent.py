from __future__ import annotations

import asyncio
import json
from typing import Any

from llm_harness.builtin_plugins.model_choice import ModelChoice, model_choice_for
from llm_harness.builtin_plugins.system_prompt import build_system_prompt
from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord, EventToAppend
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunFailed,
    LlmRunRequested,
    ModelSelected,
    SessionCreated,
    SessionStateChanged,
    SystemMessageCreated,
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
THINKING_LEVELS = ("none", "low", "medium", "high")
WAIT_MODES = ("any", "all")
RECURSIVE_SUBAGENT_LIMIT = "recursive_subagent_limit"


class SubagentTool:
    """Describe and validate requests handled by :class:`SubagentPlugin`."""

    name = "subagent"
    description = (
        "Start a subagent in a child session. The tool returns the new session ID immediately; "
        "after both sessions finish, the subagent's final response is sent back to this session. "
        "By default provider, model, thinking level, toolsets, and reasoning summary are inherited "
        "from the calling run. Provider, model, thinking_level, and same_container can be overridden. "
        "Child sessions cannot start subagents unless recursive_subagent_limit is supplied."
    )
    input_schema = {
        "type": "object",
        "properties": {
            "context": {
                "type": "string",
                "pattern": r"\S",
                "description": "The task and context to give to the subagent.",
            },
            "provider": {
                "type": "string",
                "pattern": r"\S",
                "description": "Optional provider override.",
            },
            "model": {
                "type": "string",
                "pattern": r"\S",
                "description": "Optional model override.",
            },
            "thinking_level": {
                "type": "string",
                "enum": list(THINKING_LEVELS),
                "description": "Optional thinking level override.",
            },
            "same_container": {
                "type": "boolean",
                "default": False,
                "description": "Use the calling session's effective terminal container.",
            },
            RECURSIVE_SUBAGENT_LIMIT: {
                "type": "integer",
                "minimum": 1,
                "description": "Number of further subagent levels this child may create.",
            },
        },
        "required": ["context"],
        "additionalProperties": False,
    }

    async def run(self, call: ToolCall) -> ToolResult:
        if not isinstance(call.input, dict) or set(call.input) - {
            "context", "provider", "model", "thinking_level", "same_container",
            RECURSIVE_SUBAGENT_LIMIT,
        }:
            raise ValueError("tool input contains unknown fields")
        context = call.input.get("context")
        if not isinstance(context, str) or not context.strip():
            raise ValueError("tool input requires non-empty string field 'context'")
        metadata: dict[str, Any] = {}
        for field in ("provider", "model"):
            if field not in call.input:
                continue
            value = call.input[field]
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"tool input field '{field}' must be a non-empty string")
            metadata[field] = value.strip()

        thinking = call.input.get("thinking_level")
        if "thinking_level" in call.input and (
            not isinstance(thinking, str) or thinking not in THINKING_LEVELS
        ):
            raise ValueError(
                "tool input field 'thinking_level' must be one of: "
                + ", ".join(THINKING_LEVELS)
            )
        if thinking is not None:
            metadata["thinking_level"] = thinking

        same_container = call.input.get("same_container", False)
        if type(same_container) is not bool:
            raise ValueError("tool input field 'same_container' must be a boolean")
        if same_container:
            metadata["same_container"] = True

        recursive_limit = call.input.get(RECURSIVE_SUBAGENT_LIMIT)
        if RECURSIVE_SUBAGENT_LIMIT in call.input and (
            type(recursive_limit) is not int or recursive_limit < 1
        ):
            raise ValueError(
                f"tool input field '{RECURSIVE_SUBAGENT_LIMIT}' must be a positive integer"
            )
        if recursive_limit is not None:
            metadata[RECURSIVE_SUBAGENT_LIMIT] = recursive_limit

        return ToolResult(output=context, metadata=metadata)


class SubagentStateTool:
    """Read child state, optionally waiting for a terminal child state."""

    name = "subagent_state"
    description = (
        "Return the durable state of subagent child sessions. Provide session_ids and optionally "
        "wait_for='any' or 'all'; omitted wait_for returns immediately. A satisfied wait resumes "
        "the normal parent tool-result flow."
    )
    input_schema = {
        "type": "object",
        "properties": {
            "session_ids": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "pattern": r"\S"},
                "uniqueItems": True,
                "description": "Subagent child session IDs to inspect.",
            },
            "wait_for": {
                "type": "string",
                "enum": list(WAIT_MODES),
                "description": "Wait for any or all requested children to finish.",
            },
        },
        "required": ["session_ids"],
        "additionalProperties": False,
    }

    async def run(self, call: ToolCall) -> ToolResult:
        if not isinstance(call.input, dict) or set(call.input) - {"session_ids", "wait_for"}:
            raise ValueError("tool input contains unknown fields")
        session_ids = _validated_session_ids(call.input)
        wait_for = call.input.get("wait_for")
        if "wait_for" in call.input and (
            not isinstance(wait_for, str) or wait_for not in WAIT_MODES
        ):
            raise ValueError("tool input field 'wait_for' must be one of: any, all")
        return ToolResult(
            output="",
            metadata={
                "session_ids": session_ids,
                "wait_for": wait_for,
            },
        )


class SubagentPlugin(EventConsumer):
    """Start children, fulfill durable state waits, and copy legacy answers."""

    name = "subagent"
    subscriber = "plugin:subagent"
    event_filter = EventFilter(
        names=frozenset({ToolCallRequested.name, SessionStateChanged.name})
    )

    def __init__(
        self,
        *,
        tool: SubagentTool,
        state_tool: SubagentStateTool | None = None,
        settings: Settings | None = None,
    ):
        self.tool = tool
        self.state_tool = state_tool or SubagentStateTool()
        self.settings = settings or Settings.from_env()
        # Replay-before-append and overlapping waits must be idempotent even if
        # this consumer is configured with parallelity greater than one.
        self._mutation_lock = asyncio.Lock()

    async def run(self, bus: EventBus, *, registry: Any = None) -> None:
        # A request may have been acknowledged by the consumer just before a
        # restart while its result was intentionally still pending. Rebuild the
        # pending set from events before consuming new events.
        async with self._mutation_lock:
            await self._fulfill_pending_state_requests(bus)
        await super().run(bus, registry=registry)

    async def process_pending(self, bus: EventBus, *, registry: Any = None) -> None:
        await super().process_pending(bus, registry=registry)
        async with self._mutation_lock:
            await self._fulfill_pending_state_requests(bus)

    async def process_event(
        self, bus: EventBus, event: EventRecord, *, registry: Any = None
    ) -> None:
        async with self._mutation_lock:
            if event.name == ToolCallRequested.name:
                if event.tags.get("tool") == self.tool.name:
                    await self._start_subagent(bus, event)
                elif event.tags.get("tool") == self.state_tool.name:
                    await self._fulfill_state_request(bus, event)
                return

            # State results must be delivered before checking the legacy copy
            # path. Their durable metadata then makes the copy suppression
            # decision deterministic during replay as well.
            await self._fulfill_pending_state_requests(bus, event.tags.get("session"))
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
        parent_session_id = event.tags["session"]
        parent_limit = _recursive_subagent_limit(bus, parent_session_id)
        if _is_recursive_subagent_session(bus, parent_session_id):
            if parent_limit < 1:
                raise ValueError(
                    "subagent sessions cannot start subagents without recursive_subagent_limit"
                )
            if RECURSIVE_SUBAGENT_LIMIT in validated.metadata:
                raise ValueError(
                    "recursive_subagent_limit may only be set when starting a non-subagent session"
                )
            child_limit = parent_limit - 1
        else:
            child_limit = validated.metadata.get(RECURSIVE_SUBAGENT_LIMIT, 0)
        choice = _model_choice_for_request(bus, event, settings=self.settings)
        for field in ("provider", "model"):
            configured = validated.metadata.get(field)
            if isinstance(configured, str):
                choice = ModelChoice(
                    provider=configured if field == "provider" else choice.provider,
                    model=configured if field == "model" else choice.model,
                    toolsets=choice.toolsets,
                    thinking_level=choice.thinking_level,
                    reasoning_summary=choice.reasoning_summary,
                )
        if "thinking_level" in validated.metadata:
            choice = ModelChoice(
                provider=choice.provider,
                model=choice.model,
                toolsets=choice.toolsets,
                thinking_level=validated.metadata["thinking_level"],
                reasoning_summary=choice.reasoning_summary,
            )

        child_session_id = new_session_id()
        metadata: dict[str, Any] = {RECURSIVE_SUBAGENT_LIMIT: child_limit}
        if validated.metadata.get("same_container") is True:
            metadata["terminal_container_owner_session_id"] = _container_owner_session_id(
                bus, parent_session_id
            )
        correlation_id = event.correlation_id or event.id
        system_prompt = build_system_prompt(self.settings.skills)
        messages = (
            SessionCreated(
                session_id=child_session_id,
                title=SUBAGENT_SESSION_TAG,
                session_tags=(SUBAGENT_SESSION_TAG,),
                parent_session_id=parent_session_id,
                metadata=metadata,
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
                    "parent_session_id": parent_session_id,
                    "tool_request_event_id": event.id,
                    RECURSIVE_SUBAGENT_LIMIT: child_limit,
                },
            ),
            *(
                (SystemMessageCreated(session_id=child_session_id, content=system_prompt),)
                if system_prompt
                else ()
            ),
            UserMessageCreated(
                session_id=child_session_id,
                content=validated.output,
                metadata={
                    "subagent": True,
                    "parent_session_id": parent_session_id,
                    "tool_request_event_id": event.id,
                },
            ),
            ToolMessageCreated(
                session_id=parent_session_id,
                content=f"subagent started: {child_session_id}",
                tool=self.tool.name,
                run_id=event.tags["run"],
                metadata={
                    "subagent_session_id": child_session_id,
                    "parent_session_id": parent_session_id,
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

    async def _fulfill_state_request(self, bus: EventBus, request: EventRecord) -> None:
        if _tool_result_for_request(bus, request) is not None:
            return
        result = await self._state_result_for_request(bus, request)
        if result is None:
            return
        await self._append_state_result(bus, request, result)

    async def _fulfill_pending_state_requests(
        self, bus: EventBus, changed_session_id: str | None = None
    ) -> None:
        requests = bus.replay(
            EventFilter(names=frozenset({ToolCallRequested.name}), tags={"tool": self.state_tool.name})
        )
        for request in requests:
            if changed_session_id is not None:
                ids = _validated_session_ids(request.payload.get("input", {}))
                if changed_session_id not in ids:
                    continue
            await self._fulfill_state_request(bus, request)

    async def _state_result_for_request(
        self, bus: EventBus, request: EventRecord
    ) -> tuple[str, dict[str, Any]] | None:
        call = ToolCall(
            session=ToolSession(id=request.tags["session"]),
            name=self.state_tool.name,
            input=request.payload.get("input", {}),
        )
        validated = await self.state_tool.run(call)
        session_ids = validated.metadata["session_ids"]
        children = {child.tags["session"] for child in _subagent_children(bus, request.tags["session"])}
        if any(session_id not in children for session_id in session_ids):
            raise ValueError("all session_ids must be subagent children of the calling session")

        wait_for = validated.metadata["wait_for"]
        states = [_subagent_state(bus, session_id) for session_id in session_ids]
        terminal = [state for state in states if state["state"] in {"finished", "failed"}]
        satisfied = wait_for is None or (
            bool(terminal) if wait_for == "any" else len(terminal) == len(states)
        )
        if not satisfied:
            return None
        document = {"states": states}
        return json.dumps(document, sort_keys=True, separators=(",", ":")), {
            "subagent_state": True,
            "session_ids": session_ids,
            "terminal_session_ids": [state["session_id"] for state in terminal],
            "wait_for": wait_for,
        }

    async def _append_state_result(
        self, bus: EventBus, request: EventRecord, result: tuple[str, dict[str, Any]]
    ) -> None:
        output, metadata = result
        await bus.append_message(
            ToolMessageCreated(
                session_id=request.tags["session"],
                content=output,
                tool=self.state_tool.name,
                run_id=request.tags["run"],
                metadata=metadata,
            ),
            producer=self.name,
            causation_id=request.id,
            correlation_id=request.correlation_id or request.id,
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

    async def _copy_response_if_ready(self, bus: EventBus, child: EventRecord) -> None:
        child_session_id = child.tags["session"]
        parent_session_id = child.tags.get("parent_session")
        if parent_session_id is None:
            return
        child_state = _latest_state(bus, child_session_id)
        parent_state = _latest_state(bus, parent_session_id)
        if not _is_finished(child_state) or not _is_finished(parent_state):
            return
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
        if _response_delivered_by_state(bus, parent_session_id, child_session_id):
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
    calling_run = _event_by_id(bus, assistant.causation_id)
    if calling_run is None or calling_run.name != LlmRunRequested.name:
        return ModelChoice(
            provider=assistant.tags.get("provider", fallback.provider),
            model=assistant.tags.get("model", fallback.model),
            toolsets=fallback.toolsets,
            thinking_level=fallback.thinking_level,
            reasoning_summary=fallback.reasoning_summary,
        )
    payload = calling_run.payload
    toolsets = payload.get("toolsets", fallback.toolsets)
    if not isinstance(toolsets, (list, tuple)):
        toolsets = fallback.toolsets
    return ModelChoice(
        provider=payload.get("provider", calling_run.tags.get("provider", fallback.provider)),
        model=payload.get("model", calling_run.tags.get("model", fallback.model)),
        toolsets=tuple(toolsets),
        thinking_level=payload.get("thinking_level"),
        reasoning_summary=bool(payload.get("reasoning_summary", False)),
    )


def _is_recursive_subagent_session(bus: EventBus, session_id: str) -> bool:
    return bool(bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}),
            tags={"session": session_id, f"session_tag:{SUBAGENT_SESSION_TAG}": "true"},
        ),
        limit=1,
    ))


def _recursive_subagent_limit(bus: EventBus, session_id: str) -> int:
    session = bus.replay(
        EventFilter(names=frozenset({SessionCreated.name}), tags={"session": session_id}),
        limit=1,
    )
    if not session:
        return 0
    metadata = session[0].payload.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    limit = metadata.get(RECURSIVE_SUBAGENT_LIMIT)
    return limit if type(limit) is int and limit >= 0 else 0


def _validated_session_ids(input_: Any) -> list[str]:
    if not isinstance(input_, dict):
        raise ValueError("tool input requires non-empty array field 'session_ids'")
    session_ids = input_.get("session_ids")
    if (
        not isinstance(session_ids, list)
        or not session_ids
        or any(not isinstance(value, str) or not value.strip() for value in session_ids)
        or len(set(session_ids)) != len(session_ids)
    ):
        raise ValueError("tool input field 'session_ids' must be a non-empty array of unique strings")
    normalized = [value.strip() for value in session_ids]
    if len(set(normalized)) != len(normalized):
        raise ValueError("tool input field 'session_ids' must contain unique strings")
    return normalized


def _child_for_request(bus: EventBus, request: EventRecord) -> EventRecord | None:
    events = bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}),
            tags={"parent_session": request.tags["session"]},
            causation_id=request.id,
            producer=SubagentPlugin.name,
        ),
        limit=1,
    )
    if events and _is_subagent_session(events[0]):
        return events[0]
    return None


def _session_created(bus: EventBus, session_id: str) -> EventRecord | None:
    events = bus.replay(
        EventFilter(names=frozenset({SessionCreated.name}), tags={"session": session_id}), limit=1
    )
    return events[0] if events else None


def _container_owner_session_id(bus: EventBus, session_id: str) -> str:
    current = session_id
    seen: set[str] = set()
    while current not in seen:
        seen.add(current)
        created = _session_created(bus, current)
        if created is None:
            break
        metadata = created.payload.get("metadata")
        owner = metadata.get("terminal_container_owner_session_id") if isinstance(metadata, dict) else None
        if not isinstance(owner, str) or not owner or owner == current:
            break
        current = owner
    return current


def _subagent_children(bus: EventBus, parent_session_id: str) -> list[EventRecord]:
    return [
        event
        for event in bus.replay(
            EventFilter(names=frozenset({SessionCreated.name}), tags={"parent_session": parent_session_id})
        )
        if _is_subagent_session(event)
    ]


def _is_subagent_session(event: EventRecord) -> bool:
    return event.tags.get(f"session_tag:{SUBAGENT_SESSION_TAG}") == "true"


def _latest_state(bus: EventBus, session_id: str) -> EventRecord | None:
    return bus.latest(EventFilter(names=frozenset({SessionStateChanged.name}), tags={"session": session_id}))


def _subagent_state(bus: EventBus, session_id: str) -> dict[str, Any]:
    state_event = _latest_state(bus, session_id)
    failed = bus.latest(
        EventFilter(names=frozenset({LlmRunFailed.name}), tags={"session": session_id})
    )
    # A provider failure is terminal as soon as it is persisted.  The
    # session-state consumer may not have projected its corresponding state
    # event yet, especially during replay.
    if failed is not None and (state_event is None or failed.id > state_event.id):
        return _failed_state(session_id, failed)
    if state_event is None:
        has_user_message = bus.exists(
            EventFilter(names=frozenset({UserMessageCreated.name}), tags={"session": session_id})
        )
        return {"session_id": session_id, "state": "running" if has_user_message else "starting"}
    if state_event.tags.get("state") != "finished":
        return {"session_id": session_id, "state": "running"}

    source = _terminal_source(bus, state_event, session_id)
    if state_event.payload.get("outcome") == "failed" or (
        source is not None and source.name == LlmRunFailed.name
    ):
        return _failed_state(session_id, source)
    if source is None or source.name != AssistantMessageCreated.name:
        # The session state is still terminal, but never expose an unrelated
        # event merely because its ID was placed in source_event_id.
        return {"session_id": session_id, "state": "finished", "result": ""}
    return {
        "session_id": session_id,
        "state": "finished",
        "result": _text_content(source.payload.get("content")),
    }


def _terminal_source(
    bus: EventBus, state_event: EventRecord, session_id: str
) -> EventRecord | None:
    source = _event_by_id(bus, state_event.payload.get("source_event_id"))
    if source is None or source.tags.get("session") != session_id:
        return None
    return source


def _failed_state(session_id: str, failed: EventRecord | None) -> dict[str, Any]:
    error = failed.payload.get("error") if failed is not None else None
    return {
        "session_id": session_id,
        "state": "failed",
        "error": error if isinstance(error, str) else "subagent run failed",
    }

def _tool_result_for_request(bus: EventBus, request: EventRecord) -> EventRecord | None:
    return bus.latest(
        EventFilter(
            names=frozenset({ToolMessageCreated.name}),
            tags={"session": request.tags["session"], "tool": request.tags["tool"], "run": request.tags["run"]},
            causation_id=request.id,
        )
    )


def _response_already_copied(bus: EventBus, parent_session_id: str, child_session_id: str) -> bool:
    for event in bus.replay(EventFilter(names=frozenset({UserMessageCreated.name}), tags={"session": parent_session_id})):
        metadata = event.payload.get("metadata")
        if event.producer == SubagentPlugin.name and isinstance(metadata, dict) and metadata.get("subagent_session_id") == child_session_id:
            return True
    return False


def _response_delivered_by_state(bus: EventBus, parent_session_id: str, child_session_id: str) -> bool:
    for event in bus.replay(
        EventFilter(names=frozenset({ToolMessageCreated.name}), tags={"session": parent_session_id, "tool": SubagentStateTool.name})
    ):
        metadata = event.payload.get("metadata")
        delivered = metadata.get("terminal_session_ids") if isinstance(metadata, dict) else None
        if isinstance(delivered, list) and child_session_id in delivered:
            return True
    return False


def _event_by_id(bus: EventBus, event_id: Any) -> EventRecord | None:
    if not isinstance(event_id, int) or isinstance(event_id, bool):
        return None
    return bus.get_event(event_id)


def _is_finished(event: EventRecord | None) -> bool:
    return event is not None and event.tags.get("state") == "finished"


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
