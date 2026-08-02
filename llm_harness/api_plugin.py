from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import AsyncIterator
from typing import Any, Literal

from fastapi import Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from llm_harness.builtin_plugins.queued_messages import (
    pending_queued_messages,
    trigger_already_delivered_queue,
)
from llm_harness.config import Settings
from llm_harness.core.events import BusEvent, EventBus, EventFilter
from llm_harness.core.types import (
    MESSAGE_CREATED_NAMES,
    PARENT_SESSION,
    QUEUE_AFTER_RESPONSE,
    ModelSelected,
    QueuedMessage,
    SessionCreated,
    SessionRenamed,
    SessionStateChanged,
    SecretAsk,
    ToolCallRequested,
    UserMessageCreated,
    new_run_id,
    new_session_id,
)
from llm_harness.plugins import Registry

MESSAGE_TIMELINE_NAMES = MESSAGE_CREATED_NAMES | frozenset(
    {"llm.run.failed", QueuedMessage.name, ToolCallRequested.name, SecretAsk.name}
)
MESSAGE_UPDATE_NAMES = MESSAGE_TIMELINE_NAMES | frozenset({"llm.delta", SessionStateChanged.name})


class CreateSessionRequest(BaseModel):
    title: str | None = None
    tags: list[str] = Field(default_factory=list)


class CreateMessageRequest(BaseModel):
    content: str
    queue_mode: Literal["after_tool", "after_response"] | None = None


class RunToolRequest(BaseModel):
    input: dict[str, Any] = Field(default_factory=dict)


class SelectModelRequest(BaseModel):
    provider: str
    model: str
    toolsets: list[str] | None = None
    session_id: str | None = None
    thinking_level: Literal["none", "low", "medium", "high"] | None = None
    reasoning_summary: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class HarnessApiPlugin:
    name = "harness-api"

    def __init__(self, *, settings: Settings):
        self.settings = settings

    def install_api(self, *, app, bus: EventBus, registry: Registry) -> None:
        @app.get("/health")
        async def health() -> dict[str, str]:
            return {"status": "ok"}

        @app.get("/providers")
        async def providers() -> dict[str, list[str]]:
            return {"providers": sorted(registry.providers)}

        @app.get("/tools")
        async def tools() -> dict[str, list[str]]:
            return {"tools": sorted(registry.tools)}

        @app.get("/toolsets")
        async def toolsets() -> dict[str, list[str]]:
            return {"toolsets": sorted(registry.toolsets)}

        @app.post("/model-selection")
        async def select_model(request: SelectModelRequest) -> dict[str, Any]:
            if request.session_id is not None:
                _require_session_event(bus, request.session_id)
            toolsets = request.toolsets if request.toolsets is not None else list(self.settings.default_toolsets)
            _require_toolsets(registry, toolsets)
            event = await bus.append_message(
                ModelSelected(
                    provider=request.provider,
                    model=request.model,
                    toolsets=tuple(toolsets),
                    thinking_level=request.thinking_level,
                    reasoning_summary=request.reasoning_summary,
                    session_id=request.session_id,
                    metadata=request.metadata,
                ),
                producer="harness-api",
            )
            return _dump_bus_payload(event)

        @app.get("/sessions/{session_id}/model-selection")
        async def get_session_model_selection(session_id: str) -> dict[str, Any]:
            _require_session_event(bus, session_id)
            return _model_selection_for(bus, session_id, settings=self.settings)

        @app.post("/sessions")
        async def create_session(request: CreateSessionRequest) -> dict[str, Any]:
            session_id = new_session_id()
            event = await bus.append_message(
                SessionCreated(session_id=session_id, title=request.title, session_tags=tuple(request.tags)),
                producer="harness-api",
            )
            return _session_from_events(bus, event)

        @app.get("/sessions")
        async def list_sessions(tag: str | None = None) -> list[dict[str, Any]]:
            if bus.projections_ready:
                rows = bus.conn.execute(
                    "SELECT session_id, parent_session_id, title, tags_json, created_event_id, created_at_ms "
                    "FROM projected_sessions WHERE parent_session_id IS NULL ORDER BY created_event_id"
                ).fetchall()
                sessions = [_session_from_projection_row(row) for row in rows]
                if tag is None:
                    return sessions
                return [session for session in sessions if tag in session["tags"]]
            sessions = [
                _session_from_events(bus, event)
                for event in bus.replay(EventFilter(names=frozenset({SessionCreated.name})))
                if "parent_session" not in event.tags
            ]
            if tag is None:
                return sessions
            return [session for session in sessions if tag in session["tags"]]

        @app.get("/sessions/{session_id}/children")
        async def list_child_sessions(session_id: str) -> list[dict[str, Any]]:
            _require_session_event(bus, session_id)
            if bus.projections_ready:
                rows = bus.conn.execute(
                    "SELECT session_id, parent_session_id, title, tags_json, created_event_id, created_at_ms "
                    "FROM projected_sessions WHERE parent_session_id = ? ORDER BY created_event_id",
                    (session_id,),
                ).fetchall()
                return [_session_from_projection_row(row) for row in rows]
            return [
                _session_from_events(bus, event)
                for event in bus.replay(
                    EventFilter(
                        names=frozenset({SessionCreated.name}),
                        tags={PARENT_SESSION: session_id},
                    )
                )
            ]

        @app.get("/session-states")
        async def list_session_states() -> list[dict[str, Any]]:
            """Return each session's latest state, newest activity first."""
            if bus.projections_ready:
                rows = bus.conn.execute(
                    "SELECT session_id, state, read_state, archived, source_event_id, outcome, event_id, created_at_ms "
                    "FROM projected_session_states ORDER BY event_id DESC"
                ).fetchall()
                return [_session_state_from_projection_row(row) for row in rows]
            latest_by_session: dict[str, BusEvent] = {}
            for event in bus.replay(
                EventFilter(names=frozenset({SessionStateChanged.name}))
            ):
                latest_by_session[event.tags["session"]] = event
            return [
                _session_state_from_event(event)
                for event in sorted(
                    latest_by_session.values(), key=lambda item: item.id, reverse=True
                )
            ]

        @app.post("/sessions/{session_id}/state/read")
        async def mark_session_state_read(session_id: str) -> dict[str, Any]:
            _require_session_event(bus, session_id)
            if bus.projections_ready:
                row = bus.conn.execute(
                    "SELECT session_id, state, read_state, archived, source_event_id, outcome, event_id, created_at_ms "
                    "FROM projected_session_states WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                if row is None or row["state"] != "finished":
                    raise HTTPException(
                        status_code=409, detail="only a finished session can be marked read"
                    )
                if row["read_state"] == "read":
                    return _session_state_from_projection_row(row)
                event = await bus.append_message(
                    SessionStateChanged(
                        session_id=session_id,
                        state="finished",
                        source_event_id=row["source_event_id"],
                        read="read",
                        outcome=row["outcome"],
                    ),
                    producer="harness-api",
                    causation_id=row["event_id"],
                    correlation_id=row["event_id"],
                )
                return _session_state_from_event(event)
            state_events = bus.replay(
                EventFilter(
                    names=frozenset({SessionStateChanged.name}),
                    tags={"session": session_id},
                )
            )
            if not state_events or state_events[-1].tags["state"] != "finished":
                raise HTTPException(
                    status_code=409, detail="only a finished session can be marked read"
                )
            latest = state_events[-1]
            if latest.tags.get("read") == "read":
                return _session_state_from_event(latest)
            event = await bus.append_message(
                SessionStateChanged(
                    session_id=session_id,
                    state="finished",
                    source_event_id=latest.payload["source_event_id"],
                    read="read",
                    outcome=latest.payload.get("outcome"),
                ),
                producer="harness-api",
                causation_id=latest.id,
                correlation_id=latest.correlation_id or latest.id,
            )
            return _session_state_from_event(event)

        @app.post("/sessions/{session_id}/state/archive")
        async def archive_session_state(session_id: str) -> dict[str, Any]:
            _require_session_event(bus, session_id)
            if bus.projections_ready:
                row = bus.conn.execute(
                    "SELECT session_id, state, read_state, archived, source_event_id, outcome, event_id, created_at_ms "
                    "FROM projected_session_states WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                if row is not None and row["archived"]:
                    return _session_state_from_projection_row(row)
                if row is None:
                    created_row = bus.conn.execute(
                        "SELECT created_event_id FROM projected_sessions WHERE session_id = ?",
                        (session_id,),
                    ).fetchone()
                    state = "finished"
                    read = "read"
                    source_event_id = created_row["created_event_id"]
                    outcome = None
                    causation_id = created_row["created_event_id"]
                    correlation_id = created_row["created_event_id"]
                else:
                    state = row["state"]
                    read = row["read_state"]
                    source_event_id = row["source_event_id"]
                    outcome = row["outcome"]
                    causation_id = row["event_id"]
                    correlation_id = row["event_id"]
                event = await bus.append_message(
                    SessionStateChanged(
                        session_id=session_id,
                        state=state,
                        source_event_id=source_event_id,
                        read=read,
                        outcome=outcome,
                        archived=True,
                    ),
                    producer="harness-api",
                    causation_id=causation_id,
                    correlation_id=correlation_id,
                )
                return _session_state_from_event(event)
            state_events = bus.replay(
                EventFilter(
                    names=frozenset({SessionStateChanged.name}),
                    tags={"session": session_id},
                )
            )
            latest = state_events[-1] if state_events else None
            if latest is not None and latest.tags.get("archive") == "true":
                return _session_state_from_event(latest)

            if latest is None:
                created = bus.replay(
                    EventFilter(
                        names=frozenset({SessionCreated.name}),
                        tags={"session": session_id},
                    ),
                    limit=1,
                )[0]
                state = "finished"
                read = "read"
                source_event_id = created.id
                outcome = None
                causation_id = created.id
                correlation_id = created.correlation_id or created.id
            else:
                state = latest.tags["state"]
                read = latest.tags.get("read")
                source_event_id = latest.payload["source_event_id"]
                outcome = latest.payload.get("outcome")
                causation_id = latest.id
                correlation_id = latest.correlation_id or latest.id

            event = await bus.append_message(
                SessionStateChanged(
                    session_id=session_id,
                    state=state,
                    source_event_id=source_event_id,
                    read=read,
                    outcome=outcome,
                    archived=True,
                ),
                producer="harness-api",
                causation_id=causation_id,
                correlation_id=correlation_id,
            )
            return _session_state_from_event(event)

        @app.get("/sessions/{session_id}/state-events")
        async def list_session_state_events(session_id: str) -> list[dict[str, Any]]:
            _require_session_event(bus, session_id)
            return [
                _session_state_from_event(event)
                for event in bus.replay(
                    EventFilter(
                        names=frozenset({SessionStateChanged.name}),
                        tags={"session": session_id},
                    )
                )
            ]

        @app.get("/sessions/{session_id}/messages")
        async def list_messages(session_id: str) -> list[dict[str, Any]]:
            _require_session_event(bus, session_id)
            return [
                _message_from_event(event)
                for event in bus.replay(
                    EventFilter(names=MESSAGE_TIMELINE_NAMES, tags={"session": session_id})
                )
            ]

        @app.get("/sessions/{session_id}/events")
        async def list_events(session_id: str) -> list[dict[str, Any]]:
            _require_session_event(bus, session_id)
            return [
                _dump_bus_payload(event)
                for event in bus.replay(EventFilter(tags={"session": session_id}))
            ]

        @app.get("/sessions/{session_id}/messages/updates")
        async def stream_message_updates(
            session_id: str,
            since_id: int | None = None,
            last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
        ) -> StreamingResponse:
            _require_session_event(bus, session_id)
            effective_since_id = since_id if since_id is not None else _parse_last_event_id(last_event_id)

            async def events() -> AsyncIterator[str]:
                event_filter = EventFilter(
                    since_id=effective_since_id,
                    names=MESSAGE_UPDATE_NAMES,
                    tags={"session": session_id},
                )
                sent_ids: set[int] = set()
                async with bus.subscribe(event_filter) as queue:
                    for event in bus.replay(event_filter):
                        sent_ids.add(event.id)
                        yield _sse(event.type, _message_update_from_event(event), event_id=event.id)

                    while True:
                        try:
                            event = await asyncio.wait_for(queue.get(), timeout=15)
                        except TimeoutError:
                            yield _sse("heartbeat", {})
                            continue
                        if event.id in sent_ids:
                            continue
                        sent_ids.add(event.id)
                        yield _sse(event.type, _message_update_from_event(event), event_id=event.id)

            return StreamingResponse(events(), media_type="text/event-stream")

        @app.post("/sessions/{session_id}/messages")
        async def create_message(session_id: str, request: CreateMessageRequest) -> dict[str, Any]:
            _require_session_event(bus, session_id)
            event = await bus.append_message(
                _message_command(session_id, request),
                producer="harness-api",
            )
            return _message_from_event(event)

        @app.get("/sessions/{session_id}/events/stream")
        async def stream_events(session_id: str) -> StreamingResponse:
            _require_session_event(bus, session_id)

            async def events() -> AsyncIterator[str]:
                async with bus.subscribe(EventFilter(tags={"session": session_id})) as queue:
                    while True:
                        try:
                            event = await asyncio.wait_for(queue.get(), timeout=15)
                        except TimeoutError:
                            yield _sse("heartbeat", {})
                            continue
                        yield _sse(event.type, _dump_bus_payload(event))

            return StreamingResponse(events(), media_type="text/event-stream")

        @app.post("/sessions/{session_id}/messages/stream")
        async def stream_message(session_id: str, request: CreateMessageRequest) -> StreamingResponse:
            _require_session_event(bus, session_id)

            async def events() -> AsyncIterator[str]:
                async with bus.subscribe(EventFilter(tags={"session": session_id})) as queue:
                    event = await bus.append_message(
                        _message_command(session_id, request),
                        producer="harness-api",
                    )
                    yield _sse("message.accepted", {"event_id": event.id})
                    while True:
                        try:
                            event = await asyncio.wait_for(queue.get(), timeout=15)
                        except TimeoutError:
                            yield _sse("heartbeat", {})
                            continue
                        yield _sse(event.type, _dump_bus_payload(event))
                        if _is_terminal_stream_event(event, bus=bus):
                            break

            return StreamingResponse(events(), media_type="text/event-stream")

        @app.post("/sessions/{session_id}/tools/{tool_name}")
        async def run_tool(session_id: str, tool_name: str, request: RunToolRequest) -> dict[str, Any]:
            _require_session_event(bus, session_id)
            event = await bus.append_message(
                ToolCallRequested(
                    session_id=session_id,
                    tool=tool_name,
                    input=request.input,
                    run_id=new_run_id("tool"),
                ),
                producer="harness-api",
            )
            return {"status": "accepted", "event": _dump_bus_payload(event)}


def _message_command(session_id: str, request: CreateMessageRequest):
    if request.queue_mode is None:
        return UserMessageCreated(session_id=session_id, content=request.content)
    return QueuedMessage(
        session_id=session_id,
        content=request.content,
        mode=request.queue_mode,
    )


def _is_terminal_stream_event(
    event: BusEvent, *, bus: EventBus | None = None
) -> bool:
    if event.type not in {"llm.run.failed", "chat.message.assistant.created"}:
        return False
    if (
        event.type == "chat.message.assistant.created"
        and _contains_function_call(event.payload.get("content"))
    ):
        return False
    if bus is not None and _response_continues_from_queue(bus, event):
        return False
    return True


def _response_continues_from_queue(bus: EventBus, event: BusEvent) -> bool:
    session_id = event.tags["session"]
    return trigger_already_delivered_queue(
        bus,
        session_id=session_id,
        producer="session-state",
        trigger_event_id=event.id,
    ) or bool(
        pending_queued_messages(
            bus,
            session_id=session_id,
            mode=QUEUE_AFTER_RESPONSE,
        )
    )


def _contains_function_call(content: Any) -> bool:
    if not isinstance(content, list):
        return False
    return any(
        isinstance(item, dict)
        and (item.get("type") == "function_call" or bool(item.get("tool_calls")))
        for item in content
    )


def _require_session_event(bus: EventBus, session_id: str) -> None:
    if bus.projections_ready:
        row = bus.conn.execute(
            "SELECT 1 FROM projected_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if row is not None:
            return
        raise HTTPException(status_code=404, detail=f"unknown session: {session_id}")
    events = bus.replay(EventFilter(names=frozenset({"session.created"}), tags={"session": session_id}), limit=1)
    if not events:
        raise HTTPException(status_code=404, detail=f"unknown session: {session_id}")


def _require_toolsets(registry: Registry, toolsets: list[str]) -> None:
    unknown = sorted(set(toolsets) - set(registry.toolsets))
    if unknown:
        raise HTTPException(status_code=400, detail=f"unknown toolset: {', '.join(unknown)}")



def _session_from_projection_row(row: sqlite3.Row) -> dict[str, Any]:
    session = {
        "id": row["session_id"],
        "title": row["title"],
        "tags": json.loads(row["tags_json"]),
        "created_at_ms": row["created_at_ms"],
        "event_id": row["created_event_id"],
    }
    parent_session_id = row["parent_session_id"]
    if parent_session_id is not None:
        session["parent_session_id"] = parent_session_id
    return session


def _session_state_from_projection_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "state": row["state"],
        "read": row["read_state"],
        "archive": "true" if row["archived"] else None,
        "source_event_id": row["source_event_id"],
        "outcome": row["outcome"],
        "event_id": row["event_id"],
        "created_at_ms": row["created_at_ms"],
    }


def _session_state_from_event(event: BusEvent) -> dict[str, Any]:
    return {
        "session_id": event.tags["session"],
        "state": event.tags["state"],
        "read": event.tags.get("read"),
        "archive": event.tags.get("archive"),
        "source_event_id": event.payload["source_event_id"],
        "outcome": event.payload.get("outcome"),
        "event_id": event.id,
        "created_at_ms": event.created_at_ms,
    }


def _session_from_events(bus: EventBus, event: BusEvent) -> dict[str, Any]:
    title = event.payload.get("title")
    parent_session_id = event.tags.get(PARENT_SESSION)
    if bus.projections_ready:
        row = bus.conn.execute(
            "SELECT title, parent_session_id FROM projected_sessions WHERE session_id = ?",
            (event.tags["session"],),
        ).fetchone()
        if row is not None:
            title = row["title"]
            parent_session_id = row["parent_session_id"]
    else:
        latest_rename = bus.latest(
            EventFilter(names=frozenset({SessionRenamed.name}), tags={"session": event.tags["session"]})
        )
        if latest_rename is not None:
            title = latest_rename.payload["title"]
    session = {
        "id": event.tags["session"],
        "title": title,
        "tags": event.payload.get("tags", []),
        "created_at_ms": event.created_at_ms,
        "event_id": event.id,
    }
    if parent_session_id is not None:
        session["parent_session_id"] = parent_session_id
    return session
def _message_from_event(event: BusEvent) -> dict[str, Any]:
    if event.name == "llm.run.failed":
        return _failed_run_message_from_event(event)
    if event.name == ToolCallRequested.name:
        return {
            "id": event.id,
            "session_id": event.tags["session"],
            "role": "tool_request",
            "content": event.payload.get("input", {}),
            "provider": None,
            "model": None,
            "tool": event.payload.get("tool"),
            "run_id": event.payload.get("run_id"),
            "metadata": {},
            "queue_mode": None,
            "event_name": event.name,
            "created_at_ms": event.created_at_ms,
        }
    if event.name == SecretAsk.name:
        return {
            "id": event.id,
            "session_id": event.tags["session"],
            "role": "tool_request",
            "content": event.payload.get("description", ""),
            "provider": None,
            "model": None,
            "tool": "retrieve-secret",
            "run_id": event.payload.get("run_id"),
            "metadata": {
                "identifier": event.payload.get("identifier"),
                "container": event.payload.get("container"),
            },
            "queue_mode": None,
            "event_name": event.name,
            "created_at_ms": event.created_at_ms,
        }
    return {
        "id": event.id,
        "session_id": event.tags["session"],
        "role": event.tags["role"],
        "content": event.payload["content"],
        "provider": event.payload.get("provider"),
        "model": event.payload.get("model"),
        "tool": event.payload.get("tool"),
        "run_id": event.payload.get("run_id"),
        "metadata": event.payload.get("metadata", {}),
        "queue_mode": event.payload.get("mode") if event.name == QueuedMessage.name else None,
        "event_name": event.name,
        "created_at_ms": event.created_at_ms,
    }


def _failed_run_message_from_event(event: BusEvent) -> dict[str, Any]:
    error = event.payload.get("error") or "LLM run failed"
    return {
        "id": event.id,
        "session_id": event.tags["session"],
        "role": "assistant",
        "content": f"LLM run failed: {error}",
        "provider": event.payload.get("provider"),
        "model": event.payload.get("model"),
        "tool": None,
        "run_id": event.payload.get("run_id"),
        "metadata": {
            "error": error,
            "retryable": event.payload.get("retryable", False),
        },
        "queue_mode": None,
        "event_name": event.name,
        "created_at_ms": event.created_at_ms,
    }


def _message_update_from_event(event: BusEvent) -> dict[str, Any]:
    if event.name in MESSAGE_TIMELINE_NAMES:
        return {"event": _dump_bus_payload(event), "message": _message_from_event(event)}
    return {"event": _dump_bus_payload(event)}


def _model_selection_for(bus: EventBus, session_id: str, *, settings: Settings) -> dict[str, Any]:
    if bus.projections_ready:
        rows = bus.conn.execute(
            "SELECT scope_key, provider, model, toolsets_json, thinking_level, reasoning_summary, event_id, created_at_ms "
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
            return {
                "provider": session_row["provider"],
                "model": session_row["model"],
                "toolsets": json.loads(session_row["toolsets_json"]),
                "thinking_level": session_row["thinking_level"],
                "reasoning_summary": bool(session_row["reasoning_summary"]),
                "scope": "session",
                "session_id": session_id,
                "event_id": session_row["event_id"],
                "created_at_ms": session_row["created_at_ms"],
            }
        if global_row is not None:
            return {
                "provider": global_row["provider"],
                "model": global_row["model"],
                "toolsets": json.loads(global_row["toolsets_json"]),
                "thinking_level": global_row["thinking_level"],
                "reasoning_summary": bool(global_row["reasoning_summary"]),
                "scope": "global",
                "session_id": session_id,
                "event_id": global_row["event_id"],
                "created_at_ms": global_row["created_at_ms"],
            }
        return {
            "provider": settings.default_provider,
            "model": settings.default_model,
            "toolsets": list(settings.default_toolsets),
            "thinking_level": None,
            "reasoning_summary": False,
            "scope": "default",
            "session_id": session_id,
            "event_id": None,
            "created_at_ms": None,
        }
    selected = bus.replay(EventFilter(names=frozenset({ModelSelected.name})))
    session_event: BusEvent | None = None
    global_event: BusEvent | None = None
    for event in selected:
        if event.tags.get("session") == session_id:
            session_event = event
        elif "session" not in event.tags:
            global_event = event

    if session_event is not None:
        return _model_selection_from_event(session_event, scope="session")
    if global_event is not None:
        return _model_selection_from_event(global_event, scope="global")
    return {
        "provider": settings.default_provider,
        "model": settings.default_model,
        "toolsets": list(settings.default_toolsets),
        "thinking_level": None,
        "reasoning_summary": False,
        "scope": "default",
        "session_id": session_id,
        "event_id": None,
        "created_at_ms": None,
    }


def _model_selection_from_event(event: BusEvent, *, scope: str) -> dict[str, Any]:
    return {
        "provider": event.tags["provider"],
        "model": event.tags["model"],
        "toolsets": event.payload.get("toolsets", []),
        "thinking_level": event.payload.get("thinking_level"),
        "reasoning_summary": event.payload.get("reasoning_summary", False),
        "scope": scope,
        "session_id": event.tags.get("session"),
        "event_id": event.id,
        "created_at_ms": event.created_at_ms,
    }


def _parse_last_event_id(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _sse(event: str, payload: dict[str, Any], *, event_id: int | None = None) -> str:
    prefix = f"id: {event_id}\n" if event_id is not None else ""
    return f"{prefix}event: {event}\ndata: {json.dumps(payload)}\n\n"


def _dump_bus_payload(event: BusEvent) -> dict[str, Any]:
    return {
        "session_id": event.session_id,
        "payload": event.payload,
        "id": event.id,
        "name": event.name,
        "tags": event.tags,
        "created_at_ms": event.created_at_ms,
        "producer": event.producer,
        "causation_id": event.causation_id,
        "correlation_id": event.correlation_id,
        "durable": event.durable,
        "persisted_event_id": event.persisted_event_id,
    }
