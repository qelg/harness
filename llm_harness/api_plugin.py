from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from llm_harness.config import Settings
from llm_harness.core.events import BusEvent, EventBus, EventFilter
from llm_harness.core.types import (
    MESSAGE_CREATED_NAMES,
    ModelSelected,
    SessionCreated,
    ToolCallRequested,
    UserMessageCreated,
    new_run_id,
    new_session_id,
)
from llm_harness.plugins import Registry

MESSAGE_TIMELINE_NAMES = MESSAGE_CREATED_NAMES | frozenset({"llm.run.failed"})
MESSAGE_UPDATE_NAMES = MESSAGE_TIMELINE_NAMES | frozenset({"llm.delta"})


class CreateSessionRequest(BaseModel):
    title: str | None = None
    tags: list[str] = Field(default_factory=list)


class CreateMessageRequest(BaseModel):
    content: str


class RunToolRequest(BaseModel):
    input: dict[str, Any] = Field(default_factory=dict)


class SelectModelRequest(BaseModel):
    provider: str
    model: str
    toolsets: list[str] | None = None
    session_id: str | None = None
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
            return _session_from_events(event)

        @app.get("/sessions")
        async def list_sessions(tag: str | None = None) -> list[dict[str, Any]]:
            sessions = [_session_from_events(event) for event in bus.replay(EventFilter(names=frozenset({"session.created"})))]
            if tag is None:
                return sessions
            return [session for session in sessions if tag in session["tags"]]

        @app.get("/sessions/{session_id}/messages")
        async def list_messages(session_id: str) -> list[dict[str, Any]]:
            _require_session_event(bus, session_id)
            return [
                _message_from_event(event)
                for event in bus.replay(
                    EventFilter(names=MESSAGE_TIMELINE_NAMES, tags={"session": session_id})
                )
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
                UserMessageCreated(
                    session_id=session_id,
                    content=request.content,
                ),
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
                        UserMessageCreated(
                            session_id=session_id,
                            content=request.content,
                        ),
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
                        if event.type in {"chat.message.assistant.created", "llm.run.failed"}:
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


def _require_session_event(bus: EventBus, session_id: str) -> None:
    events = bus.replay(EventFilter(names=frozenset({"session.created"}), tags={"session": session_id}), limit=1)
    if not events:
        raise HTTPException(status_code=404, detail=f"unknown session: {session_id}")


def _require_toolsets(registry: Registry, toolsets: list[str]) -> None:
    unknown = sorted(set(toolsets) - set(registry.toolsets))
    if unknown:
        raise HTTPException(status_code=400, detail=f"unknown toolset: {', '.join(unknown)}")


def _session_from_events(event: BusEvent) -> dict[str, Any]:
    return {
        "id": event.tags["session"],
        "title": event.payload.get("title"),
        "tags": event.payload.get("tags", []),
        "created_at_ms": event.created_at_ms,
        "event_id": event.id,
    }


def _message_from_event(event: BusEvent) -> dict[str, Any]:
    if event.name == "llm.run.failed":
        return _failed_run_message_from_event(event)
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
        "event_name": event.name,
        "created_at_ms": event.created_at_ms,
    }


def _message_update_from_event(event: BusEvent) -> dict[str, Any]:
    if event.name in MESSAGE_TIMELINE_NAMES:
        return {"event": _dump_bus_payload(event), "message": _message_from_event(event)}
    return {"event": _dump_bus_payload(event)}


def _model_selection_for(bus: EventBus, session_id: str, *, settings: Settings) -> dict[str, Any]:
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
        "persisted_event_id": event.persisted_event_id,
    }
