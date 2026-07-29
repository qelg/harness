from __future__ import annotations

import asyncio

from llm_harness.builtin_plugins.namer import NAMER_SYSTEM_PROMPT, NamerPlugin
from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import (
    AssistantMessageCreated,
    SessionCreated,
    SessionRenamed,
    SessionStateChanged,
    ToolMessageCreated,
    UserMessageCreated,
)


def _settings(tmp_path, monkeypatch) -> Settings:
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_NAMER_PROVIDER", "mock-llm")
    monkeypatch.setenv("HARNESS_NAMER_MODEL", "summary-model")
    return Settings.from_env()


def test_namer_prompt_requests_a_title_for_the_entire_conversation():
    assert NAMER_SYSTEM_PROMPT == (
        "Summarize the entire conversation, considering all user and assistant messages, "
        "in 5-10 words. Use the summary as the conversation title. "
        "Reply solely with the title. Do not use tools."
    )


def test_state_change_starts_tagged_tool_free_namer_session(tmp_path, monkeypatch):
    settings = _settings(tmp_path, monkeypatch)
    bus = EventService(settings.event_database_path)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent", title="Old")))
    user = asyncio.run(
        bus.append_message(UserMessageCreated(session_id="sess_parent", content="Fix the parser"))
    )
    asyncio.run(
        bus.append_message(
            ToolMessageCreated(
                session_id="sess_parent",
                content="tool output must not be copied",
                tool="terminal",
                run_id="tool_1",
            )
        )
    )
    state = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id="sess_parent", state="running", source_event_id=user.id
            )
        )
    )

    asyncio.run(NamerPlugin(settings=settings).process_event(bus, state))

    children = [
        event
        for event in bus.replay(EventFilter(names=frozenset({SessionCreated.name})))
        if event.tags.get("namer") == "true"
    ]
    assert len(children) == 1
    child = children[0]
    assert child.tags["session_tag:namer"] == "true"
    assert child.tags["session_tag:no-auto-llm-run"] == "true"
    assert child.tags["parent_session"] == "sess_parent"
    assert child.payload["parent_session"] == "sess_parent"

    child_events = bus.replay(EventFilter(tags={"session": child.tags["session"]}))
    messages = [event for event in child_events if event.name.startswith("chat.message.")]
    assert [(event.tags["role"], event.payload["content"]) for event in messages] == [
        ("system", NAMER_SYSTEM_PROMPT),
        ("user", "Fix the parser"),
    ]
    request = next(event for event in child_events if event.name == "llm.run.requested")
    assert request.tags["provider"] == "mock-llm"
    assert request.tags["model"] == "summary-model"
    assert request.payload["toolsets"] == []


def test_namer_reply_renames_parent_idempotently(tmp_path, monkeypatch):
    settings = _settings(tmp_path, monkeypatch)
    bus = EventService(settings.event_database_path)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    child = asyncio.run(
        bus.append_message(
            SessionCreated(
                session_id="sess_namer",
                session_tags=("namer",),
                parent_session_id="sess_parent",
                namer=True,
            ),
            producer="namer",
        )
    )
    reply = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_namer",
                content="Fix parser error handling safely",
                provider="mock-llm",
                model="summary-model",
                run_id="namer_1",
            ),
            correlation_id=child.id,
        )
    )
    plugin = NamerPlugin(settings=settings)

    asyncio.run(plugin.process_event(bus, reply))
    asyncio.run(plugin.process_event(bus, reply))

    renamed = bus.replay(EventFilter(names=frozenset({SessionRenamed.name})))
    assert len(renamed) == 1
    assert renamed[0].tags["session"] == "sess_parent"
    assert renamed[0].payload == {
        "title": "Fix parser error handling safely",
        "namer_session_id": "sess_namer",
    }
    assert renamed[0].causation_id == reply.id


def test_namer_ignores_non_changes_archived_and_namer_sessions(tmp_path, monkeypatch):
    settings = _settings(tmp_path, monkeypatch)
    bus = EventService(settings.event_database_path)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    first = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id="sess_parent", state="running", source_event_id=1
            )
        )
    )
    repeated = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id="sess_parent", state="running", source_event_id=2
            )
        )
    )
    archived = asyncio.run(
        bus.append(
            "session.state",
            {"source_event_id": 3},
            tags={"session": "sess_parent", "chat": "sess_parent", "state": "archived"},
        )
    )
    asyncio.run(
        bus.append_message(
            SessionCreated(
                session_id="sess_child",
                parent_session_id="sess_parent",
            )
        )
    )
    child_state = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id="sess_child", state="running", source_event_id=4
            )
        )
    )
    plugin = NamerPlugin(settings=settings)

    asyncio.run(plugin.process_event(bus, first))
    asyncio.run(plugin.process_event(bus, repeated))
    asyncio.run(plugin.process_event(bus, archived))
    asyncio.run(plugin.process_event(bus, child_state))

    generated = [
        event
        for event in bus.replay(EventFilter(names=frozenset({SessionCreated.name})))
        if event.producer == "namer"
    ]
    assert len(generated) == 1
