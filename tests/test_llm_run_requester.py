from __future__ import annotations

import asyncio

from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import ModelSelected, SessionCreated, UserMessageCreated
from llm_harness.builtin_plugins.llm_run_requester import LlmRunRequesterPlugin


def test_user_message_requests_llm_run_with_global_model_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(ModelSelected(provider="mock-llm", model="global-model")))
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    user_message = asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="hello")))

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
    assert requests[0].tags["provider"] == "mock-llm"
    assert requests[0].tags["model"] == "global-model"
    assert requests[0].tags["run"].startswith("llm_")
    assert requests[0].payload["user_message_event_id"] == user_message.id
    assert requests[0].causation_id == user_message.id


def test_llm_run_requester_declares_subscriber_and_event_filter(monkeypatch, tmp_path):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    assert plugin.subscriber == "plugin:llm-run-requester"
    assert plugin.event_filter.names == frozenset({"chat.message.user.created"})


def test_session_model_selection_overrides_global_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(ModelSelected(provider="mock-llm", model="global-model")))
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    asyncio.run(
        bus.append_message(ModelSelected(provider="openrouter", model="session-model", session_id="sess_1"))
    )
    asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="hello")))

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
    assert requests[0].tags["provider"] == "openrouter"
    assert requests[0].tags["model"] == "session-model"


def test_llm_run_requester_does_not_duplicate_requests(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="hello")))

    asyncio.run(plugin.process_pending(bus))
    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
