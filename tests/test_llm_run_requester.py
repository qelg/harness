from __future__ import annotations

import asyncio

from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import (
    LlmRetry, LlmRunFailed, ModelSelected, SessionCreated, SessionRenamed,
    UserMessageCreated,
)
from llm_harness.builtin_plugins.llm_run_requester import LlmRunRequesterPlugin


def test_user_message_requests_llm_run_with_global_model_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(ModelSelected(provider="mock-llm", model="global-model", toolsets=("default",))))
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    user_message = asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="hello")))

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
    assert requests[0].tags["provider"] == "mock-llm"
    assert requests[0].tags["model"] == "global-model"
    assert requests[0].payload["toolsets"] == ["default"]
    assert requests[0].tags["run"].startswith("llm_")
    assert requests[0].payload["user_message_event_id"] == user_message.id
    assert requests[0].causation_id == user_message.id


def test_llm_run_requester_declares_subscriber_and_event_filter(monkeypatch, tmp_path):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    assert plugin.subscriber == "plugin:llm-run-requester"
    assert plugin.event_filter.names == frozenset({"chat.message.user.created", "llm.retry"})


def test_session_model_selection_overrides_global_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(ModelSelected(provider="mock-llm", model="global-model")))
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    asyncio.run(
        bus.append_message(
            ModelSelected(
                provider="openrouter",
                model="session-model",
                toolsets=("default",),
                session_id="sess_1",
            )
        )
    )
    asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="hello")))

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
    assert requests[0].tags["provider"] == "openrouter"
    assert requests[0].tags["model"] == "session-model"
    assert requests[0].payload["toolsets"] == ["default"]


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


def test_session_tag_can_disable_automatic_llm_runs(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())
    asyncio.run(
        bus.append_message(
            SessionCreated(
                session_id="sess_manual",
                session_tags=("no-auto-llm-run",),
            )
        )
    )
    asyncio.run(
        bus.append_message(
            UserMessageCreated(session_id="sess_manual", content="hello")
        )
    )

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(
        EventFilter(
            names=frozenset({"llm.run.requested"}),
            tags={"session": "sess_manual"},
        )
    )
    assert requests == []


def test_retry_requests_llm_run_only_after_failed_run(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    retry_before_failure = asyncio.run(bus.append_message(LlmRetry(session_id="sess_1")))
    asyncio.run(plugin.process_event(bus, retry_before_failure))
    assert bus.replay(EventFilter(names=frozenset({"llm.run.requested"}))) == []

    user_message = asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="hello")))
    request = asyncio.run(plugin.process_event(bus, user_message))
    requested = bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))[0]
    failed = asyncio.run(bus.append_message(LlmRunFailed(
        session_id="sess_1", provider="mock", model="model", run_id=requested.tags["run"],
        error="failed",
    ), causation_id=requested.id))
    retry = asyncio.run(bus.append_message(LlmRetry(session_id="sess_1")))
    asyncio.run(plugin.process_event(bus, retry))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))
    assert len(requests) == 2
    assert requests[-1].causation_id == retry.id
    assert requests[-1].payload["user_message_event_id"] == user_message.id
    assert failed.tags["session"] == "sess_1"


def test_retry_requester_does_not_duplicate_for_same_event(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    failed = asyncio.run(bus.append_message(LlmRunFailed(
        session_id="sess_1", provider="mock", model="model", run_id="run_1", error="failed"
    )))
    retry = asyncio.run(bus.append_message(LlmRetry(session_id="sess_1")))
    asyncio.run(plugin.process_event(bus, retry))
    asyncio.run(plugin.process_event(bus, retry))
    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))
    assert len(requests) == 1
    assert requests[0].causation_id == retry.id
    assert failed.id < retry.id


def test_retry_allows_session_metadata_events_but_not_new_activity(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = LlmRunRequesterPlugin(settings=Settings.from_env())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    failed = asyncio.run(bus.append_message(LlmRunFailed(
        session_id="sess_1", provider="mock", model="model", run_id="run_1", error="failed"
    )))
    asyncio.run(bus.append_message(SessionRenamed("sess_1", "renamed", "sess_1")))
    retry = asyncio.run(bus.append_message(LlmRetry(session_id="sess_1")))
    asyncio.run(plugin.process_event(bus, retry))
    assert len(bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))) == 1

    failed_2 = asyncio.run(bus.append_message(LlmRunFailed(
        session_id="sess_1", provider="mock", model="model", run_id="run_2", error="failed"
    )))
    asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="new message")))
    retry_2 = asyncio.run(bus.append_message(LlmRetry(session_id="sess_1")))
    asyncio.run(plugin.process_event(bus, retry_2))
    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))
    assert len(requests) == 1
    assert failed.id < retry.id and failed_2.id < retry_2.id
