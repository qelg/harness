from __future__ import annotations

import asyncio

from llm_harness.builtin_plugins.session_state import SessionStatePlugin
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import (
    AssistantMessageCreated,
    SessionStateChanged,
    UserMessageCreated,
)


def test_user_message_projects_running_state_with_causation(tmp_path):
    bus = EventService(tmp_path / "events.db")
    source = asyncio.run(
        bus.append_message(UserMessageCreated(session_id="sess_1", content="hello"))
    )
    plugin = SessionStatePlugin()

    asyncio.run(plugin.process_event(bus, source))

    state = _state_events(bus)[0]
    assert state.tags == {
        "session": "sess_1",
        "chat": "sess_1",
        "state": "running",
    }
    assert state.payload == {"source_event_id": source.id}
    assert state.causation_id == source.id
    assert state.correlation_id == source.id
    assert state.producer == "session-state"


def test_final_assistant_message_projects_unread_finished_state(tmp_path):
    bus = EventService(tmp_path / "events.db")
    source = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                content="done",
                provider="mock",
                model="test",
                run_id="llm_1",
                metadata={"provider_response": {"finish_reason": "stop"}},
            ),
            correlation_id=123,
        )
    )
    plugin = SessionStatePlugin()

    asyncio.run(plugin.process_event(bus, source))

    state = _state_events(bus)[0]
    assert state.tags["state"] == "finished"
    assert state.tags["read"] == "unread"
    assert state.payload == {"source_event_id": source.id, "outcome": "stop"}
    assert state.causation_id == source.id
    assert state.correlation_id == 123


def test_assistant_tool_call_does_not_finish_session(tmp_path):
    bus = EventService(tmp_path / "events.db")
    source = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                content=[
                    {
                        "type": "function_call",
                        "name": "terminal",
                        "arguments": '{"cmd":"pwd"}',
                    }
                ],
                provider="mock",
                model="test",
                run_id="llm_1",
            )
        )
    )

    asyncio.run(SessionStatePlugin().process_event(bus, source))

    assert _state_events(bus) == []


def test_replayed_source_event_is_projected_only_once(tmp_path):
    bus = EventService(tmp_path / "events.db")
    source = asyncio.run(
        bus.append_message(UserMessageCreated(session_id="sess_1", content="hello"))
    )
    plugin = SessionStatePlugin()

    asyncio.run(plugin.process_event(bus, source))
    asyncio.run(plugin.process_event(bus, source))

    assert len(_state_events(bus)) == 1


def _state_events(bus: EventService):
    return bus.replay(EventFilter(names=frozenset({SessionStateChanged.name})))


def test_failed_run_finishes_session_with_failed_outcome(tmp_path):
    from llm_harness.core.types import LlmRunFailed

    bus = EventService(tmp_path / "events.db")
    source = asyncio.run(
        bus.append_message(
            LlmRunFailed(
                session_id="sess_1",
                provider="mock",
                model="test",
                run_id="llm_1",
                error="boom",
            )
        )
    )

    asyncio.run(SessionStatePlugin().process_event(bus, source))

    state = _state_events(bus)[0]
    assert state.tags["state"] == "finished"
    assert state.tags["read"] == "unread"
    assert state.payload["outcome"] == "failed"


def test_retryable_provider_result_keeps_session_running(tmp_path):
    bus = EventService(tmp_path / "events.db")
    source = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                content="temporarily unavailable",
                provider="mock",
                model="test",
                run_id="llm_1",
                metadata={
                    "provider_response": {
                        "error": {"code": "server_is_overloaded"}
                    }
                },
            )
        )
    )

    asyncio.run(SessionStatePlugin().process_event(bus, source))

    assert _state_events(bus) == []


def test_final_response_atomically_emits_queued_users_instead_of_finished_state(
    tmp_path, monkeypatch
):
    from llm_harness.builtin_plugins.llm_run_requester import LlmRunRequesterPlugin
    from llm_harness.config import Settings
    from llm_harness.core.types import ModelSelected, QueuedMessage

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    asyncio.run(
        bus.append_message(ModelSelected(provider="mock-llm", model="mock-model"))
    )
    first = asyncio.run(
        bus.append_message(
            QueuedMessage("sess_1", "first follow-up", "after_response")
        )
    )
    second = asyncio.run(
        bus.append_message(
            QueuedMessage("sess_1", "second follow-up", "after_response")
        )
    )
    final = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                content="done",
                provider="mock-llm",
                model="mock-model",
                run_id="llm_1",
            )
        )
    )
    plugin = SessionStatePlugin()

    async def process_twice():
        await asyncio.gather(
            plugin.process_event(bus, final), plugin.process_event(bus, final)
        )

    asyncio.run(process_twice())

    assert _state_events(bus) == []
    users = bus.replay(
        EventFilter(
            names=frozenset({"chat.message.user.created"}),
            tags={"session": "sess_1"},
        )
    )
    assert [event.payload["content"] for event in users] == [
        "first follow-up",
        "second follow-up",
    ]
    assert [event.causation_id for event in users] == [first.id, second.id]
    assert [
        event.payload["metadata"]["queued_message_requests_llm"] for event in users
    ] == [False, True]

    requester = LlmRunRequesterPlugin(settings=Settings.from_env())
    asyncio.run(requester.process_pending(bus))
    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))
    assert len(requests) == 1
    assert requests[0].causation_id == users[-1].id
    assert users[-1].id < requests[0].id


def test_message_accepted_before_state_consumer_runs_continues_response(tmp_path):
    from llm_harness.core.types import QueuedMessage

    bus = EventService(tmp_path / "events.db")
    final = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                content="done",
                provider="mock",
                model="test",
                run_id="llm_1",
            )
        )
    )
    asyncio.run(
        bus.append_message(QueuedMessage("sess_1", "too late", "after_response"))
    )

    asyncio.run(SessionStatePlugin().process_event(bus, final))

    assert _state_events(bus) == []
    users = bus.replay(
        EventFilter(names=frozenset({"chat.message.user.created"}))
    )
    assert [event.payload["content"] for event in users] == ["too late"]


def test_queued_message_before_latest_request_does_not_leak_into_later_response(
    tmp_path,
):
    from llm_harness.core.types import LlmRunRequested, QueuedMessage

    bus = EventService(tmp_path / "events.db")
    asyncio.run(
        bus.append_message(QueuedMessage("sess_1", "missed boundary", "after_response"))
    )
    asyncio.run(
        bus.append_message(
            LlmRunRequested(
                session_id="sess_1",
                provider="mock",
                model="test",
                run_id="llm_2",
            )
        )
    )
    final = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                content="later turn",
                provider="mock",
                model="test",
                run_id="llm_2",
            )
        )
    )

    asyncio.run(SessionStatePlugin().process_event(bus, final))

    assert _state_events(bus)[0].tags["state"] == "finished"
    assert bus.replay(
        EventFilter(names=frozenset({"chat.message.user.created"}))
    ) == []
