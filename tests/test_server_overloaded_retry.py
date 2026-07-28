from __future__ import annotations

import asyncio

from llm_harness.builtin_plugins.server_overloaded_retry import ServerOverloadedRetryPlugin
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import AssistantMessageCreated, LlmRunRequested


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def test_server_overloaded_response_is_retried_after_initial_delay(tmp_path):
    asyncio.run(_assert_server_overloaded_response_is_retried_after_initial_delay(tmp_path))


async def _assert_server_overloaded_response_is_retried_after_initial_delay(tmp_path):
    bus = EventService(tmp_path / "events.db")
    sleep = RecordingSleep()
    plugin = ServerOverloadedRetryPlugin(sleep=sleep)
    request = await bus.append_message(
        LlmRunRequested(
            session_id="sess_1",
            provider="chatgpt-codex",
            model="codex-model",
            run_id="llm_1",
            toolsets=("default",),
            user_message_event_id=123,
            metadata={"existing": "value"},
        )
    )
    assistant = await bus.append_message(
        AssistantMessageCreated(
            session_id="sess_1",
            content="",
            provider="chatgpt-codex",
            model="codex-model",
            run_id="llm_1",
            metadata={"provider_response": {"error": {"code": "server_is_overloaded"}}},
        ),
        causation_id=request.id,
        correlation_id=999,
    )

    await plugin.process_pending(bus)

    requests = bus.replay(EventFilter(names=frozenset({LlmRunRequested.name}), tags={"session": "sess_1"}))
    assert sleep.delays == [30.0]
    assert len(requests) == 2
    retry = requests[1]
    assert retry.tags["provider"] == "chatgpt-codex"
    assert retry.tags["model"] == "codex-model"
    assert retry.tags["run"].startswith("llm_")
    assert retry.tags["run"] != "llm_1"
    assert retry.payload["toolsets"] == ["default"]
    assert retry.payload["user_message_event_id"] == 123
    assert retry.payload["metadata"] == {
        "existing": "value",
        "trigger": "server_is_overloaded",
        "retry_attempt": 1,
        "retry_delay_seconds": 30.0,
        "previous_run_id": "llm_1",
        "assistant_message_event_id": assistant.id,
    }
    assert retry.causation_id == assistant.id
    assert retry.correlation_id == 999


def test_repeated_server_overload_uses_exponential_backoff(tmp_path):
    asyncio.run(_assert_repeated_server_overload_uses_exponential_backoff(tmp_path))


async def _assert_repeated_server_overload_uses_exponential_backoff(tmp_path):
    bus = EventService(tmp_path / "events.db")
    sleep = RecordingSleep()
    plugin = ServerOverloadedRetryPlugin(sleep=sleep)
    first_request = await bus.append_message(
        LlmRunRequested(session_id="sess_1", provider="provider", model="model", run_id="llm_1")
    )
    first_error = await _append_overloaded_response(bus, first_request, correlation_id=first_request.id)
    await plugin.process_pending(bus)

    requests = bus.replay(EventFilter(names=frozenset({LlmRunRequested.name}), tags={"session": "sess_1"}))
    second_request = requests[-1]
    await _append_overloaded_response(bus, second_request, correlation_id=first_request.id)
    await plugin.process_pending(bus)

    requests = bus.replay(EventFilter(names=frozenset({LlmRunRequested.name}), tags={"session": "sess_1"}))
    assert sleep.delays == [30.0, 60.0]
    assert len(requests) == 3
    assert requests[-1].payload["metadata"]["retry_attempt"] == 2
    assert requests[-1].payload["metadata"]["retry_delay_seconds"] == 60.0
    assert requests[-1].correlation_id == first_request.id
    assert requests[1].causation_id == first_error.id


async def _append_overloaded_response(bus, request, *, correlation_id):
    return await bus.append_message(
        AssistantMessageCreated(
            session_id=request.tags["session"],
            content="",
            provider=request.tags["provider"],
            model=request.tags["model"],
            run_id=request.tags["run"],
            metadata={"provider_response": {"error": {"code": "server_is_overloaded"}}},
        ),
        causation_id=request.id,
        correlation_id=correlation_id,
    )


def test_non_overload_provider_error_is_not_retried(tmp_path):
    bus = EventService(tmp_path / "events.db")
    sleep = RecordingSleep()
    plugin = ServerOverloadedRetryPlugin(sleep=sleep)
    asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                content="",
                provider="provider",
                model="model",
                run_id="llm_1",
                metadata={"provider_response": {"error": {"code": "invalid_request"}}},
            )
        )
    )

    asyncio.run(plugin.process_pending(bus))

    assert sleep.delays == []
    assert bus.replay(EventFilter(names=frozenset({LlmRunRequested.name}))) == []


def test_retry_is_idempotent_when_overload_event_is_replayed(tmp_path):
    bus = EventService(tmp_path / "events.db")
    sleep = RecordingSleep()
    plugin = ServerOverloadedRetryPlugin(sleep=sleep)
    request = asyncio.run(
        bus.append_message(LlmRunRequested(session_id="sess_1", provider="provider", model="model", run_id="llm_1"))
    )
    asyncio.run(_append_overloaded_response(bus, request, correlation_id=request.id))

    asyncio.run(plugin.process_pending(bus))
    # Simulate replaying the assistant event despite the durable cursor.
    bus.ack(plugin.subscriber, 0)
    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({LlmRunRequested.name}), tags={"session": "sess_1"}))
    assert len(requests) == 2
    assert sleep.delays == [30.0]


def test_server_overloaded_retry_is_registered_as_its_own_builtin_plugin(tmp_path, monkeypatch):
    from llm_harness.builtin import register
    from llm_harness.plugins import Registry

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    registry = Registry()

    register(registry, bus=bus)

    plugins = [plugin for plugin in registry.event_consumer_plugins if plugin.name == "server-overloaded-retry"]
    assert len(plugins) == 1
    assert plugins[0].subscriber == "plugin:server-overloaded-retry"
    assert plugins[0].event_filter.names == frozenset({AssistantMessageCreated.name})
