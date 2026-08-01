from __future__ import annotations

import asyncio

from llm_harness.builtin_plugins.tool_result_llm_requester import ToolResultLlmRequesterPlugin
from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import (
    AssistantMessageCreated,
    ModelSelected,
    SessionCreated,
    ToolCallRequested,
    ToolMessageCreated,
)


def test_tool_result_waits_for_parallel_tool_requests_before_requesting_llm(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = ToolResultLlmRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(ModelSelected(provider="mock-llm", model="mock-model", toolsets=("default",))))
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    assistant = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                provider="mock-llm",
                model="mock-model",
                run_id="llm_1",
                content=[
                    {"type": "function_call", "call_id": "tool_1", "name": "terminal"},
                    {"type": "function_call", "call_id": "tool_2", "name": "terminal"},
                ],
            )
        )
    )
    tool_1 = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="terminal", input={"cmd": "echo one"}, run_id="tool_1"),
            causation_id=assistant.id,
            correlation_id=assistant.id,
        )
    )
    tool_2 = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="terminal", input={"cmd": "echo two"}, run_id="tool_2"),
            causation_id=assistant.id,
            correlation_id=assistant.id,
        )
    )

    result_1 = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="terminal", content="one\n", run_id="tool_1"),
            causation_id=tool_1.id,
            correlation_id=assistant.id,
        )
    )
    asyncio.run(plugin.process_pending(bus))

    assert bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"})) == []

    result_2 = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="terminal", content="two\n", run_id="tool_2"),
            causation_id=tool_2.id,
            correlation_id=assistant.id,
        )
    )
    asyncio.run(plugin.process_pending(bus))
    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
    assert requests[0].tags["provider"] == "mock-llm"
    assert requests[0].tags["model"] == "mock-model"
    assert requests[0].payload["toolsets"] == ["default"]
    assert requests[0].payload["metadata"] == {
        "trigger": "tool_results_completed",
        "assistant_message_event_id": assistant.id,
        "tool_request_event_ids": [tool_1.id, tool_2.id],
        "tool_result_event_ids": [result_1.id, result_2.id],
    }
    assert requests[0].causation_id == assistant.id
    assert requests[0].correlation_id == assistant.id


def test_tool_result_ignores_results_without_tool_request_causation(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = ToolResultLlmRequesterPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    asyncio.run(
        bus.append_message(ToolMessageCreated(session_id="sess_1", tool="terminal", content="one\n", run_id="tool_1"))
    )

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert requests == []


def test_parallel_results_for_one_assistant_request_one_follow_up_run(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = ToolResultLlmRequesterPlugin(settings=Settings.from_env())
    asyncio.run(bus.append_message(ModelSelected(provider="mock-llm", model="mock-model")))
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    assistant = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                provider="mock-llm",
                model="mock-model",
                run_id="llm_1",
                content=[],
            )
        )
    )
    first = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="terminal", input={}, run_id="one"),
            causation_id=assistant.id,
        )
    )
    second = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="terminal", input={}, run_id="two"),
            causation_id=assistant.id,
        )
    )
    first_result = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="terminal", content="one", run_id="one"),
            causation_id=first.id,
        )
    )
    second_result = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="terminal", content="two", run_id="two"),
            causation_id=second.id,
        )
    )

    async def process_both():
        await asyncio.gather(
            plugin.process_event(bus, first_result), plugin.process_event(bus, second_result)
        )

    asyncio.run(process_both())
    runs = bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))
    assert len(runs) == 1
    assert runs[0].payload["metadata"]["tool_result_event_ids"] == [
        first_result.id,
        second_result.id,
    ]


def test_wrong_result_tags_do_not_complete_another_tool_request(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = ToolResultLlmRequesterPlugin(settings=Settings.from_env())
    asyncio.run(bus.append_message(ModelSelected(provider="mock-llm", model="mock-model")))
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    assistant = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1", provider="mock-llm", model="mock-model", run_id="llm_1", content=[]
            )
        )
    )
    first = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="terminal", input={}, run_id="one"), causation_id=assistant.id
        )
    )
    second = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="terminal", input={}, run_id="two"), causation_id=assistant.id
        )
    )
    forged = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="terminal", content="bad", run_id="wrong"), causation_id=first.id
        )
    )
    valid_second = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="terminal", content="two", run_id="two"), causation_id=second.id
        )
    )
    asyncio.run(plugin.process_event(bus, forged))
    asyncio.run(plugin.process_event(bus, valid_second))
    assert bus.replay(EventFilter(names=frozenset({"llm.run.requested"}))) == []

    valid_first = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="terminal", content="one", run_id="one"), causation_id=first.id
        )
    )
    asyncio.run(plugin.process_event(bus, valid_first))
    assert len(bus.replay(EventFilter(names=frozenset({"llm.run.requested"})))) == 1
