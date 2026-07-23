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
                    {"type": "function_call", "call_id": "tool_1", "name": "podman-shell"},
                    {"type": "function_call", "call_id": "tool_2", "name": "podman-shell"},
                ],
            )
        )
    )
    tool_1 = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="podman-shell", input={"cmd": "echo one"}, run_id="tool_1"),
            causation_id=assistant.id,
            correlation_id=assistant.id,
        )
    )
    tool_2 = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="podman-shell", input={"cmd": "echo two"}, run_id="tool_2"),
            causation_id=assistant.id,
            correlation_id=assistant.id,
        )
    )

    result_1 = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="podman-shell", content="one\n", run_id="tool_1"),
            causation_id=tool_1.id,
            correlation_id=assistant.id,
        )
    )
    asyncio.run(plugin.process_pending(bus))

    assert bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"})) == []

    result_2 = asyncio.run(
        bus.append_message(
            ToolMessageCreated(session_id="sess_1", tool="podman-shell", content="two\n", run_id="tool_2"),
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
        bus.append_message(ToolMessageCreated(session_id="sess_1", tool="podman-shell", content="one\n", run_id="tool_1"))
    )

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"llm.run.requested"}), tags={"session": "sess_1"}))
    assert requests == []
