from __future__ import annotations

import asyncio

from llm_harness.builtin_plugins.tool_call_requester import ToolCallRequesterPlugin
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import AssistantMessageCreated


def test_tool_call_requester_creates_requests_from_responses_output(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = ToolCallRequesterPlugin()

    assistant = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                provider="chatgpt-codex",
                model="codex",
                run_id="llm_1",
                content=[
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "podman-shell",
                        "arguments": "{\"cmd\":\"echo hello\"}",
                    }
                ],
            )
        )
    )

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"tool.call.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
    assert requests[0].tags["tool"] == "podman-shell"
    assert requests[0].tags["run"] == "call_1"
    assert requests[0].payload["input"] == {"cmd": "echo hello"}
    assert requests[0].causation_id == assistant.id


def test_tool_call_requester_creates_multiple_requests_from_openai_tool_calls(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = ToolCallRequesterPlugin()

    asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                provider="openrouter",
                model="test",
                run_id="llm_1",
                content=[
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {"name": "podman-shell", "arguments": "{\"cmd\":\"echo one\"}"},
                            },
                            {
                                "id": "call_2",
                                "type": "function",
                                "function": {"name": "podman-shell", "arguments": "{\"cmd\":\"echo two\"}"},
                            },
                        ],
                    }
                ],
            )
        )
    )

    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"tool.call.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 2
    assert [request.tags["run"] for request in requests] == ["call_1", "call_2"]
    assert [request.payload["input"]["cmd"] for request in requests] == ["echo one", "echo two"]


def test_tool_call_requester_is_idempotent_for_same_assistant_message(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = ToolCallRequesterPlugin()

    asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_1",
                provider="chatgpt-codex",
                model="codex",
                run_id="llm_1",
                content=[{"type": "function_call", "name": "podman-shell", "arguments": "{\"cmd\":\"echo hello\"}"}],
            )
        )
    )

    asyncio.run(plugin.process_pending(bus))
    asyncio.run(plugin.process_pending(bus))

    requests = bus.replay(EventFilter(names=frozenset({"tool.call.requested"}), tags={"session": "sess_1"}))
    assert len(requests) == 1
