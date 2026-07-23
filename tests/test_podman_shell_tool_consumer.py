from __future__ import annotations

import asyncio

from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import SessionCreated, ToolCallRequested, ToolResult
from llm_harness.tools.podman_shell import PodmanShellTool


class FakePodmanShellTool(PodmanShellTool):
    def __init__(self, *, settings):
        super().__init__(settings=settings)
        self.calls = []

    async def run(self, call):
        self.calls.append(call)
        return ToolResult(output="hello\n", metadata={"container": "fake-container"})


def test_podman_shell_consumes_tool_request_and_writes_tool_message(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    tool = FakePodmanShellTool(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1", session_tags=("project-a",))))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="podman-shell",
                input={"cmd": "echo hello"},
                run_id="tool_1",
            )
        )
    )

    asyncio.run(tool.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert messages[0].payload["content"] == "hello\n"
    assert messages[0].payload["metadata"]["container"] == "fake-container"
    assert messages[0].causation_id == request.id
    assert tool.calls[0].session.id == "sess_1"
    assert tool.calls[0].session.tags == ("project-a",)
    assert tool.calls[0].input == {"cmd": "echo hello"}


def test_podman_shell_consumer_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    tool = FakePodmanShellTool(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="podman-shell",
                input={"cmd": "echo hello"},
                run_id="tool_1",
            )
        )
    )

    asyncio.run(tool.process_pending(bus))
    asyncio.run(tool.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert len(tool.calls) == 1
