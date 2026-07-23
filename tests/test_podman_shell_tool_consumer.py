from __future__ import annotations

import asyncio
import logging

import pytest

from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import SessionCreated, ToolCall, ToolCallRequested, ToolResult, ToolSession
from llm_harness.tools.podman_shell import PodmanShellTool, PodmanShellToolConsumer


class FakePodmanShellTool(PodmanShellTool):
    def __init__(self, *, settings):
        super().__init__(settings=settings)
        self.calls = []

    async def run(self, call):
        self.calls.append(call)
        return ToolResult(output="hello\n", metadata={"container": "fake-container"})


class FailingPodmanShellTool(PodmanShellTool):
    async def run(self, call):
        raise RuntimeError("boom")


class FakeProcess:
    def __init__(self, *, returncode, stdout=b"", stderr=b""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.killed = False

    async def communicate(self):
        return self.stdout, self.stderr

    def kill(self):
        self.killed = True


def test_podman_shell_consumes_tool_request_and_writes_tool_message(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    tool = FakePodmanShellTool(settings=Settings.from_env())
    consumer = PodmanShellToolConsumer(tool=tool)

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

    with caplog.at_level(logging.INFO, logger="llm_harness.tools.podman_shell"):
        asyncio.run(consumer.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert messages[0].payload["content"] == "hello\n"
    assert messages[0].payload["metadata"]["container"] == "fake-container"
    assert messages[0].causation_id == request.id
    assert tool.calls[0].session.id == "sess_1"
    assert tool.calls[0].session.tags == ("project-a",)
    assert tool.calls[0].input == {"cmd": "echo hello"}
    assert "starting tool execution tool=podman-shell session=sess_1 run=tool_1" in caplog.text
    assert "finished tool execution tool=podman-shell session=sess_1 run=tool_1" in caplog.text


def test_podman_shell_consumer_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    tool = FakePodmanShellTool(settings=Settings.from_env())
    consumer = PodmanShellToolConsumer(tool=tool)

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

    asyncio.run(consumer.process_pending(bus))
    asyncio.run(consumer.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert len(tool.calls) == 1


def test_podman_shell_consumer_logs_failure(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    tool = FailingPodmanShellTool(settings=Settings.from_env())
    consumer = PodmanShellToolConsumer(tool=tool)

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

    with caplog.at_level(logging.INFO, logger="llm_harness.tools.podman_shell"):
        with pytest.raises(RuntimeError, match="boom"):
            asyncio.run(consumer.process_pending(bus))

    assert "starting tool execution tool=podman-shell session=sess_1 run=tool_1" in caplog.text
    assert "tool execution failed tool=podman-shell session=sess_1 run=tool_1" in caplog.text


def test_podman_shell_returns_result_for_nonzero_exit(monkeypatch):
    settings = Settings.from_env()
    tool = PodmanShellTool(settings=settings)

    async def fake_ensure_container(name):
        assert name == "llm-harness-session-sess_1"

    async def fake_create_subprocess_exec(*args, stdout=None, stderr=None):
        assert args == ("podman", "exec", "llm-harness-session-sess_1", "sh", "-lc", "bad-command")
        return FakeProcess(returncode=2, stdout=b"partial\n", stderr=b"not found\n")

    monkeypatch.setattr("shutil.which", lambda name: "/bin/podman" if name == "podman" else None)
    monkeypatch.setattr(tool, "_ensure_container", fake_ensure_container)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    result = asyncio.run(
        tool.run(
            ToolCall(
                session=ToolSession(id="sess_1"),
                name="podman-shell",
                input={"cmd": "bad-command"},
            )
        )
    )

    assert result.output == "command exited with code 2\n\nstdout:\npartial\n\nstderr:\nnot found\n"
    assert result.metadata == {
        "container": "llm-harness-session-sess_1",
        "stderr": "not found\n",
        "exit_code": 2,
        "success": False,
    }
