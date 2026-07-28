from __future__ import annotations

import asyncio
import logging


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
                tool="terminal",
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
    assert "starting tool execution tool=terminal session=sess_1 run=tool_1" in caplog.text
    assert "finished tool execution tool=terminal session=sess_1 run=tool_1" in caplog.text


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
                tool="terminal",
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


def test_podman_shell_consumer_writes_tool_message_for_exception(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    tool = FailingPodmanShellTool(settings=Settings.from_env())
    consumer = PodmanShellToolConsumer(tool=tool)

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="terminal",
                input={"cmd": "echo hello"},
                run_id="tool_1",
            )
        )
    )

    with caplog.at_level(logging.INFO, logger="llm_harness.tools.podman_shell"):
        asyncio.run(consumer.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert messages[0].payload["content"] == "tool execution failed: RuntimeError: boom\n"
    assert messages[0].payload["metadata"] == {
        "success": False,
        "error_type": "RuntimeError",
        "error": "boom",
    }
    assert messages[0].causation_id == request.id
    assert bus.last_acked(consumer.subscriber) == request.id
    assert "starting tool execution tool=terminal session=sess_1 run=tool_1" in caplog.text
    assert "tool execution failed tool=terminal session=sess_1 run=tool_1" in caplog.text


def test_podman_shell_returns_result_for_nonzero_exit(monkeypatch):
    settings = Settings.from_env()
    tool = PodmanShellTool(settings=settings)

    async def fake_ensure_container(name):
        assert name == "llm-harness-session-sess_1"

    async def fake_stop_container(name):
        assert name == "llm-harness-session-sess_1"

    async def fake_create_subprocess_exec(*args, stdout=None, stderr=None):
        assert args == ("podman", "exec", "llm-harness-session-sess_1", "sh", "-lc", "bad-command")
        return FakeProcess(returncode=2, stdout=b"partial\n", stderr=b"not found\n")

    monkeypatch.setattr("shutil.which", lambda name: "/bin/podman" if name == "podman" else None)
    monkeypatch.setattr(tool, "_ensure_container", fake_ensure_container)
    monkeypatch.setattr(tool, "_stop_container", fake_stop_container)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    result = asyncio.run(
        tool.run(
            ToolCall(
                session=ToolSession(id="sess_1"),
                name="terminal",
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


def test_podman_shell_starts_existing_stopped_container(monkeypatch):
    tool = PodmanShellTool(settings=Settings.from_env())
    commands = []

    async def fake_create_subprocess_exec(*args, stdout=None, stderr=None):
        commands.append(args)
        if args[:3] == ("podman", "container", "inspect"):
            return FakeProcess(returncode=0, stdout=b"false\n")
        if args[:2] == ("podman", "start"):
            return FakeProcess(returncode=0, stdout=b"llm-harness-session-sess_1\n")
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    asyncio.run(tool._ensure_container("llm-harness-session-sess_1"))

    assert commands == [
        (
            "podman",
            "container",
            "inspect",
            "--format",
            "{{.State.Running}}",
            "llm-harness-session-sess_1",
        ),
        ("podman", "start", "llm-harness-session-sess_1"),
    ]


def test_podman_shell_stops_container_without_podman_grace_period(monkeypatch):
    tool = PodmanShellTool(settings=Settings.from_env())
    commands = []

    async def fake_create_subprocess_exec(*args, stdout=None, stderr=None):
        commands.append(args)
        return FakeProcess(returncode=0, stdout=b"llm-harness-session-sess_1\n")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    asyncio.run(tool._stop_container("llm-harness-session-sess_1"))

    assert commands == [("podman", "stop", "--time", "0", "llm-harness-session-sess_1")]


def test_podman_shell_stops_container_after_last_parallel_command(monkeypatch):
    tool = PodmanShellTool(settings=Settings.from_env())
    started = [asyncio.Event(), asyncio.Event()]
    finish = [asyncio.Event(), asyncio.Event()]
    ensure_calls = []
    stop_calls = []
    process_count = 0

    class BlockingProcess(FakeProcess):
        def __init__(self, index):
            super().__init__(returncode=0, stdout=f"result {index}\n".encode())
            self.index = index

        async def communicate(self):
            started[self.index].set()
            await finish[self.index].wait()
            return self.stdout, self.stderr

    async def fake_ensure_container(name):
        ensure_calls.append(name)

    async def fake_stop_container(name):
        stop_calls.append(name)

    async def fake_create_subprocess_exec(*args, stdout=None, stderr=None):
        nonlocal process_count
        assert args[:2] == ("podman", "exec")
        process = BlockingProcess(process_count)
        process_count += 1
        return process

    monkeypatch.setattr("shutil.which", lambda name: "/bin/podman" if name == "podman" else None)
    monkeypatch.setattr(tool, "_ensure_container", fake_ensure_container)
    monkeypatch.setattr(tool, "_stop_container", fake_stop_container)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    async def run_parallel_commands():
        call = ToolCall(session=ToolSession(id="sess_1"), name="terminal", input={"cmd": "wait"})
        first = asyncio.create_task(tool.run(call))
        await started[0].wait()
        second = asyncio.create_task(tool.run(call))
        await started[1].wait()

        assert ensure_calls == ["llm-harness-session-sess_1"]
        assert stop_calls == []

        finish[0].set()
        assert (await first).output == "result 0\n"
        assert stop_calls == []

        finish[1].set()
        assert (await second).output == "result 1\n"
        assert stop_calls == ["llm-harness-session-sess_1"]

    asyncio.run(run_parallel_commands())
