import asyncio
import json

from llm_harness.builtin_plugins.container_cleanup import ContainerCleanupPlugin, PodmanContainerManager
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import SessionCreated, SessionStateChanged


class Process:
    def __init__(self, returncode=0, stdout=b"", stderr=b""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr

    async def communicate(self):
        return self.stdout, self.stderr


def test_lists_only_session_terminal_containers_and_associates_shared_owner(tmp_path, monkeypatch):
    bus = EventService(tmp_path / "events.db")
    asyncio.run(bus.append_message(SessionCreated(session_id="parent")))
    asyncio.run(bus.append_message(SessionCreated(session_id="child", parent_session_id="parent", metadata={"terminal_container_owner_session_id": "parent"})))
    payload = json.dumps([
        {"Id": "abc", "Names": ["llm-harness-session-parent"], "Size": "1.5MB (virtual 99MB)"},
        {"Id": "other", "Names": ["unrelated"], "Size": "9GB (virtual 9GB)"},
    ]).encode()

    async def subprocess(*args, **kwargs):
        assert args == ("podman", "ps", "--all", "--size", "--format", "json")
        return Process(stdout=payload)

    monkeypatch.setattr("shutil.which", lambda _: "/podman")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", subprocess)
    containers = asyncio.run(PodmanContainerManager().containers(bus))
    assert len(containers) == 1
    assert containers[0].id == "abc"
    assert containers[0].size_bytes == 1_500_000
    assert containers[0].session_ids == ("child", "parent")


def test_archiving_parent_removes_each_container_in_descendant_tree_and_logs_saved_space(tmp_path, monkeypatch, caplog):
    bus = EventService(tmp_path / "events.db")
    asyncio.run(bus.append_message(SessionCreated(session_id="parent")))
    asyncio.run(bus.append_message(SessionCreated(session_id="child", parent_session_id="parent")))
    archive = asyncio.run(bus.append_message(SessionStateChanged(session_id="parent", state="finished", source_event_id=1, read="read", archived=True)))
    responses = [
        Process(stdout=json.dumps([
            {"Id": "parent-id", "Names": ["llm-harness-session-parent"], "Size": "2MB (virtual 2MB)"},
            {"Id": "child-id", "Names": ["llm-harness-session-child"], "Size": "3MB (virtual 3MB)"},
        ]).encode()),
        Process(), Process(),
    ]
    commands = []

    async def subprocess(*args, **kwargs):
        commands.append(args)
        return responses.pop(0)

    monkeypatch.setattr("shutil.which", lambda _: "/podman")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", subprocess)
    with caplog.at_level("INFO"):
        asyncio.run(ContainerCleanupPlugin(manager=PodmanContainerManager()).process_event(bus, archive))
    assert ("podman", "container", "rm", "--force", "parent-id") in commands
    assert ("podman", "container", "rm", "--force", "child-id") in commands
    assert "saved_bytes=5000000" in caplog.text
