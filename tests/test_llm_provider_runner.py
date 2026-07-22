from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import LlmRunRequested, Message, SessionCreated, ToolMessageCreated, UserMessageCreated
from llm_harness.plugins import Registry
from plugins.llm_provider_runner import LlmProviderRunnerPlugin


class CapturingProvider:
    name = "capture"

    def __init__(self) -> None:
        self.messages: Sequence[Message] = ()

    async def stream_chat(self, *, model: str, messages: Sequence[Message]) -> AsyncIterator[str]:
        self.messages = messages
        yield "hel"
        yield "lo"


def test_llm_provider_runner_streams_deltas_and_creates_assistant_message(tmp_path):
    asyncio.run(_assert_llm_provider_runner_streams_deltas_and_creates_assistant_message(tmp_path))


async def _assert_llm_provider_runner_streams_deltas_and_creates_assistant_message(tmp_path):
    bus = EventService(tmp_path / "events.db")
    provider = CapturingProvider()
    registry = Registry()
    registry.add_provider(provider)
    plugin = LlmProviderRunnerPlugin()

    await bus.append_message(SessionCreated(session_id="sess_1"))
    await bus.append_message(UserMessageCreated(session_id="sess_1", content="hello"))
    await bus.append_message(ToolMessageCreated(session_id="sess_1", content="tool output", tool="shell", run_id="tool_1"))
    request = await bus.append_message(
        LlmRunRequested(session_id="sess_1", provider="capture", model="test-model", run_id="llm_1")
    )

    async with bus.subscribe(EventFilter(names=frozenset({"llm.delta"}), tags={"run": "llm_1"})) as queue:
        await plugin.process_pending(bus, registry=registry)
        deltas = [queue.get_nowait(), queue.get_nowait()]

    assert [message.role.value for message in provider.messages] == ["user", "tool"]
    assert [message.content for message in provider.messages] == ["hello", "tool output"]
    assert [event.payload["delta"] for event in deltas] == ["hel", "lo"]
    assert [event.durable for event in deltas] == [False, False]
    assert bus.replay(EventFilter(names=frozenset({"llm.delta"}), tags={"run": "llm_1"})) == []

    started = bus.replay(EventFilter(names=frozenset({"llm.run.started"}), tags={"run": "llm_1"}))
    assistant = bus.replay(EventFilter(names=frozenset({"chat.message.assistant.created"}), tags={"run": "llm_1"}))
    assert len(started) == 1
    assert len(assistant) == 1
    assert assistant[0].payload["content"] == "hello"
    assert assistant[0].causation_id == request.id


def test_llm_provider_runner_writes_failed_event_for_unknown_provider(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = LlmProviderRunnerPlugin()

    request = asyncio.run(
        bus.append_message(LlmRunRequested(session_id="sess_1", provider="missing", model="test", run_id="llm_1"))
    )

    asyncio.run(plugin.process_pending(bus, registry=Registry()))

    failed = bus.replay(EventFilter(names=frozenset({"llm.run.failed"}), tags={"run": "llm_1"}))
    assert len(failed) == 1
    assert failed[0].payload["error"] == "unknown provider: missing"
    assert failed[0].causation_id == request.id
