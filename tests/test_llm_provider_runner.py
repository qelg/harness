from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import (
    LlmRunRequested,
    Message,
    ProviderStreamEvent,
    SessionCreated,
    ToolMessageCreated,
    ToolSpec,
    UserMessageCreated,
)
from llm_harness.plugins import Registry
from llm_harness.builtin_plugins.llm_provider_runner import LlmProviderRunnerPlugin


class CapturingProvider:
    name = "capture"

    def __init__(self) -> None:
        self.messages: Sequence[Message] = ()
        self.tools: Sequence[ToolSpec] = ()

    async def stream_chat(
        self,
        *,
        model: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> AsyncIterator[str]:
        self.messages = messages
        self.tools = tools
        yield "hel"
        yield "lo"


class TestToolSet:
    name = "test-tools"

    def tools(self, *, registry) -> Sequence[ToolSpec]:
        return [
            ToolSpec(
                name="echo",
                description="Echo input text.",
                input_schema={"type": "object", "properties": {"text": {"type": "string"}}},
            )
        ]


class FullResponseProvider:
    name = "full-response"

    async def stream_response(
        self,
        *,
        model: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> AsyncIterator[ProviderStreamEvent]:
        yield ProviderStreamEvent(type="delta", delta="thinking")
        yield ProviderStreamEvent(
            type="completed",
            response={
                "id": "resp_1",
                "output": [{"type": "function_call", "name": "echo", "arguments": "{\"text\":\"hi\"}"}],
                "usage": {"input_tokens": 10, "output_tokens": 4},
            },
        )


def test_llm_provider_runner_streams_deltas_and_creates_assistant_message(tmp_path):
    asyncio.run(_assert_llm_provider_runner_streams_deltas_and_creates_assistant_message(tmp_path))


async def _assert_llm_provider_runner_streams_deltas_and_creates_assistant_message(tmp_path):
    bus = EventService(tmp_path / "events.db")
    provider = CapturingProvider()
    registry = Registry()
    registry.add_provider(provider)
    registry.add_toolset(TestToolSet())
    plugin = LlmProviderRunnerPlugin()

    await bus.append_message(SessionCreated(session_id="sess_1"))
    await bus.append_message(UserMessageCreated(session_id="sess_1", content="hello"))
    await bus.append_message(ToolMessageCreated(session_id="sess_1", content="tool output", tool="shell", run_id="tool_1"))
    request = await bus.append_message(
        LlmRunRequested(
            session_id="sess_1",
            provider="capture",
            model="test-model",
            run_id="llm_1",
            toolsets=("test-tools",),
        )
    )

    async with bus.subscribe(EventFilter(names=frozenset({"llm.delta"}), tags={"run": "llm_1"})) as queue:
        await plugin.process_pending(bus, registry=registry)
        deltas = [queue.get_nowait(), queue.get_nowait()]

    assert [message.role.value for message in provider.messages] == ["user", "tool"]
    assert [message.content for message in provider.messages] == ["hello", "tool output"]
    assert [tool.name for tool in provider.tools] == ["echo"]
    assert [event.payload["delta"] for event in deltas] == ["hel", "lo"]
    assert [event.durable for event in deltas] == [False, False]
    assert bus.replay(EventFilter(names=frozenset({"llm.delta"}), tags={"run": "llm_1"})) == []

    started = bus.replay(EventFilter(names=frozenset({"llm.run.started"}), tags={"run": "llm_1"}))
    assistant = bus.replay(EventFilter(names=frozenset({"chat.message.assistant.created"}), tags={"run": "llm_1"}))
    assert len(started) == 1
    assert len(assistant) == 1
    assert assistant[0].payload["content"] == "hello"
    assert assistant[0].causation_id == request.id


def test_llm_provider_runner_persists_full_provider_response(tmp_path):
    asyncio.run(_assert_llm_provider_runner_persists_full_provider_response(tmp_path))


async def _assert_llm_provider_runner_persists_full_provider_response(tmp_path):
    bus = EventService(tmp_path / "events.db")
    registry = Registry()
    registry.add_provider(FullResponseProvider())
    plugin = LlmProviderRunnerPlugin()

    await bus.append_message(SessionCreated(session_id="sess_1"))
    await bus.append_message(UserMessageCreated(session_id="sess_1", content="please call echo"))
    await bus.append_message(
        LlmRunRequested(session_id="sess_1", provider="full-response", model="test-model", run_id="llm_1")
    )

    await plugin.process_pending(bus, registry=registry)

    assistant = bus.replay(EventFilter(names=frozenset({"chat.message.assistant.created"}), tags={"run": "llm_1"}))
    assert len(assistant) == 1
    assert '"type": "function_call"' in assistant[0].payload["content"]
    assert '"name": "echo"' in assistant[0].payload["content"]
    assert assistant[0].payload["metadata"]["provider_response"]["output"][0]["type"] == "function_call"
    assert assistant[0].payload["metadata"]["provider_response"]["usage"]["input_tokens"] == 10


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
