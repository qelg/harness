from __future__ import annotations

import asyncio

import pytest

from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import (
    AssistantMessageCreated,
    SessionCreated,
    SessionStateChanged,
    ToolCall,
    ToolCallRequested,
    ToolSession,
)
from llm_harness.tools.subagent import (
    SUBAGENT_RESPONSE_PREFIX,
    SubagentPlugin,
    SubagentTool,
)


def test_subagent_tool_requires_context():
    tool = SubagentTool()

    assert tool.name == "subagent"
    assert tool.input_schema["required"] == ["context"]
    result = asyncio.run(
        tool.run(
            ToolCall(
                session=ToolSession(id="sess_parent"),
                name="subagent",
                input={"context": "Investigate the failure"},
            )
        )
    )
    assert result.output == "Investigate the failure"

    with pytest.raises(ValueError, match="non-empty string field 'context'"):
        asyncio.run(
            tool.run(
                ToolCall(
                    session=ToolSession(id="sess_parent"),
                    name="subagent",
                    input={"context": "  "},
                )
            )
        )


def test_tool_request_starts_tagged_child_and_acknowledges_session_id(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent",
                input={"context": "Inspect the parser and report back."},
                run_id="call_subagent_1",
            ),
            correlation_id=42,
        )
    )

    asyncio.run(plugin.process_event(bus, request))
    asyncio.run(plugin.process_event(bus, request))

    children = bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}),
            tags={"parent_session": "sess_parent"},
        )
    )
    assert len(children) == 1
    child = children[0]
    child_session_id = child.tags["session"]
    assert child.payload == {
        "title": "subagent",
        "tags": ["subagent"],
        "parent_session": "sess_parent",
    }
    assert child.tags["session_tag:subagent"] == "true"
    assert child.producer == "subagent"
    assert child.causation_id == request.id
    assert child.correlation_id == 42

    child_messages = bus.replay(
        EventFilter(
            names=frozenset({"chat.message.user.created"}),
            tags={"session": child_session_id},
        )
    )
    assert len(child_messages) == 1
    assert child_messages[0].payload["content"] == "Inspect the parser and report back."
    assert child_messages[0].payload["metadata"]["tool_request_event_id"] == request.id

    acknowledgements = bus.replay(
        EventFilter(
            names=frozenset({"chat.message.tool.created"}),
            tags={"session": "sess_parent", "tool": "subagent", "run": "call_subagent_1"},
        )
    )
    assert len(acknowledgements) == 1
    assert child_session_id in acknowledgements[0].payload["content"]
    assert acknowledgements[0].payload["metadata"]["subagent_session_id"] == child_session_id
    assert acknowledgements[0].causation_id == request.id


def test_response_waits_for_child_and_parent_to_finish_and_is_copied_once(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent",
                input={"context": "Find the answer."},
                run_id="call_subagent_1",
            )
        )
    )
    asyncio.run(plugin.process_event(bus, request))
    child_session_id = bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}),
            tags={"parent_session": "sess_parent"},
        )
    )[0].tags["session"]

    child_response = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id=child_session_id,
                content=[
                    {"type": "reasoning", "summary": []},
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": "The answer is 42."}],
                    },
                ],
                provider="mock-llm",
                model="test-model",
                run_id="llm_child",
            )
        )
    )
    child_finished = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id=child_session_id,
                state="finished",
                source_event_id=child_response.id,
                read="unread",
            ),
            correlation_id=request.id,
        )
    )

    asyncio.run(plugin.process_event(bus, child_finished))
    assert _copied_responses(bus) == []

    parent_response = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_parent",
                content="Waiting for the subagent.",
                provider="mock-llm",
                model="test-model",
                run_id="llm_parent",
            )
        )
    )
    parent_finished = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id="sess_parent",
                state="finished",
                source_event_id=parent_response.id,
                read="unread",
            )
        )
    )

    asyncio.run(plugin.process_event(bus, parent_finished))
    asyncio.run(plugin.process_event(bus, parent_finished))
    asyncio.run(plugin.process_event(bus, child_finished))

    copied = _copied_responses(bus)
    assert len(copied) == 1
    assert copied[0].payload["content"] == (
        f"{SUBAGENT_RESPONSE_PREFIX} The answer is 42."
    )
    assert copied[0].payload["metadata"] == {
        "subagent": True,
        "subagent_session_id": child_session_id,
        "subagent_response_event_id": child_response.id,
        "subagent_state_event_id": child_finished.id,
    }
    assert copied[0].causation_id == child_finished.id


def test_response_does_not_use_a_historical_finished_parent_state(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    old_parent_response = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_parent",
                content="old response",
                provider="mock-llm",
                model="test-model",
                run_id="llm_old",
            )
        )
    )
    asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id="sess_parent",
                state="finished",
                source_event_id=old_parent_response.id,
                read="unread",
            )
        )
    )
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent",
                input={"context": "Do work."},
                run_id="call_subagent_1",
            )
        )
    )
    asyncio.run(plugin.process_event(bus, request))
    child_session_id = bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}),
            tags={"parent_session": "sess_parent"},
        )
    )[0].tags["session"]
    child_response = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id=child_session_id,
                content="done",
                provider="mock-llm",
                model="test-model",
                run_id="llm_child",
            )
        )
    )
    child_finished = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id=child_session_id,
                state="finished",
                source_event_id=child_response.id,
                read="unread",
            )
        )
    )

    asyncio.run(plugin.process_event(bus, child_finished))

    assert _copied_responses(bus) == []


def _copied_responses(bus: EventService):
    return [
        event
        for event in bus.replay(
            EventFilter(
                names=frozenset({"chat.message.user.created"}),
                tags={"session": "sess_parent"},
            )
        )
        if event.producer == "subagent"
        and event.payload.get("metadata", {}).get("subagent_session_id")
    ]
