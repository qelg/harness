from __future__ import annotations

import asyncio
import json

import pytest

from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunRequested,
    LlmRunFailed,
    ModelSelected,
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
    assert result.metadata == {}

    configured = asyncio.run(
        tool.run(
            ToolCall(
                session=ToolSession(id="sess_parent"),
                name="subagent",
                input={"context": "Investigate", "model": "  specialist-model  "},
            )
        )
    )
    assert configured.metadata == {"model": "specialist-model"}

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

    with pytest.raises(ValueError, match="field 'model' must be a non-empty string"):
        asyncio.run(
            tool.run(
                ToolCall(
                    session=ToolSession(id="sess_parent"),
                    name="subagent",
                    input={"context": "Investigate", "model": "  "},
                )
            )
        )


def test_subagent_inherits_calling_model_and_run_options(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    calling_run = asyncio.run(
        bus.append_message(
            LlmRunRequested(
                session_id="sess_parent",
                provider="openrouter",
                model="calling-model",
                run_id="llm_parent",
                toolsets=("default",),
                thinking_level="high",
                reasoning_summary=True,
            )
        )
    )
    calling_assistant = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_parent",
                content="delegating",
                provider="openrouter",
                model="calling-model",
                run_id="llm_parent",
            ),
            causation_id=calling_run.id,
        )
    )
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent",
                input={"context": "Investigate."},
                run_id="call_subagent_1",
            ),
            causation_id=calling_assistant.id,
        )
    )

    asyncio.run(plugin.process_event(bus, request))

    child = bus.replay(
        EventFilter(
            names=frozenset({SessionCreated.name}),
            tags={"parent_session": "sess_parent"},
        )
    )[0]
    selection = bus.replay(
        EventFilter(
            names=frozenset({ModelSelected.name}),
            tags={"session": child.tags["session"]},
        )
    )[0]
    assert selection.tags["provider"] == "openrouter"
    assert selection.tags["model"] == "calling-model"
    assert selection.payload["toolsets"] == ["default"]
    assert selection.payload["thinking_level"] == "high"
    assert selection.payload["reasoning_summary"] is True


def test_subagent_model_parameter_overrides_calling_model(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    calling_assistant = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_parent",
                content="delegating",
                provider="openrouter",
                model="calling-model",
                run_id="llm_parent",
            )
        )
    )
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent",
                input={"context": "Investigate.", "model": "specialist-model"},
                run_id="call_subagent_1",
            ),
            causation_id=calling_assistant.id,
        )
    )

    asyncio.run(plugin.process_event(bus, request))

    selection = bus.replay(
        EventFilter(names=frozenset({ModelSelected.name}))
    )[0]
    assert selection.tags["provider"] == "openrouter"
    assert selection.tags["model"] == "specialist-model"
    acknowledgement = bus.replay(
        EventFilter(
            names=frozenset({"chat.message.tool.created"}),
            tags={"run": "call_subagent_1"},
        )
    )[0]
    assert acknowledgement.payload["metadata"]["model"] == "specialist-model"


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


def test_subagent_controls_validate_and_override_independently(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    calling_run = asyncio.run(
        bus.append_message(
            LlmRunRequested(
                session_id="sess_parent",
                provider="calling-provider",
                model="calling-model",
                run_id="llm_parent",
                toolsets=("default", "research"),
                thinking_level="low",
                reasoning_summary=True,
            )
        )
    )
    assistant = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_parent",
                content="delegate",
                provider="calling-provider",
                model="calling-model",
                run_id="llm_parent",
            ),
            causation_id=calling_run.id,
        )
    )
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent",
                input={
                    "context": "work",
                    "provider": "other-provider",
                    "thinking_level": "none",
                    "same_container": True,
                },
                run_id="call_subagent",
            ),
            causation_id=assistant.id,
        )
    )
    asyncio.run(plugin.process_event(bus, request))
    child = bus.replay(EventFilter(names=frozenset({SessionCreated.name}), tags={"parent_session": "sess_parent"}))[0]
    selection = bus.replay(EventFilter(names=frozenset({ModelSelected.name}), tags={"session": child.tags["session"]}))[0]
    assert selection.tags["provider"] == "other-provider"
    assert selection.tags["model"] == "calling-model"
    assert selection.payload["toolsets"] == ["default", "research"]
    assert selection.payload["thinking_level"] == "none"
    assert selection.payload["reasoning_summary"] is True
    assert child.payload["metadata"]["terminal_container_owner_session_id"] == "sess_parent"

    with pytest.raises(ValueError, match="must be a boolean"):
        asyncio.run(
            SubagentTool().run(
                ToolCall(
                    session=ToolSession(id="sess_parent"),
                    name="subagent",
                    input={"context": "work", "same_container": 1},
                )
            )
        )
    with pytest.raises(ValueError, match="thinking_level"):
        asyncio.run(
            SubagentTool().run(
                ToolCall(
                    session=ToolSession(id="sess_parent"),
                    name="subagent",
                    input={"context": "work", "thinking_level": "maximum"},
                )
            )
        )


def test_subagent_state_wait_is_event_derived_and_suppresses_legacy_copy(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_parent")))
    start = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent",
                input={"context": "work"},
                run_id="start",
            )
        )
    )
    asyncio.run(plugin.process_event(bus, start))
    child_id = bus.replay(EventFilter(names=frozenset({SessionCreated.name}), tags={"parent_session": "sess_parent"}))[0].tags["session"]
    wait = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_parent",
                tool="subagent_state",
                input={"session_ids": [child_id], "wait_for": "all"},
                run_id="state",
            )
        )
    )
    asyncio.run(plugin.process_event(bus, wait))
    assert not bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "state"}))

    child_answer = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id=child_id,
                content="answer from child",
                provider="mock-llm",
                model="test-model",
                run_id="child-run",
            )
        )
    )
    child_finished = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id=child_id,
                state="finished",
                source_event_id=child_answer.id,
                read="unread",
            )
        )
    )
    asyncio.run(plugin.process_event(bus, child_finished))
    result = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "state"}))
    assert len(result) == 1
    assert json.loads(result[0].payload["content"])["states"][0] == {
        "session_id": child_id,
        "state": "finished",
        "result": "answer from child",
    }

    parent_answer = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                session_id="sess_parent",
                content="parent",
                provider="mock-llm",
                model="test-model",
                run_id="parent-run",
            )
        )
    )
    parent_finished = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id="sess_parent",
                state="finished",
                source_event_id=parent_answer.id,
                read="unread",
            )
        )
    )
    asyncio.run(plugin.process_event(bus, parent_finished))
    assert _copied_responses(bus) == []
    asyncio.run(plugin.process_event(bus, parent_finished))
    assert len(result) == 1


def test_subagent_state_rejects_non_children_and_reports_failure(tmp_path):
    bus = EventService(tmp_path / "events.db")
    plugin = SubagentPlugin(tool=SubagentTool())
    asyncio.run(bus.append_message(SessionCreated(session_id="parent")))
    asyncio.run(bus.append_message(SessionCreated(session_id="unrelated")))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="parent",
                tool="subagent_state",
                input={"session_ids": ["unrelated"]},
                run_id="bad",
            )
        )
    )
    with pytest.raises(ValueError, match="direct|children"):
        asyncio.run(plugin.process_event(bus, request))

    # A real child is created by the normal subagent flow.
    start = asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="parent", tool="subagent", input={"context": "x"}, run_id="start")
        )
    )
    asyncio.run(plugin.process_event(bus, start))
    child_id = bus.replay(EventFilter(names=frozenset({SessionCreated.name}), tags={"parent_session": "parent"}))[0].tags["session"]
    failed_run = asyncio.run(
        bus.append_message(
            LlmRunFailed(
                session_id=child_id,
                provider="mock-llm",
                model="test-model",
                run_id="failed",
                error="provider unavailable",
            )
        )
    )
    finished = asyncio.run(
        bus.append_message(
            SessionStateChanged(
                session_id=child_id,
                state="finished",
                source_event_id=failed_run.id,
                outcome="failed",
                read="unread",
            )
        )
    )
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="parent",
                tool="subagent_state",
                input={"session_ids": [child_id]},
                run_id="failure-state",
            )
        )
    )
    asyncio.run(plugin.process_event(bus, finished))
    asyncio.run(plugin.process_event(bus, request))
    payload = json.loads(bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "failure-state"}))[0].payload["content"])
    assert payload["states"][0] == {
        "session_id": child_id,
        "state": "failed",
        "error": "provider unavailable",
    }
