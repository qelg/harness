from __future__ import annotations

import asyncio
import json

import pytest

from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import SessionCreated, ToolCall, ToolCallRequested, ToolSession
from llm_harness.tools.tasks import TasksTool, TasksToolConsumer


def call(tool: TasksTool, session: str, input_: dict):
    return asyncio.run(tool.run(ToolCall(session=ToolSession(id=session), name=tool.name, input=input_)))


def state(result):
    return json.loads(result.output)


def test_tasks_are_isolated_and_ids_are_allocated_per_session():
    tool = TasksTool()

    assert state(call(tool, "one", {"actions": [{"type": "add", "name": "first", "state": "todo"}]})) == {
        "tasks": [{"id": 0, "name": "first", "state": "todo"}],
        "total": 1,
        "finished": 0,
        "in_progress": 0,
    }
    assert state(call(tool, "two", {"actions": [{"type": "add", "name": "other", "state": "finished"}]}))["tasks"][0]["id"] == 0

    result = call(
        tool,
        "one",
        {
            "actions": [
                {"type": "update", "id": 0, "name": "renamed", "state": "in_progress"},
                {"type": "add", "name": "second", "state": "finished"},
            ]
        },
    )
    assert state(result) == {
        "tasks": [
            {"id": 0, "name": "renamed", "state": "in_progress"},
            {"id": 1, "name": "second", "state": "finished"},
        ],
        "total": 2,
        "finished": 1,
        "in_progress": 1,
    }

    assert state(call(tool, "one", {"actions": [{"type": "remove", "id": 0, "state": "todo"}]}))["tasks"] == [
        {"id": 1, "name": "second", "state": "finished"}
    ]
    assert state(call(tool, "one", {"actions": [{"type": "add", "name": "third", "state": "todo"}]}))["tasks"][-1]["id"] == 2


def test_task_links_are_optional_and_can_be_updated():
    tool = TasksTool()

    result = call(
        tool,
        "one",
        {
            "actions": [
                {
                    "type": "add",
                    "name": "wait for pull request",
                    "link": "https://github.com/qelg/harness/pull/1",
                    "state": "in_progress",
                }
            ]
        },
    )
    assert state(result)["tasks"] == [
        {
            "id": 0,
            "name": "wait for pull request",
            "link": "https://github.com/qelg/harness/pull/1",
            "state": "in_progress",
        }
    ]

    result = call(
        tool,
        "one",
        {
            "actions": [
                {
                    "type": "update",
                    "id": 0,
                    "link": "https://github.com/qelg/harness/pull/1#checks",
                    "state": "in_progress",
                }
            ]
        },
    )
    assert state(result)["tasks"][0]["link"].endswith("#checks")


def test_no_actions_returns_current_state():
    tool = TasksTool()
    assert state(call(tool, "one", {})) == {
        "tasks": [],
        "total": 0,
        "finished": 0,
        "in_progress": 0,
    }


@pytest.mark.parametrize(
    "action",
    [
        {"type": "add", "state": "todo"},
        {"type": "update", "state": "todo"},
        {"type": "remove", "id": 0},
        {"type": "add", "name": "x", "state": "bad"},
        {"type": "add", "name": "x", "link": "", "state": "todo"},
        {"type": "add", "name": "x", "link": 42, "state": "todo"},
    ],
)
def test_invalid_actions_are_rejected(action):
    with pytest.raises(ValueError):
        call(TasksTool(), "one", {"actions": [action]})


def test_consumer_persists_result_and_replays_state(tmp_path):
    bus = EventService(tmp_path / "events.db")
    tool = TasksTool()
    consumer = TasksToolConsumer(tool=tool)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    first = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="tasks",
                input={"actions": [{"type": "add", "name": "write tests", "state": "todo"}]},
                run_id="tool_1",
            )
        )
    )
    asyncio.run(consumer.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert state(type("Result", (), {"output": messages[0].payload["content"]})()) ["tasks"][0]["id"] == 0
    assert messages[0].payload["metadata"]["next_id"] == 1

    restored = TasksTool()
    asyncio.run(TasksToolConsumer(tool=restored).process_event(bus, first))  # already completed
    second = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="tasks",
                input={"actions": [{"type": "add", "name": "ship", "state": "finished"}]},
                run_id="tool_2",
            )
        )
    )
    asyncio.run(TasksToolConsumer(tool=restored).process_event(bus, second))
    result = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_2"}))[0]
    assert json.loads(result.payload["content"])["tasks"][-1]["id"] == 1
