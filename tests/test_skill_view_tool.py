from __future__ import annotations

import asyncio

import pytest

from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import SessionCreated, ToolCall, ToolCallRequested, ToolSession
from llm_harness.tools.skill_view import SkillViewTool, SkillViewToolConsumer


def configured_tool(tmp_path, monkeypatch):
    skill_directory = tmp_path / "skills"
    skill = skill_directory / "python"
    (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text("# Python\nUse type hints.\n")
    (skill / "references" / "testing.md").write_text("one\ntwo\nthree\n")
    monkeypatch.setenv("HARNESS_SKILLS", f'["{skill_directory}"]')
    return SkillViewTool(settings=Settings.from_env())


def call(tool, input_):
    return asyncio.run(tool.run(ToolCall(session=ToolSession(id="sess_1"), name="skill_view", input=input_)))


def test_description_is_static(tmp_path, monkeypatch):
    """The tool description is a static string; skills are listed in the system prompt instead."""
    tool = configured_tool(tmp_path, monkeypatch)
    assert tool.description == (
        "Read a configured skill's SKILL.md instructions or one of its supporting files."
    )


def test_reads_skill_instructions_and_supporting_file_range(tmp_path, monkeypatch):
    tool = configured_tool(tmp_path, monkeypatch)
    assert call(tool, {"name": "python"}).output == "# Python\nUse type hints.\n"

    result = call(
        tool,
        {"name": "python", "file": "references/testing.md", "line_start": 2, "line_end": 3},
    )
    assert result.output == "two\nthree\n"
    assert result.metadata["file"] == "references/testing.md"


def test_rejects_files_outside_skill(tmp_path, monkeypatch):
    tool = configured_tool(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="within the skill directory"):
        call(tool, {"name": "python", "file": "../secret"})


def test_consumer_persists_tool_result(tmp_path, monkeypatch):
    tool = configured_tool(tmp_path, monkeypatch)
    bus = EventService(tmp_path / "events.db")
    consumer = SkillViewToolConsumer(tool=tool)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    asyncio.run(
        bus.append_message(
            ToolCallRequested(session_id="sess_1", tool="skill_view", input={"name": "python"}, run_id="tool_1")
        )
    )

    asyncio.run(consumer.process_pending(bus))
    asyncio.run(consumer.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert messages[0].payload["content"].startswith("# Python")
    assert messages[0].payload["metadata"]["skill"] == "python"


def test_consumer_persists_failing_result_for_invalid_input(tmp_path, monkeypatch):
    tool = configured_tool(tmp_path, monkeypatch)
    bus = EventService(tmp_path / "events.db")
    consumer = SkillViewToolConsumer(tool=tool)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="skill_view",
                input={"name": "python", "file": "../secret"},
                run_id="tool_1",
            )
        )
    )

    asyncio.run(consumer.process_pending(bus))

    messages = bus.replay(EventFilter(names=frozenset({"chat.message.tool.created"}), tags={"run": "tool_1"}))
    assert len(messages) == 1
    assert messages[0].payload["content"] == (
        "tool execution failed: ValueError: skill file must stay within the skill directory\n"
    )
    assert messages[0].payload["metadata"] == {
        "success": False,
        "error_type": "ValueError",
        "error": "skill file must stay within the skill directory",
    }
    assert messages[0].causation_id == request.id
    assert bus.last_acked(consumer.subscriber) == request.id
