from __future__ import annotations

import logging
from pathlib import Path

from llm_harness.config import Settings, Skill
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import ToolCall, ToolMessageCreated, ToolResult, ToolSession

logger = logging.getLogger(__name__)


class SkillViewTool:
    name = "skill_view"
    description = "Read a configured skill's SKILL.md instructions or one of its supporting files."
    input_schema = {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Name of the configured skill to view.",
            },
            "file": {
                "type": "string",
                "description": "Optional path, relative to the skill directory. Defaults to SKILL.md.",
                "default": "SKILL.md",
            },
            "line_start": {
                "type": "integer",
                "description": "Optional first line to return (1-based).",
                "minimum": 1,
            },
            "line_end": {
                "type": "integer",
                "description": "Optional last line to return (1-based, inclusive).",
                "minimum": 1,
            },
        },
        "required": ["name"],
        "additionalProperties": False,
    }

    def __init__(self, *, settings: Settings):
        self.skills = {skill.name: skill for skill in settings.skills}

    async def run(self, call: ToolCall) -> ToolResult:
        name = call.input.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("tool input requires non-empty string field 'name'")
        skill = self.skills.get(name)
        if skill is None:
            available = ", ".join(sorted(self.skills)) or "none"
            raise ValueError(f"unknown skill {name!r}; available skills: {available}")

        relative_file = call.input.get("file", "SKILL.md")
        if not isinstance(relative_file, str) or not relative_file.strip():
            raise ValueError("tool input field 'file' must be a non-empty string")
        file_path = _skill_file(skill, relative_file)
        if not file_path.is_file():
            raise ValueError(f"skill file does not exist or is not a regular file: {relative_file}")

        text = file_path.read_text(encoding="utf-8", errors="replace")
        line_start, line_end = _line_range(call.input)
        if line_start is not None or line_end is not None:
            lines = text.splitlines(keepends=True)
            start_index = (line_start or 1) - 1
            text = "".join(lines[start_index:line_end])

        return ToolResult(
            output=text,
            metadata={
                "skill": skill.name,
                "file": relative_file,
                "line_start": line_start,
                "line_end": line_end,
            },
        )


class SkillViewToolConsumer(EventConsumer):
    name = "skill-view"
    subscriber = "plugin:skill-view"
    event_filter = EventFilter(names=frozenset({"tool.call.requested"}), tags={"tool": "skill_view"})

    def __init__(self, *, tool: SkillViewTool):
        self.tool = tool

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry=None) -> None:
        if await self._already_completed(bus, event):
            return
        call = ToolCall(
            session=ToolSession(id=event.tags["session"]),
            name=self.tool.name,
            input=event.payload.get("input", {}),
        )
        logger.info(
            "starting tool execution tool=%s session=%s run=%s input=%s",
            self.tool.name,
            event.tags["session"],
            event.tags["run"],
            call.input,
        )
        try:
            result = await self.tool.run(call)
        except Exception as exc:
            logger.exception(
                "tool execution failed tool=%s session=%s run=%s",
                self.tool.name,
                event.tags["session"],
                event.tags["run"],
            )
            result = _exception_result(exc)
        await bus.append_message(
            ToolMessageCreated(
                session_id=event.tags["session"],
                content=result.output,
                tool=self.tool.name,
                run_id=event.tags["run"],
                metadata=result.metadata,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    async def _already_completed(self, bus: EventBus, event: EventRecord) -> bool:
        messages = bus.replay(
            EventFilter(
                names=frozenset({ToolMessageCreated.name}),
                tags={"session": event.tags["session"], "tool": self.tool.name, "run": event.tags["run"]},
            )
        )
        return bool(messages)


def _skill_file(skill: Skill, relative_file: str) -> Path:
    relative = Path(relative_file)
    if relative.is_absolute():
        raise ValueError("skill file must be relative to the skill directory")
    root = skill.path.resolve()
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root):
        raise ValueError("skill file must stay within the skill directory")
    return candidate


def _line_range(input_: dict) -> tuple[int | None, int | None]:
    line_start = input_.get("line_start")
    line_end = input_.get("line_end")
    for field, value in (("line_start", line_start), ("line_end", line_end)):
        if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 1):
            raise ValueError(f"tool input field {field!r} must be a positive integer")
    if line_start is not None and line_end is not None and line_end < line_start:
        raise ValueError("line_end must be greater than or equal to line_start")
    # Preserve the requested range in metadata, even when it extends beyond EOF.
    return line_start, line_end


def _exception_result(exc: Exception) -> ToolResult:
    error_type = type(exc).__name__
    error_message = str(exc)
    description = f"{error_type}: {error_message}" if error_message else error_type
    return ToolResult(
        output=f"tool execution failed: {description}\n",
        metadata={
            "success": False,
            "error_type": error_type,
            "error": error_message,
        },
    )
