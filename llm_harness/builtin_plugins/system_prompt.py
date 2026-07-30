from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from llm_harness.config import Settings, Skill
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord, EventToAppend
from llm_harness.core.types import SessionCreated, SystemMessageCreated, to_event_parts


class SystemPromptPlugin(EventConsumer):
    """Inject a system message with skill information when a session is created.

    The system message is created before any user messages, which gives the LLM
    early awareness of available skills and improves prompt-cache routing.
    """

    name = "system-prompt"
    subscriber = "plugin:system-prompt"
    event_filter = EventFilter(names=frozenset({SessionCreated.name}))

    def __init__(self, *, settings: Settings):
        self.settings = settings

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        if self._already_injected(bus, event):
            return

        system_prompt = build_system_prompt(self.settings.skills)
        if not system_prompt:
            return

        name, payload, tags = to_event_parts(
            SystemMessageCreated(
                session_id=event.tags["session"],
                content=system_prompt,
            )
        )
        await bus.append(
            name=name,
            payload=payload,
            tags=tags,
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    def _already_injected(self, bus: EventBus, event: EventRecord) -> bool:
        return bool(
            bus.replay(
                EventFilter(
                    names=frozenset({SystemMessageCreated.name}),
                    tags={"session": event.tags["session"]},
                ),
                limit=1,
            )
        )


def build_system_prompt(skills: tuple[Skill, ...]) -> str:
    """Build a system prompt from the configured skills.

    The prompt consists of two parts:

    1.  Optional content from a ``SYSTEM.md`` file found in any of the
        directories that contain the skills.  Multiple files are concatenated
        in directory order.
    2.  A list of the available skills, each with its name and the description
        from the YAML front matter of ``SKILL.md``.
    """
    if not skills:
        return ""

    # Collect the root directories that contain the skills, preserving order.
    root_dirs: list[Path] = []
    seen: set[Path] = set()
    for skill in skills:
        root = skill.path.parent
        if root not in seen:
            seen.add(root)
            root_dirs.append(root)

    parts: list[str] = []

    # 1. Optional SYSTEM.md content from each root directory.
    for root in root_dirs:
        system_file = root / "SYSTEM.md"
        if system_file.is_file():
            content = system_file.read_text(encoding="utf-8", errors="replace").strip()
            if content:
                parts.append(content)

    # 2. Skill descriptions.
    skill_lines: list[str] = []
    for skill in skills:
        description = _parse_skill_description(skill.path)
        if description:
            skill_lines.append(f"- {skill.name}: {description}")
        else:
            skill_lines.append(f"- {skill.name}")

    if skill_lines:
        if parts:
            parts.append("")  # blank separator
        parts.append("You have the following skills available:")
        parts.extend(skill_lines)
        parts.append("")
        parts.append("Use the skill_view tool to read the full instructions for any skill.")

    return "\n".join(parts)


def _parse_skill_description(skill_path: Path) -> str | None:
    """Parse the ``description`` field from the YAML front matter of a SKILL.md."""
    skill_md = skill_path / "SKILL.md"
    if not skill_md.is_file():
        return None
    text = skill_md.read_text(encoding="utf-8", errors="replace")
    m = re.match(r"^---\s*\n(.*?)\n---", text, re.DOTALL)
    if not m:
        return None
    yaml_block = m.group(1)
    for line in yaml_block.splitlines():
        if line.startswith("description:"):
            desc = line[len("description:"):].strip().strip("\"'")
            return desc if desc else None
    return None
