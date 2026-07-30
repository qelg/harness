from __future__ import annotations

import asyncio

from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import SessionCreated, UserMessageCreated
from llm_harness.builtin_plugins.system_prompt import SystemPromptPlugin, build_system_prompt


def test_system_prompt_injected_on_session_creation(tmp_path, monkeypatch):
    """When skills are configured, session creation injects a system message."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    skill_dir = skills_dir / "test-skill"
    skill_dir.mkdir()
    skill_dir.joinpath("SKILL.md").write_text(
        "---\nname: test-skill\ndescription: A test skill for testing.\n---\n\ninstructions"
    )

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_SKILLS", f'["{skills_dir}"]')
    bus = EventService(tmp_path / "events.db")
    plugin = SystemPromptPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))

    asyncio.run(plugin.process_pending(bus))

    system_messages = bus.replay(
        EventFilter(names=frozenset({"chat.message.system.created"}), tags={"session": "sess_1"})
    )
    assert len(system_messages) == 1
    content = system_messages[0].payload["content"]
    assert "test-skill" in content
    assert "A test skill for testing" in content
    assert "skill_view" in content


def test_system_prompt_comes_before_user_message(tmp_path, monkeypatch):
    """The system message is created on session creation, before any user message.

    In the real event flow:
      1. SessionCreated is emitted
      2. SystemPromptPlugin creates SystemMessageCreated
      3. Later, the user sends a message -> UserMessageCreated
    """
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    skill_dir = skills_dir / "test-skill"
    skill_dir.mkdir()
    skill_dir.joinpath("SKILL.md").write_text(
        "---\nname: test-skill\ndescription: A skill.\n---\n\ninstructions"
    )

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_SKILLS", f'["{skills_dir}"]')
    bus = EventService(tmp_path / "events.db")
    plugin = SystemPromptPlugin(settings=Settings.from_env())

    # Step 1: session is created
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))

    # Step 2: system prompt plugin processes the session event
    asyncio.run(plugin.process_pending(bus))

    # Step 3: later, the user sends a message
    asyncio.run(bus.append_message(UserMessageCreated(session_id="sess_1", content="hello")))

    # The system message should be the first message in the session
    first_message = bus.replay(
        EventFilter(
            names=frozenset({"chat.message.system.created", "chat.message.user.created"}),
            tags={"session": "sess_1"},
        ),
        limit=1,
    )
    assert first_message[0].name == "chat.message.system.created"


def test_system_prompt_not_injected_twice(tmp_path, monkeypatch):
    """Only one system message per session, even if the session event is replayed."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    skill_dir = skills_dir / "test-skill"
    skill_dir.mkdir()
    skill_dir.joinpath("SKILL.md").write_text(
        "---\nname: test-skill\ndescription: A skill.\n---\n\ninstructions"
    )

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_SKILLS", f'["{skills_dir}"]')
    bus = EventService(tmp_path / "events.db")
    plugin = SystemPromptPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))

    asyncio.run(plugin.process_pending(bus))
    asyncio.run(plugin.process_pending(bus))

    system_messages = bus.replay(
        EventFilter(names=frozenset({"chat.message.system.created"}), tags={"session": "sess_1"})
    )
    assert len(system_messages) == 1


def test_system_prompt_not_created_without_skills(tmp_path, monkeypatch):
    """Without skills, no system message is injected."""
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    bus = EventService(tmp_path / "events.db")
    plugin = SystemPromptPlugin(settings=Settings.from_env())

    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))

    asyncio.run(plugin.process_pending(bus))

    system_messages = bus.replay(
        EventFilter(names=frozenset({"chat.message.system.created"}), tags={"session": "sess_1"})
    )
    assert system_messages == []


def test_system_prompt_includes_optional_system_md(tmp_path, monkeypatch):
    """A SYSTEM.md file in the skills directory adds extra content to the prompt."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    skills_dir.joinpath("SYSTEM.md").write_text(
        "You are a helpful assistant specialized in repository management."
    )
    skill_dir = skills_dir / "test-skill"
    skill_dir.mkdir()
    skill_dir.joinpath("SKILL.md").write_text(
        "---\nname: test-skill\ndescription: A skill.\n---\n\ninstructions"
    )

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_SKILLS", f'["{skills_dir}"]')
    plugin = SystemPromptPlugin(settings=Settings.from_env())

    prompt = build_system_prompt(plugin.settings.skills)
    assert "repository management" in prompt
    assert "test-skill" in prompt
    assert "A skill" in prompt


def test_system_md_comes_before_skill_list(tmp_path, monkeypatch):
    """SYSTEM.md content comes before the skill list in the prompt."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    skills_dir.joinpath("SYSTEM.md").write_text("Extra instructions.")
    skill_dir = skills_dir / "test-skill"
    skill_dir.mkdir()
    skill_dir.joinpath("SKILL.md").write_text(
        "---\nname: test-skill\ndescription: A skill.\n---\n\ninstructions"
    )

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_SKILLS", f'["{skills_dir}"]')
    plugin = SystemPromptPlugin(settings=Settings.from_env())

    prompt = build_system_prompt(plugin.settings.skills)
    assert prompt.startswith("Extra instructions.")
    assert "You have the following skills available:" in prompt
    assert "test-skill" in prompt


def test_build_system_prompt_empty_skills(tmp_path):
    """build_system_prompt returns empty string for no skills."""
    assert build_system_prompt(()) == ""


def test_build_system_prompt_skill_without_description(tmp_path):
    """A skill without a description in YAML front matter is still listed by name."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    skill_dir = skills_dir / "minimal-skill"
    skill_dir.mkdir()
    skill_dir.joinpath("SKILL.md").write_text("---\nname: minimal-skill\n---\n\n# Minimal")

    monkeypatch = __import__("pytest").MonkeyPatch()
    monkeypatch.setenv("HARNESS_SKILLS", f'["{skills_dir}"]')
    settings = Settings.from_env()

    prompt = build_system_prompt(settings.skills)
    assert "minimal-skill" in prompt
    assert "minimal-skill:" not in prompt
