import json
from pathlib import Path

import pytest

from llm_harness.config import Settings, parse_tag_container_map


def test_parse_tag_container_map():
    assert parse_tag_container_map("a=container-a, b = container-b") == {
        "a": "container-a",
        "b": "container-b",
    }


def test_settings_reads_event_database_path(monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", "/tmp/events.db")

    assert Settings.from_env().event_database_path == Path("/tmp/events.db")


def test_settings_reads_default_toolsets(monkeypatch):
    monkeypatch.setenv("HARNESS_DEFAULT_TOOLSETS", "default, readonly")

    assert Settings.from_env().default_toolsets == ("default", "readonly")


def test_settings_reads_provider_event_logging_flag(monkeypatch):
    monkeypatch.setenv("HARNESS_LOG_PROVIDER_EVENTS", "1")

    assert Settings.from_env().log_provider_events is True


def test_settings_discovers_skills_in_configured_directories(tmp_path, monkeypatch):
    directory = tmp_path / "skills"
    (directory / "python").mkdir(parents=True)
    (directory / "python" / "SKILL.md").write_text(
        "---\nname: python\ndescription: Help with Python code.\n---\n# Python\n"
    )
    (directory / "without-instructions").mkdir()
    (directory / "README.md").write_text("not a skill\n")
    monkeypatch.setenv("HARNESS_SKILLS", json.dumps([str(directory)]))

    skills = Settings.from_env().skills

    assert [(skill.name, skill.path, skill.description) for skill in skills] == [
        ("python", directory / "python", "Help with Python code.")
    ]


def test_settings_reads_quoted_skill_description(tmp_path, monkeypatch):
    directory = tmp_path / "skills"
    skill = directory / "review"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        '---\nname: review\ndescription: "Review code: carefully."\n---\n'
    )
    monkeypatch.setenv("HARNESS_SKILLS", json.dumps([str(directory)]))

    assert Settings.from_env().skills[0].description == "Review code: carefully."


def test_settings_discovers_linked_skill_directories(tmp_path, monkeypatch):
    source = tmp_path / "source" / "nixos"
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("# NixOS\n")
    directory = tmp_path / "skills"
    directory.mkdir()
    (directory / "nixos").symlink_to(source, target_is_directory=True)
    monkeypatch.setenv("HARNESS_SKILLS", json.dumps([str(directory)]))

    assert [skill.name for skill in Settings.from_env().skills] == ["nixos"]


def test_settings_rejects_duplicate_discovered_skill_names(tmp_path, monkeypatch):
    directories = [tmp_path / "first", tmp_path / "second"]
    for directory in directories:
        skill = directory / "python"
        skill.mkdir(parents=True)
        (skill / "SKILL.md").write_text("# Python\n")
    monkeypatch.setenv("HARNESS_SKILLS", json.dumps([str(path) for path in directories]))

    with pytest.raises(ValueError, match="duplicate skill name"):
        Settings.from_env()
