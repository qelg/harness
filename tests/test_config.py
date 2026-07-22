from pathlib import Path

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
