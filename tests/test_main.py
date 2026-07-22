from __future__ import annotations

from llm_harness.__main__ import _uvicorn_log_level


def test_uvicorn_log_level_is_info_when_provider_event_logging_enabled(monkeypatch):
    monkeypatch.setenv("HARNESS_LOG_PROVIDER_EVENTS", "1")

    assert _uvicorn_log_level() == "info"


def test_uvicorn_log_level_defaults_to_warning(monkeypatch):
    monkeypatch.delenv("HARNESS_LOG_PROVIDER_EVENTS", raising=False)

    assert _uvicorn_log_level() == "warning"
