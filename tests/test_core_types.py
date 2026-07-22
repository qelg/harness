from __future__ import annotations

import pytest

from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmDelta,
    LlmRunFailed,
    ModelSelected,
    LlmRunRequested,
    LlmRunStarted,
    ToolMessageCreated,
    UserMessageCreated,
    new_session_id,
    required_tags_for,
    session_tags,
    validate_required_tags,
)


def test_session_created_requires_session_tag():
    assert required_tags_for("session.created") == frozenset({"session"})

    with pytest.raises(ValueError, match="missing required tags: session"):
        validate_required_tags("session.created", {})


def test_chat_message_roles_have_separate_event_types():
    assert UserMessageCreated(session_id="sess_1", content="hi").name == "chat.message.user.created"
    assert (
        AssistantMessageCreated(session_id="sess_1", content="hi", provider="mock", model="test", run_id="llm_1").name
        == "chat.message.assistant.created"
    )
    assert (
        ToolMessageCreated(session_id="sess_1", content="hi", tool="shell", run_id="tool_1").name
        == "chat.message.tool.created"
    )

    assert required_tags_for("chat.message.user.created") == frozenset({"session"})
    assert required_tags_for("chat.message.assistant.created") == frozenset({"session", "provider", "model", "run"})
    assert required_tags_for("chat.message.tool.created") == frozenset({"session", "tool", "run"})


def test_llm_run_events_share_run_tag():
    requested = LlmRunRequested(session_id="sess_1", provider="mock", model="test", run_id="llm_1")
    started = LlmRunStarted(session_id="sess_1", provider="mock", model="test", run_id="llm_1")
    delta = LlmDelta(session_id="sess_1", provider="mock", model="test", run_id="llm_1", delta="he", sequence=1)

    assert requested.name == "llm.run.requested"
    assert started.name == "llm.run.started"
    assert delta.name == "llm.delta"
    assert requested.tags()["run"] == started.tags()["run"] == delta.tags()["run"] == "llm_1"
    assert required_tags_for("llm.run.requested") == frozenset({"session", "provider", "model", "run"})
    assert required_tags_for("llm.run.started") == frozenset({"session", "provider", "model", "run"})
    assert required_tags_for("llm.delta") == frozenset({"session", "provider", "model", "run"})


def test_llm_failed_event_has_run_tags():
    failed = LlmRunFailed(
        session_id="sess_1",
        provider="mock",
        model="test",
        run_id="llm_1",
        error="boom",
    )

    assert failed.name == "llm.run.failed"
    assert failed.tags()["run"] == "llm_1"
    assert required_tags_for("llm.run.failed") == frozenset({"session", "provider", "model", "run"})


def test_model_selection_can_be_global_or_session_scoped():
    global_selection = ModelSelected(provider="mock", model="global")
    session_selection = ModelSelected(
        provider="openrouter",
        model="session-model",
        toolsets=("default",),
        session_id="sess_1",
    )

    assert global_selection.name == "llm.model.selected"
    assert global_selection.tags() == {"provider": "mock", "model": "global"}
    assert session_selection.tags()["session"] == "sess_1"
    assert session_selection.payload()["toolsets"] == ["default"]
    assert required_tags_for("llm.model.selected") == frozenset({"provider", "model"})


def test_new_session_id_is_session_scoped_and_unique():
    first = new_session_id()
    second = new_session_id()

    assert first != second
    assert first.startswith("sess_")
    assert second.startswith("sess_")


def test_session_tags_include_session_chat_and_user_tags():
    session_id = new_session_id()

    assert session_tags(session_id, "project-a") == {
        "session": session_id,
        "chat": session_id,
        "session_tag:project-a": "true",
    }
