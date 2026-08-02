from __future__ import annotations

import pytest

from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmDelta,
    LlmRunFailed,
    ModelSelected,
    SessionStateChanged,
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


def test_session_state_event_validates_state_and_read_tags():
    running = SessionStateChanged(
        session_id="sess_1", state="running", source_event_id=1
    )
    finished = SessionStateChanged(
        session_id="sess_1",
        state="finished",
        source_event_id=2,
        read="unread",
        outcome="stop",
    )

    assert running.name == "session.state"
    assert running.tags() == {
        "session": "sess_1",
        "chat": "sess_1",
        "state": "running",
    }
    assert finished.tags()["read"] == "unread"
    archived = SessionStateChanged(
        session_id="sess_1",
        state="finished",
        source_event_id=2,
        read="read",
        archived=True,
    )
    assert archived.tags()["archive"] == "true"
    assert finished.payload()["outcome"] == "stop"
    assert required_tags_for("session.state") == frozenset({"session", "state"})

    with pytest.raises(ValueError, match="require a read tag"):
        SessionStateChanged(
            session_id="sess_1", state="finished", source_event_id=3
        )


def test_queued_message_is_a_validated_session_command():
    from llm_harness.core.types import QueuedMessage

    queued = QueuedMessage(
        session_id="sess_1",
        content="change direction",
        mode="after_tool",
    )

    assert queued.name == "queued.message"
    assert queued.payload() == {
        "content": "change direction",
        "mode": "after_tool",
        "metadata": {},
    }
    assert queued.tags() == {
        "session": "sess_1",
        "chat": "sess_1",
        "role": "user",
        "queue_mode": "after_tool",
    }
    assert required_tags_for("queued.message") == frozenset(
        {"session", "queue_mode"}
    )

    with pytest.raises(ValueError, match="invalid queued message mode"):
        QueuedMessage(session_id="sess_1", content="no", mode="later")
