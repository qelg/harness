from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from llm_harness.api import create_app
from llm_harness.core.types import LlmRunFailed, SessionCreated, ToolCallRequested


def test_api_creates_session_and_lists_sessions_from_events(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    session_response = client.post("/sessions", json={"title": "mock-test", "tags": ["test"]})
    assert session_response.status_code == 200
    session = session_response.json()
    assert session["id"].startswith("sess_")
    assert session["tags"] == ["test"]

    sessions_response = client.get("/sessions")
    assert sessions_response.status_code == 200
    assert sessions_response.json() == [session]

    restarted_client = TestClient(create_app())
    restarted_response = restarted_client.get("/sessions")
    assert restarted_response.status_code == 200
    assert restarted_response.json() == [session]


def test_api_hides_derived_sessions_from_top_level_list(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)
    parent = client.post("/sessions", json={"title": "user session"}).json()
    asyncio.run(
        app.state.bus.append_message(
            SessionCreated(
                session_id="sess_derived",
                title="internal session",
                parent_session_id=parent["id"],
            ),
            producer="test-plugin",
        )
    )

    response = client.get("/sessions")

    assert response.status_code == 200
    assert response.json() == [parent]


def test_api_lists_direct_child_sessions(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)
    parent = client.post("/sessions", json={"title": "parent"}).json()
    for session in (
        SessionCreated(
            session_id="sess_child_1",
            title="first child",
            parent_session_id=parent["id"],
        ),
        SessionCreated(
            session_id="sess_child_2",
            title="second child",
            parent_session_id=parent["id"],
        ),
        SessionCreated(
            session_id="sess_grandchild",
            title="grandchild",
            parent_session_id="sess_child_1",
        ),
        SessionCreated(
            session_id="sess_unrelated",
            title="unrelated",
            parent_session_id="sess_other",
        ),
    ):
        asyncio.run(app.state.bus.append_message(session, producer="test-plugin"))

    response = client.get(f"/sessions/{parent['id']}/children")

    assert response.status_code == 200
    assert [session["id"] for session in response.json()] == [
        "sess_child_1",
        "sess_child_2",
    ]
    assert {
        session["parent_session_id"] for session in response.json()
    } == {parent["id"]}
    assert client.get("/sessions/missing/children").status_code == 404


def test_api_serves_frontend(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/frontend/")

    assert response.status_code == 200
    assert "LLM Harness" in response.text
    assert "mock-llm" in response.text
    assert 'src="./app.js?v=8"' in response.text
    assert "loginChatGPT" in response.text


def test_api_serves_frontend_javascript_that_loads_providers(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/frontend/app.js")

    assert response.status_code == 200
    assert 'request("/providers")' in response.text
    assert 'request("/toolsets")' in response.text
    assert "function init()" in response.text
    assert "/messages/updates" in response.text
    assert "new EventSource" in response.text
    assert "function formatProviderOutput" in response.text
    assert "Tool call:" in response.text


def test_api_lists_builtin_providers(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/providers")

    assert response.status_code == 200
    assert {"chatgpt-codex", "mock-llm", "openrouter", "openai-codex"}.issubset(set(response.json()["providers"]))


def test_api_lists_builtin_toolsets(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/toolsets")

    assert response.status_code == 200
    assert response.json()["toolsets"] == ["default"]


def test_api_registers_builtin_tool_call_requester(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()

    consumers = {plugin.name for plugin in app.state.registry.event_consumer_plugins}
    assert "tool-call-requester" in consumers
    assert "terminal" in consumers


def test_api_creates_message_event_and_lists_messages_from_events(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    session_id = client.post("/sessions", json={"title": "mock-test", "tags": ["test"]}).json()["id"]
    message_response = client.post(
        f"/sessions/{session_id}/messages",
        json={"content": "hello"},
    )

    assert message_response.status_code == 200
    assert message_response.json()["content"] == "hello"
    assert message_response.json()["event_name"] == "chat.message.user.created"

    messages_response = client.get(f"/sessions/{session_id}/messages")
    assert messages_response.status_code == 200
    messages = messages_response.json()
    assert [message["role"] for message in messages] == ["user"]
    assert messages[0]["content"] == "hello"
    assert messages[0]["provider"] is None
    assert messages[0]["model"] is None
    assert messages[0]["event_name"] == "chat.message.user.created"


def test_api_lists_all_low_level_events_for_session(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)

    first = client.post("/sessions", json={"title": "first"}).json()
    second = client.post("/sessions", json={"title": "second"}).json()
    client.post(f"/sessions/{first['id']}/messages", json={"content": "hello"})
    client.post(f"/sessions/{second['id']}/messages", json={"content": "other"})

    response = client.get(f"/sessions/{first['id']}/events")

    assert response.status_code == 200
    events = response.json()
    assert [event["name"] for event in events] == [
        "session.created",
        "chat.message.user.created",
    ]
    assert all(event["tags"]["session"] == first["id"] for event in events)
    assert events[0]["producer"] == "harness-api"
    assert events[0]["durable"] is True
    assert events[0]["persisted_event_id"] == events[0]["id"]
    assert set(events[0]) == {
        "session_id",
        "payload",
        "id",
        "name",
        "tags",
        "created_at_ms",
        "producer",
        "causation_id",
        "correlation_id",
        "durable",
        "persisted_event_id",
    }


def test_api_rejects_event_list_for_unknown_session(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/sessions/missing/events")

    assert response.status_code == 404


def test_api_lists_messages_for_later_session(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    first = client.post("/sessions", json={"title": "first"}).json()
    second = client.post("/sessions", json={"title": "second"}).json()

    sessions = client.get("/sessions").json()
    assert [session["id"] for session in sessions] == [first["id"], second["id"]]

    response = client.get(f"/sessions/{second['id']}/messages")

    assert response.status_code == 200
    assert response.json() == []


def test_api_lists_failed_llm_run_as_latest_message(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)

    session_id = client.post("/sessions", json={"title": "failure"}).json()["id"]
    client.post(f"/sessions/{session_id}/messages", json={"content": "hello"})
    asyncio.run(
        app.state.bus.append_message(
            LlmRunFailed(
                session_id=session_id,
                provider="mock-llm",
                model="test-model",
                run_id="llm_1",
                error="provider exploded",
            )
        )
    )

    response = client.get(f"/sessions/{session_id}/messages")

    assert response.status_code == 200
    messages = response.json()
    assert [message["event_name"] for message in messages] == ["chat.message.user.created", "llm.run.failed"]
    assert messages[-1]["role"] == "assistant"
    assert messages[-1]["content"] == "LLM run failed: provider exploded"
    assert messages[-1]["metadata"]["retryable"] is False


def test_api_creates_model_selection_event(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    session_id = client.post("/sessions", json={"title": "model-test"}).json()["id"]
    response = client.post(
        "/model-selection",
        json={
            "provider": "openrouter",
            "model": "anthropic/claude",
            "session_id": session_id,
            "thinking_level": "medium",
            "reasoning_summary": True,
        },
    )

    assert response.status_code == 200
    event = response.json()
    assert event["name"] == "llm.model.selected"
    assert event["tags"]["session"] == session_id
    assert event["tags"]["provider"] == "openrouter"
    assert event["tags"]["model"] == "anthropic/claude"
    assert event["payload"]["toolsets"] == ["default"]
    assert event["payload"]["thinking_level"] == "medium"
    assert event["payload"]["reasoning_summary"] is True
    selection = client.get(f"/sessions/{session_id}/model-selection").json()
    assert selection["thinking_level"] == "medium"
    assert selection["reasoning_summary"] is True


def test_api_rejects_unknown_thinking_level(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.post(
        "/model-selection",
        json={"provider": "mock-llm", "model": "test-model", "thinking_level": "extreme"},
    )

    assert response.status_code == 422


def test_api_returns_effective_model_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_DEFAULT_PROVIDER", "mock-llm")
    monkeypatch.setenv("HARNESS_DEFAULT_MODEL", "default-model")
    client = TestClient(create_app())

    session_id = client.post("/sessions", json={"title": "model-test"}).json()["id"]

    default_response = client.get(f"/sessions/{session_id}/model-selection")
    assert default_response.status_code == 200
    assert default_response.json()["provider"] == "mock-llm"
    assert default_response.json()["model"] == "default-model"
    assert default_response.json()["toolsets"] == ["default"]
    assert default_response.json()["scope"] == "default"

    client.post("/model-selection", json={"provider": "openrouter", "model": "global-model"})
    global_response = client.get(f"/sessions/{session_id}/model-selection")
    assert global_response.status_code == 200
    assert global_response.json()["provider"] == "openrouter"
    assert global_response.json()["model"] == "global-model"
    assert global_response.json()["scope"] == "global"

    client.post(
        "/model-selection",
        json={"provider": "mock-llm", "model": "session-model", "session_id": session_id},
    )
    session_response = client.get(f"/sessions/{session_id}/model-selection")
    assert session_response.status_code == 200
    assert session_response.json()["provider"] == "mock-llm"
    assert session_response.json()["model"] == "session-model"
    assert session_response.json()["toolsets"] == ["default"]
    assert session_response.json()["scope"] == "session"


def test_api_model_selection_accepts_toolsets(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    session_id = client.post("/sessions", json={"title": "toolset-test"}).json()["id"]
    response = client.post(
        "/model-selection",
        json={"provider": "mock-llm", "model": "test-model", "session_id": session_id, "toolsets": ["default"]},
    )

    assert response.status_code == 200
    assert response.json()["payload"]["toolsets"] == ["default"]
    assert client.get(f"/sessions/{session_id}/model-selection").json()["toolsets"] == ["default"]


def test_api_creates_tool_request_event(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    session_response = client.post("/sessions", json={"title": "podman-test", "tags": ["podman-test"]})
    assert session_response.status_code == 200
    session_id = session_response.json()["id"]

    tool_response = client.post(
        f"/sessions/{session_id}/tools/terminal",
        json={"input": {"cmd": "echo hello", "timeout": 10}},
    )

    assert tool_response.status_code == 200
    payload = tool_response.json()
    assert payload["status"] == "accepted"
    assert payload["event"]["name"] == "tool.call.requested"
    assert payload["event"]["tags"]["session"] == session_id
    assert payload["event"]["tags"]["tool"] == "terminal"
    assert payload["event"]["payload"]["input"]["cmd"] == "echo hello"


def test_api_includes_tool_requests_with_names_and_inputs_in_message_timeline(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)
    session_id = client.post("/sessions", json={"title": "tools"}).json()["id"]
    asyncio.run(app.state.bus.append_message(ToolCallRequested(
        session_id=session_id,
        tool="terminal",
        input={"cmd": "echo hello", "timeout": 10},
        run_id="call_1",
    )))

    message = client.get(f"/sessions/{session_id}/messages").json()[0]
    assert message["event_name"] == "tool.call.requested"
    assert message["role"] == "tool_request"
    assert message["tool"] == "terminal"
    assert message["run_id"] == "call_1"
    assert message["content"] == {"cmd": "echo hello", "timeout": 10}


def test_api_lists_latest_session_states_by_activity(tmp_path, monkeypatch):
    from llm_harness.builtin_plugins.session_state import SessionStatePlugin
    from llm_harness.core.events import EventFilter
    from llm_harness.core.types import AssistantMessageCreated, UserMessageCreated

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)
    plugin = SessionStatePlugin()

    first = client.post("/sessions", json={"title": "first"}).json()["id"]
    second = client.post("/sessions", json={"title": "second"}).json()["id"]
    first_message = asyncio.run(
        app.state.bus.append_message(UserMessageCreated(session_id=first, content="one"))
    )
    asyncio.run(plugin.process_event(app.state.bus, first_message))
    second_message = asyncio.run(
        app.state.bus.append_message(UserMessageCreated(session_id=second, content="two"))
    )
    asyncio.run(plugin.process_event(app.state.bus, second_message))
    answer = asyncio.run(
        app.state.bus.append_message(
            AssistantMessageCreated(
                session_id=first,
                content="done",
                provider="mock-llm",
                model="test-model",
                run_id="llm_1",
                metadata={"provider_response": {"finish_reason": "stop"}},
            )
        )
    )
    asyncio.run(plugin.process_event(app.state.bus, answer))

    response = client.get("/session-states")

    assert response.status_code == 200
    states = response.json()
    assert [state["session_id"] for state in states] == [first, second]
    assert states[0]["state"] == "finished"
    assert states[0]["read"] == "unread"
    assert states[0]["outcome"] == "stop"
    assert states[0]["source_event_id"] == answer.id
    assert states[1]["state"] == "running"
    assert states[1]["read"] is None

    history = client.get(f"/sessions/{first}/state-events")
    assert history.status_code == 200
    assert [state["state"] for state in history.json()] == ["running", "finished"]
    state_events = app.state.bus.replay(
        EventFilter(names=frozenset({"session.state"}), tags={"session": first})
    )
    assert [event.id for event in state_events] == [
        state["event_id"] for state in history.json()
    ]


def test_api_rejects_state_history_for_unknown_session(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/sessions/missing/state-events")

    assert response.status_code == 404


def test_api_marks_finished_session_read_idempotently(tmp_path, monkeypatch):
    from llm_harness.builtin_plugins.session_state import SessionStatePlugin
    from llm_harness.core.types import AssistantMessageCreated

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)
    session_id = client.post("/sessions", json={"title": "finished"}).json()["id"]
    answer = asyncio.run(
        app.state.bus.append_message(
            AssistantMessageCreated(
                session_id=session_id,
                content="done",
                provider="mock-llm",
                model="test-model",
                run_id="llm_1",
            )
        )
    )
    asyncio.run(SessionStatePlugin().process_event(app.state.bus, answer))

    first = client.post(f"/sessions/{session_id}/state/read")
    second = client.post(f"/sessions/{session_id}/state/read")

    assert first.status_code == 200
    assert first.json()["state"] == "finished"
    assert first.json()["read"] == "read"
    assert first.json()["source_event_id"] == answer.id
    assert second.json()["event_id"] == first.json()["event_id"]
    assert [state["read"] for state in client.get(
        f"/sessions/{session_id}/state-events"
    ).json()] == ["unread", "read"]


def test_api_does_not_mark_running_session_read(tmp_path, monkeypatch):
    from llm_harness.builtin_plugins.session_state import SessionStatePlugin
    from llm_harness.core.types import UserMessageCreated

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)
    session_id = client.post("/sessions", json={"title": "running"}).json()["id"]
    message = asyncio.run(
        app.state.bus.append_message(
            UserMessageCreated(session_id=session_id, content="hello")
        )
    )
    asyncio.run(SessionStatePlugin().process_event(app.state.bus, message))

    response = client.post(f"/sessions/{session_id}/state/read")

    assert response.status_code == 409


def test_api_archives_latest_session_state_until_activity_changes(tmp_path, monkeypatch):
    from llm_harness.builtin_plugins.session_state import SessionStatePlugin
    from llm_harness.core.events import EventFilter
    from llm_harness.core.types import UserMessageCreated

    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)
    plugin = SessionStatePlugin()
    session_id = client.post("/sessions", json={"title": "archive me"}).json()["id"]
    first_message = asyncio.run(
        app.state.bus.append_message(
            UserMessageCreated(session_id=session_id, content="first")
        )
    )
    asyncio.run(plugin.process_event(app.state.bus, first_message))

    first = client.post(f"/sessions/{session_id}/state/archive")
    second = client.post(f"/sessions/{session_id}/state/archive")

    assert first.status_code == 200
    assert first.json()["archive"] == "true"
    assert first.json()["state"] == "running"
    assert first.json()["source_event_id"] == first_message.id
    assert second.json()["event_id"] == first.json()["event_id"]
    archived_events = app.state.bus.replay(
        EventFilter(
            names=frozenset({"session.state"}),
            tags={"session": session_id, "archive": "true"},
        )
    )
    assert len(archived_events) == 1

    new_message = asyncio.run(
        app.state.bus.append_message(
            UserMessageCreated(session_id=session_id, content="new activity")
        )
    )
    asyncio.run(plugin.process_event(app.state.bus, new_message))

    latest = client.get("/session-states").json()[0]
    assert latest["source_event_id"] == new_message.id
    assert latest["archive"] is None


def test_api_can_archive_session_without_projected_state(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())
    session_id = client.post("/sessions", json={"title": "empty"}).json()["id"]

    response = client.post(f"/sessions/{session_id}/state/archive")

    assert response.status_code == 200
    assert response.json()["state"] == "finished"
    assert response.json()["read"] == "read"
    assert response.json()["archive"] == "true"
