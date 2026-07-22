from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

from llm_harness.api import create_app
from llm_harness.core.types import LlmRunFailed


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


def test_api_serves_frontend(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/frontend/")

    assert response.status_code == 200
    assert "LLM Harness" in response.text
    assert "mock-llm" in response.text
    assert 'src="./app.js?v=5"' in response.text
    assert "loginChatGPT" in response.text


def test_api_serves_frontend_javascript_that_loads_providers(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    client = TestClient(create_app())

    response = client.get("/frontend/app.js")

    assert response.status_code == 200
    assert 'request("/providers")' in response.text
    assert 'request("/toolsets")' in response.text
    assert "function init()" in response.text
    assert "/messages/stream" in response.text
    assert "async function* readSse" in response.text


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
        json={"provider": "openrouter", "model": "anthropic/claude", "session_id": session_id},
    )

    assert response.status_code == 200
    event = response.json()
    assert event["name"] == "llm.model.selected"
    assert event["tags"]["session"] == session_id
    assert event["tags"]["provider"] == "openrouter"
    assert event["tags"]["model"] == "anthropic/claude"
    assert event["payload"]["toolsets"] == ["default"]


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
        f"/sessions/{session_id}/tools/podman-shell",
        json={"input": {"cmd": "echo hello", "timeout": 10}},
    )

    assert tool_response.status_code == 200
    payload = tool_response.json()
    assert payload["status"] == "accepted"
    assert payload["event"]["name"] == "tool.call.requested"
    assert payload["event"]["tags"]["session"] == session_id
    assert payload["event"]["tags"]["tool"] == "podman-shell"
    assert payload["event"]["payload"]["input"]["cmd"] == "echo hello"
