from __future__ import annotations

from fastapi.testclient import TestClient

from llm_harness.api import create_app


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
