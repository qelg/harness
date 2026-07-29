from __future__ import annotations

import asyncio
import json

import httpx
from fastapi.testclient import TestClient

from llm_harness.api import create_app
from llm_harness.builtin_plugins.unifiedpush import UnifiedPushPlugin
from llm_harness.core.events import EventService
from llm_harness.core.types import (
    AssistantMessageCreated,
    SessionCreated,
    SessionRenamed,
    SessionStateChanged,
    UserMessageCreated,
)


def test_subscription_api_persists_and_validates_endpoints(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    client = TestClient(app)

    response = client.put(
        "/push/unifiedpush/subscriptions",
        json={"instance_id": "phone-1", "endpoint": "https://push.example.test/abc"},
    )
    assert response.status_code == 204
    assert app.state.bus.conn.execute(
        "SELECT endpoint FROM unifiedpush_subscriptions WHERE instance_id = 'phone-1'"
    ).fetchone()["endpoint"] == "https://push.example.test/abc"

    assert client.put(
        "/push/unifiedpush/subscriptions",
        json={"instance_id": "phone-1", "endpoint": "http://127.0.0.1/internal"},
    ).status_code == 400
    assert client.delete("/push/unifiedpush/subscriptions/phone-1").status_code == 204


def test_finished_top_level_session_sends_title_message_and_session_id(tmp_path):
    bus = EventService(tmp_path / "events.db")
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1", title="Initial")))
    asyncio.run(
        bus.append_message(
            SessionRenamed(
                session_id="sess_1", title="Release status", namer_session_id="namer"
            )
        )
    )
    user = asyncio.run(bus.append_message(UserMessageCreated("sess_1", "status?")))
    asyncio.run(bus.append_message(SessionStateChanged("sess_1", "running", user.id)))
    answer = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                "sess_1", "The release is ready.", "mock", "model", "run_1"
            )
        )
    )
    state = asyncio.run(
        bus.append_message(
            SessionStateChanged("sess_1", "finished", answer.id, read="unread")
        )
    )
    UnifiedPushPlugin._init_schema(bus)
    bus.conn.execute(
        "INSERT INTO unifiedpush_subscriptions VALUES (?, ?, ?)",
        ("phone", "https://push.example.test/token", 1),
    )
    requests = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(201)

    client = httpx.AsyncClient(transport=httpx.MockTransport(send))
    plugin = UnifiedPushPlugin(client=client)
    asyncio.run(plugin.process_event(bus, state))
    asyncio.run(plugin.process_event(bus, state))
    asyncio.run(client.aclose())

    assert len(requests) == 1
    assert json.loads(requests[0].content) == {
        "type": "session.finished",
        "session_id": "sess_1",
        "title": "Release status",
        "content": "The release is ready.",
        "event_id": state.id,
    }


def test_child_and_repeated_finished_states_do_not_notify(tmp_path):
    bus = EventService(tmp_path / "events.db")
    asyncio.run(
        bus.append_message(
            SessionCreated("child", "Child", parent_session_id="parent")
        )
    )
    source = asyncio.run(
        bus.append_message(
            AssistantMessageCreated("child", "done", "mock", "model", "run")
        )
    )
    finished = asyncio.run(
        bus.append_message(SessionStateChanged("child", "finished", source.id, read="unread"))
    )
    UnifiedPushPlugin._init_schema(bus)
    bus.conn.execute(
        "INSERT INTO unifiedpush_subscriptions VALUES (?, ?, ?)",
        ("phone", "https://push.example.test/token", 1),
    )

    def fail_if_called(request: httpx.Request) -> httpx.Response:
        raise AssertionError("child session caused a push")

    client = httpx.AsyncClient(transport=httpx.MockTransport(fail_if_called))
    asyncio.run(UnifiedPushPlugin(client=client).process_event(bus, finished))
    asyncio.run(client.aclose())
