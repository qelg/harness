from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.testclient import TestClient

from llm_harness.config import Settings
from llm_harness.core.events import EventFilter, EventService
from llm_harness.core.types import SessionCreated, ToolCallRequested, ToolMessageCreated
from llm_harness.tools.retrieve_secret import (
    RetrieveSecretApiPlugin,
    RetrieveSecretTool,
    RetrieveSecretToolConsumer,
)


class FakeRetrieveSecretTool(RetrieveSecretTool):
    def __init__(self, *, exists: bool = True):
        super().__init__(settings=Settings.from_env())
        self.exists = exists
        self.writes: list[tuple[str, str, bytes]] = []

    async def container_exists(self, container: str) -> bool:
        return self.exists

    async def write_secret(self, *, container: str, identifier: str, value: bytes) -> str:
        self.writes.append((container, identifier, value))
        return f"/secrets/{identifier}"


def test_retrieve_secret_asks_without_exposing_value(tmp_path):
    bus = EventService(tmp_path / "events.db")
    tool = FakeRetrieveSecretTool()
    consumer = RetrieveSecretToolConsumer(tool=tool)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="retrieve-secret",
                input={"description": "GitHub personal access token"},
                run_id="tool_1",
            )
        )
    )

    asyncio.run(consumer.process_pending(bus))

    asks = bus.replay(EventFilter(names=frozenset({"secret.ask"})))
    assert len(asks) == 1
    ask = asks[0]
    assert ask.payload["description"] == "GitHub personal access token"
    assert ask.payload["container"] == "llm-harness-session-sess_1"
    assert ask.causation_id == request.id
    assert "secret" not in ask.payload
    assert not bus.replay(EventFilter(names=frozenset({ToolMessageCreated.name})))


def test_retrieve_secret_fails_before_container_exists(tmp_path):
    bus = EventService(tmp_path / "events.db")
    tool = FakeRetrieveSecretTool(exists=False)
    consumer = RetrieveSecretToolConsumer(tool=tool)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    request = asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="retrieve-secret",
                input={"description": "token"},
                run_id="tool_1",
            )
        )
    )

    asyncio.run(consumer.process_pending(bus))

    result = bus.replay(EventFilter(names=frozenset({ToolMessageCreated.name})))[0]
    assert result.causation_id == request.id
    assert "call terminal first" in result.payload["content"]
    assert not bus.replay(EventFilter(names=frozenset({"secret.ask"})))


def test_secret_upload_requires_matching_ask_and_creates_non_secret_result(tmp_path):
    bus = EventService(tmp_path / "events.db")
    tool = FakeRetrieveSecretTool()
    consumer = RetrieveSecretToolConsumer(tool=tool)
    asyncio.run(bus.append_message(SessionCreated(session_id="sess_1")))
    asyncio.run(
        bus.append_message(
            ToolCallRequested(
                session_id="sess_1",
                tool="retrieve-secret",
                input={"description": "token"},
                run_id="tool_1",
            )
        )
    )
    asyncio.run(consumer.process_pending(bus))
    ask = bus.replay(EventFilter(names=frozenset({"secret.ask"})))[0]

    app = FastAPI()
    RetrieveSecretApiPlugin(tool=tool).install_api(app=app, bus=bus, registry=None)
    client = TestClient(app)
    identifier = ask.payload["identifier"]
    assert client.post(f"/secrets/{ask.id}/wrong-{identifier}", content=b"nope").status_code == 404

    response = client.post(f"/secrets/{ask.id}/{identifier}", content=b"super-secret")
    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "path": f"/secrets/{identifier}"}
    assert tool.writes == [("llm-harness-session-sess_1", identifier, b"super-secret")]

    result = bus.replay(EventFilter(names=frozenset({ToolMessageCreated.name})))[0]
    assert "super-secret" not in result.payload["content"]
    assert result.payload["content"] == f"Secret written to /secrets/{identifier}\n"
    assert result.payload["metadata"]["secret_ask_event_id"] == ask.id
    assert result.causation_id == ask.causation_id
    assert client.post(f"/secrets/{ask.id}/{identifier}", content=b"again").status_code == 409
