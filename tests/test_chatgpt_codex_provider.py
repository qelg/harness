from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from llm_harness.api import create_app
from llm_harness.auth_plugins.token_store import ensure_oauth_schema
from llm_harness.core.types import Message, Role, ToolSpec


def test_chatgpt_codex_provider_requires_login_token(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    provider = app.state.registry.providers["chatgpt-codex"]

    async def consume() -> list[str]:
        return [delta async for delta in provider.stream_chat(model="codex", messages=[])]

    with pytest.raises(RuntimeError, match="Login with ChatGPT"):
        asyncio.run(consume())


def test_chatgpt_codex_provider_uses_stored_access_token(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    monkeypatch.setenv("HARNESS_LOG_PROVIDER_EVENTS", "1")
    app = create_app()
    conn = app.state.bus.conn
    ensure_oauth_schema(conn)
    with conn:
        conn.execute(
            """
            INSERT INTO oauth_tokens(
              provider, subject, access_token, refresh_token, token_type, scope,
              expires_at, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "openai-codex",
                "codex-user",
                "access-token",
                "refresh-token",
                "Bearer",
                "openid",
                None,
                json.dumps({"refresh_after": (datetime.now(UTC) + timedelta(hours=1)).isoformat()}),
                datetime.now(UTC).isoformat(),
                datetime.now(UTC).isoformat(),
            ),
        )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer access-token"
        assert request.url.path.endswith("/responses")
        payload = json.loads(request.content)
        assert payload["instructions"] == "You are a helpful coding assistant."
        assert payload["input"] == [{"role": "user", "content": "hello"}]
        assert payload["tools"] == [
            {
                "type": "function",
                "name": "echo",
                "description": "Echo text.",
                "parameters": {"type": "object"},
                "strict": False,
            }
        ]
        assert payload["store"] is False
        return httpx.Response(
            200,
            content=(
                b'data: {"type":"response.output_text.delta","delta":"hi"}\n\n'
                b'data: {"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":3},"output":[{"type":"function_call","name":"echo"}]}}\n\n'
                b"data: [DONE]\n\n"
            ),
            headers={"content-type": "text/event-stream"},
        )

    original_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)

    def client_factory(*args, **kwargs):
        return original_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr("llm_harness.providers.chatgpt_codex.httpx.AsyncClient", client_factory)
    provider = app.state.registry.providers["chatgpt-codex"]

    async def consume() -> list[str]:
        message = Message(id=1, session_id="sess_1", role=Role.USER, content="hello")
        tool = ToolSpec(name="echo", description="Echo text.", input_schema={"type": "object"})
        return [delta async for delta in provider.stream_chat(model="codex", messages=[message], tools=[tool])]

    with caplog.at_level(logging.INFO, logger="llm_harness.providers.chatgpt_codex"):
        assert asyncio.run(consume()) == ["hi"]
    assert "chatgpt-codex event" in caplog.text
    assert "response.output_text.delta" in caplog.text

    async def consume_events() -> list:
        message = Message(id=1, session_id="sess_1", role=Role.USER, content="hello")
        tool = ToolSpec(name="echo", description="Echo text.", input_schema={"type": "object"})
        return [event async for event in provider.stream_response(model="codex", messages=[message], tools=[tool])]

    events = asyncio.run(consume_events())
    assert events[-1].type == "completed"
    assert events[-1].response["output"][0]["type"] == "function_call"
    assert events[-1].response["usage"]["input_tokens"] == 3
    assert events[-1].response["stream_events"][0]["type"] == "response.output_text.delta"


def test_chatgpt_codex_provider_combines_response_sse_events(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    conn = app.state.bus.conn
    ensure_oauth_schema(conn)
    with conn:
        conn.execute(
            """
            INSERT INTO oauth_tokens(
              provider, subject, access_token, refresh_token, token_type, scope,
              expires_at, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "openai-codex",
                "codex-user",
                "access-token",
                "refresh-token",
                "Bearer",
                "openid",
                None,
                json.dumps({"refresh_after": (datetime.now(UTC) + timedelta(hours=1)).isoformat()}),
                datetime.now(UTC).isoformat(),
                datetime.now(UTC).isoformat(),
            ),
        )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=(
                b'data: {"type":"response.created","response":{"id":"resp_1","model":"codex"}}\n\n'
                b'data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","content":[]}}\n\n'
                b'data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"he"}\n\n'
                b'data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"llo"}\n\n'
                b'data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","name":"podman-shell"}}\n\n'
                b'data: {"type":"response.function_call_arguments.delta","output_index":1,"delta":"{\\"cmd\\":"}\n\n'
                b'data: {"type":"response.function_call_arguments.delta","output_index":1,"delta":"\\"echo hi\\"}"}\n\n'
                b'data: {"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":7}}}\n\n'
                b"data: [DONE]\n\n"
            ),
            headers={"content-type": "text/event-stream"},
        )

    original_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)

    def client_factory(*args, **kwargs):
        return original_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr("llm_harness.providers.chatgpt_codex.httpx.AsyncClient", client_factory)
    provider = app.state.registry.providers["chatgpt-codex"]

    async def consume_events() -> list:
        return [event async for event in provider.stream_response(model="codex", messages=[])]

    events = asyncio.run(consume_events())

    assert [event.delta for event in events if event.type == "delta"] == ["he", "llo"]
    completed = events[-1].response
    assert completed["id"] == "resp_1"
    assert completed["usage"]["input_tokens"] == 7
    assert completed["output"][0]["content"][0]["text"] == "hello"
    assert completed["output"][1]["name"] == "podman-shell"
    assert completed["output"][1]["arguments"] == '{"cmd":"echo hi"}'
    assert len(completed["stream_events"]) == 8


def test_chatgpt_codex_provider_includes_error_response_body(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    conn = app.state.bus.conn
    ensure_oauth_schema(conn)
    with conn:
        conn.execute(
            """
            INSERT INTO oauth_tokens(
              provider, subject, access_token, refresh_token, token_type, scope,
              expires_at, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "openai-codex",
                "codex-user",
                "access-token",
                "refresh-token",
                "Bearer",
                "openid",
                None,
                json.dumps({"refresh_after": (datetime.now(UTC) + timedelta(hours=1)).isoformat()}),
                datetime.now(UTC).isoformat(),
                datetime.now(UTC).isoformat(),
            ),
        )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": {"message": "missing instructions"}})

    original_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)

    def client_factory(*args, **kwargs):
        return original_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr("llm_harness.providers.chatgpt_codex.httpx.AsyncClient", client_factory)
    provider = app.state.registry.providers["chatgpt-codex"]

    async def consume() -> list[str]:
        return [delta async for delta in provider.stream_chat(model="codex", messages=[])]

    with pytest.raises(RuntimeError, match="missing instructions"):
        asyncio.run(consume())
