from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from llm_harness.api import create_app
from llm_harness.auth_plugins.token_store import ensure_oauth_schema


def test_chatgpt_codex_provider_requires_login_token(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))
    app = create_app()
    provider = app.state.registry.providers["chatgpt-codex"]

    async def consume() -> list[str]:
        return [delta async for delta in provider.stream_chat(model="codex", messages=[])]

    with pytest.raises(RuntimeError, match="Login with ChatGPT"):
        asyncio.run(consume())


def test_chatgpt_codex_provider_uses_stored_access_token(tmp_path, monkeypatch):
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
        assert request.headers["Authorization"] == "Bearer access-token"
        assert request.url.path.endswith("/chat/completions")
        return httpx.Response(
            200,
            content=b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\ndata: [DONE]\n\n',
            headers={"content-type": "text/event-stream"},
        )

    original_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)

    def client_factory(*args, **kwargs):
        return original_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr("llm_harness.providers.chatgpt_codex.httpx.AsyncClient", client_factory)
    provider = app.state.registry.providers["chatgpt-codex"]

    async def consume() -> list[str]:
        return [delta async for delta in provider.stream_chat(model="codex", messages=[])]

    assert asyncio.run(consume()) == ["hello"]
