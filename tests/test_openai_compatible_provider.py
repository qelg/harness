from __future__ import annotations

import asyncio
import logging

import httpx

from llm_harness.providers.openai_compatible import OpenAICompatibleProvider


def test_openai_compatible_provider_logs_raw_stream_events(monkeypatch, caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b'data: {"choices":[{"delta":{"tool_calls":[{"id":"call_1"}]}}]}\n\ndata: [DONE]\n\n',
            headers={"content-type": "text/event-stream"},
        )

    original_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)

    def client_factory(*args, **kwargs):
        return original_client(transport=transport, timeout=kwargs.get("timeout"))

    monkeypatch.setattr("llm_harness.providers.openai_compatible.httpx.AsyncClient", client_factory)
    provider = OpenAICompatibleProvider(
        name="openrouter",
        base_url="https://openrouter.ai/api/v1",
        api_key="test-key",
        log_provider_events=True,
    )

    async def consume() -> list[str]:
        return [delta async for delta in provider.stream_chat(model="test", messages=[])]

    with caplog.at_level(logging.INFO, logger="llm_harness.providers.openai_compatible"):
        assert asyncio.run(consume()) == []
    assert "openrouter event" in caplog.text
    assert "tool_calls" in caplog.text
