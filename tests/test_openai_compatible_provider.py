from __future__ import annotations

import asyncio
import json
import logging

import httpx

from llm_harness.core.types import Message, Role
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


def test_openai_compatible_provider_accumulates_reasoning_tool_calls_and_usage(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["stream_options"] == {"include_usage": True}
        return httpx.Response(
            200,
            content=(
                b'data: {"id":"chat_1","model":"openrouter-model","choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n'
                b'data: {"choices":[{"index":0,"delta":{"reasoning":"plan "}}]}\n\n'
                b'data: {"choices":[{"index":0,"delta":{"reasoning":"done"}}]}\n\n'
                b'data: {"choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.text","text":"The","format":"unknown","index":0}]}}]}\n\n'
                b'data: {"choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.text","text":" user","format":"unknown","index":0}]}}]}\n\n'
                b'data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}\n\n'
                b'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"podman-shell","arguments":"{\\"cmd\\":"}}]}}]}\n\n'
                b'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\\"echo hi\\"}"}}]},"finish_reason":"tool_calls","native_finish_reason":"tool_calls"}]}\n\n'
                b'data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":5,"total_tokens":16}}\n\n'
                b"data: [DONE]\n\n"
            ),
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
    )

    async def consume_events() -> list:
        return [event async for event in provider.stream_response(model="test", messages=[])]

    events = asyncio.run(consume_events())

    assert [event.delta for event in events if event.type == "delta"] == ["hi"]
    completed = events[-1].response
    assert completed["id"] == "chat_1"
    assert completed["usage"]["total_tokens"] == 16
    assert completed["finish_reason"] == "tool_calls"
    assert completed["message"]["content"] == "hi"
    assert completed["message"]["reasoning"] == "plan done"
    assert completed["message"]["reasoning_details"] == [
        {"type": "reasoning.text", "text": "The user", "format": "unknown", "index": 0}
    ]
    assert completed["output"][0]["tool_calls"][0]["function"]["name"] == "podman-shell"
    assert completed["output"][0]["tool_calls"][0]["function"]["arguments"] == '{"cmd":"echo hi"}'
    assert "stream_chunks" not in completed


def test_openai_compatible_provider_sends_json_message_content(monkeypatch):
    assistant_output = [{"type": "function_call", "name": "echo"}]

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["messages"] == [{"role": "assistant", "content": assistant_output}]
        return httpx.Response(
            200,
            content=b'data: {"choices":[{"index":0,"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n',
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
    )

    async def consume_events() -> list:
        message = Message(id=1, session_id="sess_1", role=Role.ASSISTANT, content=assistant_output)
        return [event async for event in provider.stream_response(model="test", messages=[message])]

    assert asyncio.run(consume_events())[0].delta == "ok"


def test_openai_compatible_provider_keeps_stream_chunks_when_logging_enabled(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b'data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}\n\ndata: [DONE]\n\n',
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

    async def consume_events() -> list:
        return [event async for event in provider.stream_response(model="test", messages=[])]

    completed = asyncio.run(consume_events())[-1].response
    assert completed["stream_chunks"][0]["choices"][0]["delta"]["content"] == "hi"
