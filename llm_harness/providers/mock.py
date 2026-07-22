from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

from llm_harness.core.types import Message


class MockLLMProvider:
    name = "mock-llm"

    def __init__(self, *, response: str):
        self.response = response

    async def stream_chat(self, *, model: str, messages: Sequence[Message]) -> AsyncIterator[str]:
        for chunk in _chunks(self.response):
            yield chunk


def _chunks(text: str) -> list[str]:
    if not text:
        return [""]
    midpoint = max(1, len(text) // 2)
    return [text[:midpoint], text[midpoint:]]
