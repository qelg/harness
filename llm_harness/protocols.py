from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Protocol, TYPE_CHECKING

from llm_harness.core.types import Message, ToolCall, ToolResult, ToolSpec

if TYPE_CHECKING:
    from fastapi import FastAPI

    from llm_harness.core.events import EventBus
    from llm_harness.plugins import Registry


class LLMProvider(Protocol):
    name: str

    async def stream_chat(
        self,
        *,
        model: str,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> AsyncIterator[str]:
        """Yield assistant text deltas."""


class Tool(Protocol):
    name: str
    description: str
    input_schema: dict

    async def run(self, call: ToolCall) -> ToolResult:
        """Run the tool and return a persisted result payload."""


class ToolSet(Protocol):
    name: str

    def tools(self, *, registry: "Registry") -> Sequence[ToolSpec]:
        """Return the tool specs this toolset exposes to LLM providers."""


class ApiPlugin(Protocol):
    name: str

    def install_api(self, *, app: "FastAPI", bus: "EventBus", registry: "Registry") -> None:
        """Install API routes and any plugin-owned database schema."""


class EventConsumerPlugin(Protocol):
    name: str

    def install_event_consumers(self, *, app: "FastAPI", bus: "EventBus", registry: "Registry") -> None:
        """Install event consumers, usually by registering FastAPI startup/shutdown tasks."""
