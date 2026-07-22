from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol


SESSION = "session"
CHAT = "chat"
ROLE = "role"
PROVIDER = "provider"
MODEL = "model"
TOOL = "tool"
TOOLSET = "toolset"
RUN = "run"


class Role(StrEnum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


@dataclass(frozen=True)
class Message:
    id: int
    session_id: str
    role: Role
    content: Any
    provider: str | None = None
    model: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None


@dataclass(frozen=True)
class ToolSession:
    id: str
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class ToolCall:
    session: ToolSession
    name: str
    input: dict[str, Any]


@dataclass(frozen=True)
class ToolResult:
    output: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True)
class ProviderStreamEvent:
    type: str
    delta: str | None = None
    response: dict[str, Any] | None = None
    raw_event: dict[str, Any] | None = None


class CoreEventMessage(Protocol):
    name: str

    def payload(self) -> dict[str, Any]:
        ...

    def tags(self) -> dict[str, str]:
        ...


@dataclass(frozen=True)
class SessionCreated:
    session_id: str
    title: str | None = None
    session_tags: tuple[str, ...] = ()

    name: str = "session.created"

    def payload(self) -> dict[str, Any]:
        return {"title": self.title, "tags": list(self.session_tags)}

    def tags(self) -> dict[str, str]:
        tags = session_tags(self.session_id, *self.session_tags)
        return tags


@dataclass(frozen=True)
class UserMessageCreated:
    session_id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)

    name: str = "chat.message.user.created"

    def payload(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "metadata": self.metadata,
        }

    def tags(self) -> dict[str, str]:
        return {SESSION: self.session_id, CHAT: self.session_id, ROLE: "user"}


@dataclass(frozen=True)
class AssistantMessageCreated:
    session_id: str
    content: Any
    provider: str
    model: str
    run_id: str
    metadata: dict[str, Any] = field(default_factory=dict)

    name: str = "chat.message.assistant.created"

    def payload(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "provider": self.provider,
            "model": self.model,
            "run_id": self.run_id,
            "metadata": self.metadata,
        }

    def tags(self) -> dict[str, str]:
        return {
            SESSION: self.session_id,
            CHAT: self.session_id,
            ROLE: "assistant",
            PROVIDER: self.provider,
            MODEL: self.model,
            RUN: self.run_id,
        }


@dataclass(frozen=True)
class ModelSelected:
    provider: str
    model: str
    toolsets: tuple[str, ...] = ()
    session_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    name: str = "llm.model.selected"

    def payload(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "toolsets": list(self.toolsets),
            "session_id": self.session_id,
            "metadata": self.metadata,
        }

    def tags(self) -> dict[str, str]:
        tags = {PROVIDER: self.provider, MODEL: self.model}
        if self.session_id is not None:
            tags[SESSION] = self.session_id
            tags[CHAT] = self.session_id
        return tags


@dataclass(frozen=True)
class LlmRunRequested:
    session_id: str
    provider: str
    model: str
    run_id: str
    toolsets: tuple[str, ...] = ()
    user_message_event_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    name: str = "llm.run.requested"

    def payload(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "run_id": self.run_id,
            "toolsets": list(self.toolsets),
            "user_message_event_id": self.user_message_event_id,
            "metadata": self.metadata,
        }

    def tags(self) -> dict[str, str]:
        return {
            SESSION: self.session_id,
            CHAT: self.session_id,
            PROVIDER: self.provider,
            MODEL: self.model,
            RUN: self.run_id,
        }


@dataclass(frozen=True)
class LlmRunStarted:
    session_id: str
    provider: str
    model: str
    run_id: str
    metadata: dict[str, Any] = field(default_factory=dict)

    name: str = "llm.run.started"

    def payload(self) -> dict[str, Any]:
        return {"provider": self.provider, "model": self.model, "run_id": self.run_id, "metadata": self.metadata}

    def tags(self) -> dict[str, str]:
        return {
            SESSION: self.session_id,
            CHAT: self.session_id,
            PROVIDER: self.provider,
            MODEL: self.model,
            RUN: self.run_id,
        }


@dataclass(frozen=True)
class LlmDelta:
    session_id: str
    provider: str
    model: str
    run_id: str
    delta: str
    sequence: int

    name: str = "llm.delta"

    def payload(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "run_id": self.run_id,
            "delta": self.delta,
            "sequence": self.sequence,
        }

    def tags(self) -> dict[str, str]:
        return {
            SESSION: self.session_id,
            CHAT: self.session_id,
            PROVIDER: self.provider,
            MODEL: self.model,
            RUN: self.run_id,
        }


@dataclass(frozen=True)
class LlmRunFailed:
    session_id: str
    provider: str
    model: str
    run_id: str
    error: str
    retryable: bool = False

    name: str = "llm.run.failed"

    def payload(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "run_id": self.run_id,
            "error": self.error,
            "retryable": self.retryable,
        }

    def tags(self) -> dict[str, str]:
        return {
            SESSION: self.session_id,
            CHAT: self.session_id,
            PROVIDER: self.provider,
            MODEL: self.model,
            RUN: self.run_id,
        }


@dataclass(frozen=True)
class ToolMessageCreated:
    session_id: str
    content: str
    tool: str
    run_id: str
    metadata: dict[str, Any] = field(default_factory=dict)

    name: str = "chat.message.tool.created"

    def payload(self) -> dict[str, Any]:
        return {"content": self.content, "tool": self.tool, "run_id": self.run_id, "metadata": self.metadata}

    def tags(self) -> dict[str, str]:
        return {SESSION: self.session_id, CHAT: self.session_id, ROLE: "tool", TOOL: self.tool, RUN: self.run_id}


@dataclass(frozen=True)
class ToolCallRequested:
    session_id: str
    tool: str
    input: dict[str, Any]
    run_id: str

    name: str = "tool.call.requested"

    def payload(self) -> dict[str, Any]:
        return {"tool": self.tool, "input": self.input, "run_id": self.run_id}

    def tags(self) -> dict[str, str]:
        return {SESSION: self.session_id, CHAT: self.session_id, TOOL: self.tool, RUN: self.run_id}


REQUIRED_TAGS: dict[str, frozenset[str]] = {
    SessionCreated.name: frozenset({SESSION}),
    UserMessageCreated.name: frozenset({SESSION}),
    AssistantMessageCreated.name: frozenset({SESSION, PROVIDER, MODEL, RUN}),
    ModelSelected.name: frozenset({PROVIDER, MODEL}),
    LlmRunRequested.name: frozenset({SESSION, PROVIDER, MODEL, RUN}),
    LlmRunStarted.name: frozenset({SESSION, PROVIDER, MODEL, RUN}),
    LlmDelta.name: frozenset({SESSION, PROVIDER, MODEL, RUN}),
    LlmRunFailed.name: frozenset({SESSION, PROVIDER, MODEL, RUN}),
    ToolMessageCreated.name: frozenset({SESSION, TOOL, RUN}),
    ToolCallRequested.name: frozenset({SESSION, TOOL, RUN}),
}

MESSAGE_CREATED_NAMES = frozenset(
    {
        UserMessageCreated.name,
        AssistantMessageCreated.name,
        ToolMessageCreated.name,
    }
)

DURABLE_DEFAULTS: dict[str, bool] = {
    "llm.delta": False,
    "tool.stdout.delta": False,
    "tool.stderr.delta": False,
}


def new_session_id() -> str:
    return f"sess_{secrets.token_urlsafe(18)}"


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}_{secrets.token_urlsafe(12)}"


def session_tags(session_id: str, *extra_tags: str) -> dict[str, str]:
    tags = {SESSION: session_id, CHAT: session_id}
    for tag in extra_tags:
        if tag.strip():
            tags[f"session_tag:{tag.strip()}"] = "true"
    return tags


def required_tags_for(event_name: str) -> frozenset[str]:
    return REQUIRED_TAGS.get(event_name, frozenset())


def durable_default_for(event_name: str) -> bool:
    return DURABLE_DEFAULTS.get(event_name, True)


def validate_required_tags(event_name: str, tags: dict[str, str]) -> None:
    missing = sorted(required_tags_for(event_name) - set(tags))
    if missing:
        raise ValueError(f"event {event_name} is missing required tags: {', '.join(missing)}")


def to_event_parts(message: CoreEventMessage) -> tuple[str, dict[str, Any], dict[str, str]]:
    return message.name, message.payload(), message.tags()
