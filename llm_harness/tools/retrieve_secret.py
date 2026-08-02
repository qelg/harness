from __future__ import annotations

import asyncio
import logging
import re
import secrets
import shutil
from typing import Any

from fastapi import HTTPException, Request

from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    SecretAsk,
    ToolCall,
    ToolCallRequested,
    ToolMessageCreated,
    ToolResult,
    ToolSession,
)
from llm_harness.tools.podman_shell import (
    _session_container_owner,
    _session_user_tags,
)

logger = logging.getLogger(__name__)
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_-]{16,128}")
_SECRET_PATH_PREFIX = "/secrets/"
_MAX_SECRET_BYTES = 1024 * 1024


class RetrieveSecretTool:
    name = "retrieve-secret"
    description = (
        "Ask the user to provide a secret without putting its value in the conversation. "
        "The secret is written to a file in the current terminal container."
    )
    input_schema = {
        "type": "object",
        "properties": {
            "description": {
                "type": "string",
                "description": "Explain which secret is needed and what it is used for.",
            },
        },
        "required": ["description"],
        "additionalProperties": False,
    }

    def __init__(self, *, settings: Settings):
        self.settings = settings
        self._write_lock = asyncio.Lock()

    def container_for(self, call: ToolCall) -> str:
        for tag in call.session.tags:
            if tag in self.settings.tag_container_map:
                return self.settings.tag_container_map[tag]
        owner_id = call.session.container_owner_id or call.session.id
        return f"llm-harness-session-{owner_id}"

    async def container_exists(self, container: str) -> bool:
        if shutil.which("podman") is None:
            return False
        inspect = await asyncio.create_subprocess_exec(
            "podman",
            "container",
            "inspect",
            container,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await inspect.communicate()
        return inspect.returncode == 0

    async def write_secret(self, *, container: str, identifier: str, value: bytes) -> str:
        if not _IDENTIFIER_RE.fullmatch(identifier):
            raise ValueError("invalid secret identifier")
        if len(value) > _MAX_SECRET_BYTES:
            raise ValueError("secret is too large")
        if shutil.which("podman") is None:
            raise RuntimeError("podman is not installed or not on PATH")

        # A terminal container may be stopped between tool calls.  Starting it
        # here makes the write independent of that implementation detail; the
        # next terminal call will stop it again when it becomes idle.
        inspect = await asyncio.create_subprocess_exec(
            "podman", "container", "inspect", "--format", "{{.State.Running}}", container,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        running, inspect_error = await inspect.communicate()
        if inspect.returncode != 0:
            raise RuntimeError(inspect_error.decode(errors="replace").strip() or "container not found")
        started = running.strip() != b"true"
        if started:
            start = await asyncio.create_subprocess_exec(
                "podman", "start", container,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            _, start_error = await start.communicate()
            if start.returncode != 0:
                raise RuntimeError(start_error.decode(errors="replace").strip() or "could not start container")

        path = f"{_SECRET_PATH_PREFIX}{identifier}"
        process = await asyncio.create_subprocess_exec(
            "podman", "exec", "-i", container, "sh", "-c",
            "umask 077; mkdir -p /secrets; cat > \"$1\"", "sh", path,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, error = await process.communicate(value)
        if process.returncode != 0:
            raise RuntimeError(error.decode(errors="replace").strip() or "could not write secret")
        return path

    async def run(self, call: ToolCall) -> ToolResult:
        # The consumer owns the event transaction because an ask must be
        # durable before the corresponding tool result can be accepted.
        raise RuntimeError("retrieve-secret must be run by its event consumer")


class RetrieveSecretToolConsumer(EventConsumer):
    name = "retrieve-secret"
    subscriber = "plugin:retrieve-secret"
    event_filter = EventFilter(
        names=frozenset({ToolCallRequested.name}),
        tags={"tool": RetrieveSecretTool.name},
    )

    def __init__(self, *, tool: RetrieveSecretTool):
        self.tool = tool

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        if bus.replay(EventFilter(names=frozenset({SecretAsk.name}), causation_id=event.id), limit=1):
            return
        description = event.payload.get("input", {}).get("description")
        if not isinstance(description, str) or not description.strip():
            await self._fail(bus, event, ValueError("tool input requires non-empty string field 'description'"))
            return

        owner_id = _session_container_owner(bus, event.tags["session"])
        call = ToolCall(
            session=ToolSession(
                id=event.tags["session"],
                tags=_session_user_tags(bus, owner_id),
                container_owner_id=owner_id,
            ),
            name=self.tool.name,
            input=event.payload.get("input", {}),
        )
        container = self.tool.container_for(call)
        if not await self.tool.container_exists(container):
            await self._fail(bus, event, RuntimeError("no terminal container exists; call terminal first"))
            return

        identifier = secrets.token_urlsafe(24)
        await bus.append_message(
            SecretAsk(
                session_id=event.tags["session"],
                description=description.strip(),
                identifier=identifier,
                container=container,
                run_id=event.tags["run"],
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    async def _fail(self, bus: EventBus, event: EventRecord, error: Exception) -> None:
        message = f"tool execution failed: {type(error).__name__}: {error}\n"
        await bus.append_message(
            ToolMessageCreated(
                session_id=event.tags["session"],
                content=message,
                tool=self.tool.name,
                run_id=event.tags["run"],
                metadata={"success": False, "error_type": type(error).__name__, "error": str(error)},
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )


class RetrieveSecretApiPlugin:
    name = "retrieve-secret-api"

    def __init__(self, *, tool: RetrieveSecretTool):
        self.tool = tool

    def install_api(self, *, app, bus: EventBus, registry) -> None:
        @app.post("/secrets/{event_id}/{identifier}")
        async def upload_secret(event_id: int, identifier: str, request: Request) -> dict[str, str]:
            if not _IDENTIFIER_RE.fullmatch(identifier):
                raise HTTPException(status_code=404, detail="secret request not found")
            ask = bus.get_event(event_id)
            if ask is None or ask.name != SecretAsk.name or ask.payload.get("identifier") != identifier:
                raise HTTPException(status_code=404, detail="secret request not found")
            if bus.replay(
                EventFilter(names=frozenset({ToolMessageCreated.name}), causation_id=ask.causation_id),
            ):
                # The causation points at the original tool request so that the
                # normal tool-result requester can continue the model run.
                if any(
                    item.payload.get("metadata", {}).get("secret_ask_event_id") == ask.id
                    for item in bus.replay(
                        EventFilter(
                            names=frozenset({ToolMessageCreated.name}),
                            tags={"session": ask.tags["session"], "tool": "retrieve-secret", "run": ask.tags["run"]},
                        )
                    )
                ):
                    raise HTTPException(status_code=409, detail="secret was already provided")
            body = await request.body()
            if len(body) > _MAX_SECRET_BYTES:
                raise HTTPException(status_code=413, detail="secret is too large")
            if not await self.tool.container_exists(ask.payload["container"]):
                raise HTTPException(status_code=409, detail="terminal container no longer exists")
            async with self.tool._write_lock:
                # Check again while serialized, preventing two uploads from
                # producing two tool replies for one ask.
                existing = bus.replay(
                    EventFilter(
                        names=frozenset({ToolMessageCreated.name}),
                        tags={"session": ask.tags["session"], "tool": "retrieve-secret", "run": ask.tags["run"]},
                    )
                )
                if any(item.payload.get("metadata", {}).get("secret_ask_event_id") == ask.id for item in existing):
                    raise HTTPException(status_code=409, detail="secret was already provided")
                path = await self.tool.write_secret(
                    container=ask.payload["container"], identifier=identifier, value=body
                )
                await bus.append_message(
                    ToolMessageCreated(
                        session_id=ask.tags["session"],
                        content=f"Secret written to {path}\n",
                        tool="retrieve-secret",
                        run_id=ask.tags["run"],
                        metadata={
                            "success": True,
                            "path": path,
                            "container": ask.payload["container"],
                            "secret_ask_event_id": ask.id,
                        },
                    ),
                    producer=self.name,
                    causation_id=ask.causation_id,
                    correlation_id=ask.correlation_id or ask.id,
                )
            return {"status": "accepted", "path": f"/secrets/{identifier}"}
