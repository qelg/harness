from __future__ import annotations

import asyncio
import logging
import re
import shutil

from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import ToolCall, ToolMessageCreated, ToolResult, ToolSession

logger = logging.getLogger(__name__)


class PodmanShellTool:
    name = "terminal"
    description = "Run a shell command"
    input_schema = {
        "type": "object",
        "properties": {
            "cmd": {
                "type": "string",
                "description": "Shell command to execute.",
            },
            "timeout": {
                "type": "number",
                "description": "Maximum runtime in seconds.",
                "default": 30,
            },
        },
        "required": ["cmd"],
        "additionalProperties": False,
    }

    def __init__(self, *, settings: Settings):
        self.settings = settings
        self._container_locks: dict[str, asyncio.Lock] = {}
        self._active_commands: dict[str, int] = {}

    async def run(self, call: ToolCall) -> ToolResult:
        cmd = call.input.get("cmd")
        timeout = float(call.input.get("timeout", 30))
        if not isinstance(cmd, str) or not cmd.strip():
            raise ValueError("tool input requires non-empty string field 'cmd'")
        if shutil.which("podman") is None:
            raise RuntimeError("podman is not installed or not on PATH")

        container = self._container_for(call)
        await self._acquire_container(container)
        try:
            process = await asyncio.create_subprocess_exec(
                "podman",
                "exec",
                container,
                "sh",
                "-lc",
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
            except TimeoutError:
                process.kill()
                raise

            output = stdout.decode(errors="replace")
            error = stderr.decode(errors="replace")
            metadata = {
                "container": container,
                "stderr": error,
                "exit_code": process.returncode,
                "success": process.returncode == 0,
            }
            if process.returncode != 0:
                output = _failed_command_output(output, error, process.returncode)
            return ToolResult(output=output, metadata=metadata)
        finally:
            await self._release_container(container)

    def _container_for(self, call: ToolCall) -> str:
        for tag in call.session.tags:
            if tag in self.settings.tag_container_map:
                return self.settings.tag_container_map[tag]
        owner_id = call.session.container_owner_id or call.session.id
        return f"llm-harness-session-{owner_id}"

    async def _acquire_container(self, name: str) -> None:
        lock = self._container_locks.setdefault(name, asyncio.Lock())
        async with lock:
            if self._active_commands.get(name, 0) == 0:
                await self._ensure_container(name)
            self._active_commands[name] = self._active_commands.get(name, 0) + 1

    async def _release_container(self, name: str) -> None:
        lock = self._container_locks[name]
        async with lock:
            remaining = self._active_commands[name] - 1
            if remaining > 0:
                self._active_commands[name] = remaining
                return
            del self._active_commands[name]
            try:
                await self._stop_container(name)
            except Exception:
                logger.exception("failed to stop idle tool container container=%s", name)

    async def _ensure_container(self, name: str) -> None:
        if not _valid_container_name(name):
            raise ValueError(f"invalid container name: {name}")
        inspect = await asyncio.create_subprocess_exec(
            "podman",
            "container",
            "inspect",
            "--format",
            "{{.State.Running}}",
            name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await inspect.communicate()
        if inspect.returncode == 0:
            if stdout.strip() == b"true":
                return
            await self._start_container(name)
            return

        command = [
            "podman",
            "run",
            "-d",
            "--name",
            name,
            "--label",
            "llm-harness=true",
        ]
        if self.settings.podman_mount_nix_store:
            command.extend(["--volume", "/nix/store:/nix/store:ro"])
        command.extend(
            [
                self.settings.podman_image,
                "sleep",
                "infinity",
            ]
        )
        start = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await start.communicate()
        if start.returncode != 0:
            raise RuntimeError(stderr.decode(errors="replace") or stdout.decode(errors="replace"))

    async def _start_container(self, name: str) -> None:
        start = await asyncio.create_subprocess_exec(
            "podman",
            "start",
            name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await start.communicate()
        if start.returncode != 0:
            raise RuntimeError(stderr.decode(errors="replace") or stdout.decode(errors="replace"))

    async def _stop_container(self, name: str) -> None:
        stop = await asyncio.create_subprocess_exec(
            "podman",
            "stop",
            "--time",
            "0",
            name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await stop.communicate()
        if stop.returncode != 0:
            raise RuntimeError(stderr.decode(errors="replace") or stdout.decode(errors="replace"))


class PodmanShellToolConsumer(EventConsumer):
    name = "terminal"
    subscriber = "plugin:terminal"
    event_filter = EventFilter(names=frozenset({"tool.call.requested"}), tags={"tool": "terminal"})

    def __init__(self, *, tool: PodmanShellTool):
        self.tool = tool

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry=None) -> None:
        if await self._already_completed(bus, event):
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
        logger.info(
            "starting tool execution tool=%s session=%s run=%s input=%s",
            self.tool.name,
            event.tags["session"],
            event.tags["run"],
            call.input,
        )
        try:
            result = await self.tool.run(call)
        except Exception as exc:
            logger.exception(
                "tool execution failed tool=%s session=%s run=%s",
                self.tool.name,
                event.tags["session"],
                event.tags["run"],
            )
            result = _exception_result(exc)
        logger.info(
            "finished tool execution tool=%s session=%s run=%s output_bytes=%d metadata=%s",
            self.tool.name,
            event.tags["session"],
            event.tags["run"],
            len(result.output.encode()),
            result.metadata,
        )
        await bus.append_message(
            ToolMessageCreated(
                session_id=event.tags["session"],
                content=result.output,
                tool=self.tool.name,
                run_id=event.tags["run"],
                metadata=result.metadata,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    async def _already_completed(self, bus: EventBus, event: EventRecord) -> bool:
        messages = bus.replay(
            EventFilter(
                names=frozenset({ToolMessageCreated.name}),
                tags={"session": event.tags["session"], "tool": self.tool.name, "run": event.tags["run"]},
            )
        )
        return bool(messages)


def _valid_container_name(name: str) -> bool:
    return bool(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", name))


def _exception_result(exc: Exception) -> ToolResult:
    error_type = type(exc).__name__
    error_message = str(exc)
    description = f"{error_type}: {error_message}" if error_message else error_type
    return ToolResult(
        output=f"tool execution failed: {description}\n",
        metadata={
            "success": False,
            "error_type": error_type,
            "error": error_message,
        },
    )


def _failed_command_output(stdout: str, stderr: str, exit_code: int | None) -> str:
    parts = [f"command exited with code {exit_code}"]
    if stdout:
        parts.extend(["", "stdout:", stdout.rstrip("\n")])
    if stderr:
        parts.extend(["", "stderr:", stderr.rstrip("\n")])
    return "\n".join(parts) + "\n"


def _session_container_owner(bus: EventBus, session_id: str) -> str:
    """Resolve durable same-container pointers without trusting a cycle."""
    current = session_id
    seen: set[str] = set()
    while current not in seen:
        seen.add(current)
        created = _session_created(bus, current)
        if created is None:
            break
        metadata = created.payload.get("metadata")
        owner = metadata.get("terminal_container_owner_session_id") if isinstance(metadata, dict) else None
        if not isinstance(owner, str) or not owner or owner == current:
            break
        current = owner
    return current


def _session_created(bus: EventBus, session_id: str) -> EventRecord | None:
    return bus.latest(
        EventFilter(names=frozenset({"session.created"}), tags={"session": session_id})
    )


def _session_user_tags(bus: EventBus, session_id: str) -> tuple[str, ...]:
    event = _session_created(bus, session_id)
    if event is None:
        return ()
    tags = event.payload.get("tags", [])
    if not isinstance(tags, list):
        return ()
    return tuple(tag for tag in tags if isinstance(tag, str))
