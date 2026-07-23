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
    name = "podman-shell"
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

    async def run(self, call: ToolCall) -> ToolResult:
        cmd = call.input.get("cmd")
        timeout = float(call.input.get("timeout", 30))
        if not isinstance(cmd, str) or not cmd.strip():
            raise ValueError("tool input requires non-empty string field 'cmd'")
        if shutil.which("podman") is None:
            raise RuntimeError("podman is not installed or not on PATH")

        container = self._container_for(call)
        await self._ensure_container(container)
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

    def _container_for(self, call: ToolCall) -> str:
        for tag in call.session.tags:
            if tag in self.settings.tag_container_map:
                return self.settings.tag_container_map[tag]
        return f"llm-harness-session-{call.session.id}"

    async def _ensure_container(self, name: str) -> None:
        if not _valid_container_name(name):
            raise ValueError(f"invalid container name: {name}")
        exists = await asyncio.create_subprocess_exec(
            "podman",
            "container",
            "exists",
            name,
        )
        code = await exists.wait()
        if code == 0:
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


class PodmanShellToolConsumer(EventConsumer):
    name = "podman-shell"
    subscriber = "plugin:podman-shell"
    event_filter = EventFilter(names=frozenset({"tool.call.requested"}), tags={"tool": "podman-shell"})

    def __init__(self, *, tool: PodmanShellTool):
        self.tool = tool

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry=None) -> None:
        if await self._already_completed(bus, event):
            return
        call = ToolCall(
            session=ToolSession(id=event.tags["session"], tags=_session_user_tags(bus, event.tags["session"])),
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
        except Exception:
            logger.exception(
                "tool execution failed tool=%s session=%s run=%s",
                self.tool.name,
                event.tags["session"],
                event.tags["run"],
            )
            raise
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


def _failed_command_output(stdout: str, stderr: str, exit_code: int | None) -> str:
    parts = [f"command exited with code {exit_code}"]
    if stdout:
        parts.extend(["", "stdout:", stdout.rstrip("\n")])
    if stderr:
        parts.extend(["", "stderr:", stderr.rstrip("\n")])
    return "\n".join(parts) + "\n"


def _session_user_tags(bus: EventBus, session_id: str) -> tuple[str, ...]:
    events = bus.replay(EventFilter(names=frozenset({"session.created"}), tags={"session": session_id}), limit=1)
    if not events:
        return ()
    tags = events[0].payload.get("tags", [])
    if not isinstance(tags, list):
        return ()
    return tuple(tag for tag in tags if isinstance(tag, str))
