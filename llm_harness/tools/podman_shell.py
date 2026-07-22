from __future__ import annotations

import asyncio
import re
import shutil

from llm_harness.config import Settings
from llm_harness.core.types import ToolCall, ToolResult


class PodmanShellTool:
    name = "podman-shell"

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
        if process.returncode != 0:
            raise RuntimeError(error or f"command failed with exit code {process.returncode}")
        return ToolResult(output=output, metadata={"container": container, "stderr": error})

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


def _valid_container_name(name: str) -> bool:
    return bool(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", name))
