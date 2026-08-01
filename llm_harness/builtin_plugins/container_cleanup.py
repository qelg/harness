"""Lifecycle and API support for per-session terminal containers."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import ARCHIVE, PARENT_SESSION, SessionCreated, SessionStateChanged

logger = logging.getLogger(__name__)
_CONTAINER_PREFIX = "llm-harness-session-"
_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgtpe]?i?b)\b", re.I)
_SIZE_UNITS = {"b": 1, "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4, "pb": 1000**5,
               "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4, "pib": 1024**5}


@dataclass(frozen=True)
class Container:
    id: str
    name: str
    size_bytes: int
    session_ids: tuple[str, ...]
    session_title: str | None

    def api_value(self) -> dict[str, Any]:
        return {"container_id": self.id, "name": self.name, "size_bytes": self.size_bytes,
                "session_ids": list(self.session_ids),
                "session_id": self.name.removeprefix(_CONTAINER_PREFIX),
                "session_title": self.session_title}


class PodmanContainerManager:
    """Only operates on containers named by the session-isolated terminal tool."""

    async def containers(self, bus: EventBus) -> list[Container]:
        if shutil.which("podman") is None:
            return []
        process = await asyncio.create_subprocess_exec(
            "podman", "ps", "--all", "--size", "--format", "json",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode:
            raise RuntimeError(stderr.decode(errors="replace").strip() or "podman ps failed")
        try:
            rows = json.loads(stdout or b"[]")
        except json.JSONDecodeError as exc:
            raise RuntimeError("podman ps returned invalid JSON") from exc
        owners = _session_ids_by_container(bus)
        containers = []
        for row in rows:
            name = _container_name(row)
            if not name.startswith(_CONTAINER_PREFIX):
                continue
            container_id = str(row.get("Id") or row.get("ID") or row.get("Id") or name)
            session_id = name.removeprefix(_CONTAINER_PREFIX)
            containers.append(Container(
                container_id, name, _size_bytes(row.get("Size")),
                tuple(sorted(owners.get(name, ()) )), _session_title(bus, session_id),
            ))
        return sorted(containers, key=lambda container: (-container.size_bytes, container.name))

    async def delete(self, container: Container) -> int:
        if shutil.which("podman") is None:
            raise RuntimeError("podman is not installed or not on PATH")
        process = await asyncio.create_subprocess_exec(
            "podman", "container", "rm", "--force", container.id,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode:
            raise RuntimeError(stderr.decode(errors="replace").strip() or stdout.decode(errors="replace").strip() or "podman container rm failed")
        logger.info("deleted terminal container container=%s sessions=%s saved_bytes=%d", container.name,
                    ",".join(container.session_ids) or "unknown", container.size_bytes)
        return container.size_bytes

    async def delete_session_tree(self, bus: EventBus, session_id: str) -> int:
        session_ids = _descendant_sessions(bus, session_id)
        # Delete containers owned by this tree. A child sharing its parent container
        # must not delete that parent container when only the child is archived.
        owners = {_container_owner(bus, item) for item in session_ids}
        containers = await self.containers(bus)
        saved = 0
        for container in containers:
            owner = container.name.removeprefix(_CONTAINER_PREFIX)
            if owner in owners:
                saved += await self.delete(container)
        return saved


class ContainerCleanupPlugin(EventConsumer):
    name = "container-cleanup"
    subscriber = "plugin:container-cleanup"
    event_filter = EventFilter(names=frozenset({SessionStateChanged.name}), tags={ARCHIVE: "true"})

    def __init__(self, *, manager: PodmanContainerManager):
        self.manager = manager

    def install_api(self, *, app, bus: EventBus, registry) -> None:
        @app.get("/containers")
        async def list_containers() -> list[dict[str, Any]]:
            try:
                return [container.api_value() for container in await self.manager.containers(bus)]
            except RuntimeError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc

        @app.delete("/containers/{container_id}")
        async def delete_container(container_id: str) -> dict[str, Any]:
            try:
                container = next((item for item in await self.manager.containers(bus)
                                  if item.id == container_id), None)
                if container is None:
                    raise HTTPException(status_code=404, detail="container not found")
                saved = await self.manager.delete(container)
                return {"container_id": container_id, "saved_bytes": saved}
            except HTTPException:
                raise
            except RuntimeError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry=None) -> None:
        saved = await self.manager.delete_session_tree(bus, event.tags["session"])
        logger.info("archived session container cleanup session=%s saved_bytes=%d", event.tags["session"], saved)


def _container_name(row: dict[str, Any]) -> str:
    names = row.get("Names") or row.get("Name") or ""
    return str(names[0]).lstrip("/") if isinstance(names, list) and names else str(names).lstrip("/")


def _size_bytes(value: Any) -> int:
    # Modern Podman JSON represents sizes exactly as {"rwSize": ..., "rootFsSize": ...}.
    # rwSize is the reclaimable writable layer; rootFsSize includes the shared image.
    if isinstance(value, dict):
        rw_size = value.get("rwSize")
        if isinstance(rw_size, int) and rw_size >= 0:
            return rw_size
        if isinstance(rw_size, float) and rw_size >= 0:
            return int(rw_size)
    # Older Podman releases report e.g. "1.2MB (virtual 123MB)".
    match = _SIZE_RE.match(str(value or ""))
    if match is None:
        return 0
    return int(float(match.group(1)) * _SIZE_UNITS[match.group(2).lower()])


def _session_ids_by_container(bus: EventBus) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for event in bus.replay(EventFilter(names=frozenset({SessionCreated.name}))):
        session_id = event.tags["session"]
        owner = _container_owner(bus, session_id)
        result.setdefault(f"{_CONTAINER_PREFIX}{owner}", set()).add(session_id)
    return result


def _container_owner(bus: EventBus, session_id: str) -> str:
    current, seen = session_id, set()
    while current not in seen:
        seen.add(current)
        event = bus.latest(EventFilter(names=frozenset({SessionCreated.name}), tags={"session": current}))
        metadata = event.payload.get("metadata") if event else None
        owner = metadata.get("terminal_container_owner_session_id") if isinstance(metadata, dict) else None
        if not isinstance(owner, str) or not owner or owner == current:
            break
        current = owner
    return current


def _descendant_sessions(bus: EventBus, root: str) -> set[str]:
    children: dict[str, set[str]] = {}
    for event in bus.replay(EventFilter(names=frozenset({SessionCreated.name}))):
        parent = event.tags.get(PARENT_SESSION)
        if parent:
            children.setdefault(parent, set()).add(event.tags["session"])
    result, pending = {root}, [root]
    while pending:
        parent = pending.pop()
        for child in children.get(parent, ()):
            if child not in result:
                result.add(child)
                pending.append(child)
    return result


def _session_title(bus: EventBus, session_id: str) -> str | None:
    event = bus.latest(EventFilter(names=frozenset({SessionCreated.name}), tags={"session": session_id}))
    if event is None:
        return None
    title = event.payload.get("title")
    return title if isinstance(title, str) and title.strip() else None
