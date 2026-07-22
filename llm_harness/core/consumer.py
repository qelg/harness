from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from dataclasses import replace
from typing import Any

from llm_harness.core.events import EventBus, EventFilter, EventRecord


class EventConsumer(ABC):
    name: str
    subscriber: str
    event_filter: EventFilter

    def install_event_consumers(self, *, app, bus: EventBus, registry) -> None:
        task: asyncio.Task[None] | None = None

        async def start() -> None:
            nonlocal task
            settings = getattr(self, "settings", None)
            if bool(getattr(settings, "workers_inline", False)):
                await self.process_pending(bus, registry=registry)
                return
            task = asyncio.create_task(self.run(bus, registry=registry))

        async def stop() -> None:
            if task is None:
                return
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        router = getattr(app, "router", app)
        router.add_event_handler("startup", start)
        router.add_event_handler("shutdown", stop)

    async def run(self, bus: EventBus, *, registry: Any = None) -> None:
        async with bus.subscribe(self._filter_from_last_ack(bus)) as queue:
            await self.process_pending(bus, registry=registry)
            while True:
                event = await queue.get()
                if event.id <= bus.last_acked(self.subscriber):
                    continue
                await self._process_and_ack(bus, event, registry=registry)

    async def process_pending(self, bus: EventBus, *, registry: Any = None) -> None:
        for event in bus.replay(self._filter_from_last_ack(bus)):
            await self._process_and_ack(bus, event, registry=registry)

    async def _process_and_ack(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        await self.process_event(bus, event, registry=registry)
        bus.ack(self.subscriber, event.id)

    def _filter_from_last_ack(self, bus: EventBus) -> EventFilter:
        return replace(self.event_filter, since_id=bus.last_acked(self.subscriber))

    @abstractmethod
    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        ...
