from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import replace
from typing import Any, Iterable

from llm_harness.core.events import EventBus, EventFilter, EventRecord


class _EventTaskPool:
    """Run callbacks concurrently while acknowledging events in input order."""

    def __init__(self, consumer: "EventConsumer", bus: EventBus, *, registry: Any, parallelity: int):
        self.consumer = consumer
        self.bus = bus
        self.registry = registry
        self.parallelity = parallelity
        self.tasks: deque[tuple[EventRecord, asyncio.Task[None]]] = deque()

    @property
    def active(self) -> set[asyncio.Task[None]]:
        return {task for _, task in self.tasks if not task.done()}

    async def submit(self, event: EventRecord) -> None:
        while len(self.active) >= self.parallelity:
            await self.wait_for_completion()
        task = asyncio.create_task(self.consumer.process_event(self.bus, event, registry=self.registry))
        self.tasks.append((event, task))

    async def wait_for_completion(self) -> None:
        active = self.active
        if active:
            await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
        await self.advance_cursor()

    async def drain(self) -> None:
        while self.tasks:
            await self.wait_for_completion()

    async def cancel(self) -> None:
        tasks = [task for _, task in self.tasks]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()

    async def advance_cursor(self) -> None:
        failed_index = next(
            (
                index
                for index, (_, task) in enumerate(self.tasks)
                if task.done() and not task.cancelled() and task.exception() is not None
            ),
            None,
        )
        if failed_index is not None:
            # Finish lower events so their work can be acknowledged. Work above
            # the failed event cannot advance the cursor and is cancelled.
            tasks = [task for _, task in self.tasks]
            for task in tasks[failed_index + 1 :]:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        while self.tasks and self.tasks[0][1].done():
            event, task = self.tasks.popleft()
            task.result()
            self.bus.ack(self.consumer.subscriber, event.id)


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
        pool = self._task_pool(bus, registry)
        last_scheduled = bus.last_acked(self.subscriber)
        queue_task: asyncio.Task[EventRecord] | None = None
        try:
            async with bus.subscribe(self._filter_from_last_ack(bus)) as queue:
                for event in bus.replay(self._filter_from_last_ack(bus)):
                    await pool.submit(event)
                    last_scheduled = event.id

                queue_task = asyncio.create_task(queue.get())
                while True:
                    # A callback may have completed immediately after it was
                    # submitted, before it could be included in an asyncio.wait.
                    await pool.advance_cursor()
                    done, _ = await asyncio.wait(
                        pool.active | {queue_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if any(task is not queue_task for task in done):
                        await pool.advance_cursor()
                    if queue_task in done:
                        event = queue_task.result()
                        queue_task = asyncio.create_task(queue.get())
                        # The subscription is opened before replay, so events
                        # published during replay can occur in both sources.
                        if event.id <= last_scheduled:
                            continue
                        await pool.submit(event)
                        last_scheduled = event.id
        finally:
            if queue_task is not None:
                queue_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await queue_task
            await pool.cancel()

    async def process_pending(self, bus: EventBus, *, registry: Any = None) -> None:
        await self._process_events(
            bus,
            bus.replay(self._filter_from_last_ack(bus)),
            registry=registry,
        )

    async def _process_events(
        self,
        bus: EventBus,
        events: Iterable[EventRecord],
        *,
        registry: Any = None,
    ) -> None:
        pool = self._task_pool(bus, registry)
        try:
            for event in events:
                await pool.submit(event)
            await pool.drain()
        finally:
            await pool.cancel()

    def _task_pool(self, bus: EventBus, registry: Any) -> _EventTaskPool:
        configured = getattr(registry, "event_consumer_parallelity", {})
        parallelity = configured.get(self.name, 1)
        return _EventTaskPool(self, bus, registry=registry, parallelity=parallelity)

    def _filter_from_last_ack(self, bus: EventBus) -> EventFilter:
        return replace(self.event_filter, since_id=bus.last_acked(self.subscriber))

    @abstractmethod
    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        ...
