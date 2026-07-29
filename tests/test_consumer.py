from __future__ import annotations

import asyncio

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventFilter, EventService
from llm_harness.plugins import Registry


class ControlledConsumer(EventConsumer):
    name = "controlled"
    subscriber = "plugin:controlled"
    event_filter = EventFilter(names=frozenset({"test.event"}))

    def __init__(self) -> None:
        self.started: asyncio.Queue[int] = asyncio.Queue()
        self.release: dict[int, asyncio.Event] = {}

    async def process_event(self, bus, event, *, registry=None) -> None:
        self.release[event.id] = asyncio.Event()
        await self.started.put(event.id)
        await self.release[event.id].wait()


def test_parallel_consumer_only_acknowledges_contiguous_completed_events(tmp_path):
    asyncio.run(_assert_parallel_consumer_ack_order(tmp_path))


async def _assert_parallel_consumer_ack_order(tmp_path):
    bus = EventService(tmp_path / "events.db")
    events = [
        await bus.append("test.event", {"sequence": sequence})
        for sequence in range(3)
    ]
    consumer = ControlledConsumer()
    registry = Registry(event_consumer_parallelity={consumer.name: 2})

    processing = asyncio.create_task(consumer.process_pending(bus, registry=registry))
    first_started = await consumer.started.get()
    second_started = await consumer.started.get()
    assert {first_started, second_started} == {events[0].id, events[1].id}
    assert consumer.started.empty()

    consumer.release[events[1].id].set()
    third_started = await asyncio.wait_for(consumer.started.get(), timeout=1)
    assert third_started == events[2].id
    assert bus.last_acked(consumer.subscriber) == 0

    consumer.release[events[2].id].set()
    await asyncio.sleep(0)
    assert bus.last_acked(consumer.subscriber) == 0

    consumer.release[events[0].id].set()
    await processing
    assert bus.last_acked(consumer.subscriber) == events[2].id
