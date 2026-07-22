from __future__ import annotations

import asyncio

from llm_harness.core.events import EventFilter, EventService, EventToAppend


def test_append_batch_persists_before_returning(tmp_path):
    service = EventService(tmp_path / "events.db")

    records = asyncio.run(service.append_batch(
        [
            EventToAppend(
                name="chat.message.create_requested",
                payload={"content": "hello"},
                tags={"session": "1", "role": "user", "provider": "mock-llm"},
                producer="test",
            ),
            EventToAppend(
                name="llm.run.requested",
                payload={"message_id": 10},
                tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "run-1"},
                producer="test",
                causation_id=None,
            ),
        ]
    ))

    assert records[0].id < records[1].id
    replayed = service.replay(EventFilter(tags={"session": "1"}))
    assert [event.name for event in replayed] == ["chat.message.create_requested", "llm.run.requested"]
    assert replayed[0].tags["provider"] == "mock-llm"


def test_event_ids_continue_after_restart(tmp_path):
    path = tmp_path / "events.db"
    first_service = EventService(path)
    first = asyncio.run(first_service.append("session.created", {"title": "one"}, tags={"session": "1"}))

    second_service = EventService(path)
    second = asyncio.run(second_service.append("session.created", {"title": "two"}, tags={"session": "2"}))

    assert second.id > first.id


def test_core_event_required_tags_are_enforced(tmp_path):
    service = EventService(tmp_path / "events.db")

    try:
        asyncio.run(service.append("session.created", {"title": "missing tag"}))
    except ValueError as exc:
        assert "missing required tags: session" in str(exc)
    else:
        raise AssertionError("expected missing required tag error")


def test_replay_applies_limit_after_tag_filtering(tmp_path):
    service = EventService(tmp_path / "events.db")

    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    expected = asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "2"}))

    replayed = service.replay(
        EventFilter(names=frozenset({"session.created"}), tags={"session": "2"}),
        limit=1,
    )

    assert replayed == [expected]


def test_subscribe_filters_by_tag(tmp_path):
    asyncio.run(_assert_subscribe_filters_by_tag(tmp_path))


async def _assert_subscribe_filters_by_tag(tmp_path):
    service = EventService(tmp_path / "events.db")

    async with service.subscribe(EventFilter(tags={"session": "2"})) as queue:
        await service.append("session.created", {"title": "one"}, tags={"session": "1"})
        expected = await service.append("session.created", {"title": "two"}, tags={"session": "2"})

        received = queue.get_nowait()

    assert received.id == expected.id


def test_ack_cursor_is_persisted(tmp_path):
    path = tmp_path / "events.db"
    service = EventService(path)
    service.ack("worker-a", 123)

    restarted = EventService(path)

    assert restarted.last_acked("worker-a") == 123
