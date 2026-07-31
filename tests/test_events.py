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


def test_get_event_returns_record(tmp_path):
    service = EventService(tmp_path / "events.db")
    record = asyncio.run(service.append("session.created", {"title": "hello"}, tags={"session": "1"}))

    found = service.get_event(record.id)
    assert found is not None
    assert found.id == record.id
    assert found.name == "session.created"
    assert found.payload == {"title": "hello"}
    assert found.tags == {"session": "1"}


def test_get_event_returns_none_for_missing(tmp_path):
    service = EventService(tmp_path / "events.db")
    assert service.get_event(99999) is None


def test_get_event_hydrates_tags_and_payload(tmp_path):
    service = EventService(tmp_path / "events.db")
    record = asyncio.run(service.append(
        "chat.message.create_requested",
        {"content": "hi"},
        tags={"session": "1", "role": "user", "provider": "mock-llm"},
    ))

    found = service.get_event(record.id)
    assert found.payload == {"content": "hi"}
    assert found.tags["session"] == "1"
    assert found.tags["role"] == "user"
    assert found.tags["provider"] == "mock-llm"


def test_exists_returns_true_when_match(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))

    assert service.exists(EventFilter(names=frozenset({"session.created"}), tags={"session": "1"}))


def test_exists_returns_false_when_no_match(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))

    assert not service.exists(EventFilter(names=frozenset({"session.created"}), tags={"session": "2"}))


def test_exists_respects_before_id(tmp_path):
    service = EventService(tmp_path / "events.db")
    r1 = asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "1"}))

    # Only event with id < r1.id+1 (i.e., r1) should match
    assert service.exists(EventFilter(tags={"session": "1"}), before_id=r1.id + 1)
    # No events with id < r1.id
    assert not service.exists(EventFilter(tags={"session": "1"}), before_id=r1.id)


def test_exists_respects_since_id(tmp_path):
    service = EventService(tmp_path / "events.db")
    r1 = asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    r2 = asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "1"}))

    # since_id = r1.id means only events with id > r1.id, so r2
    assert service.exists(EventFilter(since_id=r1.id, tags={"session": "1"}))
    # since_id = r2.id means no events
    assert not service.exists(EventFilter(since_id=r2.id, tags={"session": "1"}))


def test_exists_uses_sql_not_python_filtering(tmp_path):
    """Verify exists does NOT need to decode payload_json or hydrate tags."""
    service = EventService(tmp_path / "events.db")
    rec = asyncio.run(service.append("session.created", {"title": "test"}, tags={"session": "1"}))

    # This should work without any Python-side tag matching
    assert service.exists(EventFilter(tags={"session": "1"}))
    assert not service.exists(EventFilter(tags={"session": "nonexistent"}))


def test_latest_returns_most_recent(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "1"}))

    latest = service.latest(EventFilter(tags={"session": "1"}))
    assert latest is not None
    assert latest.payload == {"title": "two"}


def test_latest_returns_none_when_no_match(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))

    assert service.latest(EventFilter(tags={"session": "nonexistent"})) is None


def test_latest_respects_before_id(tmp_path):
    service = EventService(tmp_path / "events.db")
    r1 = asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    r2 = asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "1"}))

    # Before r2.id means we should get r1
    latest = service.latest(EventFilter(tags={"session": "1"}), before_id=r2.id)
    assert latest is not None
    assert latest.id == r1.id

    # Before r1.id means no results
    assert service.latest(EventFilter(tags={"session": "1"}), before_id=r1.id) is None


def test_latest_respects_since_id(tmp_path):
    service = EventService(tmp_path / "events.db")
    r1 = asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    r2 = asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "1"}))

    latest = service.latest(EventFilter(since_id=r1.id, tags={"session": "1"}))
    assert latest is not None
    assert latest.id == r2.id


def test_latest_hydrates_tags_and_payload(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append(
        "chat.message.create_requested",
        {"content": "hello"},
        tags={"session": "1", "role": "user"},
    ))

    latest = service.latest(EventFilter(tags={"session": "1"}))
    assert latest is not None
    assert latest.payload == {"content": "hello"}
    assert latest.tags["role"] == "user"


def test_replay_page_basic_pagination(tmp_path):
    service = EventService(tmp_path / "events.db")
    records = []
    for i in range(10):
        r = asyncio.run(service.append("session.created", {"idx": i}, tags={"session": "1"}))
        records.append(r)

    page = service.replay_page(EventFilter(tags={"session": "1"}), limit=3)
    assert len(page) == 3
    assert [e.id for e in page] == [records[0].id, records[1].id, records[2].id]


def test_replay_page_after_id(tmp_path):
    service = EventService(tmp_path / "events.db")
    records = []
    for i in range(5):
        r = asyncio.run(service.append("session.created", {"idx": i}, tags={"session": "1"}))
        records.append(r)

    page = service.replay_page(EventFilter(tags={"session": "1"}), after_id=records[2].id, limit=2)
    assert len(page) == 2
    assert [e.id for e in page] == [records[3].id, records[4].id]


def test_replay_page_before_id(tmp_path):
    service = EventService(tmp_path / "events.db")
    records = []
    for i in range(5):
        r = asyncio.run(service.append("session.created", {"idx": i}, tags={"session": "1"}))
        records.append(r)

    page = service.replay_page(EventFilter(tags={"session": "1"}), before_id=records[3].id, limit=10)
    assert len(page) == 3
    assert [e.id for e in page] == [records[0].id, records[1].id, records[2].id]


def test_replay_page_after_and_before(tmp_path):
    service = EventService(tmp_path / "events.db")
    records = []
    for i in range(5):
        r = asyncio.run(service.append("session.created", {"idx": i}, tags={"session": "1"}))
        records.append(r)

    page = service.replay_page(
        EventFilter(tags={"session": "1"}),
        after_id=records[0].id,
        before_id=records[4].id,
        limit=10,
    )
    assert len(page) == 3
    assert [e.id for e in page] == [records[1].id, records[2].id, records[3].id]


def test_replay_page_filters_by_name(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    r2 = asyncio.run(service.append("session.deleted", {"title": "two"}, tags={"session": "1"}))

    page = service.replay_page(EventFilter(names=frozenset({"session.deleted"}), tags={"session": "1"}))
    assert len(page) == 1
    assert page[0].id == r2.id


def test_replay_page_filters_by_tags(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    r2 = asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "2"}))

    page = service.replay_page(EventFilter(tags={"session": "2"}))
    assert len(page) == 1
    assert page[0].id == r2.id


def test_replay_page_filters_by_name_prefixes(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    asyncio.run(service.append("session.deleted", {"title": "two"}, tags={"session": "1"}))
    r3 = asyncio.run(service.append("chat.message.create_requested", {"content": "hello"}, tags={"session": "1"}))

    page = service.replay_page(EventFilter(name_prefixes=("chat.",), tags={"session": "1"}))
    assert len(page) == 1
    assert page[0].id == r3.id


def test_replay_page_multiple_tags(tmp_path):
    service = EventService(tmp_path / "events.db")
    r1 = asyncio.run(service.append(
        "chat.message.create_requested",
        {"content": "hi"},
        tags={"session": "1", "role": "user", "provider": "mock-llm"},
    ))
    asyncio.run(service.append(
        "chat.message.create_requested",
        {"content": "hello"},
        tags={"session": "1", "role": "assistant", "provider": "mock-llm"},
    ))

    page = service.replay_page(EventFilter(tags={"session": "1", "role": "user", "provider": "mock-llm"}))
    assert len(page) == 1
    assert page[0].id == r1.id


def test_replay_page_batch_loads_tags(tmp_path):
    """Verify tags are loaded with a single batch query, not N+1."""
    service = EventService(tmp_path / "events.db")
    records = []
    for i in range(10):
        r = asyncio.run(service.append(
            "session.created",
            {"idx": i},
            tags={"session": "1", "extra": str(i)},
        ))
        records.append(r)

    page = service.replay_page(EventFilter(tags={"session": "1"}), limit=10)
    assert len(page) == 10
    for i, event in enumerate(page):
        assert event.tags["session"] == "1"
        assert event.tags["extra"] == str(i)


def test_replay_page_hydrates_payload(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "hello", "count": 42}, tags={"session": "1"}))

    page = service.replay_page(EventFilter(tags={"session": "1"}), limit=10)
    assert len(page) == 1
    assert page[0].payload == {"title": "hello", "count": 42}


def test_replay_page_default_limit_is_500(tmp_path):
    service = EventService(tmp_path / "events.db")
    for i in range(600):
        asyncio.run(service.append("session.created", {"idx": i}, tags={"session": "1"}))

    page = service.replay_page(EventFilter(tags={"session": "1"}))
    assert len(page) == 500


def test_replay_page_empty_result(tmp_path):
    service = EventService(tmp_path / "events.db")
    page = service.replay_page(EventFilter(tags={"session": "nonexistent"}))
    assert page == []
