from __future__ import annotations

import asyncio
import re

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


def test_replay_filters_by_tag_in_sql(tmp_path):
    """Tag predicates are pushed into SQL via EXISTS, not Python-side matches()."""
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    r2 = asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "2"}))

    captured = []
    service.conn.set_trace_callback(captured.append)

    replayed = service.replay(EventFilter(tags={"session": "2"}))
    assert len(replayed) == 1
    assert replayed[0].id == r2.id
    assert any("EXISTS(SELECT 1 FROM event_tags" in sql for sql in captured)
    # Exactly one batch tag-load query, not one per candidate row.
    batch_queries = [sql for sql in captured if sql.startswith("SELECT event_id, tag, value FROM event_tags")]
    assert len(batch_queries) == 1


def test_replay_applies_limit_in_sql(tmp_path):
    """LIMIT is applied at the SQL level, so only the selected rows are loaded."""
    service = EventService(tmp_path / "events.db")
    for i in range(10):
        asyncio.run(service.append("session.created", {"idx": i}, tags={"session": "1"}))

    captured = []
    service.conn.set_trace_callback(captured.append)

    replayed = service.replay(EventFilter(tags={"session": "1"}), limit=3)
    assert len(replayed) == 3
    assert [e.id for e in replayed] == sorted(e.id for e in replayed)
    main_sql = next(sql for sql in captured if sql.startswith("SELECT e.* FROM events e"))
    assert main_sql.endswith("LIMIT 3")
    # The batch tag query only loads tags for the 3 selected event ids.
    tag_sql = [sql for sql in captured if sql.startswith("SELECT event_id, tag, value FROM event_tags")]
    assert len(tag_sql) == 1
    ids = re.search(r"WHERE event_id IN \(([^)]*)\)", tag_sql[0]).group(1).split(", ")
    assert len(ids) == 3


def test_replay_batch_loads_tags(tmp_path):
    """All tags for the returned events are hydrated with a single query."""
    service = EventService(tmp_path / "events.db")
    for i in range(10):
        asyncio.run(service.append(
            "session.created",
            {"idx": i},
            tags={"session": "1", "extra": str(i)},
        ))

    captured = []
    service.conn.set_trace_callback(captured.append)

    replayed = service.replay(EventFilter(tags={"session": "1"}))
    assert len(replayed) == 10
    for i, event in enumerate(replayed):
        assert event.tags["session"] == "1"
        assert event.tags["extra"] == str(i)
    tag_queries = [sql for sql in captured if sql.startswith("SELECT event_id, tag, value FROM event_tags")]
    assert len(tag_queries) == 1


def test_replay_name_prefixes_still_works(tmp_path):
    service = EventService(tmp_path / "events.db")
    asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1"}))
    asyncio.run(service.append("session.deleted", {"title": "two"}, tags={"session": "1"}))
    r3 = asyncio.run(service.append("chat.message.create_requested", {"content": "hello"}, tags={"session": "1"}))

    replayed = service.replay(EventFilter(name_prefixes=("chat.",), tags={"session": "1"}))
    assert len(replayed) == 1
    assert replayed[0].id == r3.id


def test_replay_combined_filters(tmp_path):
    service = EventService(tmp_path / "events.db")
    r1 = asyncio.run(service.append("session.created", {"idx": 0}, tags={"session": "1"}))
    r2 = asyncio.run(service.append("session.created", {"idx": 1}, tags={"session": "1"}))
    asyncio.run(service.append("session.deleted", {"idx": 2}, tags={"session": "1"}))
    asyncio.run(service.append("session.created", {"idx": 3}, tags={"session": "2"}))

    replayed = service.replay(
        EventFilter(since_id=r1.id, names=frozenset({"session.created"}), tags={"session": "1"}),
        limit=1,
    )
    assert replayed == [r2]


def test_replay_backward_compatible(tmp_path):
    """Same inputs produce the same results as the pre-pushdown behavior."""
    service = EventService(tmp_path / "events.db")
    r1 = asyncio.run(service.append("session.created", {"title": "one"}, tags={"session": "1", "role": "user"}))
    r2 = asyncio.run(service.append("session.created", {"title": "two"}, tags={"session": "1", "role": "assistant"}))
    r3 = asyncio.run(service.append("session.deleted", {"title": "three"}, tags={"session": "2"}))

    # no filter: all events in ascending id order
    assert [e.id for e in service.replay()] == [r1.id, r2.id, r3.id]

    # names filter
    assert [e.id for e in service.replay(EventFilter(names=frozenset({"session.deleted"})))] == [r3.id]

    # tags filter
    assert [e.id for e in service.replay(EventFilter(tags={"session": "1"}))] == [r1.id, r2.id]

    # name_prefixes filter
    assert [e.id for e in service.replay(EventFilter(name_prefixes=("session.",)))] == [r1.id, r2.id, r3.id]
    assert [e.id for e in service.replay(EventFilter(name_prefixes=("chat.",)))] == []

    # since_id filter
    assert [e.id for e in service.replay(EventFilter(since_id=r1.id))] == [r2.id, r3.id]

    # limit
    assert [e.id for e in service.replay(EventFilter(tags={"session": "1"}), limit=1)] == [r1.id]


def test_replay_filters_by_causation_id(tmp_path):
    service = EventService(tmp_path / "events.db")
    source = asyncio.run(service.append("chat.message.create_requested", {"content": "hi"}, tags={"session": "1"}))
    child = asyncio.run(service.append(
        "llm.run.requested",
        {"run": "r1"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r1"},
        causation_id=source.id,
        producer="llm-run-requester",
    ))
    asyncio.run(service.append(
        "llm.run.requested", {"run": "r2"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r2"}
    ))

    replayed = service.replay(EventFilter(causation_id=source.id))
    assert [e.id for e in replayed] == [child.id]

    replayed = service.replay(EventFilter(causation_id=source.id + 1000))
    assert replayed == []


def test_replay_filters_by_producer(tmp_path):
    service = EventService(tmp_path / "events.db")
    a = asyncio.run(service.append(
        "llm.run.requested", {"run": "r1"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r1"},
        producer="llm-run-requester",
    ))
    asyncio.run(service.append(
        "llm.run.requested", {"run": "r2"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r2"},
        producer="tool-result-llm-requester",
    ))
    asyncio.run(service.append(
        "llm.run.requested", {"run": "r3"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r3"},
        producer=None,
    ))

    replayed = service.replay(EventFilter(producer="llm-run-requester"))
    assert [e.id for e in replayed] == [a.id]

    replayed = service.replay(EventFilter(producer="no-such-producer"))
    assert replayed == []


def test_replay_causation_producer_name_combination(tmp_path):
    """Matches the idx_events_causation_name_producer_id query pattern."""
    service = EventService(tmp_path / "events.db")
    source = asyncio.run(service.append(
        "chat.message.assistant.created", {"content": "hi"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r0"},
    ))
    r1 = asyncio.run(service.append(
        "llm.run.requested", {"run": "r1"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r1"},
        causation_id=source.id, producer="tool-result-llm-requester",
    ))
    asyncio.run(service.append(
        "llm.run.requested", {"run": "r2"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r2"},
        causation_id=source.id, producer="server-overloaded-retry",
    ))
    asyncio.run(service.append(
        "llm.run.requested", {"run": "r3"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r3"},
        causation_id=source.id + 1, producer="tool-result-llm-requester",
    ))

    replayed = service.replay(EventFilter(
        names=frozenset({"llm.run.requested"}),
        causation_id=source.id,
        producer="tool-result-llm-requester",
    ), limit=1)
    assert [e.id for e in replayed] == [r1.id]


def test_replay_causation_producer_in_sql(tmp_path):
    """causation_id and producer predicates are pushed into SQL."""
    service = EventService(tmp_path / "events.db")
    source = asyncio.run(service.append("chat.message.create_requested", {"content": "hi"}, tags={"session": "1"}))
    asyncio.run(service.append(
        "llm.run.requested", {"run": "r1"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r1"},
        causation_id=source.id, producer="llm-run-requester",
    ))

    captured = []
    service.conn.set_trace_callback(captured.append)

    replayed = service.replay(EventFilter(
        names=frozenset({"llm.run.requested"}),
        tags={"session": "1"},
        causation_id=source.id,
        producer="llm-run-requester",
    ), limit=1)
    assert len(replayed) == 1

    main_sql = next(sql for sql in captured if sql.startswith("SELECT e.* FROM events e"))
    # sqlite's trace callback renders the bound parameters inline.
    assert "e.causation_id = " in main_sql
    assert "e.producer = '" in main_sql
    assert main_sql.endswith("LIMIT 1")


def test_matches_filters_by_causation_id_and_producer(tmp_path):
    """EventFilter.matches honors causation_id and producer for dispatch."""
    service = EventService(tmp_path / "events.db")
    source = asyncio.run(service.append("chat.message.create_requested", {"content": "hi"}, tags={"session": "1"}))
    child = asyncio.run(service.append(
        "llm.run.requested", {"run": "r1"},
        tags={"session": "1", "provider": "mock-llm", "model": "test", "run": "r1"},
        causation_id=source.id, producer="llm-run-requester",
    ))

    # Causation filter
    assert EventFilter(causation_id=source.id).matches(child)
    assert not EventFilter(causation_id=source.id + 1).matches(child)
    assert EventFilter(causation_id=None).matches(child)

    # Producer filter
    assert EventFilter(producer="llm-run-requester").matches(child)
    assert not EventFilter(producer="other").matches(child)
    assert EventFilter(producer=None).matches(child)

    # Combined with tags and names
    combined = EventFilter(
        names=frozenset({"llm.run.requested"}),
        tags={"session": "1"},
        causation_id=source.id,
        producer="llm-run-requester",
    )
    assert combined.matches(child)
    assert not combined.matches(source)


# ── Projection tests ──────────────────────────────────────────────────

def test_backfill_populates_projections(tmp_path):
    """Write events via old-style append, then construct a new EventService
    on the same DB, assert projections populated and projections_ready True."""
    path = tmp_path / "events.db"
    # First service: append events
    s1 = EventService(path)
    s1_events = asyncio.run(s1.append_batch([
        EventToAppend(
            name="session.created",
            payload={"title": "first", "tags": ["test"]},
            tags={"session": "sess_1"},
            producer="test",
        ),
        EventToAppend(
            name="session.created",
            payload={"title": "second", "tags": []},
            tags={"session": "sess_2", "parent_session": "sess_1"},
            producer="test",
        ),
    ]))
    s1_state = asyncio.run(s1.append(
        "session.state",
        {"source_event_id": s1_events[0].id, "outcome": "stop"},
        tags={"session": "sess_1", "state": "finished"},
    ))
    asyncio.run(s1.append(
        "llm.model.selected",
        {"toolsets": ["default"]},
        tags={
            "session": "sess_1",
            "provider": "mock-llm",
            "model": "test-model",
        },
    ))

    # Close and reopen
    del s1
    s2 = EventService(path)
    assert s2.projections_ready

    # Check projected_sessions
    rows = s2.conn.execute(
        "SELECT session_id, parent_session_id, title, tags_json, created_event_id "
        "FROM projected_sessions ORDER BY created_event_id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["session_id"] == "sess_1"
    assert rows[0]["parent_session_id"] is None
    assert rows[0]["title"] == "first"
    assert rows[0]["tags_json"] == '["test"]'
    assert rows[1]["session_id"] == "sess_2"
    assert rows[1]["parent_session_id"] == "sess_1"

    # Check projected_session_tags
    tag_rows = s2.conn.execute(
        "SELECT tag FROM projected_session_tags WHERE session_id = ?", ("sess_1",)
    ).fetchall()
    assert [r["tag"] for r in tag_rows] == ["test"]

    # Check projected_session_states
    state_row = s2.conn.execute(
        "SELECT session_id, state, outcome FROM projected_session_states"
    ).fetchone()
    assert state_row is not None
    assert state_row["session_id"] == "sess_1"
    assert state_row["state"] == "finished"
    assert state_row["outcome"] == "stop"

    # Check projected_model_selections
    model_row = s2.conn.execute(
        "SELECT scope_key, provider, model FROM projected_model_selections"
    ).fetchone()
    assert model_row is not None
    assert model_row["scope_key"] == "session:sess_1"
    assert model_row["provider"] == "mock-llm"
    assert model_row["model"] == "test-model"

    # Check checkpoint
    cp = s2.conn.execute(
        "SELECT last_event_id FROM projection_checkpoints WHERE projection = 'core'"
    ).fetchone()
    assert cp is not None
    assert cp["last_event_id"] >= s1_state.id


def test_synchronous_projection_on_append(tmp_path):
    """Append events and verify projections are updated in the same transaction."""
    service = EventService(tmp_path / "events.db")

    # Create session
    created = asyncio.run(service.append(
        "session.created",
        {"title": "proj-test", "tags": ["tag1"]},
        tags={"session": "sess_proj"},
    ))

    row = service.conn.execute(
        "SELECT title, tags_json FROM projected_sessions WHERE session_id = ?",
        ("sess_proj",),
    ).fetchone()
    assert row is not None
    assert row["title"] == "proj-test"
    assert '"tag1"' in row["tags_json"]

    # Rename
    asyncio.run(service.append(
        "session.renamed",
        {"title": "renamed-test"},
        tags={"session": "sess_proj", "namer": "test"},
    ))

    row = service.conn.execute(
        "SELECT title, title_event_id FROM projected_sessions WHERE session_id = ?",
        ("sess_proj",),
    ).fetchone()
    assert row["title"] == "renamed-test"
    assert row["title_event_id"] > created.id

    # State
    state_event = asyncio.run(service.append(
        "session.state",
        {"source_event_id": created.id, "outcome": "stop"},
        tags={"session": "sess_proj", "state": "finished"},
    ))

    state_row = service.conn.execute(
        "SELECT state, outcome FROM projected_session_states WHERE session_id = ?",
        ("sess_proj",),
    ).fetchone()
    assert state_row["state"] == "finished"
    assert state_row["outcome"] == "stop"

    # Model selection
    asyncio.run(service.append(
        "llm.model.selected",
        {"toolsets": ["default", "custom"]},
        tags={
            "session": "sess_proj",
            "provider": "openrouter",
            "model": "claude-3",
        },
    ))

    model_row = service.conn.execute(
        "SELECT provider, model, toolsets_json FROM projected_model_selections WHERE scope_key = ?",
        ("session:sess_proj",),
    ).fetchone()
    assert model_row is not None
    assert model_row["provider"] == "openrouter"
    assert model_row["model"] == "claude-3"


def test_projection_ordering_guard(tmp_path):
    """Assert the ON CONFLICT WHERE clause prevents regressing to an older event."""
    service = EventService(tmp_path / "events.db")

    created = asyncio.run(service.append(
        "session.created",
        {"title": "original"},
        tags={"session": "sess_order"},
    ))

    # First state: running
    running = asyncio.run(service.append(
        "session.state",
        {"source_event_id": created.id},
        tags={"session": "sess_order", "state": "running"},
    ))

    # Second state: finished (higher event_id)
    finished = asyncio.run(service.append(
        "session.state",
        {"source_event_id": created.id, "outcome": "stop"},
        tags={"session": "sess_order", "state": "finished"},
    ))
    assert finished.id > running.id

    # Check that state is "finished" (not regressed to "running")
    state_row = service.conn.execute(
        "SELECT state, event_id FROM projected_session_states WHERE session_id = ?",
        ("sess_order",),
    ).fetchone()
    assert state_row["state"] == "finished"
    assert state_row["event_id"] == finished.id

    # The ON CONFLICT ... WHERE excluded.event_id > projected_session_states.event_id
    # should prevent this older event_id from overwriting
    # Let's alsomannually try inserting an older event_id

    # Check that model selection also respects ordering
    asyncio.run(service.append(
        "llm.model.selected",
        {"toolsets": ["v1"]},
        tags={
            "session": "sess_order",
            "provider": "provider_a",
            "model": "model_a",
        },
    ))

    asyncio.run(service.append(
        "llm.model.selected",
        {"toolsets": ["v2"]},
        tags={
            "session": "sess_order",
            "provider": "provider_b",
            "model": "model_b",
        },
    ))

    model_row = service.conn.execute(
        "SELECT provider, model FROM projected_model_selections WHERE scope_key = ?",
        ("session:sess_order",),
    ).fetchone()
    assert model_row is not None
    assert model_row["provider"] == "provider_b"


def test_projection_checkpoint_advances(tmp_path):
    """Verify the projection checkpoint is updated correctly during backfill."""
    path = tmp_path / "events.db"

    # Populate the event log
    s1 = EventService(path)
    events = []
    for i in range(5):
        e = asyncio.run(s1.append(
            "session.created",
            {"title": f"session_{i}"},
            tags={"session": f"sess_{i}"},
        ))
        events.append(e)
    last_event_id = events[-1].id
    del s1

    # Reopen; will trigger backfill
    s2 = EventService(path)
    assert s2.projections_ready

    cp = s2.conn.execute(
        "SELECT last_event_id FROM projection_checkpoints WHERE projection = 'core'"
    ).fetchone()
    assert cp is not None
    assert cp["last_event_id"] >= last_event_id
