from __future__ import annotations

import asyncio
import contextlib
import json
import re
import sqlite3
import time
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from llm_harness.core.types import CoreEventMessage, durable_default_for, to_event_parts, validate_required_tags


@dataclass(frozen=True)
class EventToAppend:
    name: str
    payload: dict[str, Any]
    tags: dict[str, str] = field(default_factory=dict)
    producer: str | None = None
    causation_id: int | None = None
    correlation_id: int | None = None
    durable: bool | None = None


@dataclass(frozen=True)
class EventRecord:
    id: int
    name: str
    payload: dict[str, Any]
    tags: dict[str, str]
    created_at_ms: int
    producer: str | None = None
    causation_id: int | None = None
    correlation_id: int | None = None
    durable: bool = True

    @property
    def type(self) -> str:
        return self.name

    @property
    def session_id(self) -> str | None:
        return self.tags.get("session")

    @property
    def persisted_event_id(self) -> int | None:
        return self.id if self.durable else None


@dataclass(frozen=True)
class EventFilter:
    since_id: int | None = None
    before_id: int | None = None  # NEW: upper event ID bound (exclusive)
    names: frozenset[str] = frozenset()
    name_prefixes: tuple[str, ...] = ()
    tags: dict[str, str] = field(default_factory=dict)

    def matches(self, event: EventRecord) -> bool:
        if self.since_id is not None and event.id <= self.since_id:
            return False
        if self.before_id is not None and event.id >= self.before_id:
            return False
        if self.names and event.name not in self.names:
            return False
        if self.name_prefixes and not any(event.name.startswith(prefix) for prefix in self.name_prefixes):
            return False
        return all(event.tags.get(tag) == value for tag, value in self.tags.items())


class EventService:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._subscribers: dict[asyncio.Queue[EventRecord], EventFilter] = {}
        self._append_lock = asyncio.Lock()
        self.init_schema()
        self._last_event_id = self._load_last_event_id()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_at_ms INTEGER NOT NULL,
              producer TEXT,
              causation_id INTEGER,
              correlation_id INTEGER,
              durable INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS event_tags (
              event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
              tag TEXT NOT NULL,
              value TEXT NOT NULL,
              PRIMARY KEY (event_id, tag, value)
            );

            CREATE TABLE IF NOT EXISTS event_subscriptions (
              subscriber TEXT PRIMARY KEY,
              last_acked_event_id INTEGER NOT NULL DEFAULT 0,
              updated_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_name_id ON events(name, id);
            CREATE INDEX IF NOT EXISTS idx_event_tags_tag_value_event ON event_tags(tag, value, event_id);
            """
        )

    async def append_message(
        self,
        message: CoreEventMessage,
        *,
        producer: str | None = None,
        causation_id: int | None = None,
        correlation_id: int | None = None,
    ) -> EventRecord:
        name, payload, tags = to_event_parts(message)
        return await self.append(
            name,
            payload,
            tags=tags,
            producer=producer,
            causation_id=causation_id,
            correlation_id=correlation_id,
        )

    async def append(
        self,
        name: str,
        payload: dict[str, Any],
        *,
        tags: dict[str, str] | None = None,
        producer: str | None = None,
        causation_id: int | None = None,
        correlation_id: int | None = None,
        durable: bool | None = None,
    ) -> EventRecord:
        records = await self.append_batch(
            [
                EventToAppend(
                    name=name,
                    payload=payload,
                    tags=tags or {},
                    producer=producer,
                    causation_id=causation_id,
                    correlation_id=correlation_id,
                    durable=durable,
                )
            ]
        )
        return records[0]

    async def append_batch(self, events: Iterable[EventToAppend]) -> list[EventRecord]:
        pending = list(events)
        if not pending:
            return []
        for event in pending:
            _validate_event(event)

        async with self._append_lock:
            records: list[EventRecord] = []
            with self._conn:
                for event in pending:
                    event_id = self._next_event_id()
                    record = EventRecord(
                        id=event_id,
                        name=event.name,
                        payload=event.payload,
                        tags={key: str(value) for key, value in event.tags.items()},
                        created_at_ms=event_id,
                        producer=event.producer,
                        causation_id=event.causation_id,
                        correlation_id=event.correlation_id,
                        durable=durable_default_for(event.name) if event.durable is None else event.durable,
                    )
                    self._conn.execute(
                        """
                        INSERT INTO events(id, name, payload_json, created_at_ms, producer, causation_id, correlation_id, durable)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.id,
                            record.name,
                            json.dumps(record.payload),
                            record.created_at_ms,
                            record.producer,
                            record.causation_id,
                            record.correlation_id,
                            1 if record.durable else 0,
                        ),
                    )
                    if record.tags:
                        self._conn.executemany(
                            "INSERT INTO event_tags(event_id, tag, value) VALUES (?, ?, ?)",
                            [(record.id, tag, value) for tag, value in sorted(record.tags.items())],
                        )
                    records.append(record)

            for record in records:
                await self.publish(record)
            return records

    async def publish_transient(self, event_type: str, session_id: str | int | None, payload: dict[str, Any]) -> None:
        record = EventRecord(
            id=self._next_transient_id(),
            name=event_type,
            payload=payload,
            tags=_session_tags(session_id),
            created_at_ms=_epoch_ms(),
            durable=False,
        )
        await self.publish(record)

    async def publish_message_transient(
        self,
        message: CoreEventMessage,
        *,
        producer: str | None = None,
        causation_id: int | None = None,
        correlation_id: int | None = None,
    ) -> EventRecord:
        name, payload, tags = to_event_parts(message)
        _validate_event(
            EventToAppend(
                name=name,
                payload=payload,
                tags=tags,
                producer=producer,
                causation_id=causation_id,
                correlation_id=correlation_id,
                durable=False,
            )
        )
        record = EventRecord(
            id=self._next_transient_id(),
            name=name,
            payload=payload,
            tags={key: str(value) for key, value in tags.items()},
            created_at_ms=_epoch_ms(),
            producer=producer,
            causation_id=causation_id,
            correlation_id=correlation_id,
            durable=False,
        )
        await self.publish(record)
        return record

    async def publish(self, event: EventRecord) -> None:
        dead: list[asyncio.Queue[EventRecord]] = []
        for queue, event_filter in list(self._subscribers.items()):
            if not event_filter.matches(event):
                continue
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(queue)
        for queue in dead:
            self._subscribers.pop(queue, None)

    @contextlib.asynccontextmanager
    async def subscribe(self, event_filter: EventFilter | None = None) -> AsyncIterator[asyncio.Queue[EventRecord]]:
        queue: asyncio.Queue[EventRecord] = asyncio.Queue(maxsize=256)
        self._subscribers[queue] = event_filter or EventFilter()
        try:
            yield queue
        finally:
            self._subscribers.pop(queue, None)

    def replay(self, event_filter: EventFilter | None = None, *, limit: int | None = None) -> list[EventRecord]:
        event_filter = event_filter or EventFilter()
        conditions = ["e.id > ?"]
        params: list[Any] = [event_filter.since_id or 0]

        if event_filter.names:
            placeholders = ", ".join("?" for _ in event_filter.names)
            conditions.append(f"e.name IN ({placeholders})")
            params.extend(sorted(event_filter.names))

        # Tag pushdown: one EXISTS per tag so tags are filtered in SQL before
        # any row is fetched, decoded, or hydrated.
        for tag, value in sorted(event_filter.tags.items()):
            conditions.append(
                "EXISTS(SELECT 1 FROM event_tags WHERE event_id = e.id AND tag = ? AND value = ?)"
            )
            params.extend([tag, value])
        if event_filter.before_id is not None:
            conditions.append("e.id < ?")
            params.append(event_filter.before_id)

        sql = "SELECT e.* FROM events e WHERE " + " AND ".join(conditions) + " ORDER BY e.id ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        rows = self._conn.execute(sql, params).fetchall()
        if not rows:
            return []

        # name_prefixes is a startswith check that cannot be pushed into a
        # simple SQL predicate; apply it in Python before hydrating tags so we
        # never fetch tags for prefix-nonmatching events.
        if event_filter.name_prefixes:
            rows = [
                row for row in rows
                if any(row["name"].startswith(prefix) for prefix in event_filter.name_prefixes)
            ]
            if not rows:
                return []

        # Batch-load tags for all remaining rows (one query, not N+1).
        event_ids = [row["id"] for row in rows]
        tag_map = _load_tags_batch(self._conn, event_ids)

        return [
            _event_from_row_with_tags(row, tag_map.get(row["id"], {}))
            for row in rows
        ]


    def get_event(self, event_id: int) -> EventRecord | None:
        """Direct point lookup by integer primary key."""
        row = self._conn.execute(
            "SELECT * FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return None
        return _event_from_row(self._conn, row)

    def exists(self, filter: EventFilter, before_id: int | None = None) -> bool:
        """Return True if a matching event exists, without decoding payloads.

        Pushes tag filtering fully into SQL. Does NOT hydrate tags or decode
        payload_json. Uses SELECT 1 ... LIMIT 1.
        """
        conditions = ["1=1"]
        params: list[Any] = []

        if filter.since_id is not None:
            conditions.append("e.id > ?")
            params.append(filter.since_id)
        if before_id is not None:
            conditions.append("e.id < ?")
            params.append(before_id)
        if filter.names:
            placeholders = ", ".join("?" for _ in filter.names)
            conditions.append(f"e.name IN ({placeholders})")
            params.extend(sorted(filter.names))
        for tag, value in sorted(filter.tags.items()):
            conditions.append(
                "EXISTS(SELECT 1 FROM event_tags WHERE event_id = e.id AND tag = ? AND value = ?)"
            )
            params.extend([tag, value])

        sql = "SELECT 1 FROM events e WHERE " + " AND ".join(conditions) + " LIMIT 1"
        row = self._conn.execute(sql, params).fetchone()
        return row is not None

    def latest(self, filter: EventFilter, before_id: int | None = None) -> EventRecord | None:
        """Return the most recent matching event, or None.

        Pushes tag filtering into SQL, uses ORDER BY id DESC LIMIT 1.
        """
        conditions = ["1=1"]
        params: list[Any] = []

        if filter.since_id is not None:
            conditions.append("e.id > ?")
            params.append(filter.since_id)
        if before_id is not None:
            conditions.append("e.id < ?")
            params.append(before_id)
        if filter.names:
            placeholders = ", ".join("?" for _ in filter.names)
            conditions.append(f"e.name IN ({placeholders})")
            params.extend(sorted(filter.names))
        for tag, value in sorted(filter.tags.items()):
            conditions.append(
                "EXISTS(SELECT 1 FROM event_tags WHERE event_id = e.id AND tag = ? AND value = ?)"
            )
            params.extend([tag, value])

        sql = "SELECT * FROM events e WHERE " + " AND ".join(conditions) + " ORDER BY e.id DESC LIMIT 1"
        row = self._conn.execute(sql, params).fetchone()
        if row is None:
            return None
        return _event_from_row(self._conn, row)

    def replay_page(
        self,
        filter: EventFilter,
        after_id: int | None = None,
        before_id: int | None = None,
        limit: int = 500,
    ) -> list[EventRecord]:
        """Keyset pagination with tag filtering pushed into SQL.

        - after_id, before_id, names, tags, limit all in SQL.
        - name_prefixes applied in Python after SQL.
        - Tags are batch-loaded (one query with WHERE event_id IN (...)).
        """
        conditions = ["1=1"]
        params: list[Any] = []

        if after_id is not None:
            conditions.append("e.id > ?")
            params.append(after_id)
        if before_id is not None:
            conditions.append("e.id < ?")
            params.append(before_id)
        if filter.since_id is not None:
            conditions.append("e.id > ?")
            params.append(filter.since_id)
        if filter.names:
            placeholders = ", ".join("?" for _ in filter.names)
            conditions.append(f"e.name IN ({placeholders})")
            params.extend(sorted(filter.names))
        for tag, value in sorted(filter.tags.items()):
            conditions.append(
                "EXISTS(SELECT 1 FROM event_tags WHERE event_id = e.id AND tag = ? AND value = ?)"
            )
            params.extend([tag, value])

        sql = "SELECT * FROM events e WHERE " + " AND ".join(conditions) + " ORDER BY e.id ASC LIMIT ?"
        params.append(limit)

        rows = self._conn.execute(sql, params).fetchall()
        if not rows:
            return []

        # Batch-load tags for all returned rows
        ids = [row["id"] for row in rows]
        tag_rows = self._conn.execute(
            "SELECT event_id, tag, value FROM event_tags WHERE event_id IN ({})".format(
                ",".join("?" for _ in ids)
            ),
            ids,
        ).fetchall()

        # Group tags by event_id
        tag_map: dict[int, dict[str, str]] = {}
        for tr in tag_rows:
            eid = tr["event_id"]
            if eid not in tag_map:
                tag_map[eid] = {}
            tag_map[eid][tr["tag"]] = tr["value"]

        records = []
        for row in rows:
            eid = row["id"]
            record = EventRecord(
                id=int(eid),
                name=row["name"],
                payload=json.loads(row["payload_json"]),
                tags=tag_map.get(eid, {}),
                created_at_ms=int(row["created_at_ms"]),
                producer=row["producer"],
                causation_id=row["causation_id"],
                correlation_id=row["correlation_id"],
                durable=bool(row["durable"]),
            )
            records.append(record)

        # Apply name_prefixes filter in Python (can't be done efficiently in SQL)
        if filter.name_prefixes:
            records = [
                event for event in records
                if any(event.name.startswith(prefix) for prefix in filter.name_prefixes)
            ]

        return records


    def ack(self, subscriber: str, event_id: int) -> None:
        now = _epoch_ms()
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO event_subscriptions(subscriber, last_acked_event_id, updated_at_ms)
                VALUES (?, ?, ?)
                ON CONFLICT(subscriber) DO UPDATE SET
                  last_acked_event_id = excluded.last_acked_event_id,
                  updated_at_ms = excluded.updated_at_ms
                """,
                (subscriber, event_id, now),
            )

    def last_acked(self, subscriber: str) -> int:
        row = self._conn.execute(
            "SELECT last_acked_event_id FROM event_subscriptions WHERE subscriber = ?",
            (subscriber,),
        ).fetchone()
        if row is None:
            return 0
        return int(row["last_acked_event_id"])

    def _load_last_event_id(self) -> int:
        row = self._conn.execute("SELECT COALESCE(MAX(id), 0) AS id FROM events").fetchone()
        return int(row["id"])

    def _next_event_id(self) -> int:
        candidate = _epoch_ms()
        event_id = max(candidate, self._last_event_id + 1)
        self._last_event_id = event_id
        return event_id

    def _next_transient_id(self) -> int:
        event_id = max(_epoch_ms(), self._last_event_id + 1)
        self._last_event_id = event_id
        return event_id


EventBus = EventService
BusEvent = EventRecord


def _load_tags_batch(conn: sqlite3.Connection, event_ids: list[int]) -> dict[int, dict[str, str]]:
    """Load tags for many events in one query, keyed by event_id."""
    if not event_ids:
        return {}
    placeholders = ", ".join("?" for _ in event_ids)
    rows = conn.execute(
        f"SELECT event_id, tag, value FROM event_tags WHERE event_id IN ({placeholders}) ORDER BY event_id, tag",
        event_ids,
    ).fetchall()
    tag_map: dict[int, dict[str, str]] = {}
    for row in rows:
        eid = int(row["event_id"])
        tag_map.setdefault(eid, {})[row["tag"]] = row["value"]
    return tag_map


def _event_from_row_with_tags(row: sqlite3.Row, tags: dict[str, str]) -> EventRecord:
    return EventRecord(
        id=int(row["id"]),
        name=row["name"],
        payload=json.loads(row["payload_json"]),
        tags=tags,
        created_at_ms=int(row["created_at_ms"]),
        producer=row["producer"],
        causation_id=row["causation_id"],
        correlation_id=row["correlation_id"],
        durable=bool(row["durable"]),
    )


def _event_from_row(conn: sqlite3.Connection, row: sqlite3.Row) -> EventRecord:
    tag_rows = conn.execute("SELECT tag, value FROM event_tags WHERE event_id = ?", (row["id"],)).fetchall()
    return EventRecord(
        id=int(row["id"]),
        name=row["name"],
        payload=json.loads(row["payload_json"]),
        tags={tag_row["tag"]: tag_row["value"] for tag_row in tag_rows},
        created_at_ms=int(row["created_at_ms"]),
        producer=row["producer"],
        causation_id=row["causation_id"],
        correlation_id=row["correlation_id"],
        durable=bool(row["durable"]),
    )


def _validate_event(event: EventToAppend) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+", event.name):
        raise ValueError(f"invalid event name: {event.name}")
    if not isinstance(event.payload, dict):
        raise ValueError("event payload must be a JSON object")
    tags = {key: str(value) for key, value in event.tags.items()}
    validate_required_tags(event.name, tags)
    for tag, value in tags.items():
        if not re.fullmatch(r"[a-zA-Z0-9_:. -]+", tag):
            raise ValueError(f"invalid event tag: {tag}")
        if value is None:
            raise ValueError(f"event tag {tag} has null value")


def _session_tags(session_id: str | int | None) -> dict[str, str]:
    if session_id is None:
        return {}
    value = str(session_id)
    return {"session": value, "chat": value}


def _epoch_ms() -> int:
    return int(time.time() * 1000)
