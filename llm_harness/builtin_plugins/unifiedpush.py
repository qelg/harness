from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
from typing import Any
from urllib.parse import urlsplit

import httpx
from fastapi import HTTPException
from pydantic import BaseModel, Field

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunFailed,
    SessionCreated,
    SessionRenamed,
    SessionStateChanged,
)


logger = logging.getLogger(__name__)


class UnifiedPushSubscription(BaseModel):
    instance_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    endpoint: str = Field(min_length=1, max_length=4096)


class UnifiedPushPlugin(EventConsumer):
    """Send top-level session completion notifications through UnifiedPush."""

    name = "unifiedpush"
    subscriber = "plugin:unifiedpush"
    event_filter = EventFilter(
        names=frozenset({SessionStateChanged.name}), tags={"state": "finished"}
    )

    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._client = client

    async def run(self, bus: EventBus, *, registry: Any = None) -> None:
        # A distributor outage must not terminate this consumer permanently.
        # Its durable cursor and delivery rows make restarting the loop safe.
        while True:
            try:
                await super().run(bus, registry=registry)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("UnifiedPush delivery failed; retrying")
                await asyncio.sleep(30)

    def install_api(self, *, app, bus: EventBus, registry) -> None:
        self._init_schema(bus)

        @app.put("/push/unifiedpush/subscriptions", status_code=204)
        async def subscribe(request: UnifiedPushSubscription) -> None:
            _validate_endpoint(request.endpoint)
            with bus.conn:
                bus.conn.execute(
                    """
                    INSERT INTO unifiedpush_subscriptions(instance_id, endpoint, updated_at_ms)
                    VALUES (?, ?, ?)
                    ON CONFLICT(instance_id) DO UPDATE SET
                      endpoint = excluded.endpoint,
                      updated_at_ms = excluded.updated_at_ms
                    """,
                    (request.instance_id, request.endpoint, _now_ms()),
                )

        @app.delete("/push/unifiedpush/subscriptions/{instance_id}", status_code=204)
        async def unsubscribe(instance_id: str) -> None:
            if not instance_id or len(instance_id) > 128:
                raise HTTPException(status_code=400, detail="invalid UnifiedPush instance id")
            with bus.conn:
                bus.conn.execute(
                    "DELETE FROM unifiedpush_subscriptions WHERE instance_id = ?",
                    (instance_id,),
                )

    async def process_event(
        self, bus: EventBus, event: EventRecord, *, registry: Any = None
    ) -> None:
        self._init_schema(bus)
        session_id = event.tags["session"]
        created = _session_created(bus, session_id)
        if created is None or "parent_session" in created.tags:
            return
        if not _is_transition_to_finished(bus, event):
            return

        source = _event_by_id(bus, event.payload.get("source_event_id"))
        payload = {
            "type": "session.finished",
            "session_id": session_id,
            "title": _bounded_text(_session_title(bus, created, event.id), 512),
            # Keep the complete notification under the limits imposed by common
            # UnifiedPush distributors while retaining as much of the answer as possible.
            "content": _bounded_text(_notification_content(source), 3000),
            "event_id": event.id,
        }
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        subscriptions = bus.conn.execute(
            "SELECT instance_id, endpoint FROM unifiedpush_subscriptions ORDER BY instance_id"
        ).fetchall()
        delivered = {
            row["instance_id"]
            for row in bus.conn.execute(
                "SELECT instance_id FROM unifiedpush_deliveries WHERE event_id = ?",
                (event.id,),
            ).fetchall()
        }

        client = self._client or httpx.AsyncClient(timeout=10.0, follow_redirects=False)
        close_client = self._client is None
        try:
            for subscription in subscriptions:
                instance_id = subscription["instance_id"]
                if instance_id in delivered:
                    continue
                response = await client.post(
                    subscription["endpoint"],
                    content=body,
                    headers={
                        "Content-Type": "application/json",
                        "TTL": "86400",
                        "Urgency": "normal",
                        "Topic": f"session-{event.id}",
                    },
                )
                if response.status_code in {404, 410}:
                    with bus.conn:
                        bus.conn.execute(
                            "DELETE FROM unifiedpush_subscriptions WHERE instance_id = ?",
                            (instance_id,),
                        )
                    continue
                response.raise_for_status()
                with bus.conn:
                    bus.conn.execute(
                        """
                        INSERT OR IGNORE INTO unifiedpush_deliveries(event_id, instance_id)
                        VALUES (?, ?)
                        """,
                        (event.id, instance_id),
                    )
        finally:
            if close_client:
                await client.aclose()

    @staticmethod
    def _init_schema(bus: EventBus) -> None:
        with bus.conn:
            bus.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS unifiedpush_subscriptions (
                  instance_id TEXT PRIMARY KEY,
                  endpoint TEXT NOT NULL,
                  updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS unifiedpush_deliveries (
                  event_id INTEGER NOT NULL,
                  instance_id TEXT NOT NULL,
                  PRIMARY KEY (event_id, instance_id)
                );
                """
            )


def _validate_endpoint(endpoint: str) -> None:
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise HTTPException(status_code=400, detail="UnifiedPush endpoint must be an HTTPS URL")
    hostname = parsed.hostname.rstrip(".").lower()
    if hostname == "localhost" or hostname.endswith(".localhost"):
        raise HTTPException(status_code=400, detail="UnifiedPush endpoint must be public")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return
    if not address.is_global:
        raise HTTPException(status_code=400, detail="UnifiedPush endpoint must be public")


def _session_created(bus: EventBus, session_id: str) -> EventRecord | None:
    events = bus.replay(
        EventFilter(names=frozenset({SessionCreated.name}), tags={"session": session_id}),
        limit=1,
    )
    return events[0] if events else None


def _event_by_id(bus: EventBus, event_id: Any) -> EventRecord | None:
    if not isinstance(event_id, int):
        return None
    return bus.get_event(event_id)


def _is_transition_to_finished(bus: EventBus, event: EventRecord) -> bool:
    previous = [
        candidate
        for candidate in bus.replay(
            EventFilter(
                names=frozenset({SessionStateChanged.name}),
                tags={"session": event.tags["session"]},
            )
        )
        if candidate.id < event.id
    ]
    return not previous or previous[-1].tags.get("state") != "finished"


def _session_title(bus: EventBus, created: EventRecord, before_id: int) -> str:
    renamed = [
        event
        for event in bus.replay(
            EventFilter(
                names=frozenset({SessionRenamed.name}),
                tags={"session": created.tags["session"]},
            )
        )
        if event.id < before_id
    ]
    title = renamed[-1].payload.get("title") if renamed else created.payload.get("title")
    return title.strip() if isinstance(title, str) and title.strip() else "Harness session"


def _notification_content(source: EventRecord | None) -> str:
    if source is None:
        return "Session finished"
    if source.name == LlmRunFailed.name:
        detail = source.payload.get("error") or source.payload.get("message")
        return detail.strip() if isinstance(detail, str) and detail.strip() else "Session failed"
    if source.name != AssistantMessageCreated.name:
        return "Session finished"
    text = _text_content(source.payload.get("content")).strip()
    return text or "Session finished"


def _text_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, dict):
            for key in ("text", "content", "output"):
                value = item.get(key)
                extracted = _text_content(value) if isinstance(value, list) else value
                if isinstance(extracted, str) and extracted:
                    parts.append(extracted)
                    break
    return "\n".join(parts)


def _bounded_text(value: str, max_bytes: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    suffix = "…"
    prefix = encoded[: max_bytes - len(suffix.encode("utf-8"))]
    return prefix.decode("utf-8", errors="ignore") + suffix


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)
