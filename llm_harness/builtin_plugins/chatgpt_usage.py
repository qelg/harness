from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import HTTPException

from llm_harness.auth_plugins.token_store import CodexOAuthTokenStore
from llm_harness.config import Settings
from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import AssistantMessageCreated

logger = logging.getLogger(__name__)


class ChatGPTUsagePlugin(EventConsumer):
    """Fetch Codex rate-limit usage after each ChatGPT assistant response."""

    name = "chatgpt-usage"
    subscriber = "plugin:chatgpt-usage"
    event_filter = EventFilter(names=frozenset({AssistantMessageCreated.name}))

    def __init__(self, *, conn: sqlite3.Connection, settings: Settings) -> None:
        self.settings = settings
        self.tokens = CodexOAuthTokenStore(conn=conn, settings=settings)
        self.base_url = settings.codex_oauth_base_url.rstrip("/")

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry: Any = None) -> None:
        if event.tags.get("provider") != "chatgpt-codex":
            return
        if bus.replay(EventFilter(
            names=frozenset({"chatgpt.usage"}),
            causation_id=event.id,
            producer=self.name,
        )):
            return

        try:
            token = await self.tokens.access_token()
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.base_url}/usage",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/json",
                        "User-Agent": "llm-harness/0.1.0",
                    },
                )
                response.raise_for_status()
                usage = response.json()
            if not isinstance(usage, dict):
                raise RuntimeError("ChatGPT usage response was not a JSON object")
            await bus.append(
                "chatgpt.usage",
                usage,
                tags={"session": event.tags["session"], "provider": "chatgpt-codex"},
                producer=self.name,
                causation_id=event.id,
                correlation_id=event.correlation_id or event.id,
            )
        except Exception:
            # Usage is ancillary to the response. A temporary usage endpoint or
            # token failure must not prevent the event consumer from progressing.
            logger.warning("failed to fetch ChatGPT Codex usage", exc_info=True)


class ChatGPTUsageApiPlugin:
    name = "chatgpt-usage-api"

    def install_api(self, *, app, bus: EventBus, registry) -> None:
        @app.get("/chatgpt/usage")
        async def get_chatgpt_usage() -> dict[str, Any]:
            events = bus.replay(EventFilter(names=frozenset({"chatgpt.usage"})))
            if not events:
                raise HTTPException(status_code=404, detail="no ChatGPT usage available")
            usage = dict(events[-1].payload)
            rate_limit = usage.get("rate_limit")
            if isinstance(rate_limit, dict):
                enriched = dict(rate_limit)
                now = datetime.now(UTC).timestamp()
                for key in ("primary_window", "secondary_window"):
                    window = rate_limit.get(key)
                    if not isinstance(window, dict):
                        continue
                    window = dict(window)
                    used = window.get("used_percent")
                    if isinstance(used, (int, float)):
                        window["remaining_percent"] = max(0, 100 - used)
                    reset_at = window.get("reset_at")
                    if isinstance(reset_at, (int, float)):
                        seconds = max(0, int(reset_at - now))
                        window["remaining_seconds"] = seconds
                        window["remaining_hours"] = round(seconds / 3600, 2)
                        window["remaining_days"] = round(seconds / 86400, 2)
                    enriched[key] = window
                usage["rate_limit"] = enriched
            usage["event_id"] = events[-1].id
            usage["updated_at"] = events[-1].created_at_ms
            return usage
