from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from llm_harness.config import Settings


def ensure_oauth_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS oauth_device_codes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          provider TEXT NOT NULL,
          device_auth_id TEXT NOT NULL UNIQUE,
          user_code TEXT NOT NULL,
          verification_url TEXT NOT NULL,
          interval_seconds INTEGER NOT NULL,
          created_at TEXT NOT NULL,
          expires_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS oauth_tokens (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          provider TEXT NOT NULL,
          subject TEXT NOT NULL,
          access_token TEXT NOT NULL,
          refresh_token TEXT,
          token_type TEXT,
          scope TEXT,
          expires_at TEXT,
          metadata_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_oauth_device_codes_provider
          ON oauth_device_codes(provider, expires_at);
        CREATE INDEX IF NOT EXISTS idx_oauth_tokens_provider_subject
          ON oauth_tokens(provider, subject);
        """
    )


class CodexOAuthTokenStore:
    def __init__(self, *, conn: sqlite3.Connection, settings: Settings, provider: str = "openai-codex") -> None:
        self.conn = conn
        self.settings = settings
        self.provider = provider
        ensure_oauth_schema(conn)

    async def access_token(self) -> str:
        row = self.latest_token()
        if row is None:
            raise RuntimeError("no ChatGPT/Codex login token stored; use Login with ChatGPT first")

        metadata = _metadata_from(row)
        refresh_after = metadata.get("refresh_after")
        if refresh_after and _load_dt(refresh_after) <= _utc_now():
            if not row["refresh_token"]:
                raise RuntimeError("stored ChatGPT/Codex token is expired and has no refresh token")
            await self.refresh(row)
            row = self.latest_token()
            if row is None:
                raise RuntimeError("ChatGPT/Codex token disappeared after refresh")

        return str(row["access_token"])

    def latest_token(self) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM oauth_tokens
            WHERE provider = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (self.provider,),
        ).fetchone()

    async def refresh(self, row: sqlite3.Row) -> None:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": row["refresh_token"],
            "client_id": self.settings.codex_oauth_client_id,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                self.settings.codex_oauth_token_url,
                data=data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "User-Agent": "llm-harness/0.1.0",
                },
            )
        if response.status_code >= 400:
            raise RuntimeError(f"ChatGPT/Codex token refresh failed: HTTP {response.status_code}: {response.text}")

        token_payload = {"refresh_token": row["refresh_token"], **response.json()}
        now = _utc_now()
        expires_at = _expires_at(token_payload)
        metadata = _metadata(
            token_payload,
            base_url=self.settings.codex_oauth_base_url,
            refresh_after=_refresh_after(expires_at, self.settings.codex_oauth_refresh_skew_seconds),
        )
        with self.conn:
            self.conn.execute(
                """
                UPDATE oauth_tokens
                SET access_token = ?,
                    refresh_token = ?,
                    token_type = ?,
                    scope = ?,
                    expires_at = ?,
                    metadata_json = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    token_payload["access_token"],
                    token_payload.get("refresh_token"),
                    token_payload.get("token_type"),
                    token_payload.get("scope"),
                    expires_at,
                    json.dumps(metadata),
                    _dump_dt(now),
                    row["id"],
                ),
            )


def metadata_for_token(token_payload: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
    expires_at = _expires_at(token_payload)
    return _metadata(
        token_payload,
        base_url=settings.codex_oauth_base_url,
        refresh_after=_refresh_after(expires_at, settings.codex_oauth_refresh_skew_seconds),
    )


def expires_at_for_token(token_payload: dict[str, Any]) -> str | None:
    return _expires_at(token_payload)


def _metadata(token_payload: dict[str, Any], *, base_url: str, refresh_after: str | None) -> dict[str, Any]:
    return {
        "auth_mode": "chatgpt",
        "source": "device-code",
        "base_url": base_url,
        "refresh_after": refresh_after,
        "token_response": {
            key: value
            for key, value in token_payload.items()
            if key not in {"access_token", "refresh_token"}
        },
    }


def _metadata_from(row: sqlite3.Row) -> dict[str, Any]:
    return json.loads(row["metadata_json"])


def _expires_at(token_payload: dict[str, Any]) -> str | None:
    expires_in = token_payload.get("expires_in")
    if expires_in is None:
        return None
    return _dump_dt(_utc_now() + timedelta(seconds=int(expires_in)))


def _refresh_after(expires_at: str | None, skew_seconds: int) -> str | None:
    if expires_at is None:
        return None
    return _dump_dt(_load_dt(expires_at) - timedelta(seconds=skew_seconds))


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _dump_dt(value: datetime) -> str:
    return value.isoformat()


def _load_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
