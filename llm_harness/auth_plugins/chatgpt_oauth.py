from __future__ import annotations

import base64
import json
import secrets
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse

from llm_harness.config import Settings


class ChatGPTOAuthPlugin:
    name = "chatgpt-oauth"

    def __init__(self, *, settings: Settings):
        self.settings = settings

    def install_api(self, *, app, bus, registry) -> None:
        conn = bus.conn
        self._init_schema(conn)
        router = APIRouter(prefix="/auth/chatgpt", tags=["auth"])

        @router.get("/login")
        async def login(request: Request, return_to: str | None = None) -> RedirectResponse:
            config = self._require_config()
            state = secrets.token_urlsafe(32)
            now = _utc_now()
            expires_at = now + timedelta(minutes=10)
            with conn:
                conn.execute(
                    """
                    INSERT INTO oauth_states(provider, state, return_to, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (self.name, state, return_to, _dump_dt(now), _dump_dt(expires_at)),
                )

            redirect_uri = self._redirect_uri(request)
            params = {
                "response_type": "code",
                "client_id": config["client_id"],
                "redirect_uri": redirect_uri,
                "scope": self.settings.chatgpt_oauth_scope,
                "state": state,
            }
            return RedirectResponse(f"{config['authorization_url']}?{urlencode(params)}")

        @router.get("/callback")
        async def callback(
            request: Request,
            code: str = Query(...),
            state: str = Query(...),
        ) -> dict[str, Any]:
            config = self._require_config()
            state_row = self._consume_state(conn, state)
            redirect_uri = self._redirect_uri(request)
            token_payload = await self._exchange_code(config=config, code=code, redirect_uri=redirect_uri)
            userinfo = await self._load_userinfo(token_payload)
            token_id = self._store_token(conn, token_payload=token_payload, userinfo=userinfo)
            response: dict[str, Any] = {
                "token_id": token_id,
                "provider": self.name,
                "subject": _subject_from(token_payload, userinfo),
            }
            if state_row["return_to"]:
                response["return_to"] = state_row["return_to"]
            return response

        @router.get("/tokens")
        async def list_tokens() -> list[dict[str, Any]]:
            rows = conn.execute(
                """
                SELECT id, provider, subject, token_type, scope, expires_at, created_at, updated_at, metadata_json
                FROM oauth_tokens
                WHERE provider = ?
                ORDER BY updated_at DESC, id DESC
                """,
                (self.name,),
            ).fetchall()
            return [_public_token(row) for row in rows]

        @router.post("/tokens/{token_id}/refresh")
        async def refresh_token(token_id: int) -> dict[str, Any]:
            config = self._require_config()
            row = conn.execute(
                "SELECT * FROM oauth_tokens WHERE id = ? AND provider = ?",
                (token_id, self.name),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"unknown token: {token_id}")
            if not row["refresh_token"]:
                raise HTTPException(status_code=400, detail="token has no refresh token")

            token_payload = await self._refresh(config=config, refresh_token=row["refresh_token"])
            merged_payload = {
                "refresh_token": row["refresh_token"],
                **token_payload,
            }
            userinfo = json.loads(row["metadata_json"]).get("userinfo", {})
            self._update_token(conn, token_id=token_id, token_payload=merged_payload, userinfo=userinfo)
            updated = conn.execute(
                """
                SELECT id, provider, subject, token_type, scope, expires_at, created_at, updated_at, metadata_json
                FROM oauth_tokens
                WHERE id = ?
                """,
                (token_id,),
            ).fetchone()
            return _public_token(updated)

        app.include_router(router)

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS oauth_states (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              provider TEXT NOT NULL,
              state TEXT NOT NULL UNIQUE,
              return_to TEXT,
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

            CREATE INDEX IF NOT EXISTS idx_oauth_tokens_provider_subject
              ON oauth_tokens(provider, subject);
            CREATE INDEX IF NOT EXISTS idx_oauth_states_provider_state
              ON oauth_states(provider, state);
            """
        )

    def _require_config(self) -> dict[str, str]:
        required = {
            "authorization_url": self.settings.chatgpt_oauth_authorization_url,
            "token_url": self.settings.chatgpt_oauth_token_url,
            "client_id": self.settings.chatgpt_oauth_client_id,
            "client_secret": self.settings.chatgpt_oauth_client_secret,
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise HTTPException(
                status_code=503,
                detail=f"chatgpt oauth is not configured; missing: {', '.join(missing)}",
            )
        return {key: value for key, value in required.items() if value is not None}

    def _redirect_uri(self, request: Request) -> str:
        if self.settings.chatgpt_oauth_redirect_uri:
            return self.settings.chatgpt_oauth_redirect_uri
        return str(request.url_for("callback"))

    def _consume_state(self, conn: sqlite3.Connection, state: str) -> sqlite3.Row:
        now = _utc_now()
        with conn:
            row = conn.execute(
                "SELECT * FROM oauth_states WHERE provider = ? AND state = ?",
                (self.name, state),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=400, detail="invalid oauth state")
            conn.execute("DELETE FROM oauth_states WHERE id = ?", (row["id"],))
        if _load_dt(row["expires_at"]) < now:
            raise HTTPException(status_code=400, detail="expired oauth state")
        return row

    async def _exchange_code(self, *, config: dict[str, str], code: str, redirect_uri: str) -> dict[str, Any]:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
        }
        return await _post_token(config["token_url"], data)

    async def _refresh(self, *, config: dict[str, str], refresh_token: str) -> dict[str, Any]:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
        }
        return await _post_token(config["token_url"], data)

    async def _load_userinfo(self, token_payload: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.chatgpt_oauth_userinfo_url:
            return {}
        access_token = token_payload.get("access_token")
        if not access_token:
            return {}
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                self.settings.chatgpt_oauth_userinfo_url,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            response.raise_for_status()
            return response.json()

    def _store_token(
        self,
        conn: sqlite3.Connection,
        *,
        token_payload: dict[str, Any],
        userinfo: dict[str, Any],
    ) -> int:
        now = _utc_now()
        subject = _subject_from(token_payload, userinfo)
        metadata = _metadata(token_payload, userinfo)
        with conn:
            cursor = conn.execute(
                """
                INSERT INTO oauth_tokens(
                  provider, subject, access_token, refresh_token, token_type, scope,
                  expires_at, metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.name,
                    subject,
                    token_payload["access_token"],
                    token_payload.get("refresh_token"),
                    token_payload.get("token_type"),
                    token_payload.get("scope"),
                    _expires_at(token_payload),
                    json.dumps(metadata),
                    _dump_dt(now),
                    _dump_dt(now),
                ),
            )
            return int(cursor.lastrowid)

    def _update_token(
        self,
        conn: sqlite3.Connection,
        *,
        token_id: int,
        token_payload: dict[str, Any],
        userinfo: dict[str, Any],
    ) -> None:
        now = _utc_now()
        metadata = _metadata(token_payload, userinfo)
        with conn:
            conn.execute(
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
                    _expires_at(token_payload),
                    json.dumps(metadata),
                    _dump_dt(now),
                    token_id,
                ),
            )


async def _post_token(token_url: str, data: dict[str, str]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            token_url,
            data=data,
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
    if not payload.get("access_token"):
        raise HTTPException(status_code=502, detail="token response did not include access_token")
    return payload


def _subject_from(token_payload: dict[str, Any], userinfo: dict[str, Any]) -> str:
    for key in ("sub", "email", "user_id", "id"):
        value = userinfo.get(key) or token_payload.get(key)
        if value:
            return str(value)
    id_token_sub = _jwt_claim(token_payload.get("id_token"), "sub")
    if id_token_sub:
        return str(id_token_sub)
    return "chatgpt-user"


def _jwt_claim(token: str | None, claim: str) -> Any:
    if not token:
        return None
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except (ValueError, json.JSONDecodeError):
        return None
    return claims.get(claim)


def _metadata(token_payload: dict[str, Any], userinfo: dict[str, Any]) -> dict[str, Any]:
    return {
        "userinfo": userinfo,
        "token_response": {
            key: value
            for key, value in token_payload.items()
            if key not in {"access_token", "refresh_token"}
        },
    }


def _expires_at(token_payload: dict[str, Any]) -> str | None:
    expires_in = token_payload.get("expires_in")
    if expires_in is None:
        return None
    return _dump_dt(_utc_now() + timedelta(seconds=int(expires_in)))


def _public_token(row: sqlite3.Row) -> dict[str, Any]:
    metadata = json.loads(row["metadata_json"])
    return {
        "id": row["id"],
        "provider": row["provider"],
        "subject": row["subject"],
        "token_type": row["token_type"],
        "scope": row["scope"],
        "expires_at": row["expires_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "userinfo": metadata.get("userinfo", {}),
    }


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _dump_dt(value: datetime) -> str:
    return value.isoformat()


def _load_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
