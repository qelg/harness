from __future__ import annotations

import base64
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

from llm_harness.auth_plugins.token_store import ensure_oauth_schema, expires_at_for_token, metadata_for_token
from llm_harness.config import Settings


class OpenAICodexDeviceAuthPlugin:
    name = "openai-codex"

    def __init__(self, *, settings: Settings):
        self.settings = settings

    def install_api(self, *, app, bus, registry) -> None:
        conn = bus.conn
        self._init_schema(conn)
        router = APIRouter(prefix="/auth/openai-codex", tags=["auth"])

        @router.post("/device/start")
        async def start_device_login() -> dict[str, Any]:
            payload = await self._request_device_code()
            now = _utc_now()
            expires_at = now + timedelta(minutes=15)
            interval_seconds = int(payload.get("interval") or 5)
            user_code = _required_string(payload, "user_code", fallback_key="usercode")
            device_auth_id = _required_string(payload, "device_auth_id")
            verification_url = f"{self.settings.codex_oauth_issuer_url}/codex/device"

            with conn:
                cursor = conn.execute(
                    """
                    INSERT INTO oauth_device_codes(
                      provider, device_auth_id, user_code, verification_url,
                      interval_seconds, created_at, expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.name,
                        device_auth_id,
                        user_code,
                        verification_url,
                        interval_seconds,
                        _dump_dt(now),
                        _dump_dt(expires_at),
                    ),
                )

            return {
                "id": int(cursor.lastrowid),
                "provider": self.name,
                "device_auth_id": device_auth_id,
                "user_code": user_code,
                "verification_url": verification_url,
                "interval_seconds": interval_seconds,
                "expires_at": _dump_dt(expires_at),
            }

        @router.post("/device/{device_code_id}/poll")
        async def poll_device_login(device_code_id: int) -> dict[str, Any]:
            row = conn.execute(
                "SELECT * FROM oauth_device_codes WHERE id = ? AND provider = ?",
                (device_code_id, self.name),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"unknown device code: {device_code_id}")
            if _load_dt(row["expires_at"]) < _utc_now():
                raise HTTPException(status_code=400, detail="device code expired")

            authorization = await self._poll_authorization(
                device_auth_id=row["device_auth_id"],
                user_code=row["user_code"],
            )
            if authorization is None:
                return {
                    "status": "pending",
                    "interval_seconds": row["interval_seconds"],
                    "verification_url": row["verification_url"],
                    "user_code": row["user_code"],
                    "expires_at": row["expires_at"],
                }

            token_payload = await self._exchange_authorization(
                authorization_code=authorization["authorization_code"],
                code_verifier=authorization["code_verifier"],
            )
            token_id = self._store_token(conn, token_payload=token_payload)
            with conn:
                conn.execute("DELETE FROM oauth_device_codes WHERE id = ?", (device_code_id,))
            return {
                "status": "complete",
                "token": self._public_token_by_id(conn, token_id),
            }

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
            row = conn.execute(
                "SELECT * FROM oauth_tokens WHERE id = ? AND provider = ?",
                (token_id, self.name),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"unknown token: {token_id}")
            if not row["refresh_token"]:
                raise HTTPException(status_code=400, detail="token has no refresh token")

            token_payload = await self._refresh(row["refresh_token"])
            merged_payload = {
                "refresh_token": row["refresh_token"],
                **token_payload,
            }
            self._update_token(conn, token_id=token_id, token_payload=merged_payload)
            return self._public_token_by_id(conn, token_id)

        app.include_router(router)

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        ensure_oauth_schema(conn)

    async def _request_device_code(self) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.settings.codex_oauth_issuer_url}/api/accounts/deviceauth/usercode",
                json={"client_id": self.settings.codex_oauth_client_id},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="OpenAI Codex device-code login is not enabled")
        if response.status_code >= 400:
            raise HTTPException(status_code=502, detail=_upstream_error("device code request failed", response))
        return response.json()

    async def _poll_authorization(self, *, device_auth_id: str, user_code: str) -> dict[str, str] | None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.settings.codex_oauth_issuer_url}/api/accounts/deviceauth/token",
                json={"device_auth_id": device_auth_id, "user_code": user_code},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
        if response.status_code in {403, 404}:
            return None
        if response.status_code >= 400:
            raise HTTPException(status_code=502, detail=_upstream_error("device authorization failed", response))

        payload = response.json()
        return {
            "authorization_code": _required_string(payload, "authorization_code"),
            "code_verifier": _required_string(payload, "code_verifier"),
        }

    async def _exchange_authorization(self, *, authorization_code: str, code_verifier: str) -> dict[str, Any]:
        data = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": f"{self.settings.codex_oauth_issuer_url}/deviceauth/callback",
            "client_id": self.settings.codex_oauth_client_id,
            "code_verifier": code_verifier,
        }
        return await self._post_token(data)

    async def _refresh(self, refresh_token: str) -> dict[str, Any]:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.settings.codex_oauth_client_id,
        }
        return await self._post_token(data)

    async def _post_token(self, data: dict[str, str]) -> dict[str, Any]:
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
            error_text = _upstream_error("token request failed", response)
            if "refresh_token_reused" in error_text or "token_invalidated" in error_text:
                raise HTTPException(status_code=401, detail=f"{error_text}; re-login required")
            raise HTTPException(status_code=502, detail=error_text)
        payload = response.json()
        if not payload.get("access_token"):
            raise HTTPException(status_code=502, detail="token response did not include access_token")
        return payload

    def _store_token(self, conn: sqlite3.Connection, *, token_payload: dict[str, Any]) -> int:
        now = _utc_now()
        expires_at = expires_at_for_token(token_payload)
        metadata = metadata_for_token(token_payload, settings=self.settings)
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
                    _subject_from(token_payload),
                    token_payload["access_token"],
                    token_payload.get("refresh_token"),
                    token_payload.get("token_type"),
                    token_payload.get("scope"),
                    expires_at,
                    json.dumps(metadata),
                    _dump_dt(now),
                    _dump_dt(now),
                ),
            )
            return int(cursor.lastrowid)

    def _update_token(self, conn: sqlite3.Connection, *, token_id: int, token_payload: dict[str, Any]) -> None:
        now = _utc_now()
        expires_at = expires_at_for_token(token_payload)
        metadata = metadata_for_token(token_payload, settings=self.settings)
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
                    expires_at,
                    json.dumps(metadata),
                    _dump_dt(now),
                    token_id,
                ),
            )

    def _public_token_by_id(self, conn: sqlite3.Connection, token_id: int) -> dict[str, Any]:
        row = conn.execute(
            """
            SELECT id, provider, subject, token_type, scope, expires_at, created_at, updated_at, metadata_json
            FROM oauth_tokens
            WHERE id = ?
            """,
            (token_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"unknown token: {token_id}")
        return _public_token(row)


def _required_string(payload: dict[str, Any], key: str, *, fallback_key: str | None = None) -> str:
    value = payload.get(key)
    if not value and fallback_key:
        value = payload.get(fallback_key)
    if not isinstance(value, str) or not value.strip():
        raise HTTPException(status_code=502, detail=f"upstream response missing {key}")
    return value.strip()


def _subject_from(token_payload: dict[str, Any]) -> str:
    id_token_sub = _jwt_claim(token_payload.get("id_token"), "sub")
    if id_token_sub:
        return str(id_token_sub)
    for key in ("account_id", "accountId", "sub", "email"):
        value = token_payload.get(key)
        if value:
            return str(value)
    return "openai-codex-user"


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
        "auth_mode": metadata.get("auth_mode"),
        "source": metadata.get("source"),
        "base_url": metadata.get("base_url"),
        "refresh_after": metadata.get("refresh_after"),
    }


def _upstream_error(prefix: str, response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = response.text
    return f"{prefix}: HTTP {response.status_code}: {payload}"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _dump_dt(value: datetime) -> str:
    return value.isoformat()


def _load_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
