from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any
from urllib.parse import urlsplit

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from fastapi import HTTPException
from pydantic import BaseModel, Field

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import (
    AssistantMessageCreated,
    LlmRunFailed,
    SecretAsk,
    SessionCreated,
    SessionRenamed,
    SessionStateChanged,
)


logger = logging.getLogger(__name__)


class UnifiedPushSubscription(BaseModel):
    instance_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    endpoint: str = Field(min_length=1, max_length=4096)
    public_key: str = Field(min_length=1, max_length=1024)


class UnifiedPushPlugin(EventConsumer):
    """Send top-level session completion notifications through UnifiedPush."""

    name = "unifiedpush"
    subscriber = "plugin:unifiedpush"
    event_filter = EventFilter(
        names=frozenset({SessionStateChanged.name}),
        name_prefixes=(),
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
            _validate_public_key(request.public_key)
            with bus.conn:
                bus.conn.execute(
                    """
                    INSERT INTO unifiedpush_subscriptions(instance_id, endpoint, public_key, updated_at_ms)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(instance_id) DO UPDATE SET
                      endpoint = excluded.endpoint,
                      public_key = excluded.public_key,
                      updated_at_ms = excluded.updated_at_ms
                    """,
                    (request.instance_id, request.endpoint, request.public_key, _now_ms()),
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
        state = event.tags.get("state")
        if state not in {"secret.ask", "finished"} or not _is_transition_to_state(bus, event, state):
            return

        source = _event_by_id(bus, event.payload.get("source_event_id"))
        if state == "secret.ask":
            notification_type = "session.secret.ask"
            content = _secret_ask_content(source)
        else:
            notification_type = "session.finished"
            content = _notification_content(source)
        payload = {
            "type": notification_type,
            "session_id": session_id,
            "title": _bounded_text(_session_title(bus, created, event.id), 512),
            # Keep notifications under the limits imposed by common UnifiedPush
            # distributors while retaining as much useful text as possible.
            "content": _bounded_text(content, 3000),
            "event_id": event.id,
        }
        plaintext = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        subscriptions = bus.conn.execute(
            "SELECT instance_id, endpoint, public_key FROM unifiedpush_subscriptions ORDER BY instance_id"
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
                body = _encrypt_payload(
                    plaintext, subscription["public_key"], instance_id
                )
                response = await client.post(
                    subscription["endpoint"],
                    content=body,
                    headers={
                        "Content-Type": "application/json",
                        # Mozilla Autopush requires a content encoding for
                        # any non-empty notification body. The encrypted
                        # UnifiedPush envelope is opaque to the distributor.
                        "Content-Encoding": "aes128gcm",
                        "TTL": "86400",
                        "Urgency": "normal",
                        "Topic": f"session-{event.id}",
                    },
                )
                if response.status_code >= 400:
                    logger.warning(
                        "UnifiedPush delivery failed for instance %s: HTTP %s; response body: %s",
                        instance_id,
                        response.status_code,
                        response.text,
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
                  public_key TEXT,
                  updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS unifiedpush_deliveries (
                  event_id INTEGER NOT NULL,
                  instance_id TEXT NOT NULL,
                  PRIMARY KEY (event_id, instance_id)
                );
                """
            )
            columns = {
                row["name"] for row in bus.conn.execute("PRAGMA table_info(unifiedpush_subscriptions)")
            }
            if "public_key" not in columns:
                bus.conn.execute("ALTER TABLE unifiedpush_subscriptions ADD COLUMN public_key TEXT")
                # Never fall back to plaintext for subscriptions created by an
                # older client. The current Android app re-registers its endpoint.
                bus.conn.execute("DELETE FROM unifiedpush_subscriptions WHERE public_key IS NULL")


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


def _is_transition_to_state(bus: EventBus, event: EventRecord, state: str) -> bool:
    previous = bus.replay(
        EventFilter(
            names=frozenset({SessionStateChanged.name}),
            tags={"session": event.tags["session"]},
            before_id=event.id,
        )
    )
    return not previous or previous[-1].tags.get("state") != state


def _session_title(bus: EventBus, created: EventRecord, before_id: int) -> str:
    renamed = bus.replay(
        EventFilter(
            names=frozenset({SessionRenamed.name}),
            tags={"session": created.tags["session"]},
            before_id=before_id,
        )
    )
    title = renamed[-1].payload.get("title") if renamed else created.payload.get("title")
    return title.strip() if isinstance(title, str) and title.strip() else "Harness session"


def _secret_ask_content(source: EventRecord | None) -> str:
    if source is None or source.name != SecretAsk.name:
        return "A secret is required"
    description = source.payload.get("description")
    return description.strip() if isinstance(description, str) and description.strip() else "A secret is required"


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


PUSH_CRYPTO_VERSION = "P-256-HKDF-SHA256-AES-256-GCM"
_PUSH_CRYPTO_INFO = b"harness-unifiedpush-v1"


def _b64encode(value: bytes) -> str:
    return urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    return urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _validate_public_key(value: str) -> None:
    try:
        key = serialization.load_der_public_key(_b64decode(value))
    except (TypeError, ValueError) as error:
        raise HTTPException(status_code=400, detail="invalid UnifiedPush public key") from error
    if not isinstance(key, ec.EllipticCurvePublicKey) or not isinstance(key.curve, ec.SECP256R1):
        raise HTTPException(status_code=400, detail="UnifiedPush public key must use P-256")


def _encrypt_payload(plaintext: bytes, encoded_public_key: str, instance_id: str) -> bytes:
    # The distributor receives only this authenticated envelope. The long-lived
    # recipient private key is generated in AndroidKeyStore and never leaves the app.
    recipient = serialization.load_der_public_key(_b64decode(encoded_public_key))
    if not isinstance(recipient, ec.EllipticCurvePublicKey) or not isinstance(
        recipient.curve, ec.SECP256R1
    ):
        raise ValueError("stored UnifiedPush public key is not P-256")
    ephemeral = ec.generate_private_key(ec.SECP256R1())
    shared_secret = ephemeral.exchange(ec.ECDH(), recipient)
    key = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=_PUSH_CRYPTO_INFO).derive(
        shared_secret
    )
    nonce = os.urandom(12)
    envelope = {
        "version": PUSH_CRYPTO_VERSION,
        "ephemeral_public_key": _b64encode(
            ephemeral.public_key().public_bytes(
                serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
            )
        ),
        "nonce": _b64encode(nonce),
        "ciphertext": _b64encode(AESGCM(key).encrypt(nonce, plaintext, instance_id.encode())),
    }
    return json.dumps(envelope, separators=(",", ":")).encode()


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)
