from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    event_database_path: Path
    openai_api_key: str | None
    openai_base_url: str
    openrouter_api_key: str | None
    openrouter_base_url: str
    podman_image: str
    podman_mount_nix_store: bool
    tag_container_map: dict[str, str]
    chatgpt_oauth_authorization_url: str | None
    chatgpt_oauth_token_url: str | None
    chatgpt_oauth_userinfo_url: str | None
    chatgpt_oauth_client_id: str | None
    chatgpt_oauth_client_secret: str | None
    chatgpt_oauth_redirect_uri: str | None
    chatgpt_oauth_scope: str
    codex_oauth_issuer_url: str
    codex_oauth_client_id: str
    codex_oauth_token_url: str
    codex_oauth_base_url: str
    codex_oauth_refresh_skew_seconds: int
    mock_llm_response: str
    workers_inline: bool
    default_provider: str
    default_model: str
    default_toolsets: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            event_database_path=Path(os.getenv("HARNESS_EVENTS_DB", ".harness/events.db")),
            openai_api_key=os.getenv("HARNESS_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
            openai_base_url=os.getenv("HARNESS_OPENAI_BASE_URL", "https://api.openai.com/v1"),
            openrouter_api_key=os.getenv("HARNESS_OPENROUTER_API_KEY") or os.getenv("OPENROUTER_API_KEY"),
            openrouter_base_url=os.getenv("HARNESS_OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
            podman_image=os.getenv("HARNESS_PODMAN_IMAGE", "docker.io/library/python:3.12-slim"),
            podman_mount_nix_store=parse_bool(os.getenv("HARNESS_PODMAN_MOUNT_NIX_STORE", "0")),
            tag_container_map=parse_tag_container_map(os.getenv("HARNESS_TAG_CONTAINER_MAP", "")),
            chatgpt_oauth_authorization_url=os.getenv("HARNESS_CHATGPT_OAUTH_AUTHORIZATION_URL"),
            chatgpt_oauth_token_url=os.getenv("HARNESS_CHATGPT_OAUTH_TOKEN_URL"),
            chatgpt_oauth_userinfo_url=os.getenv("HARNESS_CHATGPT_OAUTH_USERINFO_URL"),
            chatgpt_oauth_client_id=os.getenv("HARNESS_CHATGPT_OAUTH_CLIENT_ID"),
            chatgpt_oauth_client_secret=os.getenv("HARNESS_CHATGPT_OAUTH_CLIENT_SECRET"),
            chatgpt_oauth_redirect_uri=os.getenv("HARNESS_CHATGPT_OAUTH_REDIRECT_URI"),
            chatgpt_oauth_scope=os.getenv("HARNESS_CHATGPT_OAUTH_SCOPE", "openid profile email"),
            codex_oauth_issuer_url=os.getenv("HARNESS_CODEX_OAUTH_ISSUER_URL", "https://auth.openai.com").rstrip("/"),
            codex_oauth_client_id=os.getenv("HARNESS_CODEX_OAUTH_CLIENT_ID", "app_EMoamEEZ73f0CkXaXp7hrann"),
            codex_oauth_token_url=os.getenv("HARNESS_CODEX_OAUTH_TOKEN_URL", "https://auth.openai.com/oauth/token"),
            codex_oauth_base_url=os.getenv("HARNESS_CODEX_OAUTH_BASE_URL", "https://chatgpt.com/backend-api/codex"),
            codex_oauth_refresh_skew_seconds=int(os.getenv("HARNESS_CODEX_OAUTH_REFRESH_SKEW_SECONDS", "120")),
            mock_llm_response=os.getenv("HARNESS_MOCK_LLM_RESPONSE", "mock llm response"),
            workers_inline=parse_bool(os.getenv("HARNESS_WORKERS_INLINE", "0")),
            default_provider=os.getenv("HARNESS_DEFAULT_PROVIDER", "mock-llm"),
            default_model=os.getenv("HARNESS_DEFAULT_MODEL", "test-model"),
            default_toolsets=parse_csv(os.getenv("HARNESS_DEFAULT_TOOLSETS", "default")),
        )


def parse_tag_container_map(raw: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        tag, sep, container = item.partition("=")
        if not sep or not tag.strip() or not container.strip():
            raise ValueError("HARNESS_TAG_CONTAINER_MAP entries must look like tag=container")
        mapping[tag.strip()] = container.strip()
    return mapping


def parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_csv(raw: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw.split(",") if item.strip())
