from __future__ import annotations

import logging
import os

import uvicorn


def main() -> None:
    _configure_logging()
    uvicorn.run(
        "llm_harness.api:create_app",
        factory=True,
        host=os.getenv("HARNESS_HOST", "127.0.0.1"),
        port=int(os.getenv("HARNESS_PORT", "8000")),
        log_level=os.getenv("HARNESS_LOG_LEVEL", _uvicorn_log_level()),
    )


def _configure_logging() -> None:
    level_name = os.getenv("HARNESS_LOG_LEVEL")
    if level_name is None and _provider_event_logging_enabled():
        level_name = "info"
    if level_name is None:
        return
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )


def _uvicorn_log_level() -> str:
    return "info" if _provider_event_logging_enabled() else "warning"


def _provider_event_logging_enabled() -> bool:
    return os.getenv("HARNESS_LOG_PROVIDER_EVENTS", "0").strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    main()
