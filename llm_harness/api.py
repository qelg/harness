from __future__ import annotations

from fastapi import FastAPI

from llm_harness.config import Settings
from llm_harness.core.events import EventBus
from llm_harness.plugins import Registry, load_plugins


def create_app() -> FastAPI:
    settings = Settings.from_env()
    bus = EventBus(settings.event_database_path)
    registry = Registry()
    load_plugins(registry)

    app = FastAPI(title="LLM Harness")
    app.state.bus = bus
    app.state.registry = registry
    registry.install_api_plugins(app=app, bus=bus)
    registry.install_event_consumer_plugins(app=app, bus=bus)
    return app
