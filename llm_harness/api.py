from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

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
    _install_frontend(app)
    return app


def _install_frontend(app: FastAPI) -> None:
    frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
    if not frontend_dir.exists():
        return

    @app.get("/")
    async def frontend_root() -> RedirectResponse:
        return RedirectResponse("/frontend/")

    app.mount("/frontend", StaticFiles(directory=frontend_dir, html=True), name="frontend")
