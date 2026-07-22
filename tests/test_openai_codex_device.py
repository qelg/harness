import base64
import json

from llm_harness.api import create_app
from llm_harness.auth_plugins.openai_codex_device import _jwt_claim, _subject_from


def test_openai_codex_device_plugin_installs_schema(tmp_path, monkeypatch):
    db_path = tmp_path / "harness.db"
    monkeypatch.setenv("HARNESS_DB", str(db_path))
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))

    app = create_app()
    conn = app.state.store._conn

    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('oauth_device_codes', 'oauth_tokens')"
        ).fetchall()
    }
    assert tables == {"oauth_device_codes", "oauth_tokens"}


def test_codex_subject_uses_id_token_sub():
    payload = base64.urlsafe_b64encode(json.dumps({"sub": "codex-user"}).encode()).decode().rstrip("=")
    token = f"header.{payload}.signature"

    assert _subject_from({"id_token": token}) == "codex-user"


def test_codex_jwt_claim_handles_invalid_token():
    assert _jwt_claim("not-a-jwt", "sub") is None
