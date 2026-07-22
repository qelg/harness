import base64
import json

from llm_harness.api import create_app
from llm_harness.auth_plugins.chatgpt_oauth import _jwt_claim, _subject_from


def test_chatgpt_oauth_plugin_installs_schema(tmp_path, monkeypatch):
    db_path = tmp_path / "harness.db"
    monkeypatch.setenv("HARNESS_DB", str(db_path))
    monkeypatch.setenv("HARNESS_EVENTS_DB", str(tmp_path / "events.db"))

    app = create_app()
    conn = app.state.store._conn

    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'oauth_%'"
        ).fetchall()
    }
    assert {"oauth_states", "oauth_tokens"}.issubset(tables)


def test_jwt_claim_reads_payload_without_verifying_signature():
    payload = base64.urlsafe_b64encode(json.dumps({"sub": "user-123"}).encode()).decode().rstrip("=")
    token = f"header.{payload}.signature"

    assert _jwt_claim(token, "sub") == "user-123"


def test_subject_prefers_userinfo_sub():
    assert _subject_from({"sub": "token-sub"}, {"sub": "userinfo-sub"}) == "userinfo-sub"
