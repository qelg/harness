from llm_harness.db import Store
from llm_harness.domain import Role


def test_store_persists_session_and_messages(tmp_path):
    store = Store(tmp_path / "harness.db")
    store.init_schema()

    session, _ = store.create_session("Demo", ["alpha", "alpha", "beta"])
    message, _ = store.append_message(session.id, Role.USER, "hello")

    assert session.tags == ("alpha", "beta")
    assert store.get_session(session.id).title == "Demo"
    assert store.list_messages(session.id)[0].id == message.id

