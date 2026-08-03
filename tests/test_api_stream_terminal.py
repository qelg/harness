from llm_harness.api_plugin import MESSAGE_UPDATE_NAMES, _is_terminal_stream_event
from llm_harness.core.events import EventRecord


def event(name, content):
    return EventRecord(id=1, name=name, payload={"content": content}, tags={"session": "sess_1"}, created_at_ms=1)


def test_function_call_assistant_is_not_terminal():
    assert not _is_terminal_stream_event(event("chat.message.assistant.created", [{"type": "function_call", "name": "terminal"}]))


def test_text_assistant_is_terminal():
    assert _is_terminal_stream_event(event("chat.message.assistant.created", [{"type": "message", "content": [{"text": "done"}]}]))


def test_failure_is_terminal():
    assert _is_terminal_stream_event(event("llm.run.failed", None))


def test_message_update_stream_includes_session_states():
    assert "session.state" in MESSAGE_UPDATE_NAMES
    assert "session.renamed" in MESSAGE_UPDATE_NAMES


def test_final_response_with_earlier_after_response_queue_is_not_terminal(tmp_path):
    import asyncio

    from llm_harness.builtin_plugins.session_state import SessionStatePlugin
    from llm_harness.core.events import EventService
    from llm_harness.core.types import AssistantMessageCreated, QueuedMessage

    bus = EventService(tmp_path / "events.db")
    asyncio.run(
        bus.append_message(QueuedMessage("sess_1", "follow up", "after_response"))
    )
    final = asyncio.run(
        bus.append_message(
            AssistantMessageCreated(
                "sess_1", "done", "mock", "test", "llm_1"
            )
        )
    )

    assert not _is_terminal_stream_event(final, bus=bus)

    # The answer remains non-terminal after the state consumer atomically
    # changes the pending command into a user message.
    asyncio.run(SessionStatePlugin().process_event(bus, final))
    assert not _is_terminal_stream_event(final, bus=bus)
