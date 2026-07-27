from llm_harness.api_plugin import _is_terminal_stream_event
from llm_harness.core.events import EventRecord


def event(name, content):
    return EventRecord(id=1, name=name, payload={"content": content}, tags={"session": "sess_1"}, created_at_ms=1)


def test_function_call_assistant_is_not_terminal():
    assert not _is_terminal_stream_event(event("chat.message.assistant.created", [{"type": "function_call", "name": "terminal"}]))


def test_text_assistant_is_terminal():
    assert _is_terminal_stream_event(event("chat.message.assistant.created", [{"type": "message", "content": [{"text": "done"}]}]))


def test_failure_is_terminal():
    assert _is_terminal_stream_event(event("llm.run.failed", None))
