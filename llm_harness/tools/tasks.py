from __future__ import annotations

import asyncio
import json
import logging
from copy import deepcopy
from typing import Any

from llm_harness.core.consumer import EventConsumer
from llm_harness.core.events import EventBus, EventFilter, EventRecord
from llm_harness.core.types import ToolCall, ToolMessageCreated, ToolResult, ToolSession

logger = logging.getLogger(__name__)

_TASK_STATES = frozenset({"todo", "in_progress", "finished"})
_ACTION_TYPES = frozenset({"add", "remove", "update"})


class TasksTool:
    """Maintain the planning task list for each calling session."""

    name = "tasks"
    description = (
        "Manage the task list for the current session. Use it to track the work "
        "you are planning to do."
    )
    input_schema = {
        "type": "object",
        "properties": {
            "actions": {
                "type": "array",
                "description": "Actions to apply before returning the current task list.",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["add", "remove", "update"],
                        },
                        "id": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Task id (for remove and update).",
                        },
                        "name": {
                            "type": "string",
                            "description": "Task name (required when adding a task).",
                        },
                        "state": {
                            "type": "string",
                            "enum": ["todo", "in_progress", "finished"],
                        },
                    },
                    "required": ["type", "state"],
                    "additionalProperties": False,
                },
            },
        },
        "additionalProperties": False,
    }

    def __init__(self) -> None:
        self._tasks: dict[str, list[dict[str, Any]]] = {}
        self._next_ids: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def run(self, call: ToolCall) -> ToolResult:
        session_id = call.session.id
        actions = call.input.get("actions", [])
        validated = _validate_actions(actions)

        # Validate everything before making changes, so one malformed action
        # cannot leave a partially applied batch behind.
        async with self._lock:
            tasks = deepcopy(self._tasks.get(session_id, []))
            next_id = self._next_ids.get(session_id, 0)
            for action in validated:
                if action["type"] == "add":
                    task = {
                        "id": next_id,
                        "name": action["name"],
                        "state": action["state"],
                    }
                    tasks.append(task)
                    next_id += 1
                elif action["type"] == "remove":
                    task_id = action["id"]
                    _task_for_id(tasks, task_id)  # Fail rather than silently do nothing.
                    tasks = [task for task in tasks if task["id"] != task_id]
                else:
                    task = _task_for_id(tasks, action["id"])
                    task["state"] = action["state"]
                    if "name" in action:
                        task["name"] = action["name"]

            self._tasks[session_id] = tasks
            self._next_ids[session_id] = next_id
            state = _task_state(tasks)

        return ToolResult(
            output=json.dumps(state, separators=(",", ":")),
            # Keep the state in the event result so a future process can
            # reconstruct the in-memory tool state after a restart.
            metadata={**state, "next_id": next_id},
        )

    async def restore(self, session_id: str, state: dict[str, Any], next_id: int | None = None) -> None:
        """Restore a previously returned state for an event-consumer restart."""
        tasks = state.get("tasks")
        if not isinstance(tasks, list):
            return
        async with self._lock:
            # The first completed result hydrates a process after restart.
            # Never overwrite live state when several events for one session
            # are being consumed concurrently.
            if session_id in self._tasks:
                return
            self._tasks[session_id] = deepcopy(tasks)
            maximum = max((task.get("id", -1) for task in tasks), default=-1) + 1
            self._next_ids[session_id] = max(maximum, next_id or 0)


class TasksToolConsumer(EventConsumer):
    name = "tasks"
    subscriber = "plugin:tasks"
    event_filter = EventFilter(names=frozenset({"tool.call.requested"}), tags={"tool": TasksTool.name})

    def __init__(self, *, tool: TasksTool):
        self.tool = tool

    async def process_event(self, bus: EventBus, event: EventRecord, *, registry=None) -> None:
        if await self._already_completed(bus, event):
            return

        session_id = event.tags["session"]
        await self._restore_from_latest_result(bus, session_id)
        call = ToolCall(
            session=ToolSession(id=session_id),
            name=self.tool.name,
            input=event.payload.get("input", {}),
        )
        try:
            result = await self.tool.run(call)
        except Exception as exc:
            logger.exception("task tool execution failed session=%s run=%s", session_id, event.tags["run"])
            result = _exception_result(exc)
        await bus.append_message(
            ToolMessageCreated(
                session_id=session_id,
                content=result.output,
                tool=self.tool.name,
                run_id=event.tags["run"],
                metadata=result.metadata,
            ),
            producer=self.name,
            causation_id=event.id,
            correlation_id=event.correlation_id or event.id,
        )

    async def _already_completed(self, bus: EventBus, event: EventRecord) -> bool:
        return bool(
            bus.replay(
                EventFilter(
                    names=frozenset({ToolMessageCreated.name}),
                    tags={
                        "session": event.tags["session"],
                        "tool": self.tool.name,
                        "run": event.tags["run"],
                    },
                )
            )
        )

    async def _restore_from_latest_result(self, bus: EventBus, session_id: str) -> None:
        results = bus.replay(
            EventFilter(names=frozenset({ToolMessageCreated.name}), tags={"session": session_id, "tool": self.tool.name})
        )
        for result in reversed(results):
            metadata = result.payload.get("metadata", {})
            if not isinstance(metadata, dict) or not isinstance(metadata.get("tasks"), list):
                continue
            await self.tool.restore(session_id, metadata, metadata.get("next_id"))
            return


def task_state_from_result(
    content: Any, metadata: Any = None
) -> dict[str, Any] | None:
    """Return a validated, complete task state from a tool result.

    The task tool includes the complete list in both its textual output and
    metadata.  Accepting metadata as a fallback lets consumers reconstruct the
    state without trusting a separately maintained task cache.
    """
    candidates: list[Any] = [content, metadata]
    for candidate in candidates:
        if isinstance(candidate, str):
            try:
                candidate = json.loads(candidate)
            except (TypeError, json.JSONDecodeError):
                continue
        if not isinstance(candidate, dict) or not isinstance(candidate.get("tasks"), list):
            continue
        tasks: list[dict[str, Any]] = []
        valid = True
        for task in candidate["tasks"]:
            if not isinstance(task, dict):
                valid = False
                break
            task_id = task.get("id")
            name = task.get("name")
            state = task.get("state")
            if (
                not isinstance(task_id, int)
                or isinstance(task_id, bool)
                or not isinstance(name, str)
                or not name.strip()
                or state not in _TASK_STATES
            ):
                valid = False
                break
            tasks.append({"id": task_id, "name": name, "state": state})
        if not valid:
            continue
        return {
            "tasks": tasks,
            "total": len(tasks),
            "finished": sum(task["state"] == "finished" for task in tasks),
            "in_progress": sum(task["state"] == "in_progress" for task in tasks),
        }
    return None


def _validate_actions(actions: Any) -> list[dict[str, Any]]:
    if not isinstance(actions, list):
        raise ValueError("tool input field 'actions' must be a list")

    validated: list[dict[str, Any]] = []
    for index, action in enumerate(actions):
        if not isinstance(action, dict):
            raise ValueError(f"action {index} must be an object")
        action_type = action.get("type")
        if not isinstance(action_type, str) or action_type not in _ACTION_TYPES:
            raise ValueError(f"action {index} has invalid type; expected add, remove, or update")
        state = action.get("state")
        if not isinstance(state, str) or state not in _TASK_STATES:
            raise ValueError(f"action {index} requires state todo, in_progress, or finished")

        allowed = {"type", "state"}
        if action_type in {"remove", "update"}:
            allowed.add("id")
            task_id = action.get("id")
            if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 0:
                raise ValueError(f"action {index} requires a non-negative integer id")
        if action_type in {"add", "update"}:
            allowed.add("name")
        unexpected = set(action) - allowed
        if unexpected:
            fields = ", ".join(sorted(unexpected))
            raise ValueError(f"action {index} has unexpected field(s): {fields}")
        if action_type == "add":
            name = action.get("name")
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"action {index} requires a non-empty string name")
        elif "name" in action:
            name = action["name"]
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"action {index} name must be a non-empty string")
        validated.append(dict(action))
    return validated


def _task_for_id(tasks: list[dict[str, Any]], task_id: int) -> dict[str, Any]:
    for task in tasks:
        if task["id"] == task_id:
            return task
    raise ValueError(f"unknown task id: {task_id}")


def _task_state(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "tasks": deepcopy(tasks),
        "total": len(tasks),
        "finished": sum(task["state"] == "finished" for task in tasks),
        "in_progress": sum(task["state"] == "in_progress" for task in tasks),
    }


def _exception_result(exc: Exception) -> ToolResult:
    error_type = type(exc).__name__
    error_message = str(exc)
    description = f"{error_type}: {error_message}" if error_message else error_type
    return ToolResult(
        output=f"tool execution failed: {description}\n",
        metadata={"success": False, "error_type": error_type, "error": error_message},
    )
