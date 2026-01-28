"""
Serialization helpers for tasks and events in the Celery queue manager.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus
from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType


def serialize_task(task: ClauqBTMTask) -> Dict[str, Any]:
    """Serialize a task for Redis storage."""
    return task.model_dump(mode="json", exclude={"executor"})


def deserialize_task(data: Dict[str, Any]) -> ClauqBTMTask:
    """Deserialize a task from Redis storage."""
    return ClauqBTMTask.model_validate(data)


def serialize_event(event: TaskEvent) -> Dict[str, Any]:
    """Serialize an event for pub/sub."""
    return {
        "event_type": event.event_type.value,
        "task_id": event.task_id,
        "queue_id": event.queue_id,
        "status": event.status.value,
        "timestamp": event.timestamp.isoformat(),
        "error": event.error,
        "progress": event.progress,
        "task": serialize_task(event.task) if event.task else None,
    }


def deserialize_event(data: Dict[str, Any]) -> TaskEvent:
    """Deserialize an event from pub/sub."""
    return TaskEvent(
        event_type=TaskEventType(data["event_type"]),
        task_id=data["task_id"],
        queue_id=data["queue_id"],
        status=TaskStatus(data["status"]),
        timestamp=datetime.fromisoformat(data["timestamp"]),
        error=data.get("error"),
        progress=data.get("progress"),
        task=deserialize_task(data["task"]) if data.get("task") else None,
    )
