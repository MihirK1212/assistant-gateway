from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from assistant_gateway.clauq_btm.events.schema import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.schemas import TaskStatus


def serialize_event(event: TaskEvent) -> Dict[str, Any]:
    return {
        "event_type": event.event_type.value,
        "task_id": event.task_id,
        "queue_id": event.queue_id,
        "status": event.status.value,
        "timestamp": event.timestamp.isoformat(),
        "error": event.error,
        "message": event.message,
        "progress": event.progress,
    }


def deserialize_event(data: Dict[str, Any]) -> TaskEvent:
    return TaskEvent(
        event_type=TaskEventType(data["event_type"]),
        task_id=data["task_id"],
        queue_id=data["queue_id"],
        status=TaskStatus(data["status"]),
        timestamp=datetime.fromisoformat(data["timestamp"]),
        error=data.get("error"),
        message=data.get("message"),
        progress=data.get("progress"),
    )
