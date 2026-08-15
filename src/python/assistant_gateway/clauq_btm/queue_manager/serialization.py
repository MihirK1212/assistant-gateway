from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict

from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus


def serialize_for_redis_hset(data: Dict[str, Any]) -> Dict[str, str]:
    return {
        k: (
            json.dumps(v)
            if isinstance(v, (dict, list))
            else str(v) if v is not None else ""
        )
        for k, v in data.items()
    }


def serialize_task(task: ClauqBTMTask) -> Dict[str, Any]:
    return task.model_dump(mode="json", exclude={"executor"})


def deserialize_task(data: Dict[str, Any]) -> ClauqBTMTask:
    return ClauqBTMTask.model_validate(data)


def serialize_event(event: TaskEvent) -> Dict[str, Any]:
    return {
        "event_type": event.event_type.value,
        "task_id": event.task_id,
        "queue_id": event.queue_id,
        "status": event.status.value,
        "timestamp": event.timestamp.isoformat(),
        "error": event.error,
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
        progress=data.get("progress"),
    )
