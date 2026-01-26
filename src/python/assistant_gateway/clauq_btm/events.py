"""
Event types for the Clau-Queue Background Task Manager.

Defines events emitted during task lifecycle for subscribers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus


class TaskEventType(str, Enum):
    """Types of task events emitted by the queue manager."""

    QUEUED = "queued"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    PROGRESS = "progress"


@dataclass
class TaskEvent:
    """Event emitted during task lifecycle."""

    event_type: TaskEventType
    task_id: str
    queue_id: str
    status: TaskStatus
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    task: Optional[ClauqBTMTask] = None
    error: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None

    @classmethod
    def from_task(
        cls,
        event_type: TaskEventType,
        task: ClauqBTMTask,
        error: Optional[str] = None,
        progress: Optional[Dict[str, Any]] = None,
    ) -> "TaskEvent":
        """Create an event from a task."""
        return cls(
            event_type=event_type,
            task_id=task.id,
            queue_id=task.queue_id,
            status=task.status,
            task=task,
            error=error,
            progress=progress,
        )
