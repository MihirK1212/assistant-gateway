from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, model_validator


class TaskStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    interrupted = "interrupted"


class ClauqBTMTask(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    is_background_task: bool = Field(
        default=False,
        description="whether this is a background task (queued) or sync task (inline execution)",
    )
    queue_id: Optional[str] = Field(
        default=None,
        description="the queue ID where this task is scheduled (required for background tasks, must be None for sync tasks)",
    )
    status: TaskStatus = TaskStatus.pending

    @model_validator(mode="after")
    def validate_queue_id_based_on_task_type(self) -> "ClauqBTMTask":
        if self.is_background_task and self.queue_id is None:
            raise ValueError("queue_id is required for background tasks (is_background_task=True)")
        if not self.is_background_task and self.queue_id is not None:
            raise ValueError("queue_id must be None for sync tasks (is_background_task=False)")
        return self

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="arbitrary data needed for task execution",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="application-specific metadata (e.g., chat_id, interaction_id)",
    )
    result: Optional[Any] = None
    error: Optional[str] = None

    # Name of registered executor (required for background tasks)
    executor_name: Optional[str] = Field(
        default=None,
        description="name of the registered executor function (looked up from ExecutorRegistry)",
    )

    def is_terminal(self) -> bool:
        return self.status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.interrupted,
        )

    def is_interrupted(self) -> bool:
        return self.status == TaskStatus.interrupted
