"""
Core schemas for the Clau-Queue Background Task Manager.

Defines the task model and status enum that are independent of any
specific application domain.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, model_validator


class TaskStatus(str, Enum):
    """Status of a background task."""

    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    interrupted = "interrupted"


class ClauqBTMTask(BaseModel):
    """
    A task for queue-based or synchronous execution.

    This is a generic task model that is independent of any specific
    application domain. It does not contain application-specific fields
    like chat_id or interaction_id - those should be stored in the payload
    or handled by the consuming application.

    Attributes:
        id: Unique task identifier
        is_background_task: Whether this is a background task (queued) or sync task
        queue_id: The queue this task belongs to (required for background tasks, must be None for sync tasks)
        status: Current task status
        created_at: When the task was created
        updated_at: When the task was last updated
        payload: Arbitrary data needed for task execution
        metadata: Application-specific metadata
        result: Task execution result (set on completion)
        error: Error message (set on failure)
        executor_name: Name of registered executor (required for background tasks)

    Execution Modes:
        1. Sync: Set `is_background_task=False`, `queue_id=None` - executes inline
        2. Background: Set `is_background_task=True`, `queue_id` required, `executor_name` required

    For background tasks, executors are looked up from the ExecutorRegistry by name.
    Executors must be pre-registered at application startup.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    is_background_task: bool = Field(
        default=False,
        description="Whether this is a background task (queued) or sync task (inline execution)",
    )
    queue_id: Optional[str] = Field(
        default=None,
        description="The queue ID where this task is scheduled (required for background tasks, must be None for sync tasks)",
    )
    status: TaskStatus = TaskStatus.pending

    @model_validator(mode="after")
    def validate_queue_id_based_on_task_type(self) -> "ClauqBTMTask":
        """Validate that queue_id is set correctly based on task type."""
        if self.is_background_task and self.queue_id is None:
            raise ValueError(
                "queue_id is required for background tasks (is_background_task=True)"
            )
        if not self.is_background_task and self.queue_id is not None:
            raise ValueError(
                "queue_id must be None for sync tasks (is_background_task=False)"
            )
        return self

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary data needed for task execution",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Application-specific metadata (e.g., chat_id, interaction_id)",
    )
    result: Optional[Any] = None
    error: Optional[str] = None

    # Name of registered executor (required for background tasks)
    executor_name: Optional[str] = Field(
        default=None,
        description="Name of the registered executor function (looked up from ExecutorRegistry)",
    )

    def is_terminal(self) -> bool:
        """Check if the task is in a terminal state."""
        return self.status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.interrupted,
        )

    def is_interrupted(self) -> bool:
        """Check if the task was interrupted."""
        return self.status == TaskStatus.interrupted
