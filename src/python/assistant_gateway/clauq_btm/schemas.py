"""
Core schemas for the Clau-Queue Background Task Manager.

Defines the task model and status enum that are independent of any
specific application domain.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Status of a background task."""

    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    interrupted = "interrupted"


class ClauqBTMTask(BaseModel):
    """
    A background task for queue-based execution.

    This is a generic task model that is independent of any specific
    application domain. It does not contain application-specific fields
    like chat_id or interaction_id - those should be stored in the payload
    or handled by the consuming application.

    Attributes:
        id: Unique task identifier
        queue_id: The queue this task belongs to (for FIFO ordering)
        status: Current task status
        created_at: When the task was created
        updated_at: When the task was last updated
        payload: Arbitrary data needed for task execution
        result: Task execution result (set on completion)
        error: Error message (set on failure)
        executor: Embedded executor function (for in-memory mode)
        executor_name: Name of registered executor (for distributed mode)

    Execution Modes:
        1. In-memory: Set `executor` directly on the task
        2. Distributed (Celery): Set `executor_name` to reference a registered executor
    """

    model_config = {"arbitrary_types_allowed": True}

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    queue_id: str = Field(description="The queue ID where this task is scheduled")
    status: TaskStatus = TaskStatus.pending
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

    # For in-memory execution: embedded executor function
    executor: Optional[Callable[["ClauqBTMTask"], Awaitable[Any]]] = Field(
        default=None,
        exclude=True,  # Don't serialize the executor
        description="The async function that executes this task (for in-memory mode)",
    )

    # For distributed execution: name of registered executor
    executor_name: Optional[str] = Field(
        default=None,
        description="Name of the registered executor function (for distributed mode)",
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

    async def execute(self) -> Any:
        """Execute this task using the embedded executor."""
        if self.executor is None:
            raise RuntimeError(
                "Task executor not set. For in-memory mode, set task.executor. "
                "For distributed mode, the worker handles execution via executor_name."
            )
        return await self.executor(self)
